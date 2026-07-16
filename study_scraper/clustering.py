"""Semantic question clustering (ROADMAP item E, v1).

'Atomausstieg rückgängig machen' and 'return to nuclear power' are the
same survey question; lexical ILIKE misses that, and the aggregation
layer (item D) cannot exist without grouping them. This module clusters
attribution questions at READ time — no migration, raw rows untouched,
same pattern as `findings.py`.

Two similarity backends:

  - **Default (offline, deterministic):** weighted-token cosine over
    normalized tokens, where tokens that map to a known political
    concept (bilingual DE/EN map below) carry extra weight. German
    compounds are decomposed by substring ("klimaschutzgesetz" emits
    both "climate" and "law"). Zero dependencies, stable across runs —
    reproducibility is a project dimension (GOAL.md #3).
  - **Pluggable embedder:** pass `embedder=` (a callable mapping a list
    of strings to dense vectors) to swap in a real embedding model /
    pgvector later without touching call sites.

Clustering is greedy single-linkage in input order with a similarity
threshold: deterministic given the same rows in the same order, which
callers guarantee by ordering in SQL.
"""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

# Similarity at or above this joins a cluster. Tuned on the repo's real
# examples: 'Atomausstieg rückgängig machen' vs 'return to nuclear
# power' scores ~0.82; 'stricter climate laws' vs 'EU climate priority'
# (different questions, same topic) scores ~0.62 and must NOT merge.
DEFAULT_THRESHOLD = 0.72

# Bilingual concept map: any token CONTAINING a key (compound-safe)
# emits the canonical concept token(s). Weighted heavier than plain
# tokens so the load-bearing word dominates the cosine.
_CONCEPTS: Dict[str, List[str]] = {
    "atomkraft": ["nuclear"],
    "atomenergie": ["nuclear"],
    "atomausstieg": ["nuclear"],
    "kernenergie": ["nuclear"],
    "kernkraft": ["nuclear"],
    "nuclear": ["nuclear"],
    "klima": ["climate"],
    "climate": ["climate"],
    "tempolimit": ["speedlimit"],
    "speed": ["speedlimit"],
    "migration": ["migration"],
    "einwanderung": ["migration"],
    "zuwanderung": ["migration"],
    "immigration": ["migration"],
    "asyl": ["asylum"],
    "asylum": ["asylum"],
    "rente": ["pension"],
    "pension": ["pension"],
    "steuer": ["tax"],
    "tax": ["tax"],
    "miete": ["housing"],
    "wohnung": ["housing"],
    "housing": ["housing"],
    # NOTE: no "rent" key — the map matches keys as substrings inside
    # tokens, and 'rent' sits inside 'rente', 'current', 'different'…
    # English rent-phrasings already share the exact token 'rent'.
    "retirement": ["pension"],
    "verteidigung": ["defense"],
    "defense": ["defense"],
    "defence": ["defense"],
    "bundeswehr": ["defense"],
    "military": ["military"],
    "wehrpflicht": ["conscription"],
    "wehrdienst": ["conscription"],
    "conscription": ["conscription"],
    "gesetz": ["law"],
    "law": ["law"],
    # 'plant/kraftwerk' keeps "build new nuclear power plants" from
    # merging with the general "use nuclear power" question (over-merge
    # observed on real data, 2026-07-05: 32% and 65% averaged to a
    # meaningless 48.5%).
    "kraftwerk": ["plant"],
    "plant": ["plant"],
    "verbot": ["ban"],
    "ban": ["ban"],
    # Polarity guards: 'Keep nuclear power' vs 'Phase out nuclear power'
    # scored 0.80 on shared nuclear/power tokens and merged — averaging
    # OPPOSITE propositions under one 'support' mean (audit 2026-07-11).
    # A phaseout/abolish concept on one side pushes such pairs below the
    # clustering threshold (0.65 for the nuclear pair). The 'return'
    # concept re-joins REVERSAL phrasings ('Atomausstieg rückgängig
    # machen' == 'Return to nuclear power', 0.77) that the phaseout tag
    # alone would wrongly split — negation flips polarity back.
    "ausstieg": ["phaseout"],
    "phase": ["phaseout"],
    "abschaff": ["abolish"],
    "abolish": ["abolish"],
    "rückgängig": ["return"],
    "ruckgangig": ["return"],
    "return": ["return"],
    "energie": ["energy"],
    "energy": ["energy"],
    "kohle": ["coal"],
    "coal": ["coal"],
    "erneuerbar": ["renewable"],
    "renewable": ["renewable"],
}
# 2.0, not 3.0: at weight 3 the shared concept dominates short
# questions so hard that nine distinct climate questions merged into
# one cluster on real data (2026-07-05). At 2.0 the ROADMAP DE/EN
# example still clusters (0.73) while 'ambitious climate policy' vs
# 'climate protection is an important task' separates (0.67).
_CONCEPT_WEIGHT = 2.0

_STOPWORDS = frozenset(
    """
    the a an of to in for on and or should be is are was were do does
    germany german germans its it this that with about more most
    der die das den dem des ein eine einen einem einer und oder soll
    sollte sollten ist sind war waren fuer mit ueber mehr sich
    machen werden wieder wollen will
    """.split()
)


def _fold(text: str) -> str:
    """Lowercase + fold umlauts/accents so 'rückgängig' == 'ruckgangig'."""
    text = text.lower().replace("ß", "ss")
    text = unicodedata.normalize("NFKD", text)
    return "".join(c for c in text if not unicodedata.combining(c))


def _tokens(question: str) -> List[str]:
    return [t for t in re.split(r"[^a-z0-9]+", _fold(question)) if t]


def question_vector(question: str) -> Dict[str, float]:
    """Sparse weighted-token vector for the default similarity backend."""
    vec: Dict[str, float] = {}
    for tok in _tokens(question):
        if tok in _STOPWORDS or len(tok) < 2:
            continue
        # Naive singular: 'laws' -> 'law'. Applied before concept lookup
        # so plural forms still hit the map.
        if len(tok) > 3 and tok.endswith("s") and not tok.endswith("ss"):
            tok = tok[:-1]
        concepts = [c for key, cs in _CONCEPTS.items() if key in tok for c in cs]
        if concepts:
            for c in concepts:
                vec[c] = vec.get(c, 0.0) + _CONCEPT_WEIGHT
        else:
            vec[tok] = vec.get(tok, 0.0) + 1.0
    return vec


def _cosine_sparse(a: Dict[str, float], b: Dict[str, float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(w * b.get(t, 0.0) for t, w in a.items())
    if dot == 0.0:
        return 0.0
    na = sum(w * w for w in a.values()) ** 0.5
    nb = sum(w * w for w in b.values()) ** 0.5
    return dot / (na * nb)


def _cosine_dense(a: Sequence[float], b: Sequence[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


def question_similarity(a: str, b: str) -> float:
    """Similarity in [0, 1] under the default offline backend."""
    return _cosine_sparse(question_vector(a), question_vector(b))


# Looser than the clustering threshold: search wants recall (ranked),
# clustering wants precision (grouped).
SEARCH_THRESHOLD = 0.35


def semantic_filter(
    query: str,
    rows: Iterable[Dict[str, Any]],
    *,
    threshold: float = SEARCH_THRESHOLD,
    key: str = "question",
) -> List[Dict[str, Any]]:
    """Rank rows by concept similarity between `query` and row[key].

    The answer layer normalizes questions to English, so a German query
    ('klimaschutzgesetz') misses them under ILIKE. The concept map folds
    both languages into shared tokens, letting the query match anyway.

    `query` may carry pipe-separated alternatives ('conscription|military
    service') — the question registry's recall aliases. A row is scored
    by its BEST alternative: extractors phrase the same proposition many
    ways ('reintroduce compulsory military service' shares zero tokens
    with 'conscription'), and requiring one canonical phrasing turns real
    data into false coverage gaps. Over-recall is fine here — the answer
    path re-clusters at the precision threshold afterwards.

    Returns matching rows best-first; rows below `threshold` drop."""
    alternatives = [alt.strip() for alt in query.split("|") if alt.strip()]
    if not alternatives:
        return []
    qvs = [question_vector(alt) for alt in alternatives]
    scored: List[tuple] = []
    for i, row in enumerate(rows):
        rv = question_vector(row.get(key) or "")
        sim = max(_cosine_sparse(qv, rv) for qv in qvs)
        if sim >= threshold:
            scored.append((-sim, i, row))
    scored.sort(key=lambda t: (t[0], t[1]))
    return [row for _sim, _i, row in scored]


def cluster_questions(
    questions: Sequence[str],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    embedder: Optional[Callable[[List[str]], List[Sequence[float]]]] = None,
) -> List[int]:
    """Assign each question a cluster id (0-based, in order of first
    appearance). Greedy single-linkage: a question joins the first
    cluster containing ANY member at/above `threshold`, else starts a
    new one. Deterministic for a fixed input order."""
    if embedder is not None:
        dense = embedder(list(questions))
        vectors: List[Any] = list(dense)
        sim = _cosine_dense
    else:
        vectors = [question_vector(q) for q in questions]
        sim = _cosine_sparse  # type: ignore[assignment]

    assignments: List[int] = []
    members_by_cluster: List[List[int]] = []
    for i, _q in enumerate(questions):
        placed = False
        for cluster_id, members in enumerate(members_by_cluster):
            if any(sim(vectors[i], vectors[j]) >= threshold for j in members):
                assignments.append(cluster_id)
                members.append(i)
                placed = True
                break
        if not placed:
            assignments.append(len(members_by_cluster))
            members_by_cluster.append([i])
    return assignments


def cluster_attributions(
    rows: Iterable[Dict[str, Any]],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    embedder: Optional[Callable[[List[str]], List[Sequence[float]]]] = None,
) -> List[Dict[str, Any]]:
    """Annotate attribution rows with `cluster_id` and `cluster_label`.

    The label is the most common question phrasing in the cluster
    (shortest on ties) — a human-readable canonical form. Rows come back
    in input order with the two keys added; raw rows are not mutated.
    """
    rows = list(rows)
    questions = [(r.get("question") or "") for r in rows]
    ids = cluster_questions(questions, threshold=threshold, embedder=embedder)

    by_cluster: Dict[int, Counter] = {}
    for cid, q in zip(ids, questions):
        by_cluster.setdefault(cid, Counter())[q] += 1
    labels = {
        cid: min(counter.items(), key=lambda kv: (-kv[1], len(kv[0])))[0]
        for cid, counter in by_cluster.items()
    }

    out: List[Dict[str, Any]] = []
    for row, cid in zip(rows, ids):
        annotated = dict(row)
        annotated["cluster_id"] = cid
        annotated["cluster_label"] = labels[cid]
        out.append(annotated)
    return out
