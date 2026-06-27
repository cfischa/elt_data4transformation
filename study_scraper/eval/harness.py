"""Pure scoring functions + loaders for the accuracy harness.

Every function takes already-loaded gold entries (list of dicts) and
returns a metrics dict, so they unit-test with no files, no DB, no API.
`run_all` wires the shipped gold files through them; `format_report`
renders markdown for `docs/study_scraper/eval/accuracy.md`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from study_scraper.claims import extract_claims_from_text
from study_scraper.extractors.llm_v1 import build_user_prompt, parse_response

GOLD_DIR = Path(__file__).resolve().parent / "gold"


# ----------------------------------------------------------------------
# Loaders
# ----------------------------------------------------------------------


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def _pct_key(pos: Optional[str], pct: Any) -> tuple:
    p = (pos or "unspecified").strip().lower()
    if pct is None:
        return (p, None)
    try:
        return (p, int(round(float(pct))))
    except (TypeError, ValueError):
        return (p, None)


# ----------------------------------------------------------------------
# Stage 1 — topic filter recall/precision
# ----------------------------------------------------------------------


def eval_topics(entries: List[Dict[str, Any]], topics_by_id: Dict[str, Any]) -> Dict[str, Any]:
    from study_scraper.topic_filter import score_text

    tp = fp = fn = tn = 0
    misses: List[str] = []
    for e in entries:
        topic = topics_by_id.get(e["topic"])
        text = f"{e.get('title') or ''} {e.get('abstract') or ''}"
        passed = bool(topic and score_text(text, topic).passes)
        gold = bool(e["on_topic"])
        if gold and passed:
            tp += 1
        elif gold and not passed:
            fn += 1
            misses.append(f"{e['topic']}: {(e.get('title') or '')[:60]}")
        elif not gold and passed:
            fp += 1
        else:
            tn += 1
    recall = tp / (tp + fn) if (tp + fn) else None
    precision = tp / (tp + fp) if (tp + fp) else None
    return {
        "n": len(entries), "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "recall": recall, "precision": precision, "false_negatives": misses,
    }


# ----------------------------------------------------------------------
# Stage 2 — claims capture rate
# ----------------------------------------------------------------------


def eval_claims(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    found = total = 0
    details: List[Dict[str, Any]] = []
    for e in entries:
        claims = extract_claims_from_text(study_id="g", text=e["text"])
        got = {
            int(round(float(c.numeric_value)))
            for c in claims
            if c.unit in ("%", "pp") and c.numeric_value is not None
        }
        exp = {int(round(float(x))) for x in e.get("expected_pcts", [])}
        hit = exp & got
        found += len(hit)
        total += len(exp)
        if hit != exp:
            details.append({
                "text": e["text"][:70],
                "missed": sorted(exp - got),
            })
    return {
        "n": len(entries), "found": found, "total": total,
        "capture_rate": (found / total) if total else None,
        "misses": details,
    }


# ----------------------------------------------------------------------
# Stage 3 — attribution precision/recall/hallucination/calibration
# ----------------------------------------------------------------------


def eval_attributions(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    parsed_total = matched = gold_total = matched_gold = hallucinated = 0
    conf_high = conf_high_correct = 0
    for e in entries:
        src = build_user_prompt(
            title=e.get("title"), abstract=e.get("abstract"),
            claim_snippets=e.get("claim_snippets") or [],
        )
        attrs = parse_response(
            e["captured_response"], study_id="g", source_text=src,
        )
        gold = e.get("gold") or []
        gold_keys = {_pct_key(g.get("position"), g.get("percentage")) for g in gold}
        parsed_keys = set()
        gold_total += len(gold)
        for a in attrs:
            parsed_total += 1
            key = _pct_key(a.position, a.percentage)
            parsed_keys.add(key)
            is_match = key in gold_keys
            if is_match:
                matched += 1
            if a.grounded is False:
                hallucinated += 1
            if (a.confidence or 0.0) >= 0.8:
                conf_high += 1
                if is_match:
                    conf_high_correct += 1
        matched_gold += len(gold_keys & parsed_keys)
    return {
        "n": len(entries),
        "parsed": parsed_total, "gold": gold_total,
        "precision": (matched / parsed_total) if parsed_total else None,
        "recall": (matched_gold / gold_total) if gold_total else None,
        "hallucination_rate": (hallucinated / parsed_total) if parsed_total else None,
        "calibration_at_0.8": (conf_high_correct / conf_high) if conf_high else None,
    }


# ----------------------------------------------------------------------
# Orchestration + report
# ----------------------------------------------------------------------


def run_all(*, gold_dir: Path = GOLD_DIR, topics_by_id: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if topics_by_id is None:
        from study_scraper.config import get_settings
        from study_scraper.topics import load_topics
        topics_by_id = {t.id: t for t in load_topics(get_settings().topics_csv_path)}
    return {
        "topics": eval_topics(load_jsonl(gold_dir / "topics.jsonl"), topics_by_id),
        "claims": eval_claims(load_jsonl(gold_dir / "claims.jsonl")),
        "attributions": eval_attributions(load_jsonl(gold_dir / "attributions.jsonl")),
    }


def _fmt(x: Optional[float]) -> str:
    return "—" if x is None else f"{x:.1%}"


def format_report(results: Dict[str, Any], *, generated_at: str = "") -> str:
    t, c, a = results["topics"], results["claims"], results["attributions"]
    lines = [
        "# Accuracy report",
        "",
        f"_Generated {generated_at}_" if generated_at else "_Generated offline_",
        "",
        "Run: `python -m study_scraper eval`. Gold lives in "
        "`study_scraper/eval/gold/`. The shipped gold is a SAMPLE — replace "
        "it with a curated set for a meaningful number (see ACCURACY.md).",
        "",
        "| Stage | Metric | Value | n |",
        "| --- | --- | --- | --- |",
        f"| Topic filter | recall | {_fmt(t['recall'])} | {t['n']} |",
        f"| Topic filter | precision | {_fmt(t['precision'])} | {t['n']} |",
        f"| Claims | capture rate | {_fmt(c['capture_rate'])} | {c['n']} |",
        f"| Attribution | precision | {_fmt(a['precision'])} | {a['n']} |",
        f"| Attribution | recall | {_fmt(a['recall'])} | {a['n']} |",
        f"| Attribution | hallucination rate | {_fmt(a['hallucination_rate'])} | {a['n']} |",
        f"| Attribution | calibration @conf≥0.8 | {_fmt(a['calibration_at_0.8'])} | {a['n']} |",
        "",
    ]
    if t["false_negatives"]:
        lines.append("**Topic false negatives (missed on-topic):**")
        lines += [f"- {m}" for m in t["false_negatives"]]
        lines.append("")
    if c["misses"]:
        lines.append("**Claims misses:**")
        lines += [f"- missed {m['missed']} in: {m['text']}" for m in c["misses"]]
        lines.append("")
    return "\n".join(lines)
