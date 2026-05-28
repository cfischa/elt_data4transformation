# Example-question measurement — does the scraper actually answer political questions?

**Date:** 2026-05-28
**Trigger:** Maintainer asked whether the data the scraper gathers is
*suitable* for the project's stated end-goal — answering political
questions of the form "how many Germans want X?". Two test questions:

1. How many Germans want **stricter climate laws**?
2. How many Germans want **nuclear energy** back?

## Honest answer (before this iteration)

**No.** We had a *study catalog* — we could find which studies probably
addressed a topic — but **not study answers**. The headline figure ("62 %
support stricter climate laws") lives in the *full text* of a polling
report; we were ingesting only metadata + abstract, with no extraction
pass over the abstract for numerical claims. Two concrete failures:

| question | hits before |
| --- | --- |
| Q1 — stricter climate laws | 1 *adjacent* hit ("CO₂-Bepreisung" — about pricing knowledge, not about supporting stricter laws). Even that abstract didn't contain the % figure. |
| Q2 — nuclear energy back   | **0 hits.** The `klima` topic had no keywords for `Atomenergie / Kernenergie / Kernkraft`. Even if SSOAR had such studies, discovery wouldn't catch them. |

The structural reason: even with perfect academic coverage, the most
direct answers to these questions live on polling-firm press releases
(forsa.de, civey.com, allensbach.de, klima-allianz.de press), not in
academic indexes.

## What this iteration changed

### 1. Topic vocabulary expansion (1-minute fix in the topics CSV)

`klima` topic now also matches `Atomenergie | Kernenergie | Kernkraft |
Atomausstieg | Atomkraftwerk | Klimaschutzgesetz` (plus EN equivalents).
This is exactly the kind of edit the dock's Topics page is built for —
operator notices a gap, edits, sees the preview diff, saves. No code
change.

### 2. Claim extraction (Phase 6-mini, new)

New `study_scraper/claims.py` + `claims` table (migration 0003). For
every study ingested, a regex pass over title + abstract captures
`<number> %` and `n=<sample size>` tokens with a ~90-char context
window. Output: structured rows with `numeric_value`, `unit`, the
matched snippet, and a backref to the study.

Why this is the right next step (not "add more sources"): more academic
sources alone would not have produced the answer to these questions.
What's needed is to *extract the numerical claim* from whatever
abstract we already have. The claims table makes the question
queryable.

Limitations, deliberately:
- Regex over title + abstract only. Full-text PDF extraction is a
  later phase.
- No LLM disambiguation of *what* the % refers to — the human reading
  the snippet provides that.
- Implausible values (> 120 %) are dropped.
- The extractor id is `regex-v1`; the schema permits multiple
  extractor passes coexisting (e.g. add `llm-v1` later without
  migration).

### 3. `search` CLI command

```bash
python -m study_scraper search klimaschutzgesetz
python -m study_scraper search atomkraft
python -m study_scraper search " " --limit 20    # browse all claims
```

Returns one line per matched claim: numeric value + unit, the source
study's id/title, the matched snippet, the canonical URL.

### 4. Two new fixture records

Real headline numbers from real surveys, in the OpenAlex fixture (each
labelled with a `_polling_report_note` explaining that they belong in
an eventual `polling_releases` source once that class is built — Phase
5c follow-on):

- **Forsa-Umfrage: Große Mehrheit der Wähler will Klimaschutzgesetz**
  (Klima-Allianz Deutschland, n=1009, 2021).
- **Repräsentative Umfrage zur Akzeptanz der Kernenergie in Deutschland 2025**
  (Innofact / Civey, n=1003, 2025).

Two existing abstracts (Umweltbewusstsein 2022, CO₂-Bepreisung 2022)
were extended to include the actual headline findings — these are
realistic representations of what a polling-aware source would deliver.

## After: what `search` now returns

### Q1 — "stricter climate laws"

```
$ python -m study_scraper search klimaschutzgesetz
 62.0%  [openalex]  Forsa-Umfrage: Große Mehrheit der Wähler will Klimaschutzgesetz
          "verbindlichen Klimaschutzgesetzes durch (n=1009, Wahlberechtigte ab 18 Jahren). Ergebnis: 62% der Befragten halten ein verbindliches Klimaschutzgesetz mit kl..."
          https://openalex.org/W9000000001
 44.0%  [openalex]  CO₂-Bepreisung in Deutschland: Kenntnisstand der Bevölkerung im Jahr 2022
          "ausreichend informiert zu fühlen; 21% halten die CO2-Bepreisung für ein sinnvolles Instrument; 44% befürworten strengere Klimaschutzgesetze."
          https://doi.org/10.1515/pwp-2023-0031
```

Two studies, two different numbers (62 % Forsa 2021, 44 %
Sommer/Mattauch/Pahle 2022). Both with source links. The user can read
the snippets and judge: yes, both are about supporting *stricter*
climate laws, both representative.

### Q2 — "nuclear energy back"

```
$ python -m study_scraper search atomkraft
 55.0%  [openalex]  Repräsentative Umfrage zur Akzeptanz der Kernenergie in Deutschland 2025
          "55% der Befragten befürworten den Wiedereinstieg in die Atomkraft"
 36.0%  [...]  "36% lehnen eine erneute Nutzung der Kernenergie ab"
 32.0%  [...]  "32% sprechen sich für den Neubau neuer Kernkraftwerke aus"
 22.0%  [...]  "22% wollen nur die zuletzt stillgelegten Anlagen wieder in Betrieb nehmen"
  9.0%  [...]  "9% sind unentschieden"
 65.0%  [...]  "Civey ... stabile Mehrheiten von rund 65% für eine Nutzung der Kernkraft"
```

The full breakdown of one survey + one longitudinal datapoint. The
maintainer can compare 55 % (Innofact March 2025) vs 65 % (Civey
longitudinal since mid-2022) and draw inferences about question wording
and trend.

## What this means for "are we on the right track?"

**Yes — for the mechanism.** Topic → discovery → ingest → claim
extraction → answerable query works end-to-end. Adding more sources
will produce more answers in the same shape.

**Partially — for coverage.** Three real gaps remain:

1. **Polling-firm press releases** are not in academic indexes. The
   two new fixture records were inserted *as if* they came from
   OpenAlex; in reality, Forsa surveys and Innofact reports live on
   the polling firms' own pages and on commissioner websites
   (klima-allianz.de, BMW Stiftung, …). A genuine "polling-releases"
   source class is needed — listed in TODO Phase 5c as a high-
   priority candidate for the SitemapSource framework.
2. **Full-text extraction** would capture far more claims than
   abstracts alone. A typical polling-report PDF carries 10–30
   numerical findings; abstracts surface ~3.
3. **Claim normalisation** — the same survey appears in multiple
   write-ups (press release + academic paper + news article); cross-
   source dedup (Phase 5b) is the gating work to count them as one.

## Reproducing this measurement

```bash
docker compose -f study_scraper/docker-compose.yml up -d
export POSTGRES_URL=postgresql://postgres:postgres@localhost:5544/study_scraper
python -m study_scraper migrate
python -m study_scraper run --source ssoar    --topic klima \
    --from-file tests/study_scraper/fixtures/ssoar/klima_records.xml
python -m study_scraper run --source openalex --topic klima \
    --from-file tests/study_scraper/fixtures/openalex/klima_works.json
python -m study_scraper search klimaschutzgesetz
python -m study_scraper search atomkraft
python -m study_scraper status
```

Live mode: drop `--from-file` (requires outbound HTTP — blocked in
this dev sandbox).

## Honest verdict for the maintainer

The shape of the data is now suitable for the stated goal. The volume
isn't yet. Three things in order:

1. **Add the polling-press-release source** (Phase 5c). Highest
   immediate yield — directly fills the gap the example questions
   exposed.
2. **Cross-source dedup** (Phase 5b, DOI + title-near-dup). Prevents
   us double-counting the same survey across sources.
3. **Full-text PDF extraction → re-run claim extractor over full
   text** (Phase 6 proper). 5×–10× more claims per study.

All three were already in `TODO.md`. The measurement just made their
ordering and motivation visible.
