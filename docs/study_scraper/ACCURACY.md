# ACCURACY — data-flow audit & fixes

The pipeline is a funnel; errors compound left to right:

```
topic scoring → regex claims → PDF/full-text → LLM attribution → dedup → answer
```

A stage-by-stage audit (2026-06-26) ranked the accuracy risks and the
sandbox-buildable fixes. This file records what was done and what remains.

## Shipped accuracy fixes

| Fix | Stage | What | PR |
|---|---|---|---|
| **F1** | LLM | `source_span` required in output + verified as a verbatim substring of the source; ungrounded findings flagged `grounded=False` and confidence capped at 0.3. Threaded through live + offline paths. | #13 |
| **F3** | LLM | German stance-verb lexicon in the prompt (befürworten/lehnen ab/unentschieden/…); parser maps stray German positions to the enum. | #13 |
| **F5** | LLM | Per-question distribution sanity: support+oppose+neutral >120% → `raw.distribution_check=false` (audit-only). | #13 |
| **F4** | claims | Regex catches German `Prozent` / `v.H.` / `vom Hundert` / `Prozentpunkte`, not just `%`. | #12 |
| **F6** | topic | German morphology: inflected/stem keywords added for the 4 topics (Zuwanderer, Emission, Studierende, Gewerbesteuer, …); excludes still short-circuit. | #14 |
| PDF | extract | Landing-page → PDF resolver, so full-text reads the actual document (SSOAR handles etc.) not the landing page. | #10 |
| dedup | answer | Cross-study finding dedup (confidence-weighted, read-time). | #16 |

**Traceability**: every finding now carries `source_span` (the verbatim
German sentence) and `grounded` (checked True/False). `provenance` carries
`resolved_pdf_url` (which document was read) and `review_rationale` (why a
study was auto-kept/rejected).

## Deferred — buildable, lower marginal value

- **F2 (claim_id FK on attributions).** A structured link from a triple to
  the exact claim row. Largely covered by F1's `source_span`; revisit if a
  structured join is needed (would add a migration + prompt field).
- **Dual-target lake attribution.** DAWUM polls are party vote-intention,
  which don't fit `(question, position=support/oppose, %)`. Forcing it
  would reduce accuracy; surface DAWUM via its typed views instead. Parser
  `source_record_id` support already exists for a future policy-opinion
  lake source.
- **F7 structured-lake metadata** (German `sample_size` parsing, Eurostat
  oversized→pending, GESIS DOI validation) — small, do when a real fixture
  exposes the loss.

## Needs the maintainer (can't be done in-sandbox)

- **Gold set** — the real accuracy number needs ~50 studies (≈12/topic)
  with hand-extracted `(question, position, percentage, source_span)`
  triples, plus ~40 on/off-topic titles for the topic-filter recall test.
  This is the long pole; everything measurable hangs off it.
- **OCR** for scanned PDFs — needs `tesseract` on the host (system pkg).
- **spaCy lemmatization** — full fix for German morphology (F6 is the
  keyword-only partial); needs `pip install spacy` + a model download.
- **Live-API / Cowork validation** of the F1/F3 prompt changes — the
  parser changes are tested offline, but real hallucination-rate and
  confidence-calibration numbers need a run against real studies. Editing
  `SYSTEM_PROMPT` invalidates the prompt cache, so batch prompt edits.

## Measurement plan (stand up next)

1. **Invariant tests (no labeling, buildable now):** topic recall@0.2 on a
   small tagged title list; claims capture-rate on German % snippets (the
   F4 acceptance number); LLM parser invariants — every stored `source_span`
   grounds, per-question sums ≤120%, stance verbs map. (F4/F6/F1/F5 tests
   already seed these.)
2. **Gold-set harness** (`eval/run_eval.py`, offline over captured model
   outputs): exact-triple accuracy, hallucination rate (span-not-found),
   false-negative rate, confidence calibration → `docs/study_scraper/eval/accuracy.md`.
3. **Spot-check CLI** (`audit --sample-size 20`): dumps random stored
   findings with `source_span` + URL for a weekly manual pass/fail.

Build order once the gold set lands: harness → calibration → tune prompt.
