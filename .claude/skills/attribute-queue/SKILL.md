---
name: attribute-queue
description: >
  Run the study-scraper LLM attribution pass over the database queue using
  THIS Claude session (subscription path — no ANTHROPIC_API_KEY, no per-call
  cost). Use when a scheduled/triggered session fires to turn regex claims
  into structured (question, position, percentage) triples, or when the user
  asks to "attribute the queue" / "run attributions". Reads the
  attribution_queue, generates structured JSON inline, applies it to Postgres.
---

# attribute-queue — autonomous LLM attribution (subscription path)

This is the **zero-extra-cost** half of the autonomy loop (DECISIONS A21,
RUNBOOK §3.5b). The crawl GitHub Action fills `studies` + `claims`; this
skill fills `attributions` by having **Claude (this session) answer the
llm-v1 prompts directly**, instead of calling the metered Anthropic API.
The pure parser (`study_scraper.extractors.llm_v1.parse_response`) and the
storage upsert are identical to the live path, so rows are identical.

## Preconditions (fail fast, report, stop)

1. `POSTGRES_URL` must be set in the environment (the Supabase Session-pooler
   connection string). Verify:
   ```bash
   python -c "import os,sys; sys.exit(0 if os.environ.get('POSTGRES_URL') else 1)" \
     && echo "POSTGRES_URL ok" || echo "POSTGRES_URL MISSING"
   ```
   If missing, STOP and report: "POSTGRES_URL not configured in this
   environment — set it in the Claude Code web environment settings."

2. Dependencies present (the web env may be fresh):
   ```bash
   pip install -q httpx pydantic pydantic-settings typer PyYAML "psycopg[binary]" || true
   ```

## Procedure

Work in the scratchpad, not the repo (no files should be committed — the
output lives in the database).

### Step 1 — dump the queue prompts
```bash
python -m study_scraper attribute-prompts --limit 25 --out /tmp/attr_prompts.jsonl
```
- If it prints `(attribution queue empty)`, STOP and report "queue empty,
  nothing to attribute" — this is a success, not an error.
- Otherwise the file has one JSON object per line: `{"study_id": "...",
  "prompt": "..."}`. The command also prints the shared SYSTEM PROMPT — that
  is the instruction set you must follow for every item.

### Step 2 — answer each prompt yourself (this is the no-cost step)
Read `/tmp/attr_prompts.jsonl`. For EACH line, act as the llm-v1 extractor:
apply the SYSTEM PROMPT rules to that study's `prompt` text and produce a
JSON object of the exact shape:

```json
{"attributions": [
  {"question": "<neutral English proposition>",
   "position": "support|oppose|neutral|unspecified",
   "percentage": <0-100 or null>,
   "population": "<who was asked, or null>",
   "confidence": <0.0-1.0>}
]}
```

Rules (same as the system prompt — follow them strictly):
- Only emit attributions grounded in the provided text; never invent figures.
- One attribution per distinct (question, position) the text reports.
- "55% befürworten" → support; "36% lehnen ab" → oppose; "9% unentschieden"
  → neutral; sample-size/field-date numbers → position=unspecified.
- Return `{"attributions": []}` if the text has no opinion findings.

Write `/tmp/attr_responses.jsonl` with one line per study:
`{"study_id": "<same id>", "response": "<the JSON string above>"}`
(The `response` value is the JSON as a STRING — the parser is lenient and
also accepts raw objects, but a JSON string is the contract.)

Process every study in the file — do them all before step 3.

### Step 3 — apply to the database
```bash
python -m study_scraper attribute-apply /tmp/attr_responses.jsonl
```
Expect: `applied N response(s), M attributions stored`.

### Step 4 — report
```bash
python -m study_scraper status
```
Summarize in one short message: how many studies attributed, how many
triples stored, and the new totals. Do NOT commit anything; the data is in
Postgres.

## Idempotency / safety
- Re-running is safe: `attribute-apply` upserts per (study, model); a study
  already attributed leaves `attribution_queue`, so the next run picks up
  only new studies.
- If `attribute-apply` errors on connection, re-check POSTGRES_URL points at
  the Session pooler (port 5432), not the Transaction pooler (6543) —
  psycopg3 prepared statements break under transaction pooling.
- Never paste the DB password into chat or commit it.

## Tuning
- `--limit` controls studies per run (default 25 here). Larger = more done
  per session but more of your subscription's output budget used. The queue
  drains over successive scheduled runs regardless.
