# RUNBOOK — going to production

This is the **machine-only checklist**. Everything buildable from the
dev sandbox has been built and tested (233 tests). What remains needs
either outbound network access, credentials, or a human eyeball — i.e.
your computer. Work top to bottom; each step says how to verify it.

---

## 0. TL;DR — the concrete moves to run on your computer now

Copy-paste these in order. Stop at the first one that errors and tell me
the output. (Full detail + verification for each is in the numbered
sections below.)

```bash
# 0a. Get the latest code
git fetch origin
git checkout claude/find-obsidian-research-task-VOj97
git pull

# 0b. Start a local Postgres for the scraper (Docker)
docker compose -f study_scraper/docker-compose.yml up -d

# 0c. Point the tools at it
export POSTGRES_URL=postgresql://postgres:postgres@localhost:5544/study_scraper
#   PowerShell instead of the line above:
#     $env:POSTGRES_URL = "postgresql://postgres:postgres@localhost:5544/study_scraper"
#   ...and to make it stick across new windows:
#     [Environment]::SetEnvironmentVariable("POSTGRES_URL","postgresql://postgres:postgres@localhost:5544/study_scraper","User")

# 0d. Install dependencies (streamlit + anthropic are NOT pulled by
#     `pip install -e .` — streamlit is a dev-group dep, so list them)
pip install httpx pydantic pydantic-settings typer PyYAML \
            "psycopg[binary]" SPARQLWrapper pypdf beautifulsoup4 \
            anthropic streamlit
#   (env quirk: if pypdf import fails with "_cffi_backend", run: pip install --upgrade cffi)

# 0e. Create the schema (applies migrations 1..8)
python -m study_scraper migrate

# 0f. First REAL crawl — drop --from-file so it hits live endpoints
python -m study_scraper run    --source ssoar    --topic klima --limit 50
python -m study_scraper run    --source openalex --topic klima --limit 50
python -m study_scraper ingest --source dawum
python -m study_scraper ingest --source gesis  --limit 100
python -m study_scraper ingest --source eurobarometer --limit 100
#   eurobarometer is the GESIS catalog filtered to Eurobarometer waves
#   (same endpoint as gesis, no auth) — see A24.
python -m study_scraper ingest --source eurostat --code env_air_gge --code nrg_bal_s
#   eurostat filters to geo=DE by default (A14.1) — without it, tables like
#   nrg_bal_s are ~69 MB and crash json parsing. Override with --geo FR, or
#   --geo "" for all countries (huge tables are skipped by the size guard).

# 0g. Pull full documents + extract statistics from the full text
python -m study_scraper fulltext --limit 50

# 0h. See where you stand
python -m study_scraper status
python -m study_scraper search klimaschutzgesetz
python -m study_scraper search atomkraft

# 0i. Operator dock (browser UI)
streamlit run study_scraper/console/Home.py
```

**Then tell me what `status` printed** and I continue from real numbers.

**Two decisions only you can make** (each has its own section below):
- **Database location** — local Docker (0b above) vs hosted Supabase
  (needed for the scheduled GitHub Action). → §1.1
- **LLM attribution** — Option A live (costs API tokens, §3.5a) vs the
  zero-extra-cost Cowork path (§3.5b). Both produce the same data.

---

## 1. One-time setup (~15 min)

### 1.1 Database

Pick ONE:

**Option A — local Postgres (fastest):**
```bash
docker compose -f study_scraper/docker-compose.yml up -d
export POSTGRES_URL=postgresql://postgres:postgres@localhost:5544/study_scraper
```

**Option B — hosted Supabase (for the scheduled GitHub Action):**
1. Create a project at supabase.com.
2. Project Settings → Database → copy the connection string.
3. `export POSTGRES_URL=<that string>` locally, AND add it as the
   repo secret `SCRAPER_POSTGRES_URL` (Settings → Secrets → Actions)
   so `.github/workflows/scrape.yml` activates.

### 1.2 Dependencies + schema

```bash
pip install httpx pydantic pydantic-settings typer PyYAML \
            "psycopg[binary]" SPARQLWrapper pypdf beautifulsoup4 \
            anthropic streamlit
python -m study_scraper migrate          # applies migrations 1..8
```
**Verify:** prints `applied 8 migration(s)` (or "up to date").
(`anthropic` is only needed for live LLM attribution — §3.5a. Skip it
if you'll use the Cowork offline path in §3.5b.)

> Env quirk seen in dev: a broken system `cryptography` package makes
> `pypdf` fail at import (`_cffi_backend` missing). Fix: `pip install
> --upgrade cffi`.

## 2. First live crawl (~10 min, network required)

Everything below ran in the sandbox only against fixtures; the live
paths share the same parsers but have never touched the real
endpoints from here. Run and watch:

```bash
python -m study_scraper run    --source ssoar    --topic klima --limit 50
python -m study_scraper run    --source openalex --topic klima --limit 50
python -m study_scraper ingest --source dawum
python -m study_scraper ingest --source gesis  --limit 100
python -m study_scraper ingest --source eurobarometer --limit 100
python -m study_scraper ingest --source eurostat --code env_air_gge --code nrg_bal_s
python -m study_scraper status
```

**Known unknowns to watch on this first run** (flagged in code
comments too):
- **SSOAR** resumption-token pagination beyond page 1 — never
  exercised live.
- **OpenAlex follower filter**: `follow --fetch` uses
  `filter=ids.openalex:W…|W…`. If OpenAlex rejects it, change the
  attribute to `openalex:` in `study_scraper/discovery/openalex.py`
  (one line, marked with a NOTE).
- **GESIS SPARQL** throughput — the catalog is ~9k datasets; the
  `--limit` keeps the first run bounded.
- **Eurostat full tables** can be large; the listed codes are
  moderate. Add `?format=json` filters later if responses are slow.

## 3. Full-document statistics (A20 — the core production loop)

```bash
python -m study_scraper fulltext --limit 50     # fetch PDFs/HTML, extract stats
python -m study_scraper reading-list            # what still needs human reading
python -m study_scraper search klimaschutzgesetz
```

How it behaves: every kept study without an artifact gets fetched
from its `canonical_url`; PDF/HTML text is extracted; ALL numbers in
the full document become claims (`extractor='regex-v2'`, coexisting
with the abstract pass). Studies that still yield no numbers land on
the `reading-list` with reason `no_claims` — that's your reading
queue. Reason `no_artifact` means fetching hasn't happened yet.

**Expect failures here**: many `canonical_url`s are landing pages,
not direct PDFs (SSOAR handles resolve to HTML pages that link the
PDF). The fetcher stores whatever it gets; HTML landing pages often
still contain abstract + stats. A landing-page→PDF-link resolver is
the top backlog item after the first real run shows the hit rate.

## 3.5 LLM attribution (A21) — the answer layer

`search` finds a number with its sentence; **attribution** turns it
into a structured `(question, position, percentage)` triple you can
query directly. Studies with claims but no attribution sit in the
`attribution_queue`. Two ways to process it — both write the same
`attributions` table; pick by your cost preference.

### 3.5a Option A — live API (you chose this)

Costs Anthropic tokens against your `ANTHROPIC_API_KEY`.

```bash
export ANTHROPIC_API_KEY=sk-ant-...
# Optional cost lever — default is claude-opus-4-8; for bulk use cheaper:
# export STUDY_SCRAPER_LLM_MODEL=claude-haiku-4-5
python -m study_scraper attribute --limit 20
python -m study_scraper ask "klimaschutzgesetz"
python -m study_scraper ask "atomkraft"
```

`ask` prints, e.g., `62.0%  support  Stricter climate laws` with the
source link.

### 3.5b Zero-extra-cost path — answer in a Cowork session

Same result, no API bill: dump the prompts, paste them into a Cowork
chat (which runs on your existing plan), save its JSON replies, apply
them.

```bash
# 1. Dump the queue's prompts (system prompt + one block per study)
python -m study_scraper attribute-prompts --limit 20 --out prompts.jsonl

# 2. In a Cowork session: paste the SYSTEM PROMPT, then for each study
#    paste its prompt and have the agent return ONLY the JSON object.
#    Save the replies as JSONL, one per line:
#       {"study_id": "<id from prompts.jsonl>", "response": "<the JSON>"}
#    into responses.jsonl

# 3. Apply them (pure parser; no API key needed)
python -m study_scraper attribute-apply responses.jsonl
python -m study_scraper ask "atomkraft"
```

You can switch between 3.5a and 3.5b freely — re-running either for the
same study just replaces that study's `llm-v1` rows.

## 4. Schedule it

**Option A — GitHub Action (recommended):** already committed at
`.github/workflows/scrape.yml` (Mon+Thu 05:00 UTC + manual trigger).
Activates automatically once `SCRAPER_POSTGRES_URL` secret exists.
Verify: Actions tab → run `scheduled-scrape` manually → green + a
`scrape-status` artifact.

**Option B — cron on any machine:**
```cron
0 5 * * 1,4  cd /path/to/repo && POSTGRES_URL=... ./scripts/scrape_all.sh >> /var/log/scraper.log 2>&1
```
(Write the loop inline or copy the commands from the workflow file.)

## 5. Operator routine (each visit, ~10 min)

```bash
streamlit run study_scraper/console/Home.py
```
1. **Home** — counts moving? runs green?
2. **Review** — triage pending candidates (promote/reject).
3. **Topics** — vocabulary gaps? (e.g. the nuclear-keywords gap found
   2026-05-28 was fixed by editing the topic, not code). Live preview
   shows the impact before saving.
4. **Lake** — spot-check new structured records.
5. CLI: `reading-list` → pick studies to read; `search <begriff>` →
   answer questions.
6. **Answer-layer products (2026-07-05)**: `answer <q>` for the
   aggregated poll-of-polls view; `digest` after each crawl (or read
   the `opinion-digest.md` workflow artifact) for shifts/novelties on
   your watches (`watch add <q>` to register one); `gaps` to see which
   question clusters are stale or single-sourced; `policy-gap --topic
   <id>` for the opinion-vs-Bundestag view; `dossier <q> --out f.md`
   when someone needs a citable report; `export --out dataset/` for
   the open CSV dump.

### 5.1 Credentials the products can use (all optional)

| Secret / env | Used by | Without it |
| --- | --- | --- |
| `SCRAPER_POSTGRES_URL` (repo secret) | scheduled crawl + digest | workflow exits early, green |
| `DIP_API_KEY` | Bundestag DIP source | falls back to the published public key (rotates yearly) |
| `ANTHROPIC_API_KEY` | live `attribute` | use the offline Cowork path (§3.5b) |

## 6. What is NOT done (honest list, also in TODO.md)

| Item | Why it waits | Effort |
| --- | --- | --- |
| Landing-page → PDF-link resolver | Needs live hit-rate data first | S |
| Destatis GENESIS source | Needs free registration credentials | M |
| GESIS microdata (.sav) downloads (Q19) | Needs GESIS account + per-study licence | M |
| LLM claim extractor (`llm-v1`) — disambiguate WHAT each % refers to | Needs API key + cost decision; schema is ready | M |
| Topic-coverage growth (more topics in topics.csv) | Operator decision, dock-editable | S |
| Eval harness + gold set (Phase 7) | You curate the gold set after real data accumulates | M |
| Q13 success thresholds | Your ratification | — |

## 7. Incident notes

- **Postgres down (local):** `docker compose -f study_scraper/docker-compose.yml up -d`.
- **Migration error mid-way:** migrations are idempotent; re-run
  `migrate`. Each file guards with IF NOT EXISTS / ON CONFLICT.
- **A source breaks (upstream schema change):** sources are isolated;
  the others keep running. Fix the one parser; its fixture test shows
  the expected shape.
- **Sticky review decisions:** re-crawls never overturn a human
  promote/reject (enforced in `upsert_study`).
