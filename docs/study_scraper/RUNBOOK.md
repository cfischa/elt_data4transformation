# RUNBOOK — going to production

This is the **machine-only checklist**. Everything buildable from the
dev sandbox has been built and tested (214 tests). What remains needs
either outbound network access, credentials, or a human eyeball — i.e.
your computer. Work top to bottom; each step says how to verify it.

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
            "psycopg[binary]" SPARQLWrapper pypdf beautifulsoup4
python -m study_scraper migrate          # applies migrations 1..7
```
**Verify:** prints `applied 7 migration(s)` (or "up to date").

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
