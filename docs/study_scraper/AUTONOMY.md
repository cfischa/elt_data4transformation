# AUTONOMY — running with you out of the loop

The goal: studies get found, claims extracted, and attributions generated
**on a schedule, with no manual step**, billed to your Claude subscription
(no per-call API cost). This doc is the single, triple-checked source of
truth for what that takes. Once the one-time setup below is done, you do
nothing routine — you only *look* (the dock) when you want to.

## The shape

```
  GitHub Actions (server cron — durable, no laptop)
  ┌───────────────────────────┐      ┌────────────────────────────┐
  │ scrape.yml                │      │ attribute.yml              │
  │ Mon+Thu 05:00 UTC         │      │ Tue+Fri 06:17 UTC          │
  │ crawl SSOAR/OpenAlex/     │      │ Claude (your subscription) │
  │ DAWUM/GESIS/Eurostat,     │      │ reads attribution_queue,   │
  │ fulltext, citation follow │      │ emits (question,position,%) │
  └────────────┬──────────────┘      └─────────────┬──────────────┘
               │  writes                            │  writes
               ▼                                    ▼
          ┌──────────────────────────────────────────────┐
          │  Supabase Postgres  (studies, claims,         │
          │  source_records, attributions)                │
          └──────────────────────────────────────────────┘
               ▲                          (you, when you want)
               │                                    ▲
          POSTGRES_URL (local)            Streamlit dock on your laptop
```

Two server-side cron jobs, one shared database. They never talk directly —
only through the DB — and each is idempotent, so a re-run never double-writes.
**Nothing runs on your computer for the loop to work.** Your laptop is only
for *looking* at results via the dock (optional).

### Why GitHub Actions and not a "scheduled Claude session"

A scheduled session inside the Claude app (or a `CronCreate` job) is tied to
a live session and expires — not durable enough for "out of the loop". A
GitHub Action is a real server-side scheduler that runs whether or not any
app is open. The attribution Action authenticates with a **subscription
OAuth token**, so it still costs no API money.

## One-time setup (do once, ~15 min)

### Step 1 — Database (Supabase)
1. supabase.com → your project `vqrrfjboauzstoltmlnv` (already created).
2. **Project Settings → Database → Connection string → "Session pooler".**
   Copy it; replace `[YOUR-PASSWORD]` with your DB password. Shape:
   ```
   postgresql://postgres.vqrrfjboauzstoltmlnv:<DB-PASSWORD>@aws-0-<region>.pooler.supabase.com:5432/postgres
   ```
   - **Session pooler (5432), NOT Transaction pooler (6543)** — psycopg3
     prepared statements break under transaction pooling.
   - The `sb_publishable_…` API key and the `…supabase.co` URL do **not**
     work here — this connects directly to Postgres, not the REST API.

### Step 2 — Subscription token for CI
On your computer (one-time):
```
claude setup-token
```
Copy the long token it prints. This lets GitHub Actions use your Claude
subscription (no API key, no per-call cost).

### Step 3 — Two GitHub secrets
Repo → Settings → Secrets and variables → Actions → New repository secret:

| Secret name | Value |
|---|---|
| `SCRAPER_POSTGRES_URL` | the Session-pooler connection string from Step 1 |
| `CLAUDE_CODE_OAUTH_TOKEN` | the token from Step 2 |

Both workflows **skip green** until their secrets exist, so order doesn't matter.

### Step 4 — Smoke-test both, manually
Actions tab → run each once via "Run workflow" (branch:
`claude/find-obsidian-research-task-VOj97`, or `main` after merge):
1. `scheduled-scrape` → should migrate + crawl + upload `scrape-status.json`.
2. `scheduled-attribute` → should attribute the queue + upload
   `attribute-status.json`.

Open each run's artifact and confirm the numbers moved (studies > 0, then
attributions > 0). If green with rising numbers, **the loop is closed.**

## After setup — what you ever do

| Cadence | You | The system |
|---|---|---|
| Routine | nothing | crawls Mon+Thu, attributes Tue+Fri, automatically |
| When curious | open the dock | shows latest studies/claims/attributions |
| Add a topic/source | review a PR | new code ships on merge |

### Looking at results (optional, your laptop)
```powershell
$env:POSTGRES_URL = "<the Session-pooler string>"
streamlit run study_scraper/console/Home.py
```
Or deploy the dock to Streamlit Community Cloud (free) so you don't need the
laptop at all — point it at `POSTGRES_URL` as a Streamlit secret.

## Verifying autonomy (the triple-check)

- [ ] `SCRAPER_POSTGRES_URL` set, value starts with `postgresql://postgres.` — not `sb_publishable_`, not `https://`.
- [ ] `CLAUDE_CODE_OAUTH_TOKEN` set.
- [ ] `scheduled-scrape` manual run: green, artifact shows studies/claims > 0.
- [ ] `scheduled-attribute` manual run: green, artifact shows attributions > 0.
- [ ] Both workflows on `main` (merge PR #5) so the cron schedules are active —
      **cron only runs on the default branch.**
- [ ] (optional) dock opens against the Supabase URL and shows the rows.

## Failure modes & fixes

| Symptom | Cause | Fix |
|---|---|---|
| crawl green but no rows | secret holds the API key/URL, not the conn string | replace `SCRAPER_POSTGRES_URL` with the Session-pooler string |
| `prepared statement "…" already exists` | using Transaction pooler (6543) | switch to Session pooler (5432) |
| attribute step "queue empty" | no claims yet, or all attributed | expected — run a crawl first |
| attribute fails auth | bad/expired OAuth token | re-run `claude setup-token`, update the secret |
| cron never fires | workflow only on a feature branch | merge to `main` (default branch) |
| one source errors | upstream API hiccup | non-fatal by design; next run retries |

## Notes
- Schedules are deliberately off the :00 mark and a day apart so attribution
  always has a fresh queue.
- Cost: crawl Action = free GitHub minutes; attribution = your Claude
  subscription via OAuth (no API metering).
- Everything here is independent of the legacy ELT stack.
