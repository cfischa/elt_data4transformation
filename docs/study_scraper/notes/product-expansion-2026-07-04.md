# Product expansion — from Q&A tool to democratic-infrastructure platform

_Note, 2026-07-04. Maintainer prompt: "For now it's answering questions
on user input. With the data we could monitor, complete research, and so
on — what else to do on the product/data side, and which use cases could
be relevant to push a pro-democratic society?"_

This note maps the expansion space on top of what already exists (the
coverage-first scraper, the claims/attributions answer layer, the
scheduled Mon+Thu crawl, the operator dock) and ends with a recommended
sequence. It is direction input, not a backlog — items that get accepted
should be broken into `agent:task` issues / ROADMAP entries the usual way.

## Where the product is today

One interaction mode: **pull**. A user (or the CLI) asks a question and
gets back deduplicated (question, position, %) findings with source,
year, and provenance — `ask "climate"` → `62.0% support · Stricter
climate laws`. The data underneath is broader than the interaction
surface exposes: a studies catalog with a citation graph, a
structured-data lake (DAWUM vote intention, GESIS survey catalog,
Eurostat indicators), full-text claims, attributions, and per-run crawl
bookkeeping. The expansion question is therefore mostly: **what other
products does the same data already support?**

## Axis 1 — from pull to push: opinion monitoring

The scheduled crawl already runs twice a week; today its output is a
status artifact nobody reads unless something breaks (the monitor agent
watches *pipeline* health, not *data* meaning). The cheapest expansion
is to make each crawl produce meaning, not just rows:

1. **Standing questions ("watches").** A `watches` table: a user
   registers a question (a topic id + free-text question, later a
   question-cluster id). After each crawl+attribution run, new findings
   matching a watch are collected into a digest. Delivery v1 = a
   Markdown digest committed to the repo / posted to a GitHub issue
   (zero new infra); email/RSS later.
2. **Shift detection.** Once findings for the same question-cluster
   form a time series, flag movements: "support for Wiedereinstieg
   Atomkraft moved 55% → 61% between March and June polls". This is
   the single most newsworthy artifact the data can produce and it
   falls out of ROADMAP items D+E (poll-of-polls + semantic
   clustering) almost for free.
3. **Novelty alerts.** "First poll ever ingested on topic X",
   "publisher never seen before", "new poll contradicts the standing
   aggregate by >10 points". The last one doubles as a data-quality
   tripwire (extraction error vs real outlier — the dock reviews it).
4. **Per-topic weekly briefing.** Auto-generated per-topic page: new
   studies this week, new numbers, current aggregate, biggest mover.
   The Streamlit dock is the operator's view; the briefing is the
   *reader's* view and can be a static artifact (committed Markdown or
   generated HTML) long before any hosted product exists.

## Axis 2 — from single answers to completed research

`ask` returns a list of findings. The step up is returning a **synthesis**
— what a researcher would assemble by hand:

5. **Research dossier per question.** For one policy question: all
   findings clustered semantically, aggregated recency- and
   sample-size-weighted (ROADMAP D), spread across institutes shown
   explicitly, methodology notes (mode, n, fieldwork window, question
   wording where captured), qualitative studies listed alongside, and
   a provenance appendix. Output: a cited Markdown report. This turns
   an answer into something a journalist or NGO can actually cite.
6. **Evidence-gap map per topic.** The inverse product, and unique to
   a coverage-first store: which questions on a topic have been polled,
   by whom, how recently — and where the holes are (no data since
   2022, single-institute only, qualitative-only). Nobody else in the
   German landscape publishes this; it tells civil society *what poll
   to commission next*.
7. **Consensus/contradiction analysis.** Where institutes durably
   disagree on the same question-cluster, surface it, and with enough
   overlapping questions estimate house effects (institute-level
   bias), the way poll-of-polls sites do for vote intention — but for
   *issue* polling, where nobody does it.
8. **Longitudinal question tracking.** Politbarometer/ALLBUS have
   asked identical questions for decades; GESIS is already a source.
   "Support for X, 1990–2026" charts are high-credibility artifacts
   and pure downstream SQL once microdata/toplines are in the lake.

## Axis 3 — data-side foundations these need

Ordered by how many of the above they unblock:

- **Semantic question clustering (ROADMAP E)** is the keystone.
  Watches, shift detection, dossiers, gap maps, and house effects all
  require knowing that "Atomausstieg rückgängig machen" and "return to
  nuclear power" are the same question. pgvector + a small embedding
  model on `attributions.question` is enough for v1.
- **Poll-of-polls aggregation (ROADMAP D)** — the answer object
  becomes (cluster, position, weighted %, spread, n_polls, date range)
  instead of a finding list.
- **Demographic breakdowns.** Studies routinely report splits (age,
  party preference, East/West). Extending the attribution schema with
  an optional `population_segment` dimension (the population-dedup
  work from A-item 2026-07-04 already points here) unlocks
  representation-gap analysis, which is where the democratic value
  concentrates.
- **Opinion ⇄ fact joins.** Eurostat/Destatis rows are *indicators*,
  not opinions — their value is juxtaposition: "X% believe crime is
  rising; recorded crime is falling". Needs the typed Eurostat views
  (ROADMAP P1.3) plus a curated mapping from topics to indicator codes.
- **Finding-level trust score.** Sample size, method, recency,
  institute track record → a displayed confidence grade per finding.
  The dock trust signals (#21) started this; it must be *visible in
  answers* before anything is shown to non-operators.
- **Open dataset export.** A versioned CSV/Parquet dump (or read-only
  API) of `attributions` + `studies` metadata. Cheap, and it turns the
  project from a tool into infrastructure others build on.

## Axis 4 — pro-democratic use cases (the "why")

The thread through all of these: **public opinion in Germany is
constantly asserted and rarely verifiable.** Politicians cite friendly
polls, media cherry-pick, paywalled archives (Allensbach etc.) hide the
record, and no open, provenance-backed archive of issue-poll findings
exists. The scraper's provenance-first design is exactly the missing
piece. Concrete use cases, roughly in order of leverage:

1. **The opinion–policy gap tracker.** Join majority opinion per
   question-cluster with what parliament actually does (Bundestag DIP
   is already ROADMAP item 6: Drucksachen, votes, structured JSON).
   "70% support X; the corresponding bill failed in committee" is the
   canonical democratic-accountability artifact. This should be the
   flagship — it is unique, entirely in-scope for the existing data
   model, and both halves are already planned sources.
2. **Fact-checking public-opinion claims.** "Most Germans want …" is
   one of the most-abused rhetorical moves in German politics. A
   journalist pastes the claim, gets the actual distribution of
   findings with spread, n, and links. This is `ask` + dossier + a
   thin public surface; it counters manufactured-majority narratives
   ("die schweigende Mehrheit") with sourced data.
3. **Poll-literacy transparency.** Because the store keeps method,
   n, fieldwork dates, and question wording, every answer can *show
   why polls differ* (Civey's opt-in river sampling vs infratest's
   probability samples; wording effects). Displaying uncertainty
   honestly is itself civic education and is what separates this from
   advocacy tooling.
4. **Representation audits.** With demographic breakdowns: whose
   preferences do enacted policies track — by age, income, region?
   The academic version (Elsässer/Schäfer for Germany, Gilens for the
   US) is famous and years out of date; a continuously updated open
   version would be genuinely new.
5. **Evidence base for deliberative processes.** Bürgerräte, citizen
   assemblies, and NGOs need "what is known about public opinion on
   our topic" as briefing input. The dossier (item 5) is precisely
   that document, and the gap map tells them what's *not* known.
6. **An open longitudinal archive.** Simply persisting every published
   topline with provenance, forever, in the open — findings otherwise
   scattered across press releases that rot — is a public good on its
   own, before any analysis layer.

## Principles and risks (read before building any public surface)

- **Non-partisanship is the product.** The democratic value comes from
  provenance and methodological transparency, never from editorial
  framing. Always show spread and dissent, never a lone number;
  publish the methodology of the aggregation itself.
- **Accuracy before reach.** A wrong percentage published under a
  democracy banner is worse than no product. The answer-layer
  correctness items (recency B, sample-size C, aggregation D) and a
  real gold-set accuracy number (P3.13) are hard gates before any
  non-operator surface ships.
- **Legal shape.** Republishing facts (toplines, %, n) with citation
  is safe ground; redistributing full texts or bulk-mirroring
  databases is not (Leistungsschutzrecht, sui-generis database
  rights). The export should stay at findings + metadata + links,
  which is what the schema stores anyway.
- **Election-period sensitivity.** Vote-intention aggregation near
  elections has extra scrutiny (and Wahlrecht §32-adjacent norms on
  exit polls). Issue polling is the differentiator anyway — DAWUM
  already covers horse-race numbers; stay focused on issues.

## Recommended sequence

1. **Finish answer-layer correctness** (ROADMAP B, C — small, [now])
   and **semantic clustering E**, then **aggregation D**. Everything
   above stands on these.
2. **Monitoring v1** on the existing cron: watches table + per-topic
   Markdown digest + shift/novelty flags. Cheap, uses only existing
   infra, and creates the first recurring reader-facing artifact.
3. **Bundestag DIP source + opinion–policy gap view** — the flagship
   democratic use case; both halves already on the roadmap.
4. **Dossier generator** (reuses the aggregation + digest machinery)
   and the **evidence-gap report**.
5. **Public read-only surface + open dataset export** — only after the
   gold-set eval reports a real accuracy number.

Step 2's watches/digest and step 4's dossier are also the natural first
consumers for the demographic-breakdown and opinion⇄fact extensions, so
schema work can trail the products rather than lead them.
