# Research: structured-data sources for German political topics

**Date:** 2026-05-28
**Trigger:** Maintainer (A13): "We should not focus on pdf study
extraction first. Future task. We want to focus on database data /
structured data like files and db."
**Goal of this doc:** identify every source where we can pull
**structured** data (JSON / CSV / SPSS / Stata / XML / RDF / API
responses) relevant to German political topics, ranked by how directly
they answer questions of the form *"how many Germans want X?"*.

## Headline finding

> **GESIS is the gold mine for issue-poll microdata. DAWUM is the gold
> mine for party polling. Statistical-office APIs (Destatis, Eurostat)
> deliver indicators, not opinions — useful for context, not the
> "want X" answer.**

For the maintainer's two example questions — climate-law support,
nuclear-energy attitudes — the most authoritative structured datasets
are all hosted by **GESIS** (search.gesis.org): the
Politbarometer 1977-2023 cumulation, the UBA Umweltbewusstsein series
(2018 / 2020 / 2022), ALLBUS, GLES, ESS, EVS/WVS. These carry the
underlying microdata as SPSS .sav, Stata .dta, and (often) CSV. Access
is **free for academic / research use**, behind a free GESIS account.

## Tier 1 — directly answer example questions (do first)

### 1.1 GESIS — political-attitude microdata

The single highest-yield source category. Six study series, all with
structured downloads, all on the same access platform.

| Study | Coverage | Cadence | Format | Topical hits for example Qs |
| --- | --- | --- | --- | --- |
| **Politbarometer 1977-2023** | Monthly attitudes on parties + issues | Monthly cumulated | SPSS / Stata / CSV (cumulation) | **High** — explicit Klimaschutz / Atomkraft trend items in the partial cumulation (82 variables, ~500 survey dates) |
| **ALLBUS** | Attitudes, values, behaviour | Biennial since 1980 | SPSS / Stata | **Medium** — climate / nuclear items in occasional modules |
| **GLES** | Election attitudes, party support | Pre/post each Bundestagswahl | SPSS / Stata / CSV | **High** for election-cycle policy questions |
| **EVS / WVS joint 2017-2022** | Values incl. environment, governance | ~10-yearly | SPSS / Stata / CSV | **Medium** — values lens, less issue-specific |
| **ESS** rounds 1-10 (Germany) | Cross-EU social attitudes | Biennial | SPSS / Stata / CSV | **Medium** — comparable across countries |
| **UBA Umweltbewusstseinsstudie** 2018, 2020, 2022 | Environmental awareness + Klimaschutz | Biennial | SPSS deposited at GESIS | **Very high** — directly about climate-law support, energy-transition acceptance, n≈2073 |

- **Access portal:** https://search.gesis.org
- **Auth:** free GESIS account (institutional affiliation usually
  required for terms-of-use compliance; private researchers can also
  register).
- **License caveats:** terms-of-use varies per study. Most permit
  derivative analysis; few permit redistribution of microdata.
  Implication for our pipeline: ingest metadata + aggregated claims,
  do not redistribute row-level microdata downstream of our own DB
  without re-checking each study's licence.
- **API status:** GESIS Search has a JSON API for *finding* studies
  but the actual data downloads are file-served via the post-login
  flow. Likely flow for our scraper: a logged-in HTTPS session that
  hits the per-study download URL.
- **Implementation note for Q17 (new):** the scraper needs
  `GESIS_USERNAME` / `GESIS_PASSWORD` env vars. Treat as a privileged
  source; off by default; opt-in via `--source gesis`.

### 1.2 DAWUM — party polling (already in legacy code)

| | |
| --- | --- |
| **What** | Aggregator of Sonntagsfrage party polls for Bundestag, Landtag, EU |
| **Format** | JSON via open API |
| **Auth** | None |
| **License** | ODC-ODbL (open) |
| **URL** | https://dawum.de/API/ |
| **Topical hits for example Qs** | **Low** — party support, not issue support. Adjacent: if a party's platform mentions climate or nuclear, party support can be a proxy. |
| **Effort** | Low — legacy `connectors/dawum_connector.py` already exists; port behind `DiscoverySource` |

Decision: port DAWUM but classify its yield correctly. It's *party*
polling, not *issue* polling. For "how many Germans want X" type
questions where X is a policy, DAWUM is at best a weak signal.

## Tier 2 — statistical indicators (context, not poll answers)

These give you facts about *what is*, not *what Germans think*. Useful
for downstream analysis, for cross-validating poll claims, and for
sub-questions like "what is Germany's actual nuclear-energy share?".

### 2.1 Destatis GENESIS

| | |
| --- | --- |
| **What** | Federal Statistical Office tables — population, economy, environment, public sector |
| **Format** | REST JSON for metadata; CSV / "Flat-File-CSV" / XLSX for table bodies |
| **Auth** | Free registration |
| **License** | DL-DE→BY-2.0 (attribution) |
| **URL** | https://www-genesis.destatis.de/genesisWS/rest/2020/ |
| **Docs** | [API intro PDF](https://www-genesis.destatis.de/datenbank/online/docs/GENESIS-Webservices_Einfuehrung.pdf) |
| **Effort** | Low — legacy `connectors/destatis_connector.py` exists |

The legacy code already implements much of this. Port + wire to
`datasets` table from Q16.

### 2.2 Eurostat

| | |
| --- | --- |
| **What** | EU statistical office; Germany comparable with other EU countries |
| **Format** | JSON-stat (cube model: dimensions × categories) |
| **Auth** | None |
| **License** | Eurostat re-use policy (attribution) |
| **URL** | `https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{DATASET_CODE}` |
| **Effort** | Low — legacy `connectors/eurostat_connector.py` exists |

### 2.3 Regionalstatistik (Bund + Länder regional)

- Same Genesis engine as Destatis with regional dimension down to
  Kreise / kreisfreie Städte.
- Auth required from **May 2025** (recently changed; free registration).
- API URL: https://www.regionalstatistik.de/
- Useful for sub-national questions (regional climate-attitude
  variance, migration distribution).

### 2.4 OECD Data Explorer

| | |
| --- | --- |
| **What** | OECD data warehouse; cross-country comparators |
| **Format** | JSON via `format=jsondata`; XML |
| **Auth** | None |
| **License** | OECD re-use terms |
| **URL** | https://data-explorer.oecd.org/ |
| **Notable for example Qs** | OECD Trust in Government data (Germany included). Adjacent. |

## Tier 3 — discovery / catalog (not data itself)

### 3.1 GovData.de (federal metadata portal)

- **What:** national CKAN-based metadata portal aggregating 60k+
  datasets from Bund / Länder / Kommunen.
- **Format:** CKAN REST API at `https://www.govdata.de/ckan/api`; also a
  SPARQL endpoint.
- **Auth:** None for metadata.
- **Yield:** zero direct poll data; very high *discovery* yield —
  this is the new-source-discovery channel from Phase 5d, except for
  government data instead of academic papers.
- **Implementation:** query CKAN by topic keyword; surface the listed
  resource URLs as candidate sources to the dock for human review.

### 3.2 Bundeswahlleiter / Federal Returning Officer

| | |
| --- | --- |
| **What** | Actual election results — Bundestag, EU, federal-state |
| **Format** | CSV + XML downloads |
| **Auth** | None |
| **URL** | https://www.bundeswahlleiterin.de/bundestagswahlen/2025/ergebnisse/opendata.html |
| **Yield for example Qs** | None directly — results, not attitudes |
| **Useful for** | Validating that party-polling aggregates (DAWUM) actually predicted real outcomes; longitudinal turnout & party share by Wahlkreis. |

### 3.3 Bundestag DIP API

| | |
| --- | --- |
| **What** | Parliamentary documents — Drucksachen, Plenarprotokolle, MP biographies |
| **Format** | JSON / XML |
| **Auth** | Free API key by email (`parlamentsdokumentation@bundestag.de`); temporary public key available |
| **URL** | https://search.dip.bundestag.de/api/v1/drucksache |
| **Docs** | https://dip.bundestag.de/über-dip/hilfe/api |
| **Yield for example Qs** | Low for direct poll answers; high for "what laws were proposed on topic X". Indirect signal via Drucksachen citing surveys. |
| **Existing code** | https://github.com/bundesAPI/dip-bundestag-api — auto-generated Python client we can vendor or reference. |

### 3.4 wahlrecht.de (HTML scrape)

- Same domain as DAWUM (party polling) but a much older site that
  goes back further and includes some institutes DAWUM doesn't.
- **Format:** HTML tables; multiple open-source scrapers exist
  ([germanpolls](https://github.com/cutterkom/germanpolls),
  [wahlrecht (Python)](https://github.com/stefanw/wahlrecht),
  [coalitions::scrape_wahlrecht](https://adibender.github.io/coalitions/reference/scrape.html))
  — we can reference their selectors without reinventing.
- **Priority:** lower than DAWUM. Pull only if DAWUM coverage gap.

### 3.5 BAMF Forschungsdatenzentrum

| | |
| --- | --- |
| **What** | BAMF microdata — Central Register of Foreigners, BSK, survey data |
| **Format** | varies (research-data centre) |
| **Auth** | Restricted to predominantly publicly-funded research institutions. Application via FDZ portal. |
| **Yield for example Qs (migration)** | Very high |
| **Operational reality** | **Not viable for our scraper** under the current access regime. Better path: BAMF's *public* publications page (Migrationsberichte, Forschungsberichte) with their underlying Excel tables — those are openly downloadable. |

## Tier 4 — structured but paywalled / restricted

Listed for completeness so the next pass doesn't waste effort:

- **Forsa / Allensbach / Infratest dimap / Innofact / Civey** —
  direct: methodology + summary only. Their underlying microdata
  occasionally lands at GESIS (Politbarometer is the Forschungsgruppe
  Wahlen example), but recent press-release polls (Forsa April 2021,
  Innofact March 2025) typically aren't deposited until 1-2 years
  later, if at all. **Implication:** we'll always lag the very latest
  press-release figures.
- **Statista** — paywall.
- **Specific polling firms' own Sonntagsfrage CSV downloads** —
  *Allensbach* publishes Sonntagsfrage tables on
  `ifd-allensbach.de/studien-und-berichte/sonntagsfrage/`, but most
  detail is in PDFs. Treat as Tier 3 HTML scrape.

## Verdict matrix — what to wire up next

| Source | Auth? | Direct yield for example Qs | Effort | Priority |
| --- | --- | --- | --- | --- |
| GESIS Search + microdata (Politbarometer, UBA Umweltbewusstsein, ALLBUS, GLES, EVS, ESS) | yes (free) | **very high** | high (auth + per-study download flow + license tracking) | **1** |
| DAWUM JSON | no | low (party not issue) | low (port legacy) | **2** |
| Destatis GENESIS | yes (free) | medium (context only) | low (port legacy) | **3** |
| Eurostat | no | medium (context only) | low (port legacy) | **3** |
| GovData CKAN | no | zero direct, high discovery | medium | **4** (Phase 5d new-source discovery) |
| Bundestag DIP | yes (free) | low | medium | **5** |
| Bundeswahlleiter CSV | no | none direct, useful validation | low | **6** |
| Regionalstatistik | yes (free) | low | low | **6** |
| OECD Data Explorer | no | low (trust series only) | low | **6** |
| wahlrecht.de scrape | no | low (DAWUM dup) | medium | defer |
| BAMF FDZ | restricted | high (migration) | not viable for scraper | defer |

## Decisions needed before next iteration

### Q17 (new) — How to handle GESIS auth?

GESIS requires login. Proposal:
- Env vars `GESIS_USERNAME`, `GESIS_PASSWORD` read at start of ingest.
- `GESISSource` opt-in via explicit `--source gesis` (never runs by
  default).
- Stores the session cookie in-memory only; no token persistence.
- Per-study `licence` field on `datasets` records what we may / may
  not redistribute.

### Q18 (new) — Microdata: store rows or just the file?

Microdata files are typically 5–100 MB SPSS .sav with thousands of
rows. Two options:
1. **File-only:** record the file URL + checksum in `datasets`, leave
   parsing to downstream tools.
2. **Row-level:** parse SPSS into `dataset_rows`; queryable directly.
   Cost: pandas / pyreadstat dependency; ~10 MB per study in our DB.

My recommendation: **file-only for v1**, row-level for hand-picked
small datasets (UBA Umweltbewusstsein 2022 specifically, since it's
the one most directly answering the example questions).

## What I am NOT proposing

- A live PDF / HTML scraper for forsa.de or innofact.de. A13 deferred
  unstructured extraction.
- A "go fetch on demand" mode. The system stays batch (see the
  workflow conversation 2026-05-28).
- Microdata redistribution. We ingest metadata + aggregated claims +
  links; row-level data stays on the source's terms.

## Concrete next iteration scope (subject to your Q16 / Q17 / Q18 calls)

1. **`datasets` schema** (Q16 endorsement → migration 0005).
2. **DAWUM source** (`study_scraper/discovery/dawum.py`) — port the
   legacy connector behind `DiscoverySource`; emits `datasets` rows.
   Free, no auth — quickest to ship.
3. **Eurostat / Destatis sources** — additive, low effort, give
   context indicators (emissions, energy mix).
4. **GESIS path planned but parked** until Q17 (auth) is settled.
   Highest yield but highest setup friction.

Sources cited inline above are the ones whose URLs and access models I
relied on; the principal references are GESIS Data Catalogue, DAWUM
API, Destatis GENESIS docs, Eurostat User Guides, GovData docs,
Bundestag DIP docs, Bundeswahlleiterin Open Data, BAMF FDZ, ESS Data
Portal, EVS Data Downloads, OECD API docs, and the wahlrecht.de
scraper repos.
