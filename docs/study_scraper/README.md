# Study Scraper — Knowledge Base

This directory is the working memory for the **study scraper** project: a focused
pivot away from the broader ELT pipeline that previously dominated this repo.

The pivot in one sentence:

> Build a high-class scraper that, given a topic list, finds and extracts
> relevant German-society **studies** (representative surveys, polls, and
> research publications containing quantitative data) from the public web —
> so that a later data-engineering step can answer *"what does German society
> want on topic X?"*.

## How to use these docs

| File             | Purpose                                                                    |
| ---------------- | -------------------------------------------------------------------------- |
| `GOAL.md`        | What we are building, what "done" looks like, what is **out of scope**.    |
| `STATUS.md`      | Honest snapshot of the repo today: what's reusable, what's broken, what's overbuilt. |
| `TODO.md`        | Ordered backlog. The single source of truth for "what's next".             |
| `DECISIONS.md`   | Accepted design decisions + **open questions that block work**.            |

## Working agreement (between Claude and the maintainer)

1. **Iterate in this knowledge base before iterating in code.** Every non-trivial
   change either has a corresponding accepted decision in `DECISIONS.md` or
   raises a new open question.
2. **The scraper is an independent project.** It will live in its own
   top-level package (proposed: `study_scraper/`) and must not depend on
   Airflow, ClickHouse, dbt, or Streamlit unless an accepted decision says so.
3. **Surface design decisions, don't bury them.** If a choice is non-obvious or
   cuts across multiple parts of the system, it goes in `DECISIONS.md` —
   either as accepted (with rationale) or as an open question with a concrete
   proposal.
4. **Status doc must reflect reality.** When something ships or breaks,
   `STATUS.md` gets updated in the same change.
5. **Goal is the anchor.** Before adding scope, check that it serves
   `GOAL.md`. If it doesn't, push back or propose a goal change.

## Status flag

This knowledge base is currently in **bootstrap** state: goal articulated,
status documented, open questions raised. **No code in `study_scraper/` yet.**
First implementation work is blocked on the open questions in
`DECISIONS.md` (Q1–Q8).
