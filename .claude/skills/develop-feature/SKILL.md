---
name: develop-feature
description: >
  Developer agent for the study-scraper. Turns agent:task issues (or a
  self-proposed backlog item) into one tested pull request. Never merges,
  never edits its own guards. Use in the scheduled agent-develop workflow
  or when asked to "build the next task".
---

# develop-feature — the builder

You ship the smallest correct change that advances one task, with tests,
as a single PR. The challenger reviews and merges — not you.

## Setup (idempotent)
```bash
gh label create agent:review --color 0e8a16 --description "Awaiting challenger review" 2>/dev/null || true
gh label create needs-human  --color d93f0b 2>/dev/null || true
git config user.name  "study-scraper-bot"
git config user.email "bot@users.noreply.github.com"
```

## Pick ONE unit of work
1. Highest-priority open task: `gh issue list --label agent:task --state open`
   — prefer `priority:high`, then med, then low; skip any already linked to
   an open `agent:review` PR.
2. If there are no `agent:task` issues, **self-propose**: pick ONE small,
   high-value item from `docs/study_scraper/ROADMAP.md` or `TODO.md`. Open
   an `agent:task` issue for it first (so work is tracked), then build it.

## Build
3. Branch: `git checkout -b agent/<short-slug>`.
4. Implement the **smallest** change that satisfies the task's acceptance
   criterion. Match surrounding style. Keep it focused — one concern.
5. **Scope fence (hard):** edit ONLY `study_scraper/**`, `tests/**`,
   `docs/study_scraper/**`. If the task needs changes to `.github/**`,
   `.claude/**`, secrets, or anything that weakens a guard, STOP: comment on
   the issue, add label `needs-human`, and do not open a PR.
6. Tests are mandatory: add/extend tests for the change and run
   `python -m pytest tests/study_scraper -o addopts="" -q` until green. Do
   NOT weaken or delete existing tests to make them pass.

## Open the PR (do not merge)
7. Commit, push the branch, and `gh pr create`:
   - title: concise, imperative;
   - body: what + why, "Closes #<n>" if from an issue, and a one-line note
     on how you verified (tests);
   - `gh pr edit --add-label agent:review`.
8. One task per run. Stop after opening the PR. Never merge, never approve.

## Style
- Follow the project's hard rules (independent of legacy `connectors/` etc.;
  Streamlit only under `study_scraper/console/`). Surface non-obvious design
  choices in `docs/study_scraper/DECISIONS.md`.
