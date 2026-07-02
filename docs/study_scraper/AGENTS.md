# AGENTS — the self-improving team

Four agents run as scheduled GitHub Actions (your Claude subscription, no
API cost) and interact through GitHub issues + PRs. Together they keep the
pipeline healthy, push the product toward `GOAL.md`, build features, and
gate every change. You stay in control via **one issue** — the rest runs
itself.

## The team

| Agent | Workflow | When | Job |
|---|---|---|---|
| **Product lead** | `agent-product.yml` | Mon 08:23 UTC | Drives toward GOAL; talks WITH you in the "Product Direction" issue; emits prioritized `agent:task`s; keeps `ROADMAP.md`. |
| **Monitor** | `agent-monitor.yml` | daily 07:11 UTC | Watches crawl/attribute runs + DB; files `agent:task` issues for anomalies. |
| **Developer** | `agent-develop.yml` | daily 12:37 UTC | Takes the top `agent:task` (or self-proposes); opens ONE tested PR. Never merges. |
| **Challenger** | `agent-review.yml` | every 4h | Runs the tests, reviews the diff adversarially, **merges only if green + sound + in scope**. |

## How they interact

```
 YOU ⇄ "Product Direction" issue ⇄ PRODUCT LEAD ──agent:task──┐
                                          ▲                    ▼
                        progress/metrics  │              DEVELOPER ──PR(agent:review)──┐
 MONITOR ──agent:task (ops)───────────────┘                                            ▼
                                                                               CHALLENGER ──merge if green+sound
```

- **You ⇄ Product lead:** your only routine touchpoint. It posts where we
  are + 1–3 questions (each with a default); you answer in comments; it
  turns decisions into tasks. Silence still moves forward via the defaults.
- **Monitor → Developer:** ops problems become `agent:task` issues.
- **Developer → Challenger:** every change is a PR; the challenger is the
  only thing that merges.

## Your control surface
- **Steer:** comment on the **Product Direction** issue.
- **Inject work:** open an issue, label it `agent:task` (+ `priority:high`).
- **Veto:** close a PR/issue, or comment `needs-human`.
- **Override merge policy:** the challenger merges only `green + reviewed +
  in scope`; you can always merge/close manually.

## Labels
`agent:task` · `priority:high|med|low` · `agent:review` ·
`agent:changes-requested` · `needs-human` · `ops`

## Safety rails (deliberate)
- **Nothing reaches `main` unmerged by the challenger**, and the challenger
  merges only after running the tests itself and an adversarial review.
- **Agents cannot edit their own guards.** Developer/product may touch only
  `study_scraper/**`, `tests/**`, `docs/study_scraper/**`. Any PR touching
  `.github/**`, `.claude/**`, or secrets is auto-flagged `needs-human` and
  never auto-merged — only you can change how the agents themselves behave.
- **Tests can't be weakened to pass** — the challenger rejects removed or
  hollowed-out tests.
- **Self-driving, not event-chained.** Bot-authored PRs don't trigger
  `pull_request` workflows (GitHub anti-recursion), so the challenger sweeps
  on a schedule and runs tests itself. Worst case is latency (hours), never
  an unverified merge.
- **Self-healing credential check.** claude-code-action exits 0 even when
  the model call fails (an expired `CLAUDE_CODE_OAUTH_TOKEN` dies in ~2s
  with `is_error=true`), which once left the whole team silently dead for
  days. Every Claude workflow now runs
  `.github/scripts/check_agent_result.sh` after the agent step: a no-op
  turns the run RED and files/updates one deduped `needs-human` issue with
  the exact fix (rotate the token). Silent failure is impossible by design.

## Cost
Four scheduled agents + a review sweep every 4h draw on your Claude
subscription. Turn any agent off by disabling its workflow in the Actions
tab; widen/narrow cadence by editing the `cron:` lines.

## First-run note
On the very first product run, answer its questions in the Product Direction
issue — that seeds the roadmap the developer builds from.
