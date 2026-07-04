#!/usr/bin/env bash
# Self-healing check for Claude agent steps (see check_agent_result.py).
#
# On FAILURE (agent no-oped — missing output, or result.is_error=true,
# the signature of a dead credential):
#   1. files or updates ONE deduped `needs-human` ops issue telling the
#      maintainer exactly how to fix it, and
#   2. fails the job, so the run shows RED instead of a silent green.
# On SUCCESS: if the ops issue is open, comments "recovered" and CLOSES
# it — the healing loop closes itself the moment a credential works
# again; no human confirmation step.
#
# Requires: GH_TOKEN env (pass ${{ github.token }}), runs from repo root.

set -uo pipefail

OUT="${RUNNER_TEMP:-/tmp}/claude-execution-output.json"
TITLE="[ops] Claude agent runs failing — rotate CLAUDE_CODE_OAUTH_TOKEN"

reason=""
if [ ! -f "$OUT" ]; then
  reason="No Claude execution output file was produced."
elif ! python3 .github/scripts/check_agent_result.py "$OUT"; then
  reason="The Claude step errored immediately (is_error=true) — the usual cause is an expired or invalid credential (CLAUDE_CODE_OAUTH_TOKEN, or ANTHROPIC_API_KEY if you use that instead)."
fi

if [ -z "$reason" ]; then
  echo "self-healing check passed"
  # Recovery: close the ops ticket automatically if it's still open.
  if command -v gh >/dev/null 2>&1 && [ -n "${GH_TOKEN:-}" ]; then
    existing=$(gh issue list -R "$GITHUB_REPOSITORY" --state open \
      --search "$TITLE in:title" --json number --jq '.[0].number' 2>/dev/null || true)
    if [ -n "$existing" ] && [ "$existing" != "null" ]; then
      gh issue close "$existing" -R "$GITHUB_REPOSITORY" --comment \
        "Recovered: workflow **${GITHUB_WORKFLOW:-?}** run ${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-}/actions/runs/${GITHUB_RUN_ID:-?} passed the self-healing check — the Claude credential works again. Closing automatically." \
        || true
      echo "auto-closed ops issue #$existing (recovered)"
    fi
  fi
  exit 0
fi

echo "::error::agent no-op detected: $reason"

BODY="Workflow **${GITHUB_WORKFLOW:-?}** (run ${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-}/actions/runs/${GITHUB_RUN_ID:-?}) detected that the Claude agent step did no work.

**Reason:** $reason

**Fix (2 min) — either credential works:**
- *Subscription:* run \`claude setup-token\` locally, update the repo secret \`CLAUDE_CODE_OAUTH_TOKEN\` (delete it if dead and you switch to an API key).
- *API key:* add repo secret \`ANTHROPIC_API_KEY\` (metered) — the workflows accept it as an alternative.

Then re-run any agent workflow. This issue **closes itself automatically** on the first passing run.

Until fixed, ALL Claude-powered workflows (agent-product / monitor / develop / review and scheduled-attribute) are silently no-oping. See docs/study_scraper/AUTONOMY.md failure modes."

if command -v gh >/dev/null 2>&1 && [ -n "${GH_TOKEN:-}" ]; then
  gh label create needs-human --color d93f0b \
    --description "Requires a human decision" -R "$GITHUB_REPOSITORY" 2>/dev/null || true
  existing=$(gh issue list -R "$GITHUB_REPOSITORY" --state open \
    --search "$TITLE in:title" --json number --jq '.[0].number' 2>/dev/null || true)
  if [ -n "$existing" ] && [ "$existing" != "null" ]; then
    gh issue comment "$existing" -R "$GITHUB_REPOSITORY" --body "$BODY" || true
    echo "updated existing ops issue #$existing"
  else
    gh issue create -R "$GITHUB_REPOSITORY" --title "$TITLE" \
      --body "$BODY" --label needs-human \
      || gh issue create -R "$GITHUB_REPOSITORY" --title "$TITLE" --body "$BODY" \
      || true
    echo "filed new ops issue"
  fi
else
  echo "gh CLI or GH_TOKEN unavailable; skipping issue filing"
fi

exit 1
