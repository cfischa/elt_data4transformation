#!/usr/bin/env python3
"""Parse the claude-code-action execution output and exit non-zero if the
agent didn't actually do work.

The action writes ${RUNNER_TEMP}/claude-execution-output.json (the SDK
message stream) but the step exits 0 even when the model call failed
(e.g. an expired CLAUDE_CODE_OAUTH_TOKEN dies in ~2s with is_error=true,
num_turns=1, $0 cost). Workflows then look green while the agent team is
silently dead. This script turns that into a loud failure.

Exit codes: 0 = agent ran fine · 2 = agent errored / no-oped.
"""

from __future__ import annotations

import json
import sys


def main(path: str) -> int:
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception as exc:  # noqa: BLE001 - any parse failure is a fail
        print(f"::error::cannot parse execution output {path}: {exc}")
        return 2

    msgs = data if isinstance(data, list) else [data]
    result = next(
        (m for m in msgs if isinstance(m, dict) and m.get("type") == "result"),
        None,
    )
    if result is None:
        print("::error::no result message in Claude execution output")
        return 2

    if result.get("is_error"):
        print(
            "::error::Claude agent result is_error=true "
            f"(turns={result.get('num_turns')}, "
            f"duration_ms={result.get('duration_ms')}) — "
            "likely an expired/invalid CLAUDE_CODE_OAUTH_TOKEN"
        )
        return 2

    print(
        "agent ok: "
        f"turns={result.get('num_turns')} "
        f"duration_ms={result.get('duration_ms')} "
        f"cost_usd={result.get('total_cost_usd')}"
    )
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: check_agent_result.py <execution-output.json>")
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1]))
