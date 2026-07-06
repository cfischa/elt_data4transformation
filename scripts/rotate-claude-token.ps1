# rotate-claude-token.ps1 — refresh the agent team's Claude credential
# and revive the automation in one command.
#
# What it does:
#   1. Runs `claude setup-token` (browser login — the only manual moment).
#   2. Auto-captures the printed token (or asks you to paste it).
#   3. Updates the GitHub repo secret CLAUDE_CODE_OAUTH_TOKEN.
#   4. Kicks scheduled-attribute + agent-develop so the team wakes now
#      instead of at the next cron. The self-healing check then closes
#      the ops issue (#24-style ticket) automatically on the first green run.
#
# One-time prerequisites:
#   winget install GitHub.cli        # then: gh auth login
#   irm https://claude.ai/install.ps1 | iex   # Claude Code CLI
#
# Usage (from anywhere):
#   powershell -ExecutionPolicy Bypass -File scripts\rotate-claude-token.ps1

param(
    [string]$Repo = "cfischa/elt_data4transformation"
)

$ErrorActionPreference = "Stop"

function Find-Claude {
    if (Get-Command claude -ErrorAction SilentlyContinue) { return "claude" }
    $npmShim = Join-Path $env:APPDATA "npm\claude.cmd"
    if (Test-Path $npmShim) { return $npmShim }
    throw "claude CLI not found. Install it:  irm https://claude.ai/install.ps1 | iex"
}

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw "GitHub CLI not found. Install:  winget install GitHub.cli  then:  gh auth login"
}

$claude = Find-Claude
Write-Host ">> Generating a fresh subscription token (your browser will open)..." -ForegroundColor Cyan

# Show output AND capture it, so interactive prompts stay visible.
$raw = & $claude setup-token 2>&1 | Tee-Object -Variable capturedLines | Out-String

# Claude Code OAuth tokens look like sk-ant-oat01-... — grab the last match.
$matches = [regex]::Matches($raw, "sk-ant-[A-Za-z0-9_\-]{20,}")
if ($matches.Count -gt 0) {
    $token = $matches[$matches.Count - 1].Value
    Write-Host ">> Token captured automatically." -ForegroundColor Green
} else {
    Write-Host ">> Could not auto-capture the token from the output." -ForegroundColor Yellow
    $token = Read-Host "Paste the token that 'claude setup-token' printed"
}
if ([string]::IsNullOrWhiteSpace($token)) { throw "No token provided — aborting." }

Write-Host ">> Updating repo secret CLAUDE_CODE_OAUTH_TOKEN on $Repo ..." -ForegroundColor Cyan
$token | gh secret set CLAUDE_CODE_OAUTH_TOKEN --repo $Repo
if ($LASTEXITCODE -ne 0) { throw "gh secret set failed (is gh authenticated? run: gh auth login)" }

Write-Host ">> Waking the agents now (instead of waiting for the next cron)..." -ForegroundColor Cyan
gh workflow run scheduled-attribute --repo $Repo | Out-Null
gh workflow run agent-develop      --repo $Repo | Out-Null

Write-Host ""
Write-Host "Done. The self-healing check auto-closes the ops issue on the first green run." -ForegroundColor Green
Write-Host "Watch progress:  gh run list --repo $Repo --limit 5"
