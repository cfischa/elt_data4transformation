# PowerShell Smoke Test Script for ELT Stack
param(
    [int]$Timeout = 120,
    [string]$AirflowUrl = "http://localhost:$($env:AIRFLOW_WEBSERVER_PORT ?? '8081')",
    [string]$ClickHouseHost = "localhost",
    [int]$ClickHousePort = ($env:CLICKHOUSE_PORT ?? 8124),
    [string]$ClickHouseUser = ($env:CLICKHOUSE_USER ?? "admin"),
    [string]$ClickHousePassword = ($env:CLICKHOUSE_PASSWORD ?? "admin123")
)

# Color functions
function Write-Success { param($Message) Write-Host "[SUCCESS] $Message" -ForegroundColor Green }
function Write-Error { param($Message) Write-Host "[ERROR] $Message" -ForegroundColor Red }
function Write-Warning { param($Message) Write-Host "[WARNING] $Message" -ForegroundColor Yellow }
function Write-Info { param($Message) Write-Host "[INFO] $Message" -ForegroundColor Cyan }

# Status tracking
$script:ChecksPassed = 0
$script:ChecksTotal = 0
$script:FailedChecks = @()

function Add-Check {
    $script:ChecksTotal++
}

function Pass-Check {
    param($Message)
    Write-Success $Message
    $script:ChecksPassed++
}

function Fail-Check {
    param($Message)
    Write-Error $Message
    $script:FailedChecks += $Message
}

function Test-DockerStatus {
    Add-Check
    Write-Info "Checking Docker status..."
    
    if (-not (Get-Command "docker" -ErrorAction SilentlyContinue)) {
        Fail-Check "Docker command not found"
        return
    }
    
    try {
        $dockerInfo = docker info 2>&1
        if ($LASTEXITCODE -eq 0) {
            Pass-Check "Docker daemon is running"
        } else {
            Fail-Check "Docker daemon is not running. Please start Docker Desktop."
            Write-Info "Error details: $dockerInfo"
        }
    }
    catch {
        Fail-Check "Docker daemon is not running: $($_.Exception.Message)"
    }
}

function Test-AirflowHealth {
    Add-Check
    Write-Info "Checking Airflow webserver health..."
    
    $healthUrl = "$AirflowUrl/health"
    
    try {
        Write-Info "Testing URL: $healthUrl"
        $response = Invoke-RestMethod -Uri $healthUrl -Method Get -TimeoutSec 10 -ErrorAction Stop
        if ($response.metadatabase.status -eq "healthy") {
            Pass-Check "Airflow webserver is healthy"
        } else {
            Fail-Check "Airflow webserver is running but not healthy"
        }
    }
    catch {
        Fail-Check "Airflow webserver is not reachable: $($_.Exception.Message)"
    }
}

function Test-ClickHouseHealth {
    Add-Check
    Write-Info "Checking ClickHouse health..."
    
    $clickhouseUrl = "http://${ClickHouseHost}:${ClickHousePort}/"
    $query = "SELECT 1"
    
    try {
        Write-Info "Testing URL: $clickhouseUrl"
        $headers = @{}
        if ($ClickHousePassword) {
            $credentials = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${ClickHouseUser}:${ClickHousePassword}"))
            $headers["Authorization"] = "Basic $credentials"
        }
        
        $response = Invoke-RestMethod -Uri "$clickhouseUrl" -Method Get -Body @{query = $query} -Headers $headers -TimeoutSec 10
        $responseStr = [string]$response
        if ($responseStr.Trim() -eq "1") {
            Pass-Check "ClickHouse is responding correctly"
        } else {
            Fail-Check "ClickHouse responded but with unexpected result: $responseStr"
        }
    }
    catch {
        Fail-Check "ClickHouse is not reachable: $($_.Exception.Message)"
    }
}

function Show-Summary {
    Write-Host ""
    Write-Host "=========================================" -ForegroundColor Cyan
    Write-Host "SMOKE TEST SUMMARY" -ForegroundColor Cyan
    Write-Host "=========================================" -ForegroundColor Cyan
    Write-Host "Checks passed: $script:ChecksPassed/$script:ChecksTotal" -ForegroundColor White
    
    if ($script:FailedChecks.Count -gt 0) {
        Write-Host ""
        Write-Host "Failed checks:" -ForegroundColor Red
        foreach ($check in $script:FailedChecks) {
            Write-Host "  - $check" -ForegroundColor Red
        }
        Write-Host ""
        Write-Error "Smoke test FAILED"
        return 1
    } else {
        Write-Host ""
        Write-Success "All smoke tests PASSED"
        return 0
    }
}

# Main execution
Write-Info "Starting smoke test suite..."

# Load environment variables from .env file
if (Test-Path ".env") {
    Write-Info "Loading environment variables from .env file"
    Get-Content ".env" | ForEach-Object {
        if ($_ -match '^([^#][^=]+)=(.*)$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($key, $value, "Process")
        }
    }
    
    # Override with environment variables
    if ($env:CLICKHOUSE_HOST) { $ClickHouseHost = $env:CLICKHOUSE_HOST }
    if ($env:CLICKHOUSE_HTTP_PORT) { $ClickHousePort = [int]$env:CLICKHOUSE_HTTP_PORT }
    if ($env:CLICKHOUSE_USER) { $ClickHouseUser = $env:CLICKHOUSE_USER }
    if ($env:CLICKHOUSE_PASSWORD) { $ClickHousePassword = $env:CLICKHOUSE_PASSWORD }
}

Write-Info "Configuration: ClickHouse at ${ClickHouseHost}:${ClickHousePort}, Airflow at $AirflowUrl"

# Run all checks
Test-DockerStatus
Test-AirflowHealth
Test-ClickHouseHealth

# Show summary and exit with appropriate code
$exitCode = Show-Summary
exit $exitCode
