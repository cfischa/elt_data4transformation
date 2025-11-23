Write-Host "🚀 Starting Streamlit in Local Development Mode..." -ForegroundColor Cyan

# Set environment variables to connect to Dockerized services from host
$Env:CLICKHOUSE_HOST = "localhost"
$Env:CLICKHOUSE_HTTP_PORT = "8124"
$Env:CLICKHOUSE_DATABASE = "analytics"
$Env:CLICKHOUSE_USER = "admin" 
# Default password from docker-compose; change if you use a custom .env value
if (-not $Env:CLICKHOUSE_PASSWORD) {
    $Env:CLICKHOUSE_PASSWORD = "asjrh25423sfa#+43qw56j"
}

Write-Host "🔗 Connecting to ClickHouse at $($Env:CLICKHOUSE_HOST):$($Env:CLICKHOUSE_HTTP_PORT)" -ForegroundColor Gray

# Run Streamlit using Poetry environment
poetry run streamlit run streamlit_app/Home.py

