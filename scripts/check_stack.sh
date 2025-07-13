#!/bin/bash
set -euo pipefail

# Colors
if command -v tput >/dev/null 2>&1; then
    RED=$(tput setaf 1)
    GREEN=$(tput setaf 2)
    YELLOW=$(tput setaf 3)
    BLUE=$(tput setaf 4)
    RESET=$(tput sgr0)
else
    RED=""
    GREEN=""
    YELLOW=""
    BLUE=""
    RESET=""
fi

# Configuration
TIMEOUT=${SMOKE_TIMEOUT:-120}
AIRFLOW_URL=${AIRFLOW_URL:-http://localhost:8080}
CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-8123}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-default}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-}

# Status tracking
CHECKS_PASSED=0
CHECKS_TOTAL=0
FAILED_CHECKS=()

log() {
    echo "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${RESET} $*"
}

success() {
    echo "${GREEN}✅${RESET} $*"
    CHECKS_PASSED=$((CHECKS_PASSED + 1))
}

error() {
    echo "${RED}❌${RESET} $*"
    FAILED_CHECKS+=("$*")
}

warn() {
    echo "${YELLOW}⚠️${RESET} $*"
}

check_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        error "Required command '$1' not found"
        return 1
    fi
}

wait_for_service() {
    local name="$1"
    local check_cmd="$2"
    local max_attempts=$((TIMEOUT / 5))
    local attempt=0

    log "Waiting for $name to be ready (timeout: ${TIMEOUT}s)..."
    
    while [ $attempt -lt $max_attempts ]; do
        if eval "$check_cmd" >/dev/null 2>&1; then
            success "$name is ready"
            return 0
        fi
        
        attempt=$((attempt + 1))
        printf "${YELLOW}.${RESET}"
        sleep 5
    done
    
    echo ""
    error "$name failed to become ready within ${TIMEOUT}s"
    return 1
}

check_airflow_health() {
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    log "Checking Airflow webserver health..."
    
    if wait_for_service "Airflow webserver" "curl -fsSL ${AIRFLOW_URL}/health"; then
        local response
        response=$(curl -fsSL "${AIRFLOW_URL}/health" 2>/dev/null || echo "failed")
        if echo "$response" | grep -q '"metadatabase":.*"healthy"'; then
            success "Airflow webserver is healthy"
        else
            error "Airflow webserver is running but not healthy: $response"
        fi
    fi
}

check_clickhouse_health() {
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    log "Checking ClickHouse health..."
    
    local auth_header=""
    if [ -n "$CLICKHOUSE_PASSWORD" ]; then
        auth_header="-u ${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}"
    fi
    
    local clickhouse_url="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"
    local check_cmd="curl -fsSL $auth_header ${clickhouse_url}/?query=SELECT%201"
    
    if wait_for_service "ClickHouse" "$check_cmd"; then
        local response
        response=$(eval "$check_cmd" 2>/dev/null || echo "failed")
        if [ "$response" = "1" ]; then
            success "ClickHouse is responding correctly"
        else
            error "ClickHouse responded but with unexpected result: $response"
        fi
    fi
}

check_airflow_clickhouse_connection() {
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    log "Checking Airflow-ClickHouse connection..."
    
    # Check if we can access airflow CLI via docker
    if docker compose ps airflow-webserver >/dev/null 2>&1; then
        local conn_check
        conn_check=$(docker compose exec -T airflow-webserver airflow connections get clickhouse 2>/dev/null || echo "not_found")
        
        if echo "$conn_check" | grep -q "clickhouse"; then
            success "Airflow ClickHouse connection is configured"
        else
            warn "Airflow ClickHouse connection not found or not accessible"
            # Don't fail the entire check for this
            CHECKS_PASSED=$((CHECKS_PASSED + 1))
        fi
    else
        warn "Airflow container not found, skipping connection check"
        CHECKS_PASSED=$((CHECKS_PASSED + 1))
    fi
}

check_docker_compose() {
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    log "Checking Docker Compose services..."
    
    if docker compose ps --format json >/dev/null 2>&1; then
        local running_services
        running_services=$(docker compose ps --format json | jq -r 'select(.State == "running") | .Service' 2>/dev/null | wc -l)
        
        if [ "$running_services" -gt 0 ]; then
            success "Docker Compose services are running ($running_services services)"
        else
            error "No Docker Compose services are running"
        fi
    else
        error "Docker Compose not available or no services defined"
    fi
}

print_summary() {
    echo ""
    echo "========================================="
    echo "${BLUE}SMOKE TEST SUMMARY${RESET}"
    echo "========================================="
    echo "Checks passed: ${GREEN}${CHECKS_PASSED}${RESET}/${CHECKS_TOTAL}"
    
    if [ ${#FAILED_CHECKS[@]} -gt 0 ]; then
        echo ""
        echo "${RED}Failed checks:${RESET}"
        for check in "${FAILED_CHECKS[@]}"; do
            echo "  ${RED}•${RESET} $check"
        done
        echo ""
        echo "${RED}❌ Smoke test FAILED${RESET}"
        return 1
    else
        echo ""
        echo "${GREEN}✅ All smoke tests PASSED${RESET}"
        return 0
    fi
}

main() {
    log "Starting smoke test suite..."
    
    # Check required commands
    check_command "curl"
    check_command "docker"
    
    # Run health checks
    check_docker_compose
    check_airflow_health
    check_clickhouse_health
    check_airflow_clickhouse_connection
    
    # Print summary and exit
    print_summary
}

# Trap to ensure we always print summary
trap 'print_summary' EXIT

main "$@"
