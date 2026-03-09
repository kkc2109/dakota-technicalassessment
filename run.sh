#!/usr/bin/env bash

set -euo pipefail

COMPOSE="docker compose"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
ENV_EXAMPLE="$PROJECT_ROOT/.env.example"

# Colour output
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log()  { echo -e "${BLUE}[$(date +'%H:%M:%S')]${NC} $*"; }
ok()   { echo -e "${GREEN}[$(date +'%H:%M:%S')] ✓${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date +'%H:%M:%S')] ⚠${NC} $*"; }
err()  { echo -e "${RED}[$(date +'%H:%M:%S')] ✗${NC} $*"; exit 1; }

wait_for_service() {
    local name=$1 url=$2 max_wait=${3:-120}
    log "Waiting for $name to be healthy..."
    local elapsed=0
    until curl -sf "$url" > /dev/null 2>&1; do
        sleep 3; elapsed=$((elapsed + 3))
        if [ "$elapsed" -ge "$max_wait" ]; then
            err "$name did not become healthy within ${max_wait}s"
        fi
        echo -n "."
    done
    echo ""
    ok "$name is healthy"
}

cmd_setup() {
    log "=== SETUP — Dakota Analytics Pipeline ==="

    # 1. Environment file
    if [ ! -f "$ENV_FILE" ]; then
        cp "$ENV_EXAMPLE" "$ENV_FILE"
        warn ".env created from .env.example — set your EIA_API_KEY before running"
    else
        ok ".env already exists"
    fi

    # 2. Build Docker images
    log "Building Docker images (this may take several minutes on first run)..."
    $COMPOSE --env-file "$ENV_FILE" build --parallel
    ok "Docker images built"

    # 3. Start services
    log "Starting all services..."
    $COMPOSE --env-file "$ENV_FILE" up -d postgres
    log "Waiting for PostgreSQL to initialise..."
    sleep 15
    $COMPOSE --env-file "$ENV_FILE" up -d api dagster-webserver dagster-daemon
    ok "All services started"

    # 4. Health checks
    log "Waiting for PostgreSQL to be healthy..."
    local elapsed=0
    until [ "$(docker inspect --format='{{.State.Health.Status}}' dakota_postgres 2>/dev/null)" = "healthy" ]; do
        sleep 3; elapsed=$((elapsed + 3))
        [ "$elapsed" -ge 60 ] && err "PostgreSQL did not become healthy within 60s"
        echo -n "."
    done
    echo ""; ok "PostgreSQL is healthy"

    wait_for_service "Enrichment API" "http://localhost:8000/health"
    wait_for_service "Dagster UI" "http://localhost:3000/health" 120

    echo ""
    ok "=== SETUP COMPLETE ==="
    echo -e "${GREEN}  Enrichment API:  ${NC}http://localhost:8000/docs"
    echo -e "${GREEN}  Dagster UI:      ${NC}http://localhost:3000"
    echo -e "${GREEN}  PostgreSQL:      ${NC}localhost:5432 / ${POSTGRES_DB:-energy_analytics}"
}

cmd_run() {
    log "=== RUN — Triggering full pipeline ==="

    $COMPOSE --env-file "$ENV_FILE" up -d postgres api dagster-webserver dagster-daemon

    wait_for_service "Enrichment API" "http://localhost:8000/health"
    wait_for_service "Dagster UI" "http://localhost:3000/health" 120

    log "Triggering daily EIA pipeline job via Dagster CLI..."
    $COMPOSE --env-file "$ENV_FILE" exec dagster-webserver \
        dagster job execute -m orchestration.definitions -j daily_eia_pipeline

    ok "Pipeline run complete — EIA ingestion + dbt transformations done"
    log "Run 'make report' to generate reports from the latest data"
    log "Dagster run history: http://localhost:3000"
}

cmd_test() {
    log "=== TEST — Running test suite ==="

    # --- Unit tests (run inside the dagster-webserver container) ---
    log "--- Running unit tests ---"
    $COMPOSE --env-file "$ENV_FILE" exec dagster-webserver \
        bash -c "cd /app && ENRICHMENT_API_URL=http://localhost:8000 PYTHONPATH=/app:/app/api \
            python -m pytest tests/unit/ -v --log-cli-level=INFO --tb=short -m 'not integration' --color=yes"
    ok "Unit tests complete"

    # --- Integration tests ---
    log "--- Running integration tests ---"
    $COMPOSE --env-file "$ENV_FILE" exec dagster-webserver \
        bash -c "cd /app && PYTHONPATH=/app:/app/api \
            python -m pytest tests/integration/ -v --log-cli-level=INFO --tb=short -m 'integration' --color=yes"
    ok "Integration tests complete"

    # --- dbt tests ---
    log "--- Running dbt data quality tests ---"
    $COMPOSE --env-file "$ENV_FILE" exec dagster-webserver \
        bash -c "cd /app/dbt/energy_analytics && dbt build --profiles-dir ."
    ok "dbt tests complete"

    echo ""
    ok "=== ALL TESTS PASSED ==="
}

cmd_report() {
    log "=== REPORT — Generating reports ==="
    $COMPOSE --env-file "$ENV_FILE" exec dagster-webserver \
        dagster job execute -m orchestration.definitions -j report_generation
    ok "Reports generated in reports/output/"
}

cmd_down() {
    log "=== DOWN — Stopping all services (data preserved) ==="
    $COMPOSE --env-file "$ENV_FILE" down
    ok "All services stopped. Data volumes intact. Run 'make setup' to restart."
}

cmd_clean() {
    warn "=== CLEAN — Stopping services and removing volumes ==="
    read -p "This will delete all pipeline data. Are you sure? (y/N) " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || { log "Cancelled"; exit 0; }
    $COMPOSE --env-file "$ENV_FILE" down -v --remove-orphans
    ok "All services stopped and volumes removed"
}


case "${1:-help}" in
    setup)   cmd_setup ;;
    run)     cmd_run ;;
    test)    cmd_test ;;
    report)  cmd_report ;;
    down)    cmd_down ;;
    clean)   cmd_clean ;;
    *)
        echo ""
        echo "Usage: ./run.sh <command>"
        echo ""
        echo "  setup   — First-time setup (copy .env, build images, start services)"
        echo "  run     — Run the full pipeline end-to-end"
        echo "  test    — Run all tests (unit → integration → dbt)"
        echo "  report  — Re-generate reports from existing data"
        echo "  down    — Stop all services (data volumes preserved)"
        echo "  clean   — Stop services and remove all data volumes"
        echo ""
        ;;
esac