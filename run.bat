@echo off
REM =============================================================================
REM run.bat — Dakota Analytics Pipeline Runner (Windows)
REM
REM Usage:
REM   run.bat setup    — First-time setup: copy .env, build images, start services
REM   run.bat run      — Run the full pipeline end-to-end
REM   run.bat test     — Run all tests (unit → integration → dbt)
REM   run.bat report   — Re-generate reports from existing data
REM   run.bat down     — Stop all services (keeps data volumes intact)
REM   run.bat clean    — Stop all services and remove volumes
REM
REM Requirements: Docker Desktop for Windows
REM =============================================================================

setlocal EnableDelayedExpansion

set COMPOSE=docker compose
set PROJECT_ROOT=%~dp0
set ENV_FILE=%PROJECT_ROOT%.env
set ENV_EXAMPLE=%PROJECT_ROOT%.env.example

REM ---------------------------------------------------------------------------
REM Parse command
REM ---------------------------------------------------------------------------
if "%1"==""        goto :show_help
if "%1"=="setup"   goto :cmd_setup
if "%1"=="run"     goto :cmd_run
if "%1"=="test"    goto :cmd_test
if "%1"=="report"  goto :cmd_report
if "%1"=="down"    goto :cmd_down
if "%1"=="clean"   goto :cmd_clean
goto :show_help

REM ---------------------------------------------------------------------------
:cmd_setup
echo [%time%] === SETUP — Dakota Analytics Pipeline ===

REM 1. Environment file
if not exist "%ENV_FILE%" (
    copy "%ENV_EXAMPLE%" "%ENV_FILE%" > nul
    echo [%time%] WARNING: .env created from .env.example — set your EIA_API_KEY before running
) else (
    echo [%time%] .env already exists
)

REM 2. Build Docker images
echo [%time%] Building Docker images (may take several minutes on first run)...
%COMPOSE% --env-file "%ENV_FILE%" build --parallel
if errorlevel 1 ( echo [%time%] ERROR: Docker build failed & exit /b 1 )
echo [%time%] Docker images built

REM 3. Start services
echo [%time%] Starting PostgreSQL first...
%COMPOSE% --env-file "%ENV_FILE%" up -d postgres
echo [%time%] Waiting 20s for PostgreSQL to initialise...
timeout /t 20 /nobreak > nul

echo [%time%] Starting remaining services...
%COMPOSE% --env-file "%ENV_FILE%" up -d api dagster-webserver dagster-daemon
if errorlevel 1 ( echo [%time%] ERROR: Services failed to start & exit /b 1 )

echo [%time%] Waiting 30s for services to become healthy...
timeout /t 30 /nobreak > nul

echo.
echo [%time%] === SETUP COMPLETE ===
echo   Enrichment API:  http://localhost:8000/docs
echo   Dagster UI:      http://localhost:3000
echo   PostgreSQL:      localhost:5432
goto :eof

REM ---------------------------------------------------------------------------
:cmd_run
echo [%time%] === RUN — Triggering full pipeline ===

%COMPOSE% --env-file "%ENV_FILE%" up -d
echo [%time%] Waiting 30s for services to be ready...
timeout /t 30 /nobreak > nul

echo [%time%] Triggering daily EIA pipeline job via Dagster CLI...
%COMPOSE% --env-file "%ENV_FILE%" exec dagster-webserver ^
    dagster job execute -m orchestration.definitions -j daily_eia_pipeline
if errorlevel 1 ( echo [%time%] ERROR: Pipeline execution failed & exit /b 1 )

echo [%time%] Pipeline run complete
echo [%time%] Reports available in: reports\output\
echo [%time%] Dagster run history:  http://localhost:3000
goto :eof

REM ---------------------------------------------------------------------------
:cmd_test
echo [%time%] === TEST — Running test suite ===

REM Install test dependencies inside the container (all deps already present)
echo [%time%] Installing test dependencies inside container...
%COMPOSE% --env-file "%ENV_FILE%" exec dagster-webserver ^
    bash -c "uv pip install --system --quiet pytest pytest-asyncio httpx respx fastapi sqlalchemy psycopg2-binary pydantic pydantic-settings python-dotenv tenacity pytest-mock 2>&1"
if errorlevel 1 ( echo [%time%] ERROR: Failed to install test dependencies & exit /b 1 )

REM Unit tests (run inside container — no live services needed)
echo [%time%] --- Running unit tests ---
%COMPOSE% --env-file "%ENV_FILE%" exec dagster-webserver ^
    bash -c "cd /app && ENRICHMENT_API_URL=http://localhost:8000 PYTHONPATH=/app:/app/api python -m pytest tests/unit/ -v --log-cli-level=INFO --tb=short -m 'not integration' --color=yes 2>&1"
if errorlevel 1 ( echo [%time%] Unit tests FAILED & exit /b 1 )
echo [%time%] Unit tests PASSED

REM Integration tests (run inside container — postgres is reachable via service name)
echo [%time%] --- Running integration tests ---
%COMPOSE% --env-file "%ENV_FILE%" exec dagster-webserver ^
    bash -c "cd /app && POSTGRES_HOST=postgres python -m pytest tests/integration/ -v --log-cli-level=INFO --tb=short -m integration --color=yes 2>&1"
if errorlevel 1 ( echo [%time%] Integration tests FAILED & exit /b 1 )
echo [%time%] Integration tests PASSED

REM dbt tests
echo [%time%] --- Running dbt data quality tests ---
%COMPOSE% --env-file "%ENV_FILE%" exec dagster-webserver ^
    bash -c "cd /app/dbt/energy_analytics && dbt build --profiles-dir . 2>&1"
if errorlevel 1 ( echo [%time%] dbt tests FAILED & exit /b 1 )
echo [%time%] dbt tests PASSED

echo.
echo [%time%] === ALL TESTS PASSED ===
goto :eof

REM ---------------------------------------------------------------------------
:cmd_report
echo [%time%] === REPORT — Generating reports ===
%COMPOSE% --env-file "%ENV_FILE%" exec dagster-webserver ^
    dagster job execute -m orchestration.definitions -j report_generation
if errorlevel 1 ( echo [%time%] ERROR: Report generation failed & exit /b 1 )
echo [%time%] Reports generated in reports\output\
goto :eof

REM ---------------------------------------------------------------------------
:cmd_down
echo [%time%] === DOWN — Stopping all services (data preserved) ===
%COMPOSE% --env-file "%ENV_FILE%" down
echo [%time%] All services stopped. Data volumes intact. Run 'run.bat setup' to restart.
goto :eof

REM ---------------------------------------------------------------------------
:cmd_clean
echo [%time%] === CLEAN — Stopping services and removing volumes ===
set /p confirm="This will delete all pipeline data. Are you sure? (y/N): "
if /i not "%confirm%"=="y" ( echo [%time%] Cancelled & goto :eof )
%COMPOSE% --env-file "%ENV_FILE%" down -v --remove-orphans
echo [%time%] All services stopped and volumes removed
goto :eof

REM ---------------------------------------------------------------------------
:show_help
echo.
echo Usage: run.bat ^<command^>
echo.
echo   setup   — First-time setup (copy .env, build images, start services)
echo   run     — Run the full pipeline end-to-end
echo   test    — Run all tests (unit -^> integration -^> dbt)
echo   report  — Re-generate reports from existing data
echo   down    — Stop all services (data volumes preserved)
echo   clean   — Stop services and remove all data volumes
echo.
goto :eof
