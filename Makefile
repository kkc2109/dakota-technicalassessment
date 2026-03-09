# =============================================================================
# Makefile — Dakota Analytics Pipeline
#
# Delegates to run.sh (Mac/Linux) or run.bat (Windows).
#
# Usage:
#   make setup    — First-time setup: copy .env, build images, start services
#   make run      — Run the full pipeline end-to-end
#   make test     — Run all tests (unit → integration → dbt)
#   make report   — Re-generate reports from existing data
#   make down     — Stop all services (keeps data volumes intact)
#   make clean    — Stop all services and remove volumes
# =============================================================================

.PHONY: setup run test report down clean

setup:
	./run.sh setup

run:
	./run.sh run

test:
	./run.sh test

report:
	./run.sh report

down:
	./run.sh down

clean:
	./run.sh clean
