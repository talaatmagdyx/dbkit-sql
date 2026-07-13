.PHONY: help sync unasync lint format type test integration bench all check

help:
	@echo "Targets: sync unasync lint format type test integration bench check"

sync:              ## install dev environment
	uv sync --extra dev

unasync:           ## regenerate src/dbkit/_sync from src/dbkit/_async
	uv run python tools/run_unasync.py

unasync-check:     ## fail if generated sync code is stale
	uv run python tools/run_unasync.py --check

lint:
	uv run ruff check src tests
	uv run ruff format --check src tests

format:
	uv run ruff format src tests
	uv run ruff check --fix src tests

type:
	uv run mypy

test:              ## unit tests only (no database)
	uv run pytest -q -m "not integration"

integration:       ## integration tests (needs PostgreSQL / DBKIT_TEST_DSN)
	uv run pytest -q -m integration

bench:
	uv run python tools/bench_overhead.py

check: unasync-check lint type test  ## everything CI runs (minus integration)

all: check integration
