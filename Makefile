.PHONY: help sync unasync lint format type test integration property security chaos bench soak all check

help:
	@echo "Targets: sync unasync lint format type test integration property security chaos bench soak check"

sync:              ## install dev environment
	uv sync --extra dev

unasync:           ## regenerate src/dbkit/_sync from src/dbkit/_async
	uv run python tools/run_unasync.py

unasync-check:     ## fail if generated sync code is stale
	uv run python tools/run_unasync.py --check

lint:              ## ruff lint (src + tests + benchmarks) and format check
	uv run ruff check src tests benchmarks
	uv run ruff format --check src tests

format:
	uv run ruff format src tests benchmarks
	uv run ruff check --fix src tests benchmarks

type:
	uv run mypy

test:              ## unit + property + security tests (no database)
	uv run pytest -q -m "not integration"

integration:       ## all integration tests (needs PostgreSQL / DBKIT_TEST_DSN)
	uv run pytest -q -m integration

property:          ## hypothesis property tests
	uv run pytest -q tests/property

security:          ## security tests (redaction + injection)
	uv run pytest -q tests/security

chaos:             ## resilience / chaos suite (needs PostgreSQL; restart needs Docker)
	uv run pytest -q -m integration tests/integration/test_resilience_scenarios.py

bench:             ## run the benchmark suite (needs PostgreSQL / DBKIT_BENCH_DSN or Docker)
	uv run python -m benchmarks

soak:              ## short soak with fault injection (override DURATION/KILL_EVERY)
	uv run python -m benchmarks.soak --duration $(or $(DURATION),120) --kill-every $(or $(KILL_EVERY),30)

check: unasync-check lint type test  ## everything CI runs (minus integration)

all: check integration
