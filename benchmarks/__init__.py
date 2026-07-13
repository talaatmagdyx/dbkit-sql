"""dbkit benchmark suite — custom asyncio/monotonic harness against real PostgreSQL.

Run all suites (starts one container, or use --dsn):

    python -m benchmarks
    python -m benchmarks --dsn postgresql+psycopg://localhost/postgres
    python -m benchmarks --only overhead
"""
