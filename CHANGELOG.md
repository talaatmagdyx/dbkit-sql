# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added — Phase 1 (Core Runtime)
- Configuration model with dict/env/YAML loaders, `${VAR}` expansion, startup validation,
  connection-budget calculation, and secret-free serialization.
- `Query` object and `sql()` wrapper (the only accepted raw-string path).
- `DatabaseTarget` and named-database routing (primary resolution).
- Typed results (`ExecutionResult`) with `map_to` mappers and cardinality enforcement.
- Normalized error hierarchy with SQLSTATE-first classification.
- Async and sync `AsyncDatabase`/`Database` facades sharing one API.
- Engine registry (one engine per target) and instrumented connection pooling with
  leak detection and long-hold warnings.
- Explicit transactions with savepoints, commit-unknown detection, and cancellation cleanup.
- Health checks (liveness/readiness), graceful startup/shutdown.
- Structured logging and a metrics protocol (Prometheus adapter behind an extra).
