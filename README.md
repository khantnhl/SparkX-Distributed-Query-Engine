# SparkX Distributed Query Engine

SparkX is a scrappy, inspectable query-engine prototype written in Rust. It owns the full path from SQL text to Arrow record batches: catalog lookup, logical planning, rule-based optimization, physical planning, bounded asynchronous execution, hash aggregation/joining, metrics, and a two-stage in-process distributed aggregate runner.

It is deliberately smaller than production systems such as Daft, DataFusion, Spark, or DuckDB. The goal is to make the important engine boundaries real and runnable without hiding them behind a framework.

## Original implementation and provenance

SparkX is an independently designed implementation, not a fork, translation, or code copy of Daft or another query engine. Daft and general query-engine literature informed the high-level separation of planning, optimization, execution, and scheduling. SparkX's Rust plan model, optimizer rules, operator pipelines, local-cluster protocol, metrics, CLI, tests, and documentation were written specifically for this repository. Third-party functionality is consumed only through the dependencies declared in `Cargo.toml`, including Apache Arrow, Parquet, Tokio, and `sqlparser`, under their respective licenses.

## What works

- SQL `SELECT`, aliases, arithmetic/comparison/boolean expressions, `WHERE`, `GROUP BY`, alias-based `HAVING`, `ORDER BY`, and `LIMIT`
- `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`, including distinct aggregate state
- Inner and left equi-hash joins, including multiple keys joined with `AND`
- Arrow-native columnar batches and vectorized expression kernels
- CSV and Parquet scans, Parquet statistics pruning, and in-memory tables
- Rule-based filter and projection pushdown
- Limited-sort physical Top-K for `ORDER BY ... LIMIT`
- Bounded Tokio channels for backpressure between streaming operators
- Concurrent partition scans and a worker-limited local cluster
- Two-stage partial/final distributed aggregation with an Arrow exchange boundary
- Logical, optimized, and physical plan explanations
- Stable per-operator IDs, output/timing/pruning metrics, and cooperative query cancellation
- Query-scoped memory reservations with a configurable limit and peak-memory metric
- Integration, golden-plan, and DuckDB differential tests plus Criterion benchmarks

## Quick start

Prerequisites: Rust 1.88 or newer and a working native linker/SDK.

```bash
cargo run --release -- \
  --input ./sales.parquet \
  --table sales \
  --sql "SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue FROM sales GROUP BY region" \
  --distributed --workers 4 --show-plan --metrics
```

Use CSV by changing the filename or passing `--format csv`. Print plans without running a query:

```bash
cargo run -- --input ./sales.csv --table sales \
  --sql "SELECT region, amount FROM sales WHERE amount > 100" --explain
```

Run the embedded-table example:

```bash
cargo run --example programmatic
```

## Developer workflow

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
cargo bench --locked --bench engine
```

On PowerShell, `./scripts/benchmark.ps1` runs the release tests and Criterion suite and records machine metadata next to Criterion's reports.

## Repository map

| Path | Responsibility |
|---|---|
| `src/session.rs` | SQL frontend, session configuration, query lifecycle |
| `src/logical.rs` | Typed logical plan and plan display |
| `src/optimizer.rs` | Rule-based filter/projection pushdown |
| `src/planner.rs` | Logical-to-physical lowering |
| `src/execution.rs` | Async operators, vectorized execution, joins, sorts, aggregates |
| `src/distributed.rs` | Local scheduler, partial aggregation, Arrow exchange and merge |
| `src/catalog.rs` | Catalog plus memory, CSV, and Parquet providers |
| `src/expr.rs` | Expression tree, type inference, Arrow kernels, scalar state |
| `src/memory.rs` | Query memory limits and RAII reservations |
| `src/metrics.rs` | Lock-free query counters and snapshots |
| `benches/engine.rs` | Expression and end-to-end Criterion benchmarks |
| `tests/engine.rs` | SQL, optimizer, I/O, join, and distributed correctness tests |
| `docs/ARCHITECTURE.md` | End-to-end HLD and component workflows |
| `docs/SQL_SUPPORT.md` | Tested SQL features, primitive types, coercions, and distributed eligibility |
| `docs/BENCHMARKS.md` | Benchmark matrix, method, and interpretation |
| `docs/ROADMAP.md` | Concrete path from prototype to serious engine |

## Honest prototype boundaries

The “distributed” implementation runs inside one process. It exercises scheduling, partition tasks, partial aggregation, exchange accounting, and final aggregation, but it does not yet have RPC, remote object storage, retries, heartbeats, or durable shuffle. Blocking operators enforce a query memory limit but still fail rather than spill to disk. Optimization is rule based, not cost based. SQL coverage is intentionally narrow.

Those boundaries are explicit seams, not hidden claims. See [the roadmap](docs/ROADMAP.md) for the order in which to replace them.

## Design principles

1. Arrow `RecordBatch` is the data-plane ABI.
2. Plans are immutable and explainable.
3. Streaming operators use bounded channels; pipeline breakers say so.
4. Distribution changes scheduling and exchange, not expression semantics.
5. Every performance claim should have a reproducible benchmark.

Licensed under Apache-2.0.
