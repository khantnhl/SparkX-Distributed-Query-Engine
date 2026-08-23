# SparkX Distributed Query Engine

SparkX is a scrappy, inspectable query-engine prototype written in Rust. It owns the full path from SQL text to Arrow record batches: catalog lookup, logical planning, rule-based optimization, physical planning, bounded asynchronous execution, hash aggregation/joining, metrics, and a two-stage in-process distributed aggregate runner with Protobuf plan fragments and a loopback Arrow Flight exchange.

It is deliberately smaller than production systems such as Daft, DataFusion, Spark, or DuckDB. The goal is to make the important engine boundaries real and runnable without hiding them behind a framework.

## Original implementation and provenance

SparkX is an independently designed implementation, not a fork, translation, or code copy of Daft or another query engine. Daft and general query-engine literature informed the high-level separation of planning, optimization, execution, and scheduling. SparkX's Rust plan model, optimizer rules, operator pipelines, local-cluster protocol, metrics, CLI, tests, and documentation were written specifically for this repository. Third-party functionality is consumed only through the dependencies declared in `Cargo.toml`, including Apache Arrow/Flight, Parquet, Tokio/Tonic, and `sqlparser`, under their respective licenses.

## What works

- SQL `SELECT`, aliases, arithmetic/comparison/boolean expressions, `WHERE`, `GROUP BY`, alias-based `HAVING`, `ORDER BY`, and `LIMIT`
- `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`, including distinct aggregate state
- Inner and left equi-hash joins, including multiple keys joined with `AND`
- Arrow-native columnar batches and vectorized expression kernels
- CSV and Parquet scans, Parquet statistics pruning, and in-memory tables
- Rule-based filter and projection pushdown
- Encoded Arrow row keys for native/distributed grouping and hash joins
- Limited-sort physical Top-K for `ORDER BY ... LIMIT`
- Bounded Tokio channels for backpressure between streaming operators
- Concurrent partition scans and a coordinator-dispatched, worker-limited local cluster
- Two-stage partial/final distributed aggregation over a loopback Arrow Flight/gRPC exchange
- Versioned Protobuf physical-plan fragments with worker-side catalog and schema validation
- Versioned, serializable coordinator/worker protocol contracts
- Deterministic coordinator state for worker heartbeats, task leases, retries, and cancellation
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
| `src/plan_codec.rs` | Versioned Protobuf physical-plan fragment codec |
| `src/protocol.rs` | Validated coordinator/worker wire contracts |
| `src/coordinator.rs` | Worker registry, stage scheduler, leases, retries, and cancellation |
| `src/execution.rs` | Async operators, vectorized execution, joins, sorts, aggregates |
| `src/distributed.rs` | Local scheduler, partial aggregation, Flight exchange and merge |
| `src/flight_exchange.rs` | Query-scoped loopback Arrow Flight/gRPC transport |
| `src/catalog.rs` | Catalog plus memory, CSV, and Parquet providers |
| `src/expr.rs` | Expression tree, type inference, Arrow kernels, scalar state |
| `src/row_key.rs` | Shared encoded grouping and join key format |
| `src/memory.rs` | Query memory limits and RAII reservations |
| `src/metrics.rs` | Lock-free query counters and snapshots |
| `benches/engine.rs` | Expression and end-to-end Criterion benchmarks |
| `tests/engine.rs` | SQL, optimizer, I/O, join, and distributed correctness tests |
| `docs/ARCHITECTURE.md` | End-to-end HLD and component workflows |
| `docs/SQL_SUPPORT.md` | Tested SQL features, primitive types, coercions, and distributed eligibility |
| `docs/BENCHMARKS.md` | Benchmark matrix, method, and interpretation |
| `docs/ROADMAP.md` | Concrete path from prototype to serious engine |

## Honest prototype boundaries

The “distributed” implementation still runs inside one process, but the local cluster now registers logical workers, obtains coordinator assignments, decodes each versioned Protobuf stage plan through the worker catalog, and reports task outcomes through the protocol state machine. Partial batches cross a real Arrow Flight/gRPC connection bound to an ephemeral loopback port. This exercises the control and data boundaries without claiming separate machines: no remote control-plane service or worker RPC server exists yet. Remote object storage, retry execution, and durable shuffle are also absent. Blocking operators enforce a query memory limit but still fail rather than spill to disk. Optimization is rule based, not cost based. SQL coverage is intentionally narrow.

Those boundaries are explicit seams, not hidden claims. See [the roadmap](docs/ROADMAP.md) for the order in which to replace them.

## Design principles

1. Arrow `RecordBatch` is the data-plane ABI.
2. Plans are immutable and explainable.
3. Streaming operators use bounded channels; pipeline breakers say so.
4. Distribution changes scheduling and exchange, not expression semantics.
5. Every performance claim should have a reproducible benchmark.

Licensed under Apache-2.0.
