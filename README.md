# SparkX Distributed Query Engine

SparkX is an Arrow-native SQL query engine written in Rust. It implements the path from SQL text to
columnar results: parsing, logical planning, optimization, physical planning, execution, scheduling,
and metrics.

The project is built for learning and inspection. Its distributed components are real and runnable,
while incomplete production features are rejected or documented instead of being implied.

## How it works

```text
SQL
  → Logical plan       what the query means
  → Optimized plan     equivalent work with avoidable cost removed
  → Physical plan      concrete algorithms and operators
  → Execution          Arrow RecordBatch streams
  → Result             columnar batches and metrics
```

A table is divided into partitions. The coordinator turns each stage into partition tasks, leases
those tasks to workers, and records their status. Workers decode physical-plan fragments, execute
their assigned partitions, and publish Arrow output blocks for the client to retrieve and verify.

### Core terms

| Term | Meaning |
|---|---|
| `RecordBatch` | A schema plus equally sized Arrow column arrays for a chunk of rows |
| Logical plan | An immutable tree describing operations such as scan, filter, and aggregate |
| Physical plan | The executable operator tree, including hash aggregate, hash join, and Top-K |
| Partition | An independently processable portion of a table |
| Task | One attempt to execute a partition of a stage |
| Stage | A group of tasks that share one plan fragment |
| Worker | A process that executes leased tasks |
| Coordinator | The scheduler that tracks workers, leases, attempts, and stage state |
| Control plane | Messages for registration, scheduling, status, and cancellation |
| Data plane | Arrow batches transferred between workers and clients |

## Current capabilities

- SQL projections, aliases, filters, grouping, `HAVING`, ordering, and limits
- `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`, including distinct aggregate state
- Inner and left equi-hash joins with single or compound keys
- SQL `NULL` behavior, primitive casts, arithmetic, comparisons, and Boolean expressions
- Memory, CSV, and Parquet tables with projection pushdown
- Parquet row-group pruning from filter predicates and column statistics
- Arrow columnar execution with bounded Tokio channels and backpressure
- Query-scoped memory reservations for blocking operators
- Native hash aggregation, hash joins, sorting, and limited-sort Top-K
- Two-stage in-process distributed aggregation with partial and final merging
- Versioned Protobuf physical-plan fragments with worker-side schema validation
- Coordinator-managed workers, slots, heartbeats, leases, attempts, retries, and cancellation
- Standalone coordinator and worker processes connected through Arrow Flight/gRPC
- Bounded worker-hosted output blocks with tickets, ownership, checksums, and deletion
- Logical, optimized, and physical plan explanations
- Query and per-operator metrics
- Integration, snapshot, differential, memory, protocol, and distributed-runtime tests

See [SQL support](docs/SQL_SUPPORT.md) for the precise language and type boundaries.

## Execution modes

| Mode | Command option | What runs |
|---|---|---|
| Native | default | One process executes the physical plan |
| Local distributed | `--distributed --workers N` | One process runs logical workers and a two-stage aggregate path |
| Remote | `--remote-coordinator URL` | Separate client, coordinator, and worker processes execute one partition-local stage |

Remote SQL currently supports `Scan`, `Filter`, and `Projection`. Aggregates, joins, sorts, and
limits require global merging or repartitioning, so remote mode rejects those plans before submission.

## Quick start

### Requirements

- Rust 1.88 or newer
- A working platform linker and native SDK
- A CSV or Parquet input file

### Run a query

```bash
cargo run --release -- \
  --input ./sales.parquet \
  --table sales \
  --sql "SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue FROM sales GROUP BY region" \
  --show-plan \
  --metrics
```

SparkX infers CSV or Parquet from the filename. Use `--format csv` or `--format parquet` to select it
explicitly.

### Explain without executing

```bash
cargo run -- \
  --input ./sales.csv \
  --table sales \
  --sql "SELECT region, amount FROM sales WHERE amount > 100" \
  --explain
```

### Run the embedded example

```bash
cargo run --example programmatic
```

## Local distributed execution

```bash
cargo run --release -- \
  --input ./sales.parquet \
  --table sales \
  --sql "SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue FROM sales GROUP BY region" \
  --distributed \
  --workers 4 \
  --show-plan \
  --metrics
```

This mode uses an in-process coordinator and logical workers. It is useful for understanding stage
planning and partial/final aggregation without deploying separate services. CSV exposes one scan
partition, while each Parquet row group becomes a partition.

## Remote execution

The planning client and every worker must register the same table name with a compatible schema.

Start the coordinator:

```bash
cargo run --bin sparkx-coordinator -- --bind 127.0.0.1:50051
```

Start a worker in another terminal:

```bash
cargo run --bin sparkx-worker -- \
  --coordinator http://127.0.0.1:50051 \
  --worker-id worker-1 \
  --table sales=./sales.parquet

# Terminal 3
cargo run --bin sparkx -- \
  --input ./sales.parquet \
  --table sales \
  --sql "SELECT region, amount FROM sales WHERE amount > 10" \
  --remote-coordinator http://127.0.0.1:50051 \
  --metrics
```

Submit a partition-local query from a third terminal:

```bash
cargo run --bin sparkx -- \
  --input ./sales.parquet \
  --table sales \
  --sql "SELECT region, amount FROM sales WHERE amount > 10" \
  --remote-coordinator http://127.0.0.1:50051 \
  --metrics
```

By default, the worker serves output on an ephemeral loopback port. For another machine to retrieve
the output, bind a reachable interface and advertise the worker's DNS name or IP:

```bash
cargo run --bin sparkx-worker -- \
  --coordinator http://COORDINATOR_HOST:50051 \
  --worker-id worker-1 \
  --table sales=./sales.parquet \
  --data-bind 0.0.0.0:50052 \
  --data-advertised-host WORKER_HOST
```

Worker output is retained in bounded memory until the client deletes it or the worker exits. Configure
the limit with `--data-storage-bytes`.

## Repository structure

| Area | Main files |
|---|---|
| SQL and query lifecycle | `src/session.rs`, `src/expr.rs` |
| Logical planning and optimization | `src/logical.rs`, `src/optimizer.rs` |
| Physical planning and execution | `src/planner.rs`, `src/execution.rs` |
| Storage and memory | `src/catalog.rs`, `src/memory.rs` |
| Local distributed execution | `src/distributed.rs`, `src/flight_exchange.rs` |
| Distributed contracts and scheduling | `src/protocol.rs`, `src/coordinator.rs`, `src/plan_codec.rs` |
| Remote transport and execution | `src/control_plane.rs`, `src/data_plane.rs`, `src/remote.rs`, `src/worker.rs` |
| Command-line programs | `src/bin/sparkx.rs`, `src/bin/sparkx-coordinator.rs`, `src/bin/sparkx-worker.rs` |
| Verification | `tests/`, `benches/`, `scripts/` |

## Verification

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
cargo bench --locked --bench engine
```

On PowerShell, `./scripts/benchmark.ps1` runs the release tests and Criterion suite and records machine
metadata alongside the reports.

## Current limitations

- Remote execution supports only partition-local scans, filters, and projections.
- Remote global aggregation, joins, sorting, and limits do not yet have exchange and merge stages.
- Worker output is memory-only and is lost when the worker exits.
- The coordinator does not persist state or recover after restart.
- Authentication, authorization, and TLS are not implemented.
- Blocking operators return a memory-limit error instead of spilling to disk.
- Optimization is rule-based; there is no cardinality estimator or cost model.
- Remote worker metrics are not fully aggregated into the client result.
- SQL and Arrow type coverage remain intentionally limited.

These boundaries are part of the implementation status, not hidden fallback behavior. See the
[architecture guide](docs/ARCHITECTURE.md) and [roadmap](docs/ROADMAP.md) for details.

## Design principles

1. Arrow `RecordBatch` is the data-plane contract.
2. Plans are immutable, typed, and explainable.
3. Streaming boundaries are bounded and observable.
4. Distributed placement must not change query semantics.
5. Unsupported behavior fails explicitly.
6. Performance claims require reproducible benchmarks.

## License

Apache-2.0
