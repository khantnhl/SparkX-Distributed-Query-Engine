# Contributing to SparkX

## Where changes belong

| Directory | Responsibility |
|---|---|
| `src/query/` | Expressions, SQL/logical planning, optimization, physical planning, session API |
| `src/runtime/` | Operators, cancellation, memory, metrics, encoded row keys |
| `src/storage/` | Catalog, table providers, storage pruning |
| `src/cluster/` | Distributed planning, protocol, coordinator, workers, Flight transport |
| `src/bin/` | CLI argument handling and process startup |
| `tests/` | Public API integration tests, differential SQL, plan snapshots |
| `benches/` | Reproducible performance comparisons |
| `docs/` | Architecture, SQL support, benchmarks, roadmap |

Keep behavior in library modules so command-line programs remain thin. Public module re-exports in
`src/lib.rs` preserve existing callers while the implementation is grouped by subsystem. Integration
tests remain Cargo-discoverable top-level files; fixtures live in `tests/sql/` and `tests/snapshots/`.

## Reviewing a change

Keep each change focused on one behavior, explain the observable result, and add regression coverage
for semantic or distributed-state changes. Update SQL support and architecture documentation when
contracts change. Protocol changes must update the protocol version and validate both producers and
consumers. Include empty input, NULL values, cancellation, and resource exhaustion where relevant.

Run the same checks as CI before submitting:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
```

Use `cargo bench --locked --bench engine` for performance changes and report the environment with
results. Do not claim performance improvements from correctness tests alone.
