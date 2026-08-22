# Benchmark plan

SparkX ships benchmarks as code, not marketing numbers. `benches/engine.rs` uses deterministic synthetic data and Criterion so regressions get distributions, confidence intervals, outlier reporting, and HTML plots.

The initial fixture contains 262,144 sales rows in eight Arrow partitions plus a 4,096-row customer dimension. This is small enough for quick iteration while still exercising multiple batches and worker tasks.

## Included benchmark matrix

| Group | Case | What it isolates | Primary metric |
|---|---|---|---|
| `vectorized_expression` | `amount_gt_50` | Arrow expression dispatch and comparison | rows/s |
| `native_queries` | `scan_filter_project` | scan channels, predicates, arithmetic projection | rows/s, allocations |
| `native_queries` | `hash_aggregate` | grouped COUNT/SUM/AVG state updates | rows/s, peak RSS |
| `native_queries` | `full_sort` | global ordering with all rows returned | rows/s, peak RSS |
| `native_queries` | `top_k` | limited ordering with 100 rows returned | rows/s, peak RSS |
| `native_queries` | `hash_join` | dimension build and fact probe | rows/s, peak RSS |
| `local_cluster` | `two_stage_hash_aggregate` | scheduling, partial states, exchange, final merge | rows/s, shuffle rows |

Run it:

```bash
cargo bench --bench engine
```

or on PowerShell:

```powershell
./scripts/benchmark.ps1
```

Criterion writes its report below `target/criterion/report/index.html`. The PowerShell wrapper also records the Git revision, Rust version, CPU, logical core count, and memory in `benchmark-results/environment.txt`.

## Measurement protocol

For numbers worth publishing:

1. Build and test the exact commit first.
2. Use a release benchmark profile on an otherwise idle machine.
3. Pin the power plan and record CPU, RAM, OS, Rust version, and commit hash.
4. Warm filesystem/object-store caches separately from cold-cache measurements.
5. Run at least 20 Criterion samples and keep raw Criterion artifacts.
6. Change one engine variable at a time.
7. Report median/mean plus variance, not the fastest iteration.
8. Validate result row counts and checksums so a “fast” regression cannot skip work.

No benchmark results are checked in yet because this repository has not been executed on a validated benchmark host. The suite is ready for the first baseline; do not fill this document with guessed numbers.

## Scaling experiments to add

### Batch-size sweep

Run 1K, 4K, 8K, 32K, and 64K rows per batch. Small batches amplify scheduling and virtual-dispatch overhead; very large batches increase cache misses and latency. Make 8K the default only until data says otherwise.

### Channel-capacity sweep

Run capacity 1, 2, 4, 8, and 16. Record throughput and peak memory. Capacity should only rise when it hides a measurable producer/consumer imbalance.

### Worker scaling

Run 1, 2, 4, 8, and physical-core-count workers. Report speedup and efficiency:

```text
speedup(N) = time(1) / time(N)
efficiency(N) = speedup(N) / N
```

The current aggregate may become merge-bound when groups are highly cardinal.

### Selectivity and cardinality

Vary filter selectivity across 0%, 1%, 10%, 50%, and 100%. Vary aggregate groups from 1 to input-row cardinality. Vary join match rate, build-side size, and key skew. These expose radically different bottlenecks from the default uniform fixture.

### Data formats

Compare in-memory, CSV, uncompressed Parquet, and ZSTD/Snappy Parquet using identical rows. Separate decode time from storage time and test projection widths of 1, 5, 20, and all columns.

### Scale classes

- Micro: 256K rows for pull-request feedback.
- Local: 10M–100M rows to expose allocator and cache behavior.
- Spill: 2–4× available memory once spilling exists.
- Cluster: 10GB, 100GB, and 1TB using a standard decision-support dataset.

## Comparative suites

After semantic coverage is sufficient, add:

- ClickBench for analytical filters/aggregations.
- TPC-H queries implemented by the supported SQL subset, with scale factors 1, 10, and 100.
- Parquet scan microbenchmarks against DataFusion and DuckDB using identical files and projection/filter semantics.
- Scheduler-only synthetic tasks to separate data-plane work from orchestration overhead.

Comparisons must pin versions, compression, threads, cache state, output materialization, and correctness checks.

## Performance improvement loop

1. Baseline and save the Criterion artifact.
2. Profile CPU (`samply`, Windows Performance Recorder, or `perf`) and allocations.
3. Identify the hottest operator rather than optimizing the query label.
4. Add a focused microbenchmark that reproduces it.
5. Implement one change and compare confidence intervals.
6. Run correctness tests and the whole suite to catch shifted costs.
7. Keep improvements only when the gain is repeatable and memory/latency do not regress unexpectedly.

The physical Top-K path now gives `ORDER BY ... LIMIT` its own benchmark next to a full sort. The next high-likelihood wins are partition pruning and Parquet predicate pushdown, avoiding scalar materialization in hash keys, a radix/Swiss-table aggregate implementation, streaming output from joins, memory pooling, and code-generated/fused expression pipelines.
