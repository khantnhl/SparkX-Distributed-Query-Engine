# SparkX build tracker

Last updated: 2026-09-08

Build target: execute remote inner and left equi-joins correctly across workers, then make shuffle
output survive failures. This is the execution checklist for the distributed work in the
[roadmap](ROADMAP.md), not a claim that the entire engine is production-ready.

## Progress board

| ID | Milestone | Status | Depends on | Completion evidence |
|---|---|---|---|---|
| B0 | Hash-partitioned aggregate exchange and source organization | Done | — | Commit `34ef3cf`; 75 tests, Clippy, and formatting passed during implementation |
| B1 | Remote join plan and exchange contracts | Done | B0 | `tests/remote_join_plan.rs`: 2 passed |
| B2 | Remote inner hash join | Next | B1 | — |
| B3 | Left joins and SQL edge cases | Pending | B2 | — |
| B4 | Resource limits and failure handling | Pending | B3 | — |
| B5 | Reproducible join demo and performance baseline | Pending | B4 | — |
| B6 | Persistent shuffle storage | Pending | B5 | — |
| B7 | Worker-loss recovery | Pending | B6 | — |

Continue with **B2**. Each milestone is a reviewable change; split it into smaller commits when needed.
Statuses are Next, In progress, Blocked, Pending, and Done. Only mark Done after the exit criterion
passes and its commit/test evidence is recorded above. The checked baseline describes prior local
verification; it does not imply that checks have been rerun today.

## B1 — Define remote join plans

Primary files: `src/cluster/distributed.rs`, `src/cluster/protocol.rs`, `src/query/session.rs`.

- [x] Define the initial supported shape: one equi-join over partition-local scan/filter/projection inputs, with optional projection/filter above the join.
- [x] Generate separate left and right producer stages and a dependent join stage.
- [x] Resolve ordered join keys to exchange columns, materializing key expressions if necessary; explicitly reject unsupported expressions before submission.
- [x] Require matching key types, key order, hash encoding, and destination count on both sides.
- [x] Build distinct dependency scans with the correct schemas and preserve output aliases.
- [x] Add plan/codec tests for both inputs, partition counts, invalid contracts, and unsupported query shapes.

Exit: a deterministic three-stage graph round-trips through the codec and rejects incompatible
inputs before any stage is submitted. Exclude chained joins, broadcast joins, global sorting, and
aggregates above joins from this first slice; document their rejection.

## B2 — Execute remote inner joins

Primary files: `src/cluster/worker.rs`, `src/cluster/coordinator.rs`, `tests/remote.rs`.

- [ ] Publish hash-routed blocks from both producer stages.
- [ ] Wait for both dependencies and assign only matching destination blocks to each join task.
- [ ] Materialize the two dependencies separately and execute the existing native hash join.
- [ ] Collect all join outputs and clean up both inputs after downstream execution completes.
- [ ] Add remote/native parity tests using multiple workers and unequal input partition counts.
- [ ] Verify duplicate keys preserve many-to-many SQL row multiplicity.

Exit: a remote inner join over two partitioned tables matches native results as an unordered
multiset, with no missing or duplicate output caused by task routing.

## B3 — Cover left joins and SQL semantics

Primary files: `src/runtime/execution.rs`, `tests/remote.rs`, `tests/sql_differential.rs`.

- [ ] Support left equi-joins and preserve unmatched left rows exactly once.
- [ ] Test compound keys and NULL keys; NULL join keys must not match each other.
- [ ] Test empty left input, empty right input, empty destination partitions, and no matching keys.
- [ ] Test aliases, projections, filters, and duplicate keys against native execution and DuckDB.
- [ ] Keep unsupported join types and query compositions rejected before submission.

Exit: the supported remote join corpus agrees with native execution and DuckDB, including row
multiplicity and NULL placement. Queries without ORDER BY must not depend on output ordering.

## B4 — Bound resources and handle failures

Primary files: `src/cluster/hash_exchange.rs`, `src/cluster/worker.rs`, `src/cluster/remote.rs`, `tests/memory.rs`.

- [ ] Account for both materialized inputs, routing buffers, join state, and retained output.
- [ ] Test hot-key skew and small memory limits; return a typed error and release reservations.
- [ ] Propagate cancellation and deadlines through dependency fetch, join execution, and output upload.
- [ ] Test missing/corrupt blocks, failed uploads, rejected commits, and stale task attempts.
- [ ] Ensure downstream failure cancels outstanding query work and cleans up known blocks where reachable.

Exit: injected failures terminate within the configured deadline, release task resources, and never
report partial results as a successful query. Record unreachable-worker cleanup limitations explicitly.

## B5 — Publish a reproducible build

Primary files: `README.md`, `docs/SQL_SUPPORT.md`, `docs/ARCHITECTURE.md`, `docs/BENCHMARKS.md`, `benches/`.

- [ ] Document a coordinator/two-worker join demo with both tables registered on the client and workers.
- [ ] Update the SQL support matrix, stage diagram, and current limitations.
- [ ] Benchmark native versus remote joins with uniform and skewed keys; record machine, data size, partition count, and commands.
- [ ] Record elapsed time, rows, shuffle volume, and available memory metrics without claiming unavailable cross-worker metrics.
- [ ] Run formatting, Clippy, the complete test suite, and CI on supported platforms.

Exit: another contributor can reproduce the join results and benchmark baseline using the checked-in
instructions. A speedup is not required; correctness and honest measurements are.

## B6 — Persist shuffle output

- [ ] Specify a block-store interface and choose the first persistent backend with a short architecture decision.
- [ ] Define atomic publication, checksums, attempt ownership, retention, and deletion semantics.
- [ ] Implement the backend behind the existing transport/storage boundary.
- [ ] Test storage exhaustion, partial writes, restart behavior, and orphan cleanup.
- [ ] Document the failure domain: worker-local disk alone does not survive loss of the worker's host.

Exit: committed blocks survive the backend's documented restart scenario; incomplete blocks never
appear as successful outputs. Prove durability with restart tests rather than in-memory tests.

## B7 — Recover from worker loss

- [ ] Define when to reuse a committed block and when to recompute its producer stage.
- [ ] Invalidate unavailable dependency manifests and reschedule affected work.
- [ ] Fence stale attempts and prevent retries from publishing duplicate logical output.
- [ ] Kill producers and consumers during a multi-worker query and verify recovery or a bounded terminal error.
- [ ] Add a small supported decision-support query corpus for repeatable failure drills.

Exit: recoverable worker failures produce the same result as a failure-free run within the retry
budget; unrecoverable failures terminate clearly. Coordinator restart recovery remains separate work.

## Updating this tracker

For each work session, set one milestone to In progress, check completed items, and add a short entry
below. Include blockers immediately. Update the progress board when a milestone closes and link its
commit or PR plus verification evidence. Use [CONTRIBUTING.md](../CONTRIBUTING.md) for repository checks.

| Date | Milestone | Update | Evidence / blocker |
|---|---|---|---|
| 2026-09-08 | B0 | Existing implementation recorded as baseline | `34ef3cf`; prior local verification reported above |
| 2026-09-08 | B1–B7 | Build plan created; implementation has not started | B1 is next |
