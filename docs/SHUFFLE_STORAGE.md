# Shuffle storage decision

Status: implemented, 2026-09-08. The first persistent backend is a dedicated worker-local directory.
The Flight API stays the same; `BlockStorage` separates memory and disk implementations. Filesystem
work runs on blocking tasks so disk access does not block the asynchronous network executor.

## Starting a worker with persistent output

```sh
cargo run --locked --bin sparkx-worker -- --worker-id worker-1 \
  --table orders=examples/data/orders.csv --table customers=examples/data/customers.csv \
  --data-bind 127.0.0.1:50052 --data-directory ./shuffle/worker-1
```

Use a different directory and port for every worker. To retrieve old manifests after restart, keep
the worker identity and advertised address/port unchanged. A process-held OS lock prevents concurrent
services from opening the same directory and is released automatically after process death.

## Publication and verification

Each file contains a versioned ticket/checksum header followed by an Arrow IPC stream. Tickets retain
query, stage, producer partition, task attempt, worker, and destination identity. Repeating an upload
with the same ticket/checksum is idempotent; conflicting content is rejected. Filename hash collisions
also fail explicitly rather than overwrite another ticket.

Writes go to `.pending` files, are flushed and fsynced, then atomically renamed to `.block`. Unix also
fsyncs the directory. Startup deletes incomplete `.pending` files and verifies committed checksums
before indexing them. Corruption or an incompatible version prevents startup instead of silently
serving invalid results. Disk capacity counts serialized bytes, including headers and empty schemas;
publication checks the encoded size before writing payload bytes. Memory mode counts Arrow buffers.

## Retention and failure domain

Committed files remain until explicit client deletion; there is no automatic age-based expiry that
could remove an active query's inputs. Files left by a lost acknowledgement or unreachable client can
be removed by an operator while the service is stopped, once the associated queries are no longer
needed. Startup automatically cleans incomplete writes, not committed orphan results. Deletion is
persisted, so removed results do not return after restart.

This backend survives service/process restarts on the same filesystem. It does not protect against
loss of the worker host or disk, nor does it make coordinator state persistent. It still decodes a
block into memory for Flight transfer and does not implement operator spilling. Filesystem durability
and rename guarantees depend on the host filesystem; do not treat this as replicated object storage.

Verification: `tests/data_plane.rs` closes and reopens the service, reuses original manifests, verifies
deletions, rejects concurrent directory owners, and injects interrupted writes, corruption, and
capacity exhaustion.

## Worker-loss recovery

Task leases already retry abandoned consumers using the same committed dependency manifests. If a
producer's output is unavailable and task retries are exhausted, opt into whole-query recomputation:

```sh
cargo run --locked --bin sparkx -- --input examples/data/orders.csv --table orders \
  --register customers=examples/data/customers.csv \
  --sql "SELECT orders.order_id, customers.name FROM orders LEFT JOIN customers ON orders.customer_id = customers.customer_id" \
  --remote-coordinator http://127.0.0.1:50051 --remote-retries 1 --metrics
```

The default is zero whole-query retries. Keep source contents and partitioning unchanged until the
query finishes. A recoverable transport/missing-output failure cancels the old graph, attempts bounded
cleanup, and resubmits all stages under a fresh query ID. This deliberately recomputes the whole graph
instead of attempting partial lineage repair. Old leases and tickets cannot commit into the new query.
The original query deadline applies across all retries; cleanup adds at most a two-second grace period
per failed attempt. Memory, semantic, and checksum errors are not retried as availability failures.

`QueryResult.recovery_attempts` and CLI metrics report whole-query retries. Task/shuffle metrics describe
the final successful graph and do not total work from failed graphs. Committed inputs are reused for
ordinary consumer lease retries, while a whole-query retry invalidates the old graph's manifests.
A restarted storage service can also serve original manifests if its identity and endpoint are stable.

Protocol version **5** carries terminal task retryability. Use matching coordinator/worker/client builds;
old persisted block versions are rejected. Coordinator restart and loss of every source copy remain
outside this recovery model. `tests/remote_recovery.rs` closes a producer's data service, abandons a
consumer lease, and checks successful recovery and bounded failure when retries are disabled. The
join SQL corpus provides the supported query shapes; these are deterministic local service drills,
not a claim of completed multi-host chaos testing.
