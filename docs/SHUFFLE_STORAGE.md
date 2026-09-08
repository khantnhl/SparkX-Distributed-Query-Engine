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
