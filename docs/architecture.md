# Event-time state and progress

## Questions

- How does one quiet source hold back a multi-partition aggregate?
- What correctness assumption changes when that source is excluded as idle?
- How can a late event update a published result without double-counting revisions?
- What needs to commit together so process recovery cannot split state from output?

## Decision: explicit source progress

A watermark is a source's estimate of event-time completeness, not the largest timestamp ever
seen and not a processing-time clock. Sources supply monotonic integer progress. The operator
uses the minimum active source progress and never rewinds its own watermark.

The model separates data from progress deliberately. A source with a corrupt future event time
cannot silently fast-forward every window. A bad explicit watermark still can; progress commands
are trusted inputs, not independently verified facts. Their accuracy must be established by the
source adapter in a real deployment.

Idle status is also explicit. There is no timer that guesses inactivity. Excluding an idle source
improves liveness but admits the possibility that data from that source will miss the acceptance
boundary. Reactivation does not rewind the global watermark or reopen a finalized window. A
reactivated slow source can hold back subsequent progress until it catches up or is excluded again.

## Decision: window-based late admission

A timestamp earlier than the watermark is not automatically dropped. Admission uses the end of
its assigned window plus the configured grace period. An event may be behind the watermark yet
still belong to a window that has not reached its end. This is why a simple `event_ms < W` filter
would implement different semantics.

For window width `D`, assignment is `start = floor(event_ms / D) * D`, `end = start + D`.
The intervals are half-open. A timestamp exactly at an end belongs to the next window.
Publication is triggered at `W >= end`; cleanup and rejection at `W >= end + lateness`.
These are this project's explicit boundary conventions, not a claim of byte-for-byte compatibility
with a particular stream framework's timestamp conventions.

The first publication has revision 1. Every correction and final snapshot increments the revision.
When progress skips beyond cleanup, one final snapshot suffices; publishing a redundant provisional
snapshot first adds no information. Active aggregate state is then deleted. The monotonic watermark
prevents later events from recreating the expired window, so no separate window tombstone is needed.

## Decision: local transactional replay

The database has three tables:

| Table | Role |
| --- | --- |
| `stream` | Immutable configuration and the current JSON state: partitions, active windows, remembered events |
| `receipts` | Canonical command content and its original result, keyed by command ID |
| `outputs` | Ordered aggregate revisions and too-late side records |

A new command runs under `BEGIN IMMEDIATE`:

1. Check for an existing receipt; a matching retry returns it, a conflict fails.
2. Check admission limits and load a transaction-local state copy.
3. Apply progress or event admission and construct output records.
4. Insert outputs, update state, and insert the receipt.
5. Commit before returning the receipt.

All failures before commit roll back all three tables. If the process disappears after commit,
retrying the command returns its original output sequence references. A new command ID carrying
an already-seen event returns `duplicate_event` without reactivating its partition or updating an
aggregate. Event-ID conflicts fail instead of silently selecting a value.

SQLite serializes writers from separate connections, and each new command reloads the committed
state. One Store connection is used per caller; sharing one connection across threads is not
supported. `inspect` reads state and output history in one consistent read transaction.

There is no external side effect inside this transaction. Publishing `outputs` to another system
would require a resumable reader and an idempotent destination keyed by result identity/revision.
The current implementation does not claim end-to-end exactly-once delivery.

## Decision: bounded experiments, retained identities

The full-state representation makes transitions easy to inspect and recovery simple, but its
cost grows with remembered events. It is not an incremental state backend. A new command reads
and rewrites the state, scans active windows when checking progress, and persists a receipt.
`inspect` returns all outputs rather than paging them.

Configuration permits 1-32 partitions, window/grace durations up to 10^9 ms, at most 10,000 active
windows, and at most 100,000 committed commands. Defaults are smaller: 1,000 windows and 10,000
commands. Event and watermark times are capped at 10^12; individual duration values at 10^9.
Aggregate integer sums remain exact in Python and are serialized as JSON integers.

The command cap bounds history growth but can halt progress before windows finalize. It is a
deliberate stop condition for a finite experiment, not a production backpressure mechanism.
Rejected commands do not consume identifiers or capacity, and matching retries work at the cap.
State-format version and all configuration fields are persisted and checked on reopening.

Removing event identities during window cleanup would save space, but could make a duplicate look
like a newly rejected late event and emit an extra side record. Retaining identities makes the
experiment's replay semantics explicit. A durable, unbounded stream would need a retention policy
related to source replay guarantees, not an arbitrary TTL.

The database is a trusted local artifact. There is no multi-tenant authorization, encryption,
tamper verification, corruption recovery, schema migration, or network-filesystem guarantee.
SQLite uses full synchronous writes; process-exit tests do not prove hardware power-loss behavior.

## References

The source-progress and late-update concepts are described in Apache Flink's primary documentation:
[event time and parallel watermarks](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/)
and [window lifecycle, allowed lateness, and revised results](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/).
This repository implements its own small state machine rather than calling Flink.
