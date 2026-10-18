# Watermark Window

An offline event-time aggregation experiment for delayed, duplicated, and out-of-order tool-call
telemetry. It answers a practical question: **when is a window complete enough to publish, and
what happens when another event arrives afterward?**

The engine combines explicit partition watermarks, idle-partition handling, provisional results,
late corrections, and final results. SQLite keeps each command, state transition, and output
record in one local transaction so the same history can be replayed across process restarts.

Python 3.11+ is sufficient. There are no third-party runtime dependencies, model calls, cloud
resources, or broker connections. This is a bounded single-host simulator, not a Flink replacement.

## Run the experiment

```sh
python3 -m watermark_window demo
python3 -m watermark_window experiment
python3 -m unittest discover -s tests -v
```

Both demos use in-memory databases and synthetic tool-call data. The first follows a quiet
partition, a delayed event, finalization, and a retry. The second applies three lateness policies
to the same history, then explicitly advances both partitions to finish the experiment:

| Allowed lateness | Final event count | Excluded as too late | Corrections | Windows retained before final flush |
| --- | --- | --- | --- | --- |
| 0 ms | 2 | 2 | 0 | 0 |
| 500 ms | 3 | 1 | 1 | 0 |
| 1500 ms | 4 | 0 | 2 | 1 |

More tolerance includes more delayed data in this fixture, but delays finalization and can emit
more revisions. This is an event-time result, not a benchmark of wall-clock latency or memory use.

## Persist and replay commands

```sh
python3 -m watermark_window init stream.sqlite3 \
  --partitions east west --window-ms 1000 --lateness-ms 500
python3 -m watermark_window replay stream.sqlite3 examples/delayed-tools.jsonl
python3 -m watermark_window inspect stream.sqlite3
```

Repeat the replay command: the saved receipts are returned with `replayed: true`, without new
aggregate changes or output records. A receipt contains the watermark **at the original command**,
not necessarily the current watermark; use `inspect` for the current state.

Each input line commits separately. An invalid line stops replay with exit 2; previously committed
lines remain committed. Fix the input and replay it with unchanged identifiers for existing lines.
Reusing an identifier with different content is an error, not an overwrite.

`init` is also a configuration check for an existing stream. It does not reset data, and rejects
different partitions or timing settings. `replay` and `inspect` require an initialized database.
Exit 0 means the command completed; accepted late-data exclusion is an ordinary result, not a
CLI error. Input and database errors use exit 2 and a JSON message on stderr.

## Input contract

Times are nonnegative integer milliseconds on a shared logical timeline. Real epoch milliseconds
are not required for the synthetic examples. No local timezone or wall clock is consulted.

```json
{"command_id":"delivery-1","kind":"event","partition":"east","event_id":"call-1","event_ms":100,"tool":"search","duration_ms":40,"failed":false}
```

```json
{"command_id":"progress-1","kind":"watermark","partition":"east","watermark_ms":1000}
```

```json
{"command_id":"quiet-1","kind":"activity","partition":"west","idle":true}
```

All shown fields are required; unknown fields are rejected. Identifiers use 1-64 ASCII letters,
digits, dots, underscores, or hyphens and start with a letter or digit. Integers reject booleans
and floating-point JSON values. A JSONL line is limited to 16 KiB; duplicate JSON keys are rejected.

`command_id` identifies a delivery or control operation. `event_id` identifies a logical event
across redeliveries with different command IDs. Both are scoped to the database and must be
globally unique within their respective namespaces. Events from different partitions cannot
reuse an event ID for different content.

## Window rules

Windows are keyed by tool and cover `[start, end)`, aligned to zero. For a window ending at `E`,
allowed lateness `L`, and global watermark `W`:

| Condition | Behavior |
| --- | --- |
| `W < E` | Accumulate without publishing |
| `E <= W < E + L` | Publish provisional state and accept late revisions |
| `W >= E + L` | Publish final state, remove active state, reject further unique events for that window |

The global watermark is the minimum watermark of active partitions, clamped so it never decreases.
An unused active partition starts at -1 and blocks progress. Marking it idle excludes it. An event,
a watermark command, or `idle: false` reactivates it. If all partitions are idle, progress stays
where it was; the engine does not invent an end-of-stream watermark.

Events do not automatically advance partition watermarks. Source progress is explicit, allowing
the same data to be tested under different assumptions about completeness.

## Read the output correctly

`on_time`, `late_first`, `correction`, and `final` records are **full replacement snapshots**, not
additive deltas. Use `(tool, start_ms, end_ms)` as the result key and keep the greatest `revision`.
Summing all revisions double-counts data. `sequence` is an ordered local output-log position.

A window first populated during its grace period emits `late_first`. A watermark jump past both
the publish and cleanup boundaries emits just one `final` record. Empty windows emit nothing.
`too_late` records carry the rejected event's identity, measurements, and timing for inspection;
they do not change finalized aggregates.

## Scope and limits

- Fixed partitions, fixed-duration tumbling windows, integer count/error/duration aggregates.
- Single-host SQLite transactions; no distributed checkpoints, broker offsets, or sink ACKs.
- Full state is serialized per new command. This favors inspectability over throughput.
- Default admission caps: 1,000 active windows and 10,000 committed commands. A command-cap hit
  leaves the stream inspectable/retryable but unable to accept new progress. Choose experiment
  bounds before starting; this is not an unbounded service.
- Event identities and receipts are retained for the stream's lifetime. Window cleanup is not
  deduplication-history deletion. There is no archival or retention job.
- Input histories can reveal tool names and timing. Use synthetic data for shared experiments.
- No browser UI or active GitHub CI workflow is included. Tests run locally with the command above.

See [architecture and tradeoffs](docs/architecture.md), [experiments and tests](docs/experiments.md),
and [roadmap](docs/roadmap.md).

An optional installed CLI is available with `python3 -m pip install .` as `watermark-window`.
Installation may fetch build tools; direct checkout execution requires no downloads.
