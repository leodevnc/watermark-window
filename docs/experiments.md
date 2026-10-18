# Delayed telemetry experiments

## Quiet source and late correction

Run `python3 -m watermark_window demo`. The fixture uses a 1,000 ms window and 500 ms of grace.

| Step | Observation |
| --- | --- |
| Receive two search events at event times 100 and 700 | Count 2, duration 100 ms, errors 1 |
| East reports watermark 1000; West has no progress | Global watermark stays -1; no output |
| Mark West idle | Global watermark reaches 1000; provisional count 2 |
| West resumes with an event at time 400 | Global watermark stays 1000; correction raises count to 3 |
| East reports 1500 | Resumed West holds global progress at 1000 |
| West reports 1500 | Final count 3, duration 120 ms, errors 1; active state removed |
| Another unique event at time 800 arrives | Too-late side record; final result stays unchanged |
| Previously accepted event is redelivered | Duplicate result; no extra aggregate or side record |

This demonstrates a consequence of idleness: excluding a partition is a completeness tradeoff,
not evidence that the source has no remaining data. Explicit resumption preserves monotonic
global progress while exposing delayed arrivals through correction or rejection.

## Compare grace policies

```sh
python3 -m watermark_window experiment
```

The same nine input operations are applied with 0, 500, and 1500 ms grace, then both source
watermarks advance to 3000. The final event counts are respectively 2, 3, and 4; excluded events
are 2, 1, and 0. With 1500 ms grace, one active window remains just before that final flush; the
smaller-grace streams have already cleaned up. Tests assert the numerical results and duration
sums of 100, 120, and 210 ms.

The counts describe this synthetic history only. They do not estimate a real late-arrival
distribution or prescribe a suitable business policy. Longer grace can increase completeness,
but it cannot repair events that arrive after the new boundary. It also requires consumers to
handle revised results; adding provisional and final snapshots together is incorrect.

## Reproduce the checks

```sh
python3 -m unittest discover -s tests -v
```

- Boundary tests cover exact window ends, exact cleanup times, zero grace, progress jumps,
  empty windows, and first arrivals during grace.
- Recovery tests actually terminate child processes before and after commit, reopen the database,
  and check state, receipts, and output counts. A separate test reopens the database after every
  command and compares the result with uninterrupted execution.
- Concurrency tests issue 16 duplicate operations from 8 worker threads with separate SQLite
  connections. They cover both identical command retries and different command IDs for one event.
- Generated histories use 30 fixed seeds and 100 mixed operations each. A separate admission
  model records accepted events, then batch-reduces them. Final aggregates must equal that batch
  result; output revisions must be consecutive; too-late counts and global progress must match.
- CLI tests exercise JSONL replay, committed prefixes on failure, duplicate keys, decoding and
  size errors, missing databases, configuration mismatches, and the executable entry point.

The generated histories are bounded tests, not a formal proof. SQLite process recovery is local
to the test machine; these checks do not simulate disk corruption, host power loss, distributed
checkpoint barriers, or an external sink. No network service is needed to run them.
