# Roadmap

## Current scope

- Fixed-partition, event-time tumbling aggregation for tool telemetry.
- Explicit watermarks and idle/resume inputs with monotonic operator progress.
- Provisional, revised, final, and too-late output records.
- Transactional local state, receipts, retained event identities, and restart-safe replay.
- Deterministic policy comparison, generated histories, process-exit and concurrent-writer tests.

## Separate questions to explore

1. How should a source estimate progress from delayed observations without trusting future-skewed
   timestamps? Compare bounded-disorder and quantile-based policies against held-out histories.
2. How do checkpoint barriers and backpressure interact across operators? Build a separate
   bounded simulator before introducing distributed workers or a broker.
3. When is deduplication history safe to retire? Relate expiry to explicit replay guarantees and
   distinguish forgotten identities from definitely new events.

There is no planned cloud deployment or production data connection for this implementation.
