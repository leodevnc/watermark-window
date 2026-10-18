"""Synthetic tool-call telemetry with a quiet partition and a delayed event."""

from .engine import Config, Store


def commands():
    return [
        {"command_id": "a", "kind": "event", "partition": "east", "event_id": "e1",
         "event_ms": 100, "tool": "search", "duration_ms": 40, "failed": False},
        {"command_id": "b", "kind": "event", "partition": "west", "event_id": "e2",
         "event_ms": 700, "tool": "search", "duration_ms": 60, "failed": True},
        {"command_id": "c", "kind": "watermark", "partition": "east", "watermark_ms": 1000},
        {"command_id": "d", "kind": "activity", "partition": "west", "idle": True},
        {"command_id": "e", "kind": "event", "partition": "west", "event_id": "e3",
         "event_ms": 400, "tool": "search", "duration_ms": 20, "failed": False},
        {"command_id": "f", "kind": "watermark", "partition": "east", "watermark_ms": 1500},
        {"command_id": "g", "kind": "watermark", "partition": "west", "watermark_ms": 1500},
        {"command_id": "h", "kind": "event", "partition": "west", "event_id": "e4",
         "event_ms": 800, "tool": "search", "duration_ms": 90, "failed": True},
        {"command_id": "retry-e3", "kind": "event", "partition": "west", "event_id": "e3",
         "event_ms": 400, "tool": "search", "duration_ms": 20, "failed": False},
    ]


def run_demo():
    with Store() as store:
        receipts = [store.apply(command) for command in commands()]
        return {"receipts": receipts, "stream": store.inspect()}


def compare_lateness():
    rows = []
    for grace in (0, 500, 1500):
        with Store(config=Config(lateness_ms=grace)) as store:
            for command in commands():
                store.apply(command)
            retained = len(store.inspect()["active_windows"])
            for partition in ("east", "west"):
                store.apply({"command_id": "flush-" + partition, "kind": "watermark",
                             "partition": partition, "watermark_ms": 3000})
            outputs = store.inspect()["outputs"]
            final = next(output for output in outputs if output["kind"] == "final")
            rows.append({"lateness_ms": grace, "final_count": final["count"],
                         "final_errors": final["errors"], "final_duration_ms": final["duration_ms"],
                         "too_late": sum(o["kind"] == "too_late" for o in outputs),
                         "corrections": sum(o["kind"] == "correction" for o in outputs),
                         "retained_windows_before_flush": retained})
    return rows
