"""Serialize event-time transitions and their output records in one SQLite transaction."""

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
import sqlite3


class InputError(ValueError):
    """A command violates the stream contract or an admission limit."""


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def identifier(value):
    if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value):
        raise InputError("identifiers must be 1-64 ASCII letters, digits, dots, underscores, or hyphens")
    return value


def integer(value, minimum=0, maximum=10**12):
    if type(value) is not int or not minimum <= value <= maximum:
        raise InputError(f"expected an integer in [{minimum}, {maximum}]")
    return value


@dataclass(frozen=True)
class Config:
    partitions: tuple[str, ...] = ("east", "west")
    window_ms: int = 1000
    lateness_ms: int = 500
    max_windows: int = 1000
    max_commands: int = 10000

    def __post_init__(self):
        if type(self.partitions) is not tuple or not 1 <= len(self.partitions) <= 32:
            raise InputError("configure 1-32 partitions as a tuple")
        for name in self.partitions:
            identifier(name)
        if len(set(self.partitions)) != len(self.partitions):
            raise InputError("partition names must be unique")
        integer(self.window_ms, 1, 10**9)
        integer(self.lateness_ms, 0, 10**9)
        integer(self.max_windows, 1, 10000)
        integer(self.max_commands, 1, 100000)
        object.__setattr__(self, "partitions", tuple(sorted(self.partitions)))


def validate_command(command, config):
    if type(command) is not dict:
        raise InputError("command must be an object")
    kind = command.get("kind")
    extra = {
        "event": {"event_id", "event_ms", "tool", "duration_ms", "failed"},
        "watermark": {"watermark_ms"},
        "activity": {"idle"},
    }
    if type(kind) is not str or kind not in extra:
        raise InputError("kind must be event, watermark, or activity")
    if set(command) != {"command_id", "kind", "partition"} | extra[kind]:
        raise InputError("command has missing or unknown fields")
    identifier(command["command_id"])
    identifier(command["partition"])
    if command["partition"] not in config.partitions:
        raise InputError("unknown partition")
    if kind == "event":
        identifier(command["event_id"])
        identifier(command["tool"])
        integer(command["event_ms"])
        integer(command["duration_ms"], 0, 10**9)
        if type(command["failed"]) is not bool:
            raise InputError("failed must be boolean")
    elif kind == "watermark":
        integer(command["watermark_ms"])
    elif type(command["idle"]) is not bool:
        raise InputError("idle must be boolean")


def transition(state, command, config):
    """Mutate a transaction-local state and return new output records."""
    outputs = []
    partition = state["partitions"][command["partition"]]

    def emit(window, kind):
        window["revision"] += 1
        outputs.append({"kind": kind, "watermark_ms": state["watermark_ms"],
                        "final": kind == "final", **window})

    def progress():
        active = [p["watermark_ms"] for p in state["partitions"].values() if not p["idle"]]
        if active:
            state["watermark_ms"] = max(state["watermark_ms"], min(active))
        watermark = state["watermark_ms"]
        for key, window in sorted(list(state["windows"].items()),
                                  key=lambda pair: (pair[1]["start_ms"], pair[1]["tool"])):
            if watermark >= window["end_ms"] + config.lateness_ms:
                # A jump beyond both thresholds emits one final snapshot, not two identical rows.
                emit(window, "final")
                del state["windows"][key]
            elif watermark >= window["end_ms"] and window["revision"] == 0:
                emit(window, "on_time")

    if command["kind"] == "event":
        identity = encode({k: v for k, v in command.items() if k != "command_id"})
        previous = state["events"].get(command["event_id"])
        if previous is not None:
            if previous != identity:
                raise InputError("event_id was already used for different content")
            return "duplicate_event", outputs
        partition["idle"] = False
        progress()
        start = command["event_ms"] // config.window_ms * config.window_ms
        end = start + config.window_ms
        state["events"][command["event_id"]] = identity
        if state["watermark_ms"] >= end + config.lateness_ms:
            outputs.append({"kind": "too_late", "event_id": command["event_id"],
                            "partition": command["partition"], "tool": command["tool"],
                            "duration_ms": command["duration_ms"], "failed": command["failed"],
                            "event_ms": command["event_ms"], "start_ms": start, "end_ms": end,
                            "watermark_ms": state["watermark_ms"]})
            return "too_late", outputs
        key = f"{command['tool']}:{start}"
        if key not in state["windows"]:
            if len(state["windows"]) >= config.max_windows:
                raise InputError("active window limit reached; advance progress or use another stream")
            state["windows"][key] = {"tool": command["tool"], "start_ms": start, "end_ms": end,
                                      "count": 0, "errors": 0, "duration_ms": 0, "revision": 0}
        window = state["windows"][key]
        window["count"] += 1
        window["errors"] += int(command["failed"])
        window["duration_ms"] += command["duration_ms"]
        if state["watermark_ms"] >= end:
            emit(window, "late_first" if window["revision"] == 0 else "correction")
        return "accepted", outputs
    if command["kind"] == "watermark":
        if command["watermark_ms"] < partition["watermark_ms"]:
            raise InputError("partition watermark cannot move backward")
        partition["watermark_ms"] = command["watermark_ms"]
        partition["idle"] = False
    else:
        partition["idle"] = command["idle"]
    progress()
    return "advanced", outputs


class Store:
    """One connection per caller; multiple connections serialize writes via SQLite.

    before_commit is a fault-injection hook for tests. It runs inside the transaction.
    This is a single-host simulator, not a distributed stream processor.
    """

    def __init__(self, path=":memory:", config=None, *, create=True):
        target = path if create else Path(path).resolve().as_uri() + "?mode=rw"
        self.connection = sqlite3.connect(target, timeout=10, isolation_level=None, uri=not create)
        self.connection.row_factory = sqlite3.Row
        try:
            self.connection.execute("PRAGMA synchronous=FULL")
            with self.transaction():
                if create:
                    self.connection.execute("CREATE TABLE IF NOT EXISTS stream (id INTEGER PRIMARY KEY CHECK(id=1), config TEXT NOT NULL, state TEXT NOT NULL)")
                    self.connection.execute("CREATE TABLE IF NOT EXISTS receipts (command_id TEXT PRIMARY KEY, command TEXT NOT NULL, result TEXT NOT NULL)")
                    self.connection.execute("CREATE TABLE IF NOT EXISTS outputs (sequence INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL)")
                row = self.connection.execute("SELECT config FROM stream WHERE id=1").fetchone()
                if row:
                    stored = json.loads(row["config"])
                    if stored.pop("format_version") != 1:
                        raise InputError("unsupported state format")
                    stored["partitions"] = tuple(stored["partitions"])
                    self.config = Config(**stored)
                    if config is not None and config != self.config:
                        raise InputError("configuration does not match the existing stream")
                else:
                    if not create:
                        raise InputError("database does not contain an initialized stream")
                    self.config = config or Config()
                    state = {"watermark_ms": -1, "windows": {}, "events": {},
                             "partitions": {name: {"watermark_ms": -1, "idle": False}
                                            for name in self.config.partitions}}
                    self.connection.execute("INSERT INTO stream VALUES (1, ?, ?)",
                                            (encode({"format_version": 1, **asdict(self.config)}), encode(state)))
        except BaseException:
            self.connection.close()
            raise

    @contextmanager
    def transaction(self):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.connection.execute("COMMIT")
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def apply(self, command, *, before_commit=None):
        validate_command(command, self.config)
        canonical = encode(command)
        with self.transaction():
            old = self.connection.execute("SELECT command, result FROM receipts WHERE command_id=?",
                                          (command["command_id"],)).fetchone()
            if old:
                if old["command"] != canonical:
                    raise InputError("command_id was already used for different content")
                return {**json.loads(old["result"]), "replayed": True}
            size = self.connection.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]
            if size >= self.config.max_commands:
                raise InputError("command limit reached; this stream is read-only except for retries")
            state = json.loads(self.connection.execute("SELECT state FROM stream WHERE id=1").fetchone()[0])
            outcome, emitted = transition(state, command, self.config)
            output_ids = []
            for output in emitted:
                cursor = self.connection.execute("INSERT INTO outputs(body) VALUES (?)", (encode(output),))
                output_ids.append(cursor.lastrowid)
            result = {"command_id": command["command_id"], "outcome": outcome,
                      "watermark_ms": state["watermark_ms"], "output_sequences": output_ids}
            self.connection.execute("UPDATE stream SET state=? WHERE id=1", (encode(state),))
            self.connection.execute("INSERT INTO receipts VALUES (?, ?, ?)",
                                    (command["command_id"], canonical, encode(result)))
            if before_commit:
                before_commit()
        return {**result, "replayed": False}

    def inspect(self):
        # A consistent read across state, journal size, and outputs without claiming a writer lock.
        self.connection.execute("BEGIN")
        try:
            state = json.loads(self.connection.execute("SELECT state FROM stream WHERE id=1").fetchone()[0])
            return {"watermark_ms": state["watermark_ms"], "partitions": state["partitions"],
                    "active_windows": sorted(state["windows"].values(), key=lambda w: (w["start_ms"], w["tool"])),
                    "remembered_events": len(state["events"]),
                    "commands": self.connection.execute("SELECT COUNT(*) FROM receipts").fetchone()[0],
                    "outputs": [{"sequence": row["sequence"], **json.loads(row["body"])} for row in
                                self.connection.execute("SELECT sequence, body FROM outputs ORDER BY sequence")]}
        finally:
            self.connection.execute("ROLLBACK")

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
