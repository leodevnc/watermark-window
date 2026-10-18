"""Local JSONL command replay. Each line is a separate durable transaction."""

import argparse
import json
from pathlib import Path
import sqlite3
import sys

from .demo import compare_lateness, run_demo
from .engine import Config, InputError, Store


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InputError("duplicate JSON object key")
        result[key] = value
    return result


def reject_constant(_):
    raise InputError("non-finite JSON number")


def replay(store, path):
    # Stream inputs and responses rather than reading an unbounded file into memory.
    with Path(path).open("rb") as stream:
        line_number = 0
        while raw := stream.readline(16385):
            line_number += 1
            if len(raw) > 16384:
                raise InputError(f"line {line_number}: exceeds 16 KiB")
            if not raw.strip():
                continue
            try:
                command = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_keys,
                                     parse_constant=reject_constant)
                result = store.apply(command)
            except (ValueError, RecursionError) as exc:
                raise InputError(f"line {line_number}: {exc}") from exc
            print(json.dumps(result, sort_keys=True))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Inspect event-time windows under delayed arrival.")
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("demo", help="run a synthetic in-memory scenario")
    sub.add_parser("experiment", help="compare three lateness policies on the same history")
    init = sub.add_parser("init", help="create a stream or verify its existing configuration")
    init.add_argument("database")
    init.add_argument("--partitions", nargs="+", default=["east", "west"])
    init.add_argument("--window-ms", type=int, default=1000)
    init.add_argument("--lateness-ms", type=int, default=500)
    play = sub.add_parser("replay", help="apply JSONL commands in file order")
    play.add_argument("database")
    play.add_argument("commands")
    show = sub.add_parser("inspect", help="print a consistent state and output snapshot")
    show.add_argument("database")
    args = parser.parse_args(argv)
    try:
        if args.action == "demo":
            print(json.dumps(run_demo(), sort_keys=True, indent=2))
        elif args.action == "experiment":
            print(json.dumps(compare_lateness(), sort_keys=True, indent=2))
        elif args.action == "init":
            config = Config(tuple(args.partitions), args.window_ms, args.lateness_ms)
            with Store(args.database, config) as store:
                print(json.dumps(store.inspect(), sort_keys=True))
        else:
            with Store(args.database, create=False) as store:
                if args.action == "replay":
                    replay(store, args.commands)
                else:
                    print(json.dumps(store.inspect(), sort_keys=True, indent=2))
    except (OSError, ValueError, sqlite3.Error, RecursionError) as exc:
        print(json.dumps({"error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    return 0
