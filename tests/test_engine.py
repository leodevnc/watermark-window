from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import unittest

from watermark_window import Config, InputError, Store
from watermark_window.demo import commands, run_demo


def event(name, time=100, partition="east", tool="search", duration=10, failed=False):
    return {"kind": "event", "command_id": name, "event_id": name, "partition": partition,
            "event_ms": time, "tool": tool, "duration_ms": duration, "failed": failed}


def watermark(name, time, partition="east"):
    return {"kind": "watermark", "command_id": name, "partition": partition, "watermark_ms": time}


def activity(name, idle, partition="west"):
    return {"kind": "activity", "command_id": name, "partition": partition, "idle": idle}


class WindowTests(unittest.TestCase):
    def setUp(self):
        self.store = Store()
        self.addCleanup(self.store.close)

    def advance(self, time, name="w"):
        self.store.apply(watermark(name + "e", time))
        return self.store.apply(watermark(name + "w", time, "west"))

    def test_partition_minimum_blocks_progress(self):
        self.store.apply(event("a"))
        self.store.apply(watermark("w", 1000))
        view = self.store.inspect()
        self.assertEqual(view["watermark_ms"], -1)
        self.assertEqual(view["outputs"], [])
        self.store.apply(watermark("v", 1000, "west"))
        self.assertEqual(self.store.inspect()["outputs"][0]["kind"], "on_time")

    def test_correction_then_cleanup_boundary(self):
        self.store.apply(event("a"))
        self.advance(1000)
        self.store.apply(event("b", 999, duration=20, failed=True))
        output = self.store.inspect()["outputs"][-1]
        self.assertEqual((output["kind"], output["count"], output["errors"], output["revision"]),
                         ("correction", 2, 1, 2))
        self.advance(1499, "q")
        self.assertEqual(self.store.apply(event("c"))["outcome"], "accepted")
        self.advance(1500, "r")
        self.assertEqual(self.store.inspect()["active_windows"], [])
        self.assertEqual(self.store.apply(event("d"))["outcome"], "too_late")
        final = [o for o in self.store.inspect()["outputs"] if o["kind"] == "final"][0]
        self.assertEqual((final["count"], final["duration_ms"]), (3, 40))

    def test_end_exclusive_window_assignment(self):
        self.store.apply(event("a", 999))
        self.store.apply(event("b", 1000))
        self.assertEqual([w["start_ms"] for w in self.store.inspect()["active_windows"]], [0, 1000])

    def test_late_first_arrival_has_revision_one(self):
        self.advance(1200)
        self.store.apply(event("a", 100))
        self.assertEqual(self.store.inspect()["outputs"][0]["kind"], "late_first")
        self.assertEqual(self.store.inspect()["outputs"][0]["revision"], 1)

    def test_idle_resume_does_not_rewind(self):
        self.store.apply(event("a"))
        self.store.apply(watermark("w", 1200))
        self.store.apply(activity("idle", True))
        self.assertEqual(self.store.inspect()["watermark_ms"], 1200)
        self.store.apply(event("b", partition="west"))
        self.assertEqual(self.store.inspect()["watermark_ms"], 1200)
        self.assertFalse(self.store.inspect()["partitions"]["west"]["idle"])
        self.store.apply(watermark("next", 2000))
        self.assertEqual(self.store.inspect()["watermark_ms"], 1200)

    def test_all_idle_does_not_flush(self):
        self.store.apply(event("a"))
        self.store.apply(activity("i1", True))
        self.store.apply(activity("i2", True, "east"))
        view = self.store.inspect()
        self.assertEqual(view["watermark_ms"], -1)
        self.assertEqual(len(view["active_windows"]), 1)

    def test_zero_lateness_and_jump_emit_single_final(self):
        with Store(config=Config(lateness_ms=0)) as store:
            store.apply(event("a"))
            store.apply(watermark("x", 1000))
            store.apply(watermark("y", 1000, "west"))
            self.assertEqual([o["kind"] for o in store.inspect()["outputs"]], ["final"])
            self.assertEqual(store.apply(event("b", 999))["outcome"], "too_late")
        self.store.apply(event("a"))
        self.advance(9000)
        self.assertEqual([o["kind"] for o in self.store.inspect()["outputs"]], ["final"])

    def test_event_timestamp_alone_does_not_advance_progress(self):
        self.store.apply(event("future", 10**12))
        self.assertEqual(self.store.inspect()["watermark_ms"], -1)

    def test_no_empty_windows(self):
        self.advance(100000)
        self.assertEqual(self.store.inspect()["outputs"], [])

    def test_event_duplicate_after_final_is_not_late(self):
        command = event("a")
        self.store.apply(command)
        self.advance(2000)
        before = self.store.inspect()
        result = self.store.apply({**command, "command_id": "retry"})
        self.assertEqual(result["outcome"], "duplicate_event")
        self.assertEqual(self.store.inspect()["outputs"], before["outputs"])

    def test_too_late_duplicate_has_one_side_output(self):
        self.advance(2000)
        command = event("a")
        self.store.apply(command)
        self.store.apply({**command, "command_id": "retry"})
        self.assertEqual(len(self.store.inspect()["outputs"]), 1)

    def test_duplicate_does_not_reactivate_idle_partition(self):
        command = event("a", partition="west")
        self.store.apply(command)
        self.store.apply(activity("idle", True))
        self.store.apply({**command, "command_id": "retry"})
        self.assertTrue(self.store.inspect()["partitions"]["west"]["idle"])

    def test_explicit_reactivation_and_watermark_activation(self):
        self.store.apply(activity("idle", True))
        self.store.apply(activity("active", False))
        self.assertFalse(self.store.inspect()["partitions"]["west"]["idle"])
        self.store.apply(activity("idle2", True))
        self.store.apply(watermark("w", 1, "west"))
        self.assertFalse(self.store.inspect()["partitions"]["west"]["idle"])

    def test_conflicting_ids_leave_state_unchanged(self):
        command = event("a")
        self.store.apply(command)
        before = self.store.inspect()
        for changed in [{**command, "duration_ms": 99},
                        {**command, "command_id": "other", "duration_ms": 99}]:
            with self.assertRaises(InputError):
                self.store.apply(changed)
            self.assertEqual(self.store.inspect(), before)

    def test_regressing_watermark_rolls_back(self):
        self.store.apply(watermark("a", 2000))
        before = self.store.inspect()
        with self.assertRaises(InputError):
            self.store.apply(watermark("b", 1999))
        self.assertEqual(self.store.inspect(), before)

    def test_command_replay_returns_original_receipt(self):
        command = event("a")
        receipt = self.store.apply(command)
        self.advance(2000)
        before = self.store.inspect()
        retry = self.store.apply(command)
        self.assertEqual(retry, {**receipt, "replayed": True})
        self.assertEqual(self.store.inspect(), before)

    def test_capacity_rejection_does_not_consume_identity(self):
        with Store(config=Config(max_windows=1)) as store:
            store.apply(event("a"))
            before = store.inspect()
            with self.assertRaises(InputError):
                store.apply(event("b", 2000))
            self.assertEqual(store.inspect(), before)
            store.apply(watermark("x", 1500))
            store.apply(watermark("y", 1500, "west"))
            self.assertEqual(store.apply(event("b", 2000))["outcome"], "accepted")

    def test_receipt_limit_still_allows_retries(self):
        with Store(config=Config(max_commands=1)) as store:
            store.apply(event("a"))
            with self.assertRaises(InputError):
                store.apply(event("b"))
            self.assertTrue(store.apply(event("a"))["replayed"])

    def test_bad_input_no_mutation(self):
        cases = [None, {}, {**event("a"), "extra": 1}, {**event("a"), "event_ms": True},
                 {**event("a"), "duration_ms": -1}, {**event("a"), "failed": 1},
                 {**event("a"), "partition": "unknown"}, {**event("a"), "tool": "../bad"},
                 {**watermark("a", 1), "watermark_ms": 1.0}, {**activity("a", True), "idle": "true"}]
        before = self.store.inspect()
        for case in cases:
            with self.subTest(case=case), self.assertRaises(InputError):
                self.store.apply(case)
            self.assertEqual(self.store.inspect(), before)

    def test_config_validation(self):
        for args in [dict(partitions=()), dict(partitions=("a", "a")), dict(window_ms=0),
                     dict(lateness_ms=-1), dict(max_commands=True), dict(partitions=["a"])]:
            with self.assertRaises(InputError):
                Config(**args)

    def test_demo_expected_outputs(self):
        result = run_demo()
        outputs = result["stream"]["outputs"]
        self.assertEqual([o["kind"] for o in outputs], ["on_time", "correction", "final", "too_late"])
        self.assertEqual([o["count"] for o in outputs[:3]], [2, 3, 3])
        self.assertEqual(outputs[2]["duration_ms"], 120)
        self.assertEqual(result["receipts"][-1]["outcome"], "duplicate_event")


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "stream.sqlite3"

    def test_reopen_matches_uninterrupted_execution(self):
        expected = run_demo()["stream"]
        for command in commands():
            with Store(self.path) as store:
                store.apply(command)
        with Store(self.path) as store:
            self.assertEqual(store.inspect(), expected)

    def test_config_mismatch_rejected_and_original_preserved(self):
        with Store(self.path) as store:
            store.apply(event("a"))
        with self.assertRaises(InputError):
            Store(self.path, Config(lateness_ms=100))
        with Store(self.path, create=False) as store:
            self.assertEqual(store.inspect()["remembered_events"], 1)

    def test_crash_before_commit_rolls_back_outputs_and_state(self):
        with Store(self.path) as store:
            store.apply(event("a"))
            store.apply(watermark("w", 2000))
            before = store.inspect()
        command = watermark("final", 2000, "west")
        code = ("import json,os,sys; from watermark_window import Store; "
                "s=Store(sys.argv[1]); s.apply(json.loads(sys.argv[2]), before_commit=lambda: os._exit(17))")
        process = subprocess.run([sys.executable, "-c", code, str(self.path), json.dumps(command)], timeout=10)
        self.assertEqual(process.returncode, 17)
        with Store(self.path) as store:
            self.assertEqual(store.inspect(), before)
            store.apply(command)
            self.assertEqual(len(store.inspect()["outputs"]), 1)

    def test_crash_after_commit_retry_does_not_duplicate(self):
        command = event("a")
        code = ("import json,os,sys; from watermark_window import Store; "
                "s=Store(sys.argv[1]); s.apply(json.loads(sys.argv[2])); os._exit(18)")
        process = subprocess.run([sys.executable, "-c", code, str(self.path), json.dumps(command)], timeout=10)
        self.assertEqual(process.returncode, 18)
        with Store(self.path) as store:
            self.assertTrue(store.apply(command)["replayed"])
            self.assertEqual(store.inspect()["active_windows"][0]["count"], 1)

    def test_concurrent_duplicate_writers(self):
        with Store(self.path):
            pass
        def worker(_):
            with Store(self.path) as store:
                return store.apply(event("a"))
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(worker, range(16)))
        self.assertEqual(sum(not result["replayed"] for result in results), 1)
        with Store(self.path) as store:
            self.assertEqual(store.inspect()["commands"], 1)

    def test_keyboard_interrupt_rolls_back(self):
        with Store(self.path) as store:
            def interrupt():
                raise KeyboardInterrupt()
            with self.assertRaises(KeyboardInterrupt):
                store.apply(event("a"), before_commit=interrupt)
            self.assertEqual(store.inspect()["commands"], 0)

    def test_concurrent_event_retries_with_different_command_ids(self):
        with Store(self.path):
            pass
        def worker(i):
            with Store(self.path) as store:
                return store.apply({**event("same-event"), "command_id": f"delivery-{i}"})
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(worker, range(16)))
        self.assertEqual(sum(result["outcome"] == "accepted" for result in results), 1)
        self.assertEqual(sum(result["outcome"] == "duplicate_event" for result in results), 15)
        with Store(self.path) as store:
            self.assertEqual(store.inspect()["active_windows"][0]["count"], 1)


class GeneratedHistoryTests(unittest.TestCase):
    def test_random_histories_against_independent_admission_oracle(self):
        # Replay command history, independently calculate admission, then batch-reduce accepted events.
        for seed in range(30):
            rng = random.Random(seed)
            progress = {"east": -1, "west": -1}
            idle = {"east": False, "west": False}
            current = -1
            accepted = []
            rejected = 0
            with Store() as store:
                for i in range(100):
                    partition = rng.choice(["east", "west"])
                    choice = rng.randrange(5)
                    if choice < 3:
                        command = event(f"e{i}", rng.randrange(4000), partition,
                                        rng.choice(["search", "fetch"]), rng.randrange(100), bool(i % 2))
                        idle[partition] = False
                    elif choice == 3:
                        progress[partition] += rng.randrange(800)
                        progress[partition] = max(0, progress[partition])
                        command = watermark(f"w{i}", progress[partition], partition)
                        idle[partition] = False
                    else:
                        idle[partition] = not idle[partition]
                        command = activity(f"a{i}", idle[partition], partition)
                    candidates = [progress[p] for p in progress if not idle[p]]
                    if candidates:
                        current = max(current, min(candidates))
                    if choice < 3:
                        end = (command["event_ms"] // 1000 + 1) * 1000
                        if current < end + 500:
                            accepted.append(command)
                        else:
                            rejected += 1
                    store.apply(command)
                    self.assertEqual(store.inspect()["watermark_ms"], current)
                for partition in progress:
                    store.apply(watermark("flush-" + partition, 100000, partition))
                expected = {}
                for command in accepted:
                    key = (command["tool"], command["event_ms"] // 1000 * 1000)
                    count, errors, duration = expected.get(key, (0, 0, 0))
                    expected[key] = (count + 1, errors + command["failed"], duration + command["duration_ms"])
                view = store.inspect()
                finals = [o for o in view["outputs"] if o["kind"] == "final"]
                actual = {(o["tool"], o["start_ms"]): (o["count"], o["errors"], o["duration_ms"]) for o in finals}
                self.assertEqual(actual, expected, seed)
                self.assertEqual(len(finals), len(expected))
                self.assertEqual(sum(o["kind"] == "too_late" for o in view["outputs"]), rejected)
                self.assertEqual(view["active_windows"], [])
                revisions = {}
                for output in view["outputs"]:
                    if output["kind"] == "too_late":
                        continue
                    key = (output["tool"], output["start_ms"])
                    self.assertEqual(output["revision"], revisions.get(key, 0) + 1)
                    revisions[key] = output["revision"]
