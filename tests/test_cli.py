import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from watermark_window import Store
from watermark_window.cli import main
from watermark_window.demo import commands, compare_lateness, run_demo


class CLITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "stream.sqlite3"

    def cli(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main([str(arg) for arg in args])
        return code, out.getvalue(), err.getvalue()

    def test_replay_and_retry_file(self):
        self.assertEqual(self.cli("init", self.db)[0], 0)
        fixture = Path(__file__).resolve().parents[1] / "examples/delayed-tools.jsonl"
        code, out, err = self.cli("replay", self.db, fixture)
        self.assertEqual(code, 0, err)
        self.assertEqual(len(out.splitlines()), 9)
        code, out, err = self.cli("inspect", self.db)
        self.assertEqual(json.loads(out), run_demo()["stream"])
        code, out, err = self.cli("replay", self.db, fixture)
        self.assertEqual(code, 0, err)
        self.assertTrue(all(json.loads(line)["replayed"] for line in out.splitlines()))

    def test_invalid_line_preserves_committed_prefix(self):
        self.cli("init", self.db)
        fixture = self.root / "bad.jsonl"
        fixture.write_text(json.dumps(commands()[0]) + '\n{"kind":"event","kind":"watermark"}\n', encoding="utf-8")
        code, out, err = self.cli("replay", self.db, fixture)
        self.assertEqual(code, 2)
        self.assertEqual(len(out.splitlines()), 1)
        self.assertIn("line 2", json.loads(err)["error"])
        with Store(self.db) as store:
            self.assertEqual(store.inspect()["commands"], 1)

    def test_missing_database_is_not_created(self):
        self.assertEqual(self.cli("inspect", self.db)[0], 2)
        self.assertFalse(self.db.exists())

    def test_bad_stream_does_not_gain_tables(self):
        import sqlite3
        with sqlite3.connect(self.db) as connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
        self.assertEqual(self.cli("inspect", self.db)[0], 2)
        with sqlite3.connect(self.db) as connection:
            tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        self.assertEqual(tables, [("unrelated",)])

    def test_input_limits_and_decoding(self):
        self.cli("init", self.db)
        fixture = self.root / "bad.jsonl"
        for payload in [b"x" * 16385, b"\xff\n", b'{"bad":NaN}\n', b"[" * 2000 + b"]" * 2000]:
            fixture.write_bytes(payload)
            code, _, _ = self.cli("replay", self.db, fixture)
            self.assertEqual(code, 2)
        with Store(self.db) as store:
            self.assertEqual(store.inspect()["commands"], 0)

    def test_init_conflict_and_repeat(self):
        self.assertEqual(self.cli("init", self.db)[0], 0)
        self.assertEqual(self.cli("init", self.db)[0], 0)
        self.assertEqual(self.cli("init", self.db, "--window-ms", 2000)[0], 2)

    def test_demo_has_no_persistent_files(self):
        code, out, err = self.cli("demo")
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out), run_demo())

    def test_lateness_experiment(self):
        rows = compare_lateness()
        self.assertEqual([row["final_count"] for row in rows], [2, 3, 4])
        self.assertEqual([row["too_late"] for row in rows], [2, 1, 0])
        self.assertEqual([row["retained_windows_before_flush"] for row in rows], [0, 0, 1])
        self.assertEqual([row["final_duration_ms"] for row in rows], [100, 120, 210])
        self.assertEqual(json.loads(self.cli("experiment")[1]), rows)

    def test_module_entrypoint(self):
        result = subprocess.run([sys.executable, "-m", "watermark_window", "experiment"],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), compare_lateness())
