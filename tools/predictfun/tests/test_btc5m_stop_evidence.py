"""A stopped live run keeps the fragment that stopped it, and its ending.

The launcher must persist the first output fragment that made it stop with
``OUTPUT_INVALID``, redacted, and must still write this run's ``ended.json``
for that stop: a run that stopped on its own child's output is exactly the run
whose evidence a human needs afterwards. The live fixture is the one
``test_btc5m_control`` owns — bound here, never copied or weakened.
"""
from __future__ import annotations

import datetime
import json
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import btc5m_control as control
import test_btc5m_control as launcher_fixture

SENTINEL = "SECRET_SENTINEL"
ENDED = {"started.json", "public-config.json", "live-context.json", "events.jsonl", "ended.json"}
# The mock child writes its two streams, answers the launcher's own ACKs, and
# exits with the code the test asked for; it never starts another process.
CHILD = '''import json, sys
for frame in open(sys.argv[1], "rb").readlines():
    sys.stdout.buffer.write(frame)
    sys.stdout.buffer.flush()
    try:
        event = json.loads(frame[len("BTC5M_EVENT "):])
    except ValueError:
        continue
    payload = event.get("payload") or {}
    if (event.get("event_kind") == "order" and payload.get("attempt_phase") == "PREPARED"
            and payload.get("signed_hash") is not None and payload.get("reason") == "NONE"):
        expected = f"A {event['event_seq']} {payload['signed_hash']}\\n".encode()
        if sys.stdin.buffer.readline() != expected:
            sys.exit(42)
sys.stderr.buffer.write(open(sys.argv[2], "rb").read())
sys.stderr.buffer.flush()
sys.exit(int(sys.argv[3]))
'''

# The launcher closes stdin when it stops, so this child writes every byte of
# its stdout, waits for that close, and only then speaks on stderr. A stderr
# tail therefore arrives after the stop, never before it.
LATE_TAIL_CHILD = '''import signal, sys
signal.signal(signal.SIGINT, signal.SIG_IGN)
sys.stdout.buffer.write(open(sys.argv[1], "rb").read())
sys.stdout.buffer.flush()
sys.stdin.buffer.read()
sys.stderr.buffer.write(open(sys.argv[2], "rb").read())
sys.stderr.buffer.flush()
sys.exit(0)
'''


class StopOutputEvidenceTests(unittest.TestCase):
    """One stopped run at a time; every child is a local stream writer."""

    # The live fixture is the one launcher test class already owns: bound here,
    # not subclassed, so its own tests are never collected twice.
    setUp = launcher_fixture.ControlledLiveSupervisorTests.setUp

    def consume_bytes(self, out: bytes, err: bytes = b"", exit_code: int = 0, patched=None,
                      child: str = CHILD):
        """Supervise one real child that writes exactly these bytes."""
        live = self.live
        started, public, context = self.metadata(out.splitlines(keepends=True)[0])
        directory = self.root / "run"
        store = live.LiveEventStore(directory, started, public, context)
        self.addCleanup(store.close)
        script = self.root / "stop_child.py"
        script.write_text(child)
        (self.root / "out.bin").write_bytes(out)
        (self.root / "err.bin").write_bytes(err)
        process = subprocess.Popen([sys.executable, "-B", str(script), str(self.root / "out.bin"),
                                    str(self.root / "err.bin"), str(exit_code)],
                                   env={"PATH": "/usr/bin:/bin"}, stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        now = datetime.datetime.now(datetime.timezone.utc)
        with patch.object(control, "CLEANUP_SECONDS", .5):
            if patched is None:
                facts, final = control.supervise(process, store, now, control.boottime())
            else:
                with patched:
                    facts, final = control.supervise(process, store, now, control.boottime())
        try:
            # The launcher's own ending step, with its own failure handling.
            control.finish_live_evidence(store, facts, final)
        except (OSError, ValueError):
            facts["launcher_exit"] = 1
        return facts, final, live.read_live_run(directory), store

    def record(self, store):
        return json.loads((store.directory / "stop-output.json").read_bytes())

    def ended(self, store):
        return json.loads((store.directory / "ended.json").read_bytes())

    def frames(self):
        return self.native_stream(813, 827)

    def test_an_unterminated_stderr_residue_is_kept_and_still_ends(self):
        residue = b"rust warning: no trailing newline"
        facts, final, read, store = self.consume_bytes(self.frames()[0], residue)
        self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
        self.assertEqual(facts["stop_output"], "WRITTEN")
        self.assertIsNone(final)
        record = self.record(store)
        self.assertEqual(record["stream"], "stderr")
        self.assertEqual(record["bytes"], len(residue))
        self.assertIs(record["newline_terminated"], False)
        self.assertIn("no trailing newline", record["text"])
        self.assertEqual(record["child_returncode"], facts["child_returncode"])
        self.assertEqual(self.ended(store)["exit_facts"]["stop_reason"], "OUTPUT_INVALID")
        self.assertEqual(read["status"], "INCOMPLETE")

    def test_a_newline_terminated_stderr_line_is_kept_and_still_ends(self):
        line = b'BTC5M_EVENT {"not":"a live event"}\n'
        facts, final, read, store = self.consume_bytes(self.frames()[0], line)
        self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
        self.assertEqual(facts["stop_output"], "WRITTEN")
        self.assertIsNone(final)
        record = self.record(store)
        self.assertEqual(record["stream"], "stderr")
        self.assertEqual(record["bytes"], len(line))
        self.assertIs(record["newline_terminated"], True)
        self.assertIsNone(self.ended(store)["final_event_seq"])
        self.assertEqual(read["status"], "INCOMPLETE")

    def test_an_oversize_line_is_kept_and_still_ends(self):
        oversize = b"BTC5M_EVENT " + b"x" * (control.MAX_LINE_BYTES + 4096) + b"\n"
        facts, final, read, store = self.consume_bytes(self.frames()[0] + oversize)
        self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
        self.assertIsNone(final)
        record = self.record(store)
        self.assertEqual(record["stream"], "stdout")
        self.assertGreater(record["bytes"], control.MAX_LINE_BYTES)
        self.assertIs(record["truncated"], True)
        self.assertEqual(len(record["text"]), control.STOP_OUTPUT_LIMIT)
        self.assertEqual(read["status"], "INCOMPLETE")

    def test_a_rejected_event_line_is_kept_and_still_ends(self):
        # run009's own shape: the child's next frame is refused by the store.
        rejected = b'BTC5M_EVENT {"event_kind":"order"}\n'
        facts, final, read, store = self.consume_bytes(self.frames()[0] + rejected)
        self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
        self.assertTrue(store.failed)
        self.assertIsNone(final)
        record = self.record(store)
        self.assertEqual(record["stream"], "stdout")
        self.assertEqual(record["bytes"], len(rejected))
        self.assertIs(record["newline_terminated"], True)
        self.assertIn("event_kind", record["text"])
        ended = self.ended(store)
        self.assertEqual(ended["exit_facts"]["stop_reason"], "OUTPUT_INVALID")
        self.assertEqual(ended["incomplete_reason"], "AUDIT_UNAVAILABLE")
        self.assertIs(ended["audit_complete"], False)
        self.assertIs(ended["takeover_required"], True)
        self.assertEqual(ended["exit_facts"]["launcher_exit"], 1)
        self.assertEqual(read["status"], "INCOMPLETE")

    def test_no_secret_shaped_token_reaches_the_kept_fragment(self):
        key = "f" * 64
        rejected = b"BTC5M_EVENT " + json.dumps({
            "note": f"PM_API_SECRET={SENTINEL} Authorization: Bearer {SENTINEL}",
            "rpc": f"https://user:{SENTINEL}@rpc.invalid/private-path",
            "key": key, "event_kind": "order"}).encode() + b"\n"
        _facts, _final, _read, store = self.consume_bytes(self.frames()[0] + rejected)
        raw = (store.directory / "stop-output.json").read_bytes()
        record = json.loads(raw)
        self.assertNotIn(SENTINEL.encode(), raw)
        self.assertNotIn(b"rpc.invalid", raw)
        self.assertNotIn(key.encode(), raw)
        self.assertIn("<REDACTED>", record["text"])
        self.assertIn("PM_API_SECRET=<REDACTED>", record["text"])
        self.assertIn("Authorization: <REDACTED>", record["text"])
        self.assertIn('"rpc": "<REDACTED>"', record["text"])
        self.assertEqual(record["bytes"], len(rejected))
        self.assertEqual(record["schema_version"], control.STOP_OUTPUT_SCHEMA)

    def test_a_failed_fragment_write_never_replaces_the_stop_reason(self):
        residue = b"unterminated child warning"
        failed = patch.object(control, "write_stop_output", side_effect=OSError("offline"))
        facts, final, read, store = self.consume_bytes(self.frames()[0], residue, patched=failed)
        self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
        self.assertEqual(facts["stop_output"], "WRITE_FAILED")
        self.assertEqual(facts["launcher_exit"], 1)
        self.assertIsNone(final)
        self.assertFalse((store.directory / "stop-output.json").exists())
        # The ending is still attempted, and it still states the true facts.
        self.assertEqual(self.ended(store)["exit_facts"]["stop_reason"], "OUTPUT_INVALID")
        self.assertEqual(read["status"], "INCOMPLETE")

    def test_the_fragment_record_is_created_once_and_never_replaced(self):
        residue = b"unterminated child warning"
        _facts, _final, _read, store = self.consume_bytes(self.frames()[0], residue)
        first = (store.directory / "stop-output.json").read_bytes()
        second = {"stream": "stdout", "raw": b"second fragment", "newline_terminated": False,
                  "occurred_at": "2026-11-22T00:00:00.000Z"}
        with self.assertRaises(OSError):
            control.write_stop_output(store, second, {"child_returncode": None})
        self.assertEqual((store.directory / "stop-output.json").read_bytes(), first)

    def test_a_clean_final_path_gains_no_file_and_changes_nothing(self):
        facts, final, read, store = self.consume_bytes(b"".join(self.frames()))
        self.assertEqual(facts["launcher_exit"], 0)
        self.assertIsNotNone(final)
        self.assertNotIn("stop_output", facts)
        self.assertEqual({path.name for path in store.directory.iterdir()}, ENDED)
        self.assertEqual(read["status"], "INCOMPLETE")
        self.assertEqual(read["ended"]["final_event_seq"], len(self.frames()))

    def ambiguous_run(self, out: bytes, err: bytes = b""):
        """A run whose first append fails ambiguously; then this tail arrives."""
        def fail_the_first_commit(_descriptor):
            raise OSError("offline")

        fsync = patch.object(self.live.os, "fsync", fail_the_first_commit)
        return self.consume_bytes(out, err, child=LATE_TAIL_CHILD, patched=fsync)

    def assert_an_ambiguous_stop_never_ends(self, facts, final, read, store):
        self.assertEqual(facts["stop_reason"], "AUDIT_UNAVAILABLE")
        self.assertTrue(store.failed)
        self.assertIsNone(final)
        self.assertNotIn("stop_output", facts)
        self.assertFalse((store.directory / "stop-output.json").exists())
        self.assertFalse((store.directory / "ended.json").exists())
        self.assertEqual(facts["launcher_exit"], 1)
        self.assertEqual(read["status"], "INCOMPLETE")

    def test_an_ambiguous_audit_failure_then_a_stdout_residue_still_ends_nothing(self):
        residue = b'BTC5M_EVENT {"x":'
        facts, final, read, store = self.ambiguous_run(self.frames()[0] + residue)
        self.assert_an_ambiguous_stop_never_ends(facts, final, read, store)

    def test_an_ambiguous_audit_failure_then_a_stderr_line_still_ends_nothing(self):
        line = b'BTC5M_EVENT {"not":"a live event"}\n'
        facts, final, read, store = self.ambiguous_run(self.frames()[0], line)
        self.assert_an_ambiguous_stop_never_ends(facts, final, read, store)

    def test_an_ambiguous_audit_failure_then_an_oversize_line_still_ends_nothing(self):
        oversize = b"BTC5M_EVENT " + b"x" * (control.MAX_LINE_BYTES + 4096) + b"\n"
        facts, final, read, store = self.ambiguous_run(self.frames()[0] + oversize)
        self.assert_an_ambiguous_stop_never_ends(facts, final, read, store)

    def test_an_ambiguous_audit_failure_never_ends_even_when_the_stop_names_its_code(self):
        # R6 puts the read's own code on the stop. A diagnostic is not a
        # licence to repair an ambiguous audit failure into an ending: the same
        # run still ends nowhere and keeps no fragment record.
        frames = self.native_stream(301, 303)
        facts, final, read, store = self.ambiguous_run(frames[0])
        self.assert_an_ambiguous_stop_never_ends(facts, final, read, store)

    def test_a_failed_startup_read_puts_its_code_on_the_stop_and_the_ending(self):
        # run011's shape: the startup account read failed and only the generic
        # reason was recorded. The code the read held now reaches the stop, the
        # terminal record and ended.json; the reason itself is unchanged.
        frames = self.native_stream(301, 303)
        _facts, final, read, store = self.consume_bytes(b"".join(frames))
        stop = next(event for event in read["events"] if event["event_kind"] == "stop")
        self.assertEqual(stop["payload"]["reason"], "RECONCILIATION_REQUIRED")
        self.assertEqual(stop["payload"]["stop_detail"], "HISTORY_NOT_CLOSED")
        self.assertEqual(final["payload"]["stop_detail"], "HISTORY_NOT_CLOSED")
        self.assertEqual(self.ended(store)["incomplete_detail"], "HISTORY_NOT_CLOSED")
        self.assertEqual(self.ended(store)["exit_facts"]["stop_reason"],
                         "RECONCILIATION_REQUIRED")

    def test_a_legal_final_before_a_trailing_residue_keeps_its_shipped_shape(self):
        # Out of scope here: R3 and R4 both keep the fragment and write no
        # ending for this shape. Pinned so this fix cannot move it either.
        residue = b'BTC5M_EVENT {"x":'
        facts, final, read, store = self.consume_bytes(b"".join(self.frames()) + residue)
        self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
        self.assertEqual(facts["stop_output"], "WRITTEN")
        self.assertEqual(facts["launcher_exit"], 1)
        self.assertIsNone(final)
        self.assertTrue((store.directory / "stop-output.json").exists())
        self.assertFalse((store.directory / "ended.json").exists())
        self.assertEqual(read["status"], "INCOMPLETE")


if __name__ == "__main__":
    unittest.main()
