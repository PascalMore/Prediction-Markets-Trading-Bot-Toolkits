"""R16 consumer regressions using original Rust output, without live venue access."""
from __future__ import annotations

import copy
import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from test_btc5m_live_events import native_stream, metadata, ended_from_native
import btc5m_control as control
import btc5m_live_events as live


# The pipe child replays intact Rust frames, never trading or signing. At an
# ACK it checks the on-disk prefix before emitting any subsequent frame.
_R22_PIPE_CHILD = r'''
import json
import signal
import sys
from pathlib import Path

signal.signal(signal.SIGINT, signal.SIG_IGN)
wire, events_path, fault_at = sys.argv[1:]
fault_at = int(fault_at)
expected = []
for index, frame in enumerate(Path(wire).read_bytes().splitlines(keepends=True)):
    if index == fault_at:
        sys.stdout.buffer.write(b"BTC5M_EVENT {\n")
        sys.stdout.buffer.flush()
        # EOF is the supervisor's stop barrier. Send the original closure and
        # following order frames only after it; no further ACK is allowed.
        if sys.stdin.buffer.read() != b"":
            sys.exit(43)
    sys.stdout.buffer.write(frame)
    sys.stdout.buffer.flush()
    if 0 <= fault_at <= index:
        continue
    event = json.loads(frame[len(b"BTC5M_EVENT "):])
    expected.append(event["event_seq"])
    payload = event["payload"]
    if (event["event_kind"] == "order" and payload["attempt_phase"] == "PREPARED"
            and payload["signed_hash"] is not None and payload["reason"] == "NONE"):
        ack = f"A {event['event_seq']} {payload['signed_hash']}\n".encode("ascii")
        if sys.stdin.buffer.readline() != ack:
            sys.exit(42)
        persisted = [json.loads(line) for line in Path(events_path).read_bytes().splitlines()]
        if [row["event_seq"] for row in persisted] != expected or (
                persisted[-1]["payload"]["signed_hash"] != payload["signed_hash"]):
            sys.exit(44)
'''


def _r22_prepared(event: dict) -> bool:
    payload = event["payload"]
    return (event["event_kind"] == "order" and payload["attempt_phase"] == "PREPARED"
            and payload["signed_hash"] is not None and payload["reason"] == "NONE")


def _r22_new_risk(event: dict) -> bool:
    kind, payload = event["event_kind"], event["payload"]
    return (_r22_prepared(event)
            or (kind == "reservation" and payload["action"] == "RESERVE")
            or (kind == "decision" and payload["decision"]["phase"] == "FINAL_SEND"))


class R16RelationsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.frames = native_stream(
            "claim_wait::the_real_producer_trades_again_after_the_exact_claim_and_the_reader_follows_it"
        )
        cls.events = [live.validate_live_event(frame) for frame in cls.frames]

    def test_historical_order_missing_diagnostic_is_null_but_object_keys_are_required(self) -> None:
        order = copy.deepcopy(next(event for event in self.events if event["event_kind"] == "order"))
        order["payload"].pop("diagnostic")
        self.assertIsNone(live.validate_live_event(order)["payload"]["diagnostic"])
        diagnostic = {
            "stage": "FACTS", "detail": "BOOK_STALE", "checked_at": order["event_time"],
            "checked_elapsed_ms": 10001, "tick_started_elapsed_ms": 0,
            "book_receipt_age_ms": 10001, "spot_receipt_age_ms": 1,
            "account_receipt_age_ms": 1, "book_request_age_ms": 10002,
            "spot_request_age_ms": 2,
        }
        order["payload"]["diagnostic"] = diagnostic
        live.validate_live_event(order)
        for key in diagnostic:
            broken = copy.deepcopy(order)
            del broken["payload"]["diagnostic"][key]
            with self.subTest(missing=key), self.assertRaises(live.LiveInvalid):
                live.validate_live_event(broken)
        for value in (True, -1, 1.5):
            broken = copy.deepcopy(order)
            broken["payload"]["diagnostic"]["book_receipt_age_ms"] = value
            with self.subTest(age=value), self.assertRaises(live.LiveInvalid):
                live.validate_live_event(broken)

    def test_diagnostic_unknown_and_duplicate_keys_are_refused(self) -> None:
        order = copy.deepcopy(next(event for event in self.events if event["event_kind"] == "order"))
        order["payload"]["diagnostic"] = {"stage": "FACTS", "detail": "NONE", "checked_at": None,
            "checked_elapsed_ms": None, "tick_started_elapsed_ms": None, "book_receipt_age_ms": None,
            "spot_receipt_age_ms": None, "account_receipt_age_ms": None, "book_request_age_ms": None,
            "spot_request_age_ms": None}
        live.validate_live_event(order)
        broken = copy.deepcopy(order)
        broken["payload"]["diagnostic"]["unexpected"] = 0
        with self.assertRaises(live.LiveInvalid):
            live.validate_live_event(broken)
        duplicate = json.dumps(order).replace('"stage": "FACTS"', '"stage": "FACTS", "stage": "FACTS"')
        with self.assertRaises(live.LiveInvalid):
            live.validate_live_event(duplicate.encode())

    def test_first_hard_stop_cannot_be_renamed_by_a_later_cash_stop(self) -> None:
        relation = live.LiveRelations(self.events[0]["run_id"], self.events[0]["risk_window_id"])
        sequence = 0
        for original in self.events:
            if original["event_kind"] == "stop":
                hard = copy.deepcopy(original)
                hard["payload"]["reason"] = "DATA_UNAVAILABLE"
                hard["payload"]["stop_detail"] = "READ_FAILED"
                sequence += 1
                hard["event_seq"] = sequence
                relation = relation.stage(hard)
            event = copy.deepcopy(original)
            sequence += 1
            event["event_seq"] = sequence
            relation = relation.stage(event)
            if event["event_kind"] == "payout" and event["payload"]["status"] == "CASH_CONFIRMED":
                self.assertFalse(relation.cash_pending)
                self.assertTrue(relation.stopped)
                self.assertEqual(relation.stop_reason, "DATA_UNAVAILABLE")
                break
        else:
            self.fail("original producer supplied no confirmed cash")

    def test_one_atom_cash_mutation_cannot_pass_the_relation_index(self) -> None:
        relation = live.LiveRelations(self.events[0]["run_id"], self.events[0]["risk_window_id"])
        for event in self.events:
            if event["event_kind"] == "payout" and event["payload"]["status"] == "CASH_CONFIRMED":
                for offset in (-1, 1):
                    broken = copy.deepcopy(event)
                    cash = str(int(broken["payload"]["claim_value_atoms"]) + offset)
                    broken["payload"]["claim_value_atoms"] = cash
                    broken["payload"]["cash_received_atoms"] = cash
                    with self.subTest(offset=offset), self.assertRaises(live.LiveInvalid):
                        relation.stage(broken)
                return
            relation = relation.stage(event)
        self.fail("original producer supplied no confirmed cash")


    def test_actual_account_adapter_two_orders_have_one_complete_original_reader_run(self) -> None:
        names = (
            ("actual_adapter_result_first_cash_then_second_real_post", 2, 2),
            ("settlement_http503_recovers_and_trades_again", 2, 2),
            ("category_http429_account_recovers_and_trades_again", 2, 1),
            ("original_drive_category_fault_recovers_without_restart_or_duplicate_post", 2, 1),
            ("actual_adapter_cash_first_result_then_second_real_post", 2, 2),
            ("original_drive_result_first_recovers_and_posts_without_restart", 2, 2),
            ("original_drive_cash_first_recovers_and_posts_without_restart", 2, 2),
            ("actual_adapter_two_claims_paid_separately_keep_wait_until_every_owner_closes", 2, 2),
            ("actual_adapter_zero_payout_removes_inventory_without_inventing_a_cash_claim", 1, 0),
            ("closed_reread_preserves_final_without_reapplying_or_stopping", 1, 1),
        )
        for fixture, attempts, confirmed in names:
            with self.subTest(fixture=fixture):
                name = "bot::runtime::live_control::r16_cash_drive::" + fixture
                result = subprocess.run(
                    ["cargo", "test", "--locked", "--offline", "--target-dir", os.environ["CARGO_TARGET_DIR"],
                     "--lib", name, "--", "--exact", "--nocapture"],
                    cwd=Path(__file__).resolve().parents[3], capture_output=True, timeout=120,
                )
                self.assertEqual(result.returncode, 0, result.stderr.decode() + result.stdout.decode())
                self.assertIn(b"1 passed", result.stdout)
                frames = [line + b"\n" for line in result.stdout.splitlines() if line.startswith(live.EVENT_PREFIX)]
                self.assertTrue(frames)
                with tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"], prefix="r16-adapter-") as root:
                    store = live.LiveEventStore(Path(root) / "run", *metadata(frames[0]))
                    try:
                        for frame in frames:
                            store.append(frame)
                        final = store.relations.final
                        self.assertIsNotNone(final)
                        self.assertTrue(final["payload"]["cash_complete"])
                        # Original explicit DEADLINE stop retains its conservative handoff flag.
                        self.assertTrue(final["payload"]["takeover_required"])
                        self.assertEqual(final["payload"]["stop_reason"], "DEADLINE")
                        self.assertEqual(final["payload"]["positions"], [])
                        self.assertEqual(len(store.relations.attempts), attempts)
                        self.assertEqual(sum(item["confirmed"] for item in store.relations.claims.values()), confirmed)
                        ended = ended_from_native(final)
                        ended["incomplete_reason"] = None
                        store.finish(ended)
                        read = live.read_live_run(store.directory)
                        self.assertEqual(read["status"], "COMPLETE", read["reasons"])
                        if fixture == "closed_reread_preserves_final_without_reapplying_or_stopping":
                            events = [live.validate_live_event(frame) for frame in frames]
                            executions = [e["payload"] for e in events if e["event_kind"] == "execution"]
                            proven = next(i for i, e in enumerate(executions) if e["closure"] == "FINAL")
                            degraded = [e for e in executions[proven + 1:] if e["closure"] == "UNPROVEN"]
                            self.assertEqual(len(degraded), 2)
                            self.assertTrue(all(not e["pages_complete"] for e in degraded))
                            self.assertTrue(all(all(v is None for v in e["delta"].values()) for e in degraded))
                            self.assertEqual(executions[-1]["closure"], "FINAL")
                            self.assertEqual(set(executions[-1]["delta"].values()), {"0"})
                            self.assertEqual([e["payload"]["reason"] for e in events if e["event_kind"] == "stop"],
                                             ["CASH_PENDING", "DEADLINE"])
                    finally:
                        store.close()


    def _r22_frames(self, fixture: str) -> list[bytes]:
        name = "bot::runtime::live_control::r16_cash_drive::" + fixture
        result = subprocess.run(
            ["cargo", "test", "--locked", "--offline", "--target-dir", os.environ["CARGO_TARGET_DIR"],
             "--lib", name, "--", "--exact", "--nocapture"],
            cwd=Path(__file__).resolve().parents[3], capture_output=True, timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode() + result.stdout.decode())
        self.assertIn(b"1 passed", result.stdout)
        frames = [line for line in result.stdout.splitlines(keepends=True)
                  if line.startswith(live.EVENT_PREFIX)]
        self.assertTrue(frames)
        return frames

    def _supervise_r22(self, frames: list[bytes], *, fault_at: int = -1) -> tuple:
        with tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"], prefix="r22-pipe-") as root:
            directory = Path(root) / "run"
            wire = Path(root) / "native.txt"
            wire.write_bytes(b"".join(frames))
            store = live.LiveEventStore(directory, *metadata(frames[0]))
            try:
                process = subprocess.Popen(
                    [sys.executable, "-B", "-c", _R22_PIPE_CHILD, str(wire),
                     str(directory / "events.jsonl"), str(fault_at)],
                    env={"PATH": "/usr/bin:/bin"}, stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                now = datetime.datetime.now(datetime.timezone.utc)
                facts, final = control.supervise(process, store, now, control.boottime(),
                                                 run_seconds=10)
                control.finish_live_evidence(store, facts, final)
                read = live.read_live_run(directory)
                persisted = (directory / "events.jsonl").read_bytes()
                self.assertEqual(persisted, b"".join(live.canonical(event) + b"\n"
                                                    for event in read["events"]))
                self.assertEqual(facts["events"], len(read["events"]))
                self.assertEqual(facts["child_returncode"], 0, (facts, read["reasons"]))
                self.assertFalse(facts["forced_cleanup"])
                return facts, final, read, len(store.relations.attempts)
            finally:
                store.close()

    def test_r22_original_drive_stream_and_irreversible_stop(self) -> None:
        frames = self._r22_frames("r22_incident_second_probe_then_original_send")
        facts, final, read, attempts = self._supervise_r22(frames)
        self.assertEqual(facts["launcher_exit"], 0, facts)
        self.assertIsNotNone(final)
        self.assertEqual(final["payload"]["stop_reason"], "DEADLINE")
        self.assertTrue(final["payload"]["takeover_required"])
        self.assertTrue(final["payload"]["cash_complete"])
        self.assertEqual(final["payload"]["positions"], [])
        self.assertEqual(attempts, 2)
        self.assertEqual(read["status"], "COMPLETE", read["reasons"])
        events = read["events"]
        self.assertEqual(events, [live.validate_live_event(frame) for frame in frames])
        target = [event for event in events if event["event_kind"] == "payout"
                  and event["token"] == "77"]
        self.assertEqual([event["payload"]["status"] for event in target],
                         ["CASH_PENDING", "CASH_CONFIRMED"])
        self.assertEqual(target[-1]["payload"]["cash_received_atoms"], "2332400000000000000")
        send = next(event for event in events if event["market_id"] == "2"
                    and event["event_kind"] == "decision"
                    and event["payload"]["decision"]["phase"] == "FINAL_SEND")
        prepared = [event for event in events if _r22_prepared(event)]
        self.assertEqual([event["market_id"] for event in prepared], ["1", "2"])
        fault_at = events.index(target[-1])
        self.assertLess(prepared[0]["event_seq"], target[-1]["event_seq"])
        self.assertLess(target[-1]["event_seq"], prepared[1]["event_seq"])
        self.assertLess(target[-1]["event_seq"], send["event_seq"])

        # After the malformed line closes the control channel, replay the exact
        # remaining bytes (including closure, PREPARED, FINAL_SEND and final).
        facts, final, read, attempts = self._supervise_r22(frames, fault_at=fault_at)
        self.assertEqual((facts["launcher_exit"], facts["stop_reason"], facts["stop_output"]),
                         (1, "OUTPUT_INVALID", "WRITTEN"))
        self.assertIsNone(final)
        self.assertEqual(attempts, 1)
        self.assertEqual(read["events"], events[:fault_at])
        self.assertEqual(read["status"], "INCOMPLETE", read["reasons"])
        ended = read["ended"]
        self.assertEqual(ended["exit_facts"]["stop_reason"], "OUTPUT_INVALID")
        self.assertEqual(ended["incomplete_reason"], "AUDIT_UNAVAILABLE")
        self.assertIsNone(ended["final_event_seq"])
        self.assertFalse(ended["cash_complete"])
        self.assertTrue(ended["takeover_required"])

        late_frames = self._r22_frames("r22_refuses_conflicts_and_late_proof")
        facts, final, read, attempts = self._supervise_r22(late_frames)
        self.assertEqual(facts["launcher_exit"], 0, facts)
        self.assertEqual(read["events"], [live.validate_live_event(frame) for frame in late_frames])
        stops = [event for event in read["events"] if event["event_kind"] == "stop"]
        self.assertTrue(stops)
        for stop in stops:
            self.assertEqual(stop["payload"]["reason"], "RECONCILIATION_REQUIRED")
            self.assertEqual(stop["payload"]["stop_detail"], "INVENTORY_MISMATCH")
        hard_stop = stops[0]
        self.assertFalse(any(_r22_new_risk(event) for event in read["events"]
                             if event["event_seq"] > hard_stop["event_seq"]))
        self.assertFalse(any(event["event_kind"] == "payout"
                             and event["payload"]["status"] == "CASH_CONFIRMED"
                             for event in read["events"]))
        self.assertIsNotNone(final)
        self.assertEqual(final["payload"]["stop_reason"], "RECONCILIATION_REQUIRED")
        self.assertTrue(final["payload"]["takeover_required"])
        self.assertFalse(final["payload"]["cash_complete"])
        self.assertEqual(attempts, 1)
        self.assertEqual(read["status"], "INCOMPLETE", read["reasons"])
        self.assertEqual(read["ended"]["exit_facts"]["stop_reason"], "RECONCILIATION_REQUIRED")
        self.assertEqual(read["ended"]["incomplete_detail"], "INVENTORY_MISMATCH")
        self.assertEqual(read["ended"]["incomplete_reason"], "FACT_INCOMPLETE")
        reserve = next(event for event in events if event["market_id"] == "2"
                       and event["event_kind"] == "reservation"
                       and event["payload"]["action"] == "RESERVE")
        for proposal in (reserve, send):
            with tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"], prefix="r22-refuse-") as root:
                store = live.LiveEventStore(Path(root) / "run", *metadata(frames[0]))
                try:
                    index = events.index(proposal)
                    for frame in frames[:index]:
                        store.append(frame)
                    # Only the hard-stop fact comes from the second native run.
                    # Sequence, time and accounting belong to this valid prefix.
                    context = events[index - 1]
                    stop = dict(hard_stop, event_seq=proposal["event_seq"],
                                event_time=context["event_time"], accounting=context["accounting"])
                    store.append(live.EVENT_PREFIX + json.dumps(stop).encode() + b"\n")
                    event_path = store.directory / "events.jsonl"
                    before = event_path.read_bytes()
                    acks = []
                    after_stop = dict(proposal, event_seq=proposal["event_seq"] + 1)
                    with self.assertRaisesRegex(live.LiveInvalid, "OUTPUT_INVALID"):
                        store.append(live.EVENT_PREFIX + json.dumps(after_stop).encode() + b"\n", acks.append)
                    self.assertEqual(acks, [])
                    self.assertEqual(event_path.read_bytes(), before)
                finally:
                    store.close()

    def test_r22_independent_cash_is_audited_once_per_claim(self) -> None:
        frames = self._r22_frames("r22_independent_c_cash_requires_its_own_redemption")
        batches: list[list[bytes]] = [[]]
        for frame in frames:
            batches[-1].append(frame)
            if live.validate_live_event(frame)["event_kind"] == "final":
                batches.append([])
        self.assertEqual(len(batches), 3)
        self.assertEqual(batches[-1], [])
        for stream, success in zip(batches[:2], (True, False)):
            with self.subTest(success=success), tempfile.TemporaryDirectory(
                dir=os.environ["TMPDIR"], prefix="r22-independent-",
            ) as root:
                store = live.LiveEventStore(Path(root) / "run", *metadata(stream[0]))
                try:
                    confirmed = []
                    for frame in stream:
                        event = store.append(frame)
                        if (event["event_kind"] == "payout"
                                and event["payload"]["status"] == "CASH_CONFIRMED"):
                            confirmed.append((event["token"], event["payload"]["cash_received_atoms"]))
                    final = store.relations.final
                    assert final is not None
                    self.assertEqual(sorted(confirmed),
                                     [("177", "2475000000000000000"), ("77", "2475000000000000000")]
                                     if success else [])
                    self.assertEqual(final["payload"]["cash_complete"], success)
                    self.assertEqual(final["payload"]["stop_reason"],
                                     "DEADLINE" if success else "RECONCILIATION_REQUIRED")
                    ended = ended_from_native(final)
                    if success:
                        ended["incomplete_reason"] = None
                    store.finish(ended)
                    self.assertEqual(live.read_live_run(store.directory)["status"],
                                     "COMPLETE" if success else "INCOMPLETE")
                finally:
                    store.close()

    def test_native_adapter_rejection_details_are_strict_valid_projections(self) -> None:
        cases = (
            ("original_limit_depth_and_empty_book_branches_have_typed_preparation_details",
             {("PREPARE", "LIMIT_PRICE"), ("PREPARE", "DEPTH"), ("PREPARE", "EMPTY_BOOK")}),
            ("a_post_failure_is_unknown_not_a_retryable_local_candidate_refusal",
             {("POST", "SUBMISSION_UNKNOWN")}),
        )
        for name, expected in cases:
            result = subprocess.run(
                ["cargo", "test", "--locked", "--offline", "--target-dir", os.environ["CARGO_TARGET_DIR"],
                 "--lib", "bot::runtime::live_control::r16_diagnostics::" + name,
                 "--", "--exact", "--nocapture"],
                cwd=Path(__file__).resolve().parents[3], capture_output=True, timeout=120,
            )
            self.assertEqual(result.returncode, 0, result.stderr.decode() + result.stdout.decode())
            observed = set()
            for line in result.stdout.splitlines(keepends=True):
                if line.startswith(live.EVENT_PREFIX):
                    event = live.validate_live_event(line)
                    if event["event_kind"] == "order" and event["payload"]["diagnostic"]:
                        diagnostic = event["payload"]["diagnostic"]
                        observed.add((diagnostic["stage"], diagnostic["detail"]))
            self.assertTrue(expected <= observed, (expected, observed))


if __name__ == "__main__":
    unittest.main()
