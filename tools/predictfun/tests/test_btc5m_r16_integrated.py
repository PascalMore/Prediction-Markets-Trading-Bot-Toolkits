"""R16 consumer regressions using original Rust output, without live venue access."""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from test_btc5m_live_events import native_stream, metadata, ended_from_native
import btc5m_live_events as live


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
