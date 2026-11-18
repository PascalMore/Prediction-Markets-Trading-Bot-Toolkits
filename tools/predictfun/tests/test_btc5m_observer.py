"""The observer consumer's own tests.

They check the strictness of the reader, the as-of relation between the two
channels, and that a broken observation never changes the trading evidence.
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import btc5m_observer as observer  # noqa: E402


RUN_ID = "observer-test"


def clock(received="2026-11-18T00:00:01.000Z", request="2026-11-18T00:00:00.000Z"):
    return {
        "source_at": None,
        "source_time_status": "MISSING",
        "freshness_basis": "RECEIPT_BOUND",
        "request_at": request,
        "received_at": received,
        "request_elapsed_ms": 0,
        "received_elapsed_ms": 1000,
    }


def record(seq=1, market_id="200", token="1001", **overrides):
    """One valid observation record."""
    data = {
        "schema_version": observer.RECORD_SCHEMA_VERSION,
        "run_id": RUN_ID,
        "observation_seq": seq,
        "market_id": market_id,
        "token": token,
        "sample_state": "ACTIVE",
        "observed_at": "2026-11-18T00:00:01.000Z",
        "observed_elapsed_ms": 1000,
        "starts_at": "2026-11-18T00:00:00.000Z",
        "ends_at": "2026-11-18T00:05:00.000Z",
        "secs_left": 299.0,
        "strike": 100.0,
        "position_net_shares": 3.0,
        "position_cost": 1.5,
        "prediction_source": "Binance",
        "actual_pair": "BTCUSDT",
        "source_state": "PRIMARY",
        "source_epoch": 1,
        "reset_reason": "FIRST_SAMPLE",
        "settlement_provider": "CHAINLINK",
        "spot": 100.2,
        "spot_clock": clock(),
        "book_clock": clock(),
        "samples": 20,
        "sigma_raw": 0.0006,
        "sigma": 0.0006,
        "p_up": 0.7,
        "p_down": 0.3,
        "model_reason": "OK",
        "up_bid": 0.4,
        "up_ask": 0.5,
        "down_bid": 0.5,
        "down_ask": 0.6,
        "down_quote_basis": "MIRRORED_YES_BOOK",
        "exit_bid_levels": [{"price": 0.4, "size": 10.0}],
        "exit_covered_shares": 3.0,
        "exit_depth_complete": True,
        "exit_vwap": 0.4,
        "exit_gross_income": 1.2,
        "exit_reason": "OK",
        "fee_basis": "PAPER_CASH_FEE",
        "fee_rate_bps": 200,
        "exit_fee": 0.024,
        "exit_net_income": 1.176,
        "exit_pnl_vs_cost": -0.324,
        "model_hold_value": 2.1,
        "model_hold_surplus": 0.6,
    }
    data.update(overrides)
    return data


def fill_event(seq=1, market_id="200", token="1001", net_shares=3.0, cost=1.5):
    """One trading fill, as the v2 protocol writes it."""
    return {
        "run_id": RUN_ID,
        "event_kind": "fill",
        "event_seq": seq,
        "market_id": market_id,
        "token": token,
        "source": {"side": "buy", "net_shares": net_shares, "cost": cost},
    }


def market_event():
    sample = record()
    return {"event_kind": "summary", "event_seq": 0, "market_id": "200", "token": None,
            "source": {"decision": {"strike": sample["strike"], "starts_at": sample["starts_at"],
                       "ends_at": sample["ends_at"], "up": {"token": "1001"},
                       "down": {"token": "1002"}, "fee_rate_bps": 200}}}


def producer(attempted=1, sent=1, last_seq=1, series=None, dropped=0, disabled=None):
    """The producer's closing report."""
    return {
        "attempted_samples": attempted,
        "encoded_samples": attempted,
        "sent_samples": sent,
        "dropped_samples": dropped,
        "clock_unavailable_samples": 0,
        "encode_failed_samples": 0,
        "send_failed_samples": 0,
        "last_observation_seq": last_seq,
        "disabled_reason": disabled,
        "series": series if series is not None else [],
    }


class RecordValidationTest(unittest.TestCase):
    def test_a_valid_record_survives_the_allowlist(self):
        rebuilt = observer.validate_record(record(), RUN_ID)
        self.assertEqual(rebuilt["observation_seq"], 1)
        self.assertEqual(rebuilt["exit_vwap"], 0.4)

    def test_an_unknown_key_is_refused(self):
        data = record()
        data["extra"] = 1
        with self.assertRaises(observer.ObserverRefused):
            observer.validate_record(data, RUN_ID)

    def test_a_missing_key_is_refused(self):
        data = record()
        del data["exit_vwap"]
        with self.assertRaises(observer.ObserverRefused):
            observer.validate_record(data, RUN_ID)

    def test_a_duplicate_key_is_refused(self):
        raw = b'{"schema_version":"x","schema_version":"y"}'
        with self.assertRaises(ValueError):
            json.loads(raw.decode(), object_pairs_hook=observer._unique_object)

    def test_a_bool_cannot_pass_for_a_number(self):
        with self.assertRaises(observer.ObserverRefused):
            observer.validate_record(record(observed_elapsed_ms=True), RUN_ID)

    def test_a_non_finite_number_is_refused(self):
        data = record()
        data["p_up"] = float("nan")
        with self.assertRaises(observer.ObserverRefused):
            observer.validate_record(data, RUN_ID)

    def test_a_wrong_run_is_refused(self):
        with self.assertRaises(observer.ObserverRefused):
            observer.validate_record(record(run_id="other-run"), RUN_ID)

    def test_a_broken_probability_pair_is_refused(self):
        with self.assertRaises(observer.ObserverRefused):
            observer.validate_record(record(p_down=0.2), RUN_ID)

    def test_a_broken_gross_formula_is_refused(self):
        with self.assertRaises(observer.ObserverRefused):
            observer.validate_record(record(exit_gross_income=9.9), RUN_ID)

    def test_a_broken_fee_formula_is_refused(self):
        with self.assertRaises(observer.ObserverRefused):
            observer.validate_record(record(exit_fee=9.9), RUN_ID)

    def test_an_unknown_fee_must_not_carry_a_rate(self):
        with self.assertRaises(observer.ObserverRefused):
            observer.validate_record(record(fee_basis="UNKNOWN"), RUN_ID)

    def test_a_model_reason_without_a_value_is_refused(self):
        with self.assertRaises(observer.ObserverRefused):
            observer.validate_record(record(model_reason="WARMUP"), RUN_ID)

    def test_an_incomplete_depth_carries_no_full_value(self):
        data = record(exit_depth_complete=False, exit_vwap=None, exit_gross_income=None,
                      exit_fee=None, exit_net_income=None, exit_pnl_vs_cost=None,
                      exit_reason="INSUFFICIENT_DEPTH", fee_basis="UNKNOWN",
                      fee_rate_bps=None, exit_bid_levels=[{"price": 0.4, "size": 2.0}],
                      exit_covered_shares=2.0)
        rebuilt = observer.validate_record(data, RUN_ID)
        self.assertIsNone(rebuilt["exit_vwap"])

    def test_an_incomplete_depth_with_a_full_value_is_refused(self):
        with self.assertRaises(observer.ObserverRefused):
            observer.validate_record(record(exit_depth_complete=False), RUN_ID)


class RelationTest(unittest.TestCase):
    def store(self, directory):
        store = observer.ObservationStore(RUN_ID, Path(directory))
        store.open()
        return store

    def test_a_sample_waits_for_its_fill(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self.store(directory)
            receiver = observer.ObserverReceiver(store, observer.TradeIndex())
            receiver.receive({"wire_version": observer.WIRE_VERSION, "kind": "READY",
                              "run_id": RUN_ID})
            receiver.receive({"wire_version": observer.WIRE_VERSION, "kind": "SAMPLE",
                              "run_id": RUN_ID, "after_event_seq": 1,
                              "record": record()})
            # The fill has not been seen yet: nothing is written.
            self.assertEqual(store.written_samples, 0)
            receiver.note_trade_event(market_event())
            receiver.note_trade_event(fill_event())
            self.assertEqual(store.written_samples, 1)
            store.close()

    def test_a_sample_without_a_fill_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self.store(directory)
            receiver = observer.ObserverReceiver(store, observer.TradeIndex())
            receiver.note_trade_event(fill_event(seq=1, market_id="999", token="9999"))
            receiver.receive({"wire_version": observer.WIRE_VERSION, "kind": "SAMPLE",
                              "run_id": RUN_ID, "after_event_seq": 1,
                              "record": record()})
            self.assertEqual(store.written_samples, 0)
            self.assertEqual(store.rejected_samples, 1)
            store.close()

    def test_a_position_that_does_not_match_the_fill_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self.store(directory)
            receiver = observer.ObserverReceiver(store, observer.TradeIndex())
            receiver.note_trade_event(fill_event(net_shares=5.0))
            receiver.receive({"wire_version": observer.WIRE_VERSION, "kind": "SAMPLE",
                              "run_id": RUN_ID, "after_event_seq": 1,
                              "record": record()})
            self.assertEqual(store.written_samples, 0)
            store.close()

    def test_a_bad_frame_is_counted_and_never_written(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self.store(directory)
            receiver = observer.ObserverReceiver(store, observer.TradeIndex())
            receiver.receive({"wire_version": observer.WIRE_VERSION, "kind": "SAMPLE",
                              "run_id": RUN_ID, "after_event_seq": 1,
                              "record": record(exit_gross_income=9.9)})
            self.assertEqual(store.written_samples, 0)
            self.assertEqual(store.rejected_samples, 1)
            self.assertIn("RELATION_INVALID", store.reasons)
            store.close()

    def test_the_end_report_gaps_the_sequence(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self.store(directory)
            receiver = observer.ObserverReceiver(store, observer.TradeIndex())
            receiver.receive({"wire_version": observer.WIRE_VERSION, "kind": "END",
                              "run_id": RUN_ID, "producer": producer(attempted=3, sent=3, last_seq=3)})
            observer.coverage_payload(store)
            self.assertEqual(store.sequence_gaps, 3)
            self.assertIn("SEQUENCE_GAP", store.reasons)
            store.close()


class CoverageTest(unittest.TestCase):
    def test_a_complete_run_reports_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            store = observer.ObservationStore(RUN_ID, Path(directory))
            store.ready_received = True
            store.end_received = True
            store.producer = producer(attempted=0, sent=0, last_seq=0)
            payload = observer.coverage_payload(store)
            self.assertEqual(payload["status"], "COMPLETE")
            self.assertEqual(payload["enabled"], True)
            self.assertEqual(payload["transport"], "UNIX_DGRAM")

    def test_a_missing_end_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            store = observer.ObservationStore(RUN_ID, Path(directory))
            store.ready_received = True
            payload = observer.coverage_payload(store)
            self.assertEqual(payload["status"], "INCOMPLETE")

    def test_the_coverage_file_is_written_once(self):
        with tempfile.TemporaryDirectory() as directory:
            store = observer.ObservationStore(RUN_ID, Path(directory))
            store.ready_received = True
            store.end_received = True
            store.producer = producer()
            self.assertTrue(observer.write_coverage(Path(directory), store))
            self.assertFalse(observer.write_coverage(Path(directory), store))


class ReaderTest(unittest.TestCase):
    def test_a_run_without_observations_is_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            result = observer.read_observations(Path(directory))
            self.assertEqual(result["status"], "OBSERVATIONS_UNAVAILABLE")
            self.assertEqual(result["records"], [])

    def test_bare_record_files_are_not_authenticated_without_trading_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            store = observer.ObservationStore(RUN_ID, Path(directory))
            store.open()
            store.write(record())
            store.close()
            store.ready_received = True
            store.end_received = True
            store.producer = producer()
            observer.write_coverage(Path(directory), store)
            result = observer.read_observations(Path(directory))
            self.assertEqual(result["status"], "INCOMPLETE")
            self.assertEqual(result["records"], [])
            self.assertIn("TRADING_EVIDENCE_INVALID", result["reasons"])

    def test_the_reader_rejects_a_broken_line(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / observer.OBSERVATIONS_FILE
            path.write_text(json.dumps(record()) + "\n" + "{not json}\n")
            result = observer.read_observations(Path(directory))
            self.assertEqual(result["status"], "INCOMPLETE")
            self.assertEqual(len(result["records"]), 0)
            self.assertIn("INVALID_FRAME", result["reasons"])

    def test_the_reader_never_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            before = sorted(os.listdir(directory))
            observer.read_observations(Path(directory))
            self.assertEqual(sorted(os.listdir(directory)), before)


class ChannelTest(unittest.TestCase):
    def test_the_channel_is_an_anonymous_datagram_pair(self):
        parent, child = observer.create_socketpair()
        try:
            self.assertEqual(parent.family, socket.AF_UNIX)
            self.assertEqual(parent.type, socket.SOCK_DGRAM)
            child.send(b'{"kind":"READY"}\n')
            self.assertEqual(parent.recv(4096), b'{"kind":"READY"}\n')
        finally:
            parent.close()
            child.close()

    def test_the_child_descriptor_is_named(self):
        self.assertEqual(observer.child_fd_environment(7),
                         {"BTC5M_POST_ENTRY_FD": "7"})


class LauncherFlagTest(unittest.TestCase):
    def test_the_flag_is_off_by_default(self):
        import btc5m_control as control
        args = control.build_parser().parse_args(["--mode", "unsigned-paper",
                                                 "--config", "c", "--run-id", "r"])
        self.assertFalse(args.post_entry_observer)

    def test_the_flag_can_be_set(self):
        import btc5m_control as control
        args = control.build_parser().parse_args(["--mode", "unsigned-paper",
                                                 "--config", "c", "--run-id", "r",
                                                 "--post-entry-observer"])
        self.assertTrue(args.post_entry_observer)

    def test_a_signed_run_refuses_the_flag(self):
        import btc5m_control as control
        with self.assertRaises(control.Refused) as caught:
            control.run(["--mode", "signed-future-live", "--config", "c",
                         "--run-id", "r", "--post-entry-observer",
                         "--predict-budget-confirmation", "x",
                         "--predict-rpc-url", "https://example.invalid"])
        self.assertEqual(caught.exception.reason, "OBSERVER_REFUSED")


class StrictCounterexampleTest(unittest.TestCase):
    def receiver(self, directory):
        store = observer.ObservationStore(RUN_ID, Path(directory))
        store.open()
        self.addCleanup(store.close)
        receiver = observer.ObserverReceiver(store, observer.TradeIndex())
        receiver.note_trade_event(market_event())
        receiver.note_trade_event(fill_event())
        return receiver

    def sample(self, data=None, watermark=1):
        return {"wire_version": observer.WIRE_VERSION, "kind": "SAMPLE", "run_id": RUN_ID,
                "after_event_seq": watermark, "record": data or record()}

    def test_all_required_nulls_and_nested_unknown_fields_are_refused(self):
        required = ("observation_seq", "sample_state", "observed_at", "observed_elapsed_ms",
                    "samples", "model_reason", "source_state", "down_quote_basis")
        for name in required:
            with self.subTest(name=name), self.assertRaises(observer.ObserverRefused):
                observer.validate_record(record(**{name: None}), RUN_ID)
        for name in ("spot_clock", "book_clock"):
            data = record()
            data[name]["arbitrary"] = "not-allowed"
            with self.assertRaises(observer.ObserverRefused):
                observer.validate_record(data, RUN_ID)

    def test_envelope_unknown_fields_and_duplicate_ready_end_are_refused(self):
        for kind in ("READY", "SAMPLE", "END"):
            with tempfile.TemporaryDirectory() as directory:
                receiver = self.receiver(directory)
                frame = {"wire_version": observer.WIRE_VERSION, "kind": kind, "run_id": RUN_ID,
                         "unexpected": 1}
                if kind == "SAMPLE": frame.update(record=record(), after_event_seq=1)
                if kind == "END": frame.update(producer=producer())
                receiver.receive(frame)
                self.assertEqual(receiver.store.written_samples, 0)
                self.assertIn("INVALID_FRAME", receiver.store.reasons)
        with tempfile.TemporaryDirectory() as directory:
            receiver = self.receiver(directory)
            ready = {"wire_version": observer.WIRE_VERSION, "kind": "READY", "run_id": RUN_ID}
            receiver.receive(ready)
            receiver.receive(ready)
            receiver.receive({"wire_version": observer.WIRE_VERSION, "kind": "END", "run_id": RUN_ID,
                              "producer": producer(attempted=0, sent=0, last_seq=0)})
            receiver.receive(self.sample())
            self.assertEqual(receiver.store.rejected_samples, 1)
            self.assertEqual(receiver.store.written_samples, 0)

    def test_cost_identity_null_inventory_and_descending_sequences_never_first_write(self):
        for changes in ({"position_cost": 1.6, "exit_pnl_vs_cost": -.424, "model_hold_surplus": .5},
                        {"strike": 101.0}, {"token": "999"}):
            with tempfile.TemporaryDirectory() as directory:
                receiver = self.receiver(directory)
                receiver.receive(self.sample(record(**changes)))
                self.assertEqual(receiver.store.written_samples, 0)
                self.assertEqual((Path(directory) / observer.OBSERVATIONS_FILE).read_text(), "")
        with tempfile.TemporaryDirectory() as directory:
            receiver = self.receiver(directory)
            receiver.receive(self.sample(record(seq=3)))
            receiver.receive(self.sample(record(seq=2)))
            receiver.receive(self.sample(record(seq=3)))
            self.assertEqual(receiver.store.written_samples, 1)
            self.assertEqual(receiver.store.rejected_samples, 2)
            self.assertIn("SEQUENCE_INVALID", receiver.store.reasons)

    def test_fifo_capacity_drops_current_frame_and_counts_actual_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            receiver = self.receiver(directory)
            for seq in range(1, 18):
                receiver.receive(self.sample(record(seq=seq), watermark=100))
            self.assertEqual(len(receiver.buffer), 16)
            self.assertGreater(receiver.buffer_bytes, 16)
            self.assertEqual(receiver.store.unwritten_samples, 1)
            self.assertEqual(receiver.buffer[0][1]["observation_seq"], 1)
            receiver.finish()
            self.assertEqual(receiver.store.rejected_samples, 16)
            self.assertEqual(receiver.buffer_bytes, 0)
        with tempfile.TemporaryDirectory() as directory, patch.object(observer, "MAX_BUFFERED_BYTES", 1):
            receiver = self.receiver(directory)
            receiver.receive(self.sample(watermark=100))
            self.assertEqual(receiver.buffer, [])
            self.assertEqual(receiver.store.unwritten_samples, 1)

    def test_as_of_active_does_not_see_later_settlement_and_sell_is_unsupported(self):
        with tempfile.TemporaryDirectory() as directory:
            receiver = self.receiver(directory)
            receiver.note_trade_event({"event_kind": "settlement", "event_seq": 2,
                                       "market_id": "200", "token": "1001", "source": {}})
            receiver.receive(self.sample())
            receiver.receive(self.sample(record(seq=2), watermark=2))
            self.assertEqual(receiver.store.written_samples, 1)
            self.assertEqual(receiver.store.rejected_samples, 1)
        with tempfile.TemporaryDirectory() as directory:
            receiver = self.receiver(directory)
            receiver.note_trade_event({"event_kind": "fill", "event_seq": 2, "market_id": "200",
                                       "token": "1001", "source": {"side": "sell"}})
            receiver.receive(self.sample(watermark=2))
            self.assertIn("UNSUPPORTED_POSITION_TRANSITION", receiver.store.reasons)
            self.assertEqual(receiver.store.written_samples, 0)

    def test_bad_producer_arithmetic_and_unterminated_series_are_incomplete(self):
        for name in ("attempted_samples", "encoded_samples", "dropped_samples", "last_observation_seq"):
            data = producer()
            data[name] += 1
            with self.assertRaises(observer.ObserverRefused): observer.validate_producer(data)
        with tempfile.TemporaryDirectory() as directory:
            receiver = self.receiver(directory)
            receiver.receive(self.sample())
            receiver.receive({"wire_version": observer.WIRE_VERSION, "kind": "END", "run_id": RUN_ID,
                              "producer": producer(series=[{"market_id": "200", "token": "1001",
                                  "terminal_state": None, "terminal_observation_seq": None}])})
            result = observer.coverage_payload(receiver.store)
            self.assertEqual(result["status"], "INCOMPLETE")
            self.assertIn("TERMINAL_MISSING", result["reasons"])

    def test_ieee_epsilon_boundary_keeps_actual_partial_coverage(self):
        data = record(exit_bid_levels=[{"price": .4, "size": 2.999999999}],
                      exit_covered_shares=2.999999999, exit_depth_complete=False,
                      exit_reason="INSUFFICIENT_DEPTH", exit_vwap=None, exit_gross_income=None,
                      fee_basis="UNKNOWN", fee_rate_bps=None, exit_fee=None,
                      exit_net_income=None, exit_pnl_vs_cost=None)
        observer.validate_record(data, RUN_ID)
        data = record(exit_bid_levels=[{"price": .4, "size": 2.9999999995}],
                      exit_covered_shares=2.9999999995)
        self.assertLess(observer.validate_record(data, RUN_ID)["exit_covered_shares"], 3.0)

    def test_zero_fee_unknown_fee_and_correct_no_side_remain_distinct(self):
        observer.validate_record(record(fee_rate_bps=0, exit_fee=0, exit_net_income=1.2,
                                        exit_pnl_vs_cost=-.3), RUN_ID)
        observer.validate_record(record(fee_basis="UNKNOWN", fee_rate_bps=None, exit_fee=None,
                                        exit_net_income=None, exit_pnl_vs_cost=None), RUN_ID)
        with tempfile.TemporaryDirectory() as directory:
            receiver = self.receiver(directory)
            receiver.note_trade_event(fill_event(seq=2, token="1002"))
            data = record(token="1002", model_hold_value=.9, model_hold_surplus=-.6,
                          exit_bid_levels=[{"price": .5, "size": 10}], exit_vwap=.5,
                          exit_gross_income=1.5, exit_fee=.03, exit_net_income=1.47, exit_pnl_vs_cost=-.03)
            receiver.receive(self.sample(data, watermark=2))
            self.assertEqual(receiver.store.written_samples, 1)

    def test_exact_mebibyte_and_frame_boundaries_are_bounded_without_reordering(self):
        with tempfile.TemporaryDirectory() as directory:
            receiver = self.receiver(directory)
            for seq in range(1, 17):
                receiver.receive(self.sample(record(seq=seq), watermark=100), 65536)
            self.assertEqual(receiver.buffer_bytes, 1024 * 1024)
            receiver.receive(self.sample(record(seq=17), watermark=100), 65536)
            self.assertEqual(receiver.store.unwritten_samples, 1)
            receiver.receive(self.sample(record(seq=18), watermark=100), 65537)
            self.assertEqual(receiver.store.rejected_samples, 1)
            self.assertEqual(len(receiver.buffer), 16)
            receiver.finish()
            self.assertEqual(receiver.store.received_samples,
                             receiver.store.written_samples + receiver.store.unwritten_samples + receiver.store.rejected_samples)

    def test_null_inventory_requires_official_settlement_inside_watermark(self):
        data = record(sample_state="SETTLED", position_net_shares=None, position_cost=None,
                      model_reason="SETTLED", exit_reason="SETTLED", fee_basis="UNKNOWN", fee_rate_bps=None)
        for name in ("sigma_raw sigma p_up p_down model_hold_value model_hold_surplus exit_bid_levels "
                     "exit_covered_shares exit_depth_complete exit_vwap exit_gross_income exit_fee "
                     "exit_net_income exit_pnl_vs_cost").split():
            data[name] = None
        with tempfile.TemporaryDirectory() as directory:
            receiver = self.receiver(directory)
            receiver.receive(self.sample(data))
            self.assertEqual(receiver.store.written_samples, 0)
            receiver.note_trade_event({"event_kind": "settlement", "event_seq": 2,
                                       "market_id": "200", "token": "1001", "source": {}})
            data["observation_seq"] = 2
            receiver.receive(self.sample(data, watermark=2))
            self.assertEqual(receiver.store.written_samples, 1)


from test_btc5m_control import LauncherFixture  # noqa: E402


class RealRustConsumerTest(LauncherFixture):
    @classmethod
    def setUpClass(cls):
        import btc5m_control as control
        target = os.environ["CARGO_TARGET_DIR"]
        result = subprocess.run(["cargo", "test", "--offline", "--locked", "--target-dir", target,
                                 "--lib", "--no-run", "--message-format=json"],
                                cwd=control.BOT, capture_output=True, timeout=120)
        if result.returncode:
            raise AssertionError(result.stderr.decode())
        artifacts = [json.loads(line) for line in result.stdout.splitlines() if line.startswith(b"{")]
        executables = [item["executable"] for item in artifacts if item.get("reason") == "compiler-artifact"
                       and item.get("executable") and item["target"]["name"] == "polymarket_toolkits"
                       and item["profile"]["test"]]
        if len(executables) != 1:
            raise AssertionError("exact libtest artifact identity required")
        cls.executable = executables[0]

    def launch(self, root, enabled=True):
        import btc5m_control as control
        validation_errors = []
        original_validate = control.validate_event
        def checked_validate(*args, **kwargs):
            try:
                return original_validate(*args, **kwargs)
            except Exception as error:
                validation_errors.append(str(error))
                raise
        def popen(_argv, **kwargs):
            child = subprocess.Popen([self.executable,
                "bot::arbitrage::observer::tests::post_entry_protocol_consumer_fixture",
                "--exact", "--nocapture", "--test-threads=1"], **kwargs)
            self.children.append(child)
            return child
        args = ["--mode", "unsigned-paper", "--config", str(self.config_path), "--run-id", RUN_ID]
        if enabled: args.append("--post-entry-observer")
        with patch.object(control, "RUN_SECONDS", 60), patch.object(control, "validate_event", side_effect=checked_validate):
            facts = control.run(args, environment=self.environment, evidence_root=root, popen=popen)
        self.assertEqual(facts["launcher_exit"], 0, (facts, validation_errors))
        self.assertEqual(facts["child_returncode"], 0, facts)
        return root / RUN_ID

    def test_actual_rust_writer_original_run_first_write_and_strict_reader(self):
        off = self.launch(self.root / "off", enabled=False)
        on = self.launch(self.root / "on")
        self.assertFalse((off / observer.OBSERVATIONS_FILE).exists())
        self.assertEqual((off / "events.jsonl").read_bytes(), (on / "events.jsonl").read_bytes())
        coverage = json.loads((on / observer.COVERAGE_FILE).read_text())
        self.assertEqual(coverage["status"], "COMPLETE", coverage)
        result = observer.read_observations(on)
        self.assertEqual(result["status"], "COMPLETE", result["reasons"])
        self.assertGreater(len(result["records"]), 1)
        first, last = result["records"][0], result["records"][-1]
        self.assertEqual(first["model_reason"], "OK")
        self.assertEqual(first["observed_elapsed_ms"], 105_000)
        self.assertEqual(first["fee_basis"], "PAPER_CASH_FEE")
        self.assertEqual(last["sample_state"], "ROUND_ENDED")
        self.assertIsNone(last["p_up"])
        self.assertIsNone(last["exit_net_income"])
        events = (on / "events.jsonl").read_bytes()
        (on / "events.jsonl").write_bytes(events.replace(b'"event_seq": 1,', b'"event_seq": 2,', 1))
        broken = observer.read_observations(on)
        self.assertIn("TRADING_EVIDENCE_INVALID", broken["reasons"])
        self.assertEqual(broken["records"], [])

    def test_observer_socket_store_receiver_and_close_errors_do_not_change_trading(self):
        off = self.launch(self.root / "off", enabled=False)
        baseline = (off / "events.jsonl").read_bytes()
        original_open = observer.ObservationStore.open
        def faulty_handle(store, *, failing_close=False):
            original_open(store)
            handle = store.handle
            class BrokenHandle:
                def write(self, text):
                    if not failing_close: raise OSError("synthetic")
                    return handle.write(text)
                def flush(self): return handle.flush()
                def close(self):
                    handle.close()
                    if failing_close: raise OSError("synthetic")
            store.handle = BrokenHandle()
        faults = [patch.object(observer, "create_socketpair", side_effect=OSError("synthetic")),
                  patch.object(observer.ObservationStore, "open", side_effect=OSError("synthetic")),
                  patch.object(observer.ObserverReceiver, "receive", side_effect=ValueError("synthetic")),
                  patch.object(observer.ObservationStore, "open", autospec=True, side_effect=faulty_handle),
                  patch.object(observer.ObservationStore, "open", autospec=True,
                               side_effect=lambda store: faulty_handle(store, failing_close=True))]
        for number, fault in enumerate(faults):
            with fault:
                directory = self.launch(self.root / str(number))
            self.assertEqual((directory / "events.jsonl").read_bytes(), baseline)
            self.assertEqual(observer.read_observations(directory)["status"], "INCOMPLETE")

    def test_bad_physical_side_frames_do_not_turn_into_legacy_output_invalid(self):
        baseline = (self.launch(self.root / "off", enabled=False) / "events.jsonl").read_bytes()
        original_pair = observer.create_socketpair
        for number, bad in enumerate((b"{}\n{}\n", b"x" * 65537,
                b'{"wire_version":"btc5m-post-entry-wire/1","kind":"READY","run_id":"observer-test","extra":1}\n',
                b'{"kind":"READY","kind":"SAMPLE"}\n')):
            def pair():
                parent, child = original_pair()
                class CorruptParent:
                    first = True
                    def recv(self, count):
                        chunk = parent.recv(count)
                        if self.first:
                            self.first = False
                            return bad
                        return chunk
                    def __getattr__(self, name): return getattr(parent, name)
                return CorruptParent(), child
            with patch.object(observer, "create_socketpair", side_effect=pair):
                directory = self.launch(self.root / f"physical-{number}")
            self.assertEqual((directory / "events.jsonl").read_bytes(), baseline)
            self.assertEqual(observer.read_observations(directory)["status"], "INCOMPLETE")


    def test_real_pipe_fragmentation_and_later_settlement_preserve_as_of_first_write(self):
        import btc5m_control as control
        baseline = (self.launch(self.root / "off", enabled=False) / "events.jsonl").read_bytes()
        original_receive, original_write = observer.ObserverReceiver.receive, observer.ObservationStore.write
        original_read, original_pair = os.read, observer.create_socketpair
        for timing in ("early", "late"):
            waiting, later, writes = [], [], []
            root = self.root / timing
            def received(receiver, frame, frame_bytes=None):
                if frame.get("kind") == "SAMPLE":
                    waiting.append(frame["after_event_seq"] > receiver.index.last_seq)
                    later.append(bool(receiver.index.settlements))
                return original_receive(receiver, frame, frame_bytes)
            def first_write(store, data):
                self.assertIn(b'"event_kind": "fill"', (store.directory / "events.jsonl").read_bytes())
                observer.validate_record(data, RUN_ID)
                writes.append(data)
                return original_write(store, data)
            def delayed_pair():
                parent, child = original_pair()
                class DelayedParent:
                    def recv(self, count):
                        text = (root / RUN_ID / "events.jsonl").read_bytes()
                        if b'"event_kind": "final"' not in text:
                            raise BlockingIOError()
                        return parent.recv(count)
                    def __getattr__(self, name): return getattr(parent, name)
                return DelayedParent(), child
            timing_patch = (patch.object(os, "read", side_effect=lambda fd, count: original_read(fd, min(count, 128)))
                            if timing == "early" else patch.object(observer, "create_socketpair", side_effect=delayed_pair))
            with timing_patch, patch.object(control, "CLEANUP_SECONDS", 2), \
                    patch.object(observer.ObserverReceiver, "receive", new=received), \
                    patch.object(observer.ObservationStore, "write", new=first_write):
                directory = self.launch(root)
            self.assertEqual((directory / "events.jsonl").read_bytes(), baseline)
            result = observer.read_observations(directory)
            self.assertEqual(result["status"], "COMPLETE", result["reasons"])
            self.assertEqual(writes, result["records"])
            self.assertTrue(any(waiting) if timing == "early" else all(later))

    def test_real_child_socket_backpressure_and_disconnection_leave_trading_identical(self):
        baseline = (self.launch(self.root / "off", enabled=False) / "events.jsonl").read_bytes()
        original_pair = observer.create_socketpair
        for disconnected in (False, True):
            def pair():
                parent, child = original_pair()
                child.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
                class UnavailableParent:
                    def recv(self, _count): raise BlockingIOError()
                    def setblocking(self, enabled):
                        parent.setblocking(enabled)
                        if disconnected: parent.close()
                    def __getattr__(self, name): return getattr(parent, name)
                return UnavailableParent(), child
            with patch.object(observer, "create_socketpair", side_effect=pair):
                directory = self.launch(self.root / str(disconnected))
            self.assertEqual((directory / "events.jsonl").read_bytes(), baseline)
            self.assertEqual(observer.read_observations(directory)["status"], "INCOMPLETE")


if __name__ == "__main__":
    unittest.main()
