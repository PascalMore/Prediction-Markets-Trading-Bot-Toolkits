"""Offline tests using original, unmixed L1 native stdout streams.

The fixed line ranges are separate subprocess tests in the captured L1 log.
No Rust execution, credentials, network or production storage is used here.
Original-supervisor wiring is held and belongs to the downstream stage.
"""
from __future__ import annotations

import copy
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import btc5m_live_events as live

NATIVE_LOG = Path("/home/pascal/.hermes/profiles/yquantdeveloper/cache/scratch/t_9edc6973-rC3-lib-final-2.log")


def native_stream(first: int, last: int) -> list[bytes]:
    """Select one intact producer boundary, not a concatenation of passing tests."""
    lines = NATIVE_LOG.read_bytes().splitlines(keepends=True)
    selected = lines[first - 1:last]
    frames = [line for line in selected if line.startswith(live.EVENT_PREFIX)]
    events = [live.validate_live_event(line) for line in frames]
    if [event["event_seq"] for event in events] != list(range(1, len(events) + 1)):
        raise AssertionError("native stream boundary changed")
    if len({(event["run_id"], event["risk_window_id"]) for event in events}) != 1:
        raise AssertionError("mixed native stream")
    return frames


def metadata(first_frame: bytes) -> tuple[dict, dict, dict]:
    """Public startup metadata only; no healthy financial wire is hand-forged."""
    event = live.validate_live_event(first_frame)
    start = live.timestamp(event["event_time"]) - dt.timedelta(seconds=30)
    deadline = start + dt.timedelta(hours=1)
    stamp = lambda value: value.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    public = {"test_public_config": "offline"}
    started = {
        "schema_version": live.SCHEMA_VERSION, "strategy_contract": "phase1_net_shares",
        "operations_mode": "supervised", "risk_window_id": event["risk_window_id"],
        "run_id": event["run_id"], "authorization_ref": "offline-auth",
        "account_identity": {"venue": "PREDICT_FUN", "chain_id": 56, "account_mode": "predict-account",
                             "trading_address": live.TRADING_ADDRESS, "signer_address": live.SIGNER_ADDRESS},
        "recovery_policy": "COLD_START_REFUSE", "cash_recovery_policy": "MANUAL_PAUSE",
        "asset_units": {"collateral_symbol": "USDT", "collateral_decimals": 18, "share_decimals": 18,
                        "gas_symbol": "BNB", "gas_decimals": 18},
        "started_at": stamp(start), "deadline_at": stamp(deadline),
        "candidate_identity": {"main_head": "a" * 40, "bot_head": "b" * 40,
                               "binary_sha256": "c" * 64, "files": {"src/main.rs": "d" * 64}},
        "public_config_identity": {"sha256": hashlib.sha256(live.canonical(public)).hexdigest(),
                                   "initial_cash": 100, "max_order_usd": 2, "max_market_usd": 2,
                                   "max_total_usd": 10, "max_loss_usd": 20},
    }
    context = {key: copy.deepcopy(started[key]) for key in set(live.CONTEXT_KEYS.split()) & set(started)}
    context.update(schema_version="btc5m-live-context/1", candidate_digest=live.candidate_digest(started),
                   gas_policy={"payer_address": live.TRADING_ADDRESS, "minimum_balance_atoms": "1",
                               "maximum_consumption_atoms": "2"})
    return started, public, context


def ended_from_native(final: dict) -> dict:
    """Supply explicit fake native exits without upgrading the native final's flags."""
    payload = final["payload"]
    return {"schema_version": live.SCHEMA_VERSION, "run_id": final["run_id"],
            "exit_facts": {"launcher_exit": 0, "child_returncode": 0, "child_signal": None,
                           "forced_cleanup": False, "stop_reason": payload["stop_reason"]},
            "final_event_seq": final["event_seq"], "final_accounting": final["accounting"],
            "incomplete_reason": "FACT_INCOMPLETE",
            **{key: payload[key] for key in ("execution_complete", "cash_complete", "audit_complete", "takeover_required")}}


class EventTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Native tests: cancelled pending POST and bounded cleanup, respectively.
        cls.pending = native_stream(745, 755)
        cls.cleanup = native_stream(813, 827)
        cls.decoded = [live.validate_live_event(frame) for frame in cls.cleanup]
        cls.payout_stream = native_stream(780, 783)
        cls.loss_stream = native_stream(1648, 1649)
        cls.two_round = native_stream(1664, 1690)
        # Schema cases are independent objects, never a combined relation stream.
        cls.schema_cases = cls.decoded + [live.validate_live_event(frame)
                                         for stream in (cls.payout_stream, cls.loss_stream)
                                         for frame in stream]

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="btc5m-l2-events-", dir=os.environ["TMPDIR"])
        self.root = Path(self.temporary.name)
        self.network = patch.object(socket.socket, "connect", side_effect=AssertionError("real network"))
        self.network_spy = self.network.start()
        self.secret = patch.dict(os.environ, {}, clear=True)
        self.secret.start()
        self.stores = []

    def tearDown(self):
        for store in self.stores:
            store.close()
        self.assertEqual(self.network_spy.call_count, 0)
        self.secret.stop()
        self.network.stop()
        self.temporary.cleanup()

    def store(self, frames=None):
        frames = self.cleanup if frames is None else frames
        started, public, context = metadata(frames[0])
        store = live.LiveEventStore(self.root / f"run-{len(self.stores)}", started, public, context)
        self.stores.append(store)
        return store

    def frame(self, event):
        return live.EVENT_PREFIX + json.dumps(event, separators=(",", ":")).encode() + b"\n"

    def test_native_producer_to_first_write_ack_reader_and_final(self):
        for frames in (self.pending, self.cleanup):
            with self.subTest(stream_length=len(frames)):
                store, acks = self.store(frames), []
                for frame in frames:
                    store.append(frame, acks.append)
                final = live.validate_live_event(frames[-1])
                store.finish(ended_from_native(final))
                read = live.read_live_run(store.directory)
                self.assertEqual(read["events"], [live.validate_live_event(frame) for frame in frames])
                self.assertEqual(read["ended"]["final_event_seq"], len(frames))
                self.assertEqual(read["status"], "INCOMPLETE")
                self.assertFalse(read["ended"]["cash_complete"])
                order = next(event for event in read["events"] if event["event_kind"] == "order")
                self.assertEqual(acks, [f"A {order['event_seq']} {order['payload']['signed_hash']}\n".encode()])

    def test_all_nested_missing_and_unknown_keys_before_first_write(self):
        def objects(value, path=()):
            if type(value) is dict:
                yield path, value
                for key, child in value.items():
                    yield from objects(child, path + (key,))
            elif type(value) is list:
                for index, child in enumerate(value):
                    yield from objects(child, path + (index,))
        samples = []
        # All native kinds available in these two original streams are validated.
        for event in self.schema_cases:
            for path, obj in objects(event):
                for key in obj:
                    broken = copy.deepcopy(event)
                    target = broken
                    for component in path:
                        target = target[component]
                    del target[key]
                    samples.append(broken)
                broken = copy.deepcopy(event)
                target = broken
                for component in path:
                    target = target[component]
                target["private_key"] = "forbidden"
                samples.append(broken)
        writes = 0
        for sample in samples:
            store = self.store()
            with self.assertRaises(live.LiveInvalid):
                live.validate_live_event(self.frame(sample))
            with patch.object(store.stream, "write", wraps=store.stream.write) as write:
                with self.assertRaises(live.LiveInvalid):
                    store.append(self.frame(sample))
                writes += write.call_count
            store.close()
        self.assertEqual(writes, 0)
        self.assertGreater(len(samples), 500)

    def test_scalar_duplicate_nonfinite_and_mixed_version_refuse_before_write(self):
        first = self.decoded[0]
        cases = []
        for key, values in {
            "event_seq": [True, 0, -1, 1.0, 2 ** 64],
            "schema_version": ["btc5m-controlled-paper.v1", "btc5m-controlled-paper.v2", "unknown"],
            "market_id": [True, "0x" + "a" * 64, "1" * 79, "١"],
            "run_id": ["_bad", "../run", "A", ""],
            "event_time": ["2026-11-19T14:00:00", "2026-11-19T14:00:00.0001Z"],
        }.items():
            for value in values:
                broken = copy.deepcopy(first)
                broken[key] = value
                cases.append(self.frame(broken))
        for value in ("01", "-0", "-1", str(live.U256_MAX + 1), True, 1, "١"):
            broken = copy.deepcopy(first)
            broken["accounting"]["cash"] = value
            cases.append(self.frame(broken))
        broken = copy.deepcopy(first)
        broken["accounting"]["net_pnl"] = "-0"
        cases.append(self.frame(broken))
        original = self.cleanup[0]
        cases.extend(original.replace(b'"event_seq":1', b'"event_seq":1,"event_seq":1') for _ in range(1))
        for token in (b"NaN", b"Infinity", b"-Infinity", b"1e999"):
            cases.append(original.replace(b'"p_up":0.9943288101922066', b'"p_up":' + token))
        cases.extend((original[:-1], original + b"x", original.replace(b"BTC5M_EVENT", b"OLD_EVENT"), b"\xff\n"))
        writes = 0
        for index, frame in enumerate(cases):
            with self.subTest(case=index):
                store = self.store()
                with patch.object(store.stream, "write", wraps=store.stream.write) as write:
                    with self.assertRaises(live.LiveInvalid):
                        store.append(frame)
                    writes += write.call_count
                self.assertEqual(store.relations.seq, 0)
                store.close()
        self.assertEqual(writes, 0)

    def test_hash_order_ref_enum_and_fact_semantics(self):
        order = next(event for event in self.decoded if event["event_kind"] == "order")
        execution = next(event for event in self.decoded if event["event_kind"] == "execution")
        cases = []
        for key, value in (("signed_hash", "0x" + "g" * 64), ("venue_order_id", "bad/ref"),
                           ("attempt_id", "-1"), ("attempt_phase", "Filled"),
                           ("min_net_shares_atoms", "-0")):
            broken = copy.deepcopy(order)
            broken["payload"][key] = value
            cases.append(broken)
        for key, value in (("generation", True), ("pages_complete", 1), ("closure", "FILLED"),
                           ("quantity_class", "ZERO"), ("settlement_status", "UNKNOWN")):
            broken = copy.deepcopy(execution)
            broken["payload"][key] = value
            cases.append(broken)
        for fact_key, field, value in (("vwap", "status", "NOT_APPLICABLE"),
                                       ("net_cash", "atoms", None),
                                       ("gross_cash", "coverage_gross_atoms", None),
                                       ("share_fee", "reason", "BROKER_BLOCKED")):
            broken = copy.deepcopy(execution)
            broken["payload"]["facts"][fact_key][field] = value
            cases.append(broken)
        writes = 0
        for case in cases:
            store = self.store()
            with self.assertRaises(live.LiveInvalid):
                live.validate_live_event(self.frame(case))
            with patch.object(store.stream, "write", wraps=store.stream.write) as write:
                with self.assertRaises(live.LiveInvalid):
                    store.append(self.frame(case))
                writes += write.call_count
            store.close()
        self.assertEqual(writes, 0)

    def test_atom_boundaries_and_boolean_numbers(self):
        for value in ("0", str(live.U256_MAX)):
            live._scalar(value, "atoms")
        for value in ("-" + str(live.U256_MAX), "0"):
            live._scalar(value, "signed")
        for value in ("-0", str(live.U256_MAX + 1), "1.0", "+1", " 1"):
            with self.assertRaises(live.LiveInvalid):
                live._scalar(value, "signed")
        for value in (float("nan"), float("inf"), True):
            with self.assertRaises(live.LiveInvalid):
                live._scalar(value, "number")

    def test_relationship_mutations_do_not_write_or_commit(self):
        # Every mutation is applied to its exact original native prefix.
        changes = [(3, ("payload", "decision", "candidate_id"), "999"),
                   (4, ("payload", "candidate_id"), "999"),
                   (5, ("payload", "signed_hash"), "0x" + "a" * 63),
                   (6, ("payload", "decision", "up", "gross_shares"), 999),
                   (7, ("payload", "signed_hash"), "0x" + "b" * 64),
                   (9, ("payload", "venue_order_id"), "different"),
                   (9, ("payload", "delta", "net_cash_atoms"), "1"),
                   (11, ("payload", "delta", "net_inventory_atoms"), "1"),
                   (11, ("payload", "facts", "net_cash", "coverage_gross_atoms"), "1"),
                   (11, ("payload", "generation"), 0),
                   (15, ("payload", "positions"), []),
                   (15, ("payload", "cash_complete"), True)]
        for sequence, path, value in changes:
            with self.subTest(seq=sequence, path=path):
                store = self.store()
                for frame in self.cleanup[:sequence - 1]:
                    store.append(frame)
                broken = copy.deepcopy(self.decoded[sequence - 1])
                target = broken
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                before = store.relations
                with patch.object(store.stream, "write", wraps=store.stream.write) as write:
                    with self.assertRaises(live.LiveInvalid):
                        store.append(self.frame(broken))
                    self.assertEqual(write.call_count, 0)
                self.assertIs(store.relations, before)

    def test_missing_seq_final_and_identity_are_not_complete(self):
        for defect in ("tail", "seq", "final", "identity", "exit", "force", "context"):
            with self.subTest(defect=defect):
                store = self.store()
                for frame in self.cleanup:
                    store.append(frame)
                ended = ended_from_native(self.decoded[-1])
                store.finish(ended)
                store.close()
                event_path = store.directory / "events.jsonl"
                lines = event_path.read_bytes().splitlines(keepends=True)
                if defect == "tail":
                    event_path.write_bytes(b"".join(lines) + b'{"bad":')
                elif defect == "seq":
                    event_path.write_bytes(b"".join(lines[:2] + lines[3:]))
                elif defect == "final":
                    event_path.write_bytes(b"".join(lines[:-1]))
                elif defect == "identity":
                    event_path.write_bytes(b"".join(lines).replace(b'"risk-window"', b'"other-window"'))
                elif defect in ("exit", "force"):
                    ended["exit_facts"]["child_returncode" if defect == "exit" else "forced_cleanup"] = 9 if defect == "exit" else True
                    (store.directory / "ended.json").write_bytes(live.canonical(ended))
                else:
                    path = store.directory / "live-context.json"
                    context = live.strict_json(path.read_bytes())
                    context["candidate_digest"] = "e" * 64
                    path.write_bytes(live.canonical(context))
                result = live.read_live_run(store.directory)
                self.assertEqual(result["status"], "INCOMPLETE")
                self.assertTrue(result["reasons"])

    def test_durability_order_and_ack_after_committed_readback(self):
        store = self.store()
        for frame in self.cleanup[:4]:
            store.append(frame)
        operations = []
        real_fsync = os.fsync
        def sync(fd):
            operations.append("fsync")
            return real_fsync(fd)
        def ack(frame):
            operations.append("ack")
            self.assertEqual(store.relations.seq, 5)
            self.assertIn(b'"event_seq":5', (store.directory / "events.jsonl").read_bytes())
        with patch.object(store.stream, "write", wraps=store.stream.write) as write, \
             patch.object(store.stream, "flush", wraps=store.stream.flush) as flush, \
             patch.object(live.os, "fsync", side_effect=sync):
            store.append(self.cleanup[4], ack)
            self.assertEqual(write.call_count, 1)
            self.assertEqual(flush.call_count, 1)
        self.assertEqual(operations, ["fsync", "ack"])

    def test_each_durability_failure_withholds_ack_and_is_sticky(self):
        for fault in ("write", "flush", "fsync", "readback", "ack"):
            with self.subTest(fault=fault):
                store = self.store()
                for frame in self.cleanup[:4]:
                    store.append(frame)
                acks = []
                if fault in ("write", "flush"):
                    injection = patch.object(store.stream, fault, side_effect=OSError())
                elif fault == "fsync":
                    injection = patch.object(live.os, "fsync", side_effect=OSError())
                elif fault == "readback":
                    injection = patch("builtins.open", return_value=io.BytesIO(b"wrong"))
                else:
                    injection = patch.object(live.os, "fsync", wraps=os.fsync)
                def ack(frame):
                    if fault == "ack":
                        raise BrokenPipeError()
                    acks.append(frame)
                with injection:
                    with self.assertRaises(live.LiveInvalid):
                        store.append(self.cleanup[4], ack)
                self.assertEqual(acks, [])
                self.assertEqual(store.relations.seq, 5 if fault == "ack" else 4)
                with self.assertRaises(live.LiveInvalid):
                    store.append(self.cleanup[4], acks.append)

    def test_no_events_after_final_and_no_run_overwrite(self):
        store = self.store()
        for frame in self.pending:
            store.append(frame)
        with self.assertRaises(live.LiveInvalid):
            store.append(self.pending[0])
        started, public, context = metadata(self.pending[0])
        with self.assertRaises(live.LiveInvalid):
            live.LiveEventStore(store.directory, started, public, context)

    def test_unavailable_history_and_unknown_version_never_upgrade(self):
        self.assertEqual(live.read_live_run(self.root / "missing")["status"], "UNAVAILABLE")
        directory = self.root / "legacy"
        directory.mkdir()
        (directory / "started.json").write_text('{"schema_version":"btc5m-controlled-paper.v1"}')
        self.assertEqual(live.read_live_run(directory)["status"], "UNAVAILABLE")

    def test_incomplete_native_final_references_all_unknown_attempts(self):
        store = self.store(self.pending)
        for frame in self.pending[:-1]:
            store.append(frame)
        final = live.validate_live_event(self.pending[-1])
        final["payload"]["unresolved"] = []
        with patch.object(store.stream, "write", wraps=store.stream.write) as write:
            with self.assertRaises(live.LiveInvalid):
                store.append(self.frame(final))
            self.assertEqual(write.call_count, 0)

    def test_native_two_round_prefix_actual_fee_duplicates_and_cash_pending(self):
        store = self.store(self.two_round)
        for frame in self.two_round:
            store.append(frame)
        self.assertEqual(store.relations.seq, 27)
        self.assertEqual(len(store.relations.attempts), 2)
        for attempt in store.relations.attempts.values():
            self.assertEqual(attempt["applied"]["net_cash_atoms"], 2 * live.SCALE)
            self.assertEqual(attempt["applied"]["share_fee_atoms"], 5 * 10 ** 16)
            self.assertEqual(attempt["applied"]["net_inventory_atoms"], 495 * 10 ** 16)
        self.assertTrue(store.relations.cash_pending)
        self.assertEqual(live.read_live_run(store.directory)["status"], "INCOMPLETE")

    def test_all_ten_payload_schemas_and_array_limits(self):
        self.assertEqual({event["event_kind"] for event in self.schema_cases}, set(live.PAYLOADS))
        for event in self.schema_cases:
            self.assertEqual(live.validate_live_event(self.frame(event)), event)
        for stream, path, value in (
            (self.payout_stream, ("payload", "payout_yes"), True),
            (self.payout_stream, ("payload", "status"), "PAID"),
            (self.loss_stream, ("payload", "latched"), False),
            (self.loss_stream, ("payload", "trigger_net_pnl_atoms"), "-0"),
        ):
            event = live.validate_live_event(stream[0])
            target = event
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            store = self.store(stream)
            with patch.object(store.stream, "write", wraps=store.stream.write) as write:
                with self.assertRaises(live.LiveInvalid):
                    store.append(self.frame(event))
                self.assertEqual(write.call_count, 0)
        event = copy.deepcopy(self.decoded[1])
        event["payload"]["open_hashes"] = ["0x" + "a" * 64] * 1001
        with self.assertRaises(live.LiveInvalid):
            live.validate_live_event(self.frame(event))

    def test_accounting_reserved_floor_and_actual_cost_delta_are_not_forged(self):
        for sequence, key, value in ((4, "reserved", "0"), (9, "cost", "0"),
                                     (11, "cost", "4000000000000000000")):
            store = self.store()
            for frame in self.cleanup[:sequence - 1]:
                store.append(frame)
            event = copy.deepcopy(self.decoded[sequence - 1])
            event["accounting"][key] = value
            with patch.object(store.stream, "write", wraps=store.stream.write) as write:
                with self.assertRaises(live.LiveInvalid):
                    store.append(self.frame(event))
                self.assertEqual(write.call_count, 0)

    def test_schema_failure_is_sticky_and_never_later_acknowledges(self):
        store = self.store()
        acks = []
        with self.assertRaises(live.LiveInvalid):
            store.append(self.cleanup[0].replace(b'"event_seq":1', b'"event_seq":true'))
        with patch.object(store.stream, "write", wraps=store.stream.write) as write:
            with self.assertRaises(live.LiveInvalid):
                store.append(self.cleanup[0], acks.append)
            self.assertEqual(write.call_count, 0)
        self.assertEqual(acks, [])
        self.assertEqual(store.relations.seq, 0)


if __name__ == "__main__":
    unittest.main()
