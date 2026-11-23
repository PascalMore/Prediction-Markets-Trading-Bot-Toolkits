"""Offline tests consuming fresh original Rust fixture stdout streams.

No real credentials, network or production storage are used.
"""
from __future__ import annotations

import functools
import subprocess
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

@functools.lru_cache(maxsize=None)
def native_stream(first, last=None):
    """Run the original offline producer; legacy range pairs name intact fixtures."""
    fixtures = {
        (745, 755): "live_shutdown_after_pending_post_cancellation_keeps_unknown_risk_and_one_hash",
        (813, 827): "live_cleanup_budget_cancels_pending_official_read_without_releasing_risk",
        (780, 783): "official_claims_replace_marks_never_credit_cash_and_zero_payout_releases_only_basis",
        (1648, 1649): "original_observe_projects_loss_and_stop_without_waiting_for_another_snapshot",
        (1664, 1690): "two_official_rounds_use_one_core_ledger_and_emitter_with_actual_fee_and_payouts",
        # run005's silhouette: a failed runtime account read, the stop it owns
        # and the terminal record the bounded shutdown writes afterwards.
        (96, 98): "live_shutdown_after_a_runtime_read_mismatch_keeps_the_failure_code",
        # run011's silhouette (R6): the startup read failed and the stop now
        # names the code that read actually held. Same three records.
        (301, 303): "startup_read_failure_stop_and_final_name_the_failure_code",
    }
    if last is None:
        name = "bot::runtime::live_control::candidate_tests::" + first
    else:
        name = "bot::runtime::live_control::candidate_tests::" + fixtures[first, last]
    result = subprocess.run(["cargo", "test", "--locked", "--offline", "--target-dir",
                             os.environ["CARGO_TARGET_DIR"], "--lib", name,
                             "--", "--exact", "--nocapture"],
                            cwd=Path(__file__).resolve().parents[3], capture_output=True, timeout=120)
    if result.returncode or b"1 passed" not in result.stdout:
        raise AssertionError(result.stderr.decode() + result.stdout.decode())
    frames = []
    for line in result.stdout.splitlines(keepends=True):
        if not line.startswith(live.EVENT_PREFIX):
            continue
        if frames and live.validate_live_event(line)["event_seq"] == 1:
            break  # The payout fixture emits separate runs; never concatenate them.
        frames.append(line)
    events = [live.validate_live_event(line) for line in frames]
    if not events or [event["event_seq"] for event in events] != list(range(1, len(events) + 1)):
        raise AssertionError("native stream boundary changed")
    if len({(event["run_id"], event["risk_window_id"]) for event in events}) != 1:
        raise AssertionError("mixed native stream")
    return frames


@functools.lru_cache(maxsize=1)
def native_revaluations() -> list[dict]:
    """Read real core rechecks, not a Python probability approximation."""
    name = "bot::arbitrage::phase1::tests::native_dynamic_valuation_consumer_fixture"
    result = subprocess.run(
        ["cargo", "test", "--locked", "--offline", "--lib", name,
         "--", "--exact", "--nocapture"],
        cwd=Path(__file__).resolve().parents[3], capture_output=True, timeout=600)
    if result.returncode or b"1 passed" not in result.stdout:
        raise AssertionError(result.stderr.decode() + result.stdout.decode())
    rows = [json.loads(line.removeprefix(b"BTC5M_REVALUATION "))
            for line in result.stdout.splitlines() if line.startswith(b"BTC5M_REVALUATION ")]
    if [row["delay_ms"] for row in rows] != [0, 100, 1000, 1500]:
        raise AssertionError("native recheck coverage changed")
    return rows


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
                                   "initial_cash": 98, "max_order_usd": 1, "max_market_usd": 1,
                                   "max_total_usd": 10, "max_loss_usd": 20},
    }
    context = {key: copy.deepcopy(started[key]) for key in set(live.CONTEXT_KEYS.split()) & set(started)}
    context.update(schema_version="btc5m-live-context/1", candidate_digest=live.candidate_digest(started),
                   gas_policy={"payer_address": live.SIGNER_ADDRESS, "minimum_balance_atoms": "1",
                               "maximum_consumption_atoms": "2"})
    return started, public, context


def ended_from_native(final: dict) -> dict:
    """Supply explicit fake native exits without upgrading the native final's flags."""
    payload = final["payload"]
    return {"schema_version": live.SCHEMA_VERSION, "run_id": final["run_id"],
            "exit_facts": {"launcher_exit": 0, "child_returncode": 0, "child_signal": None,
                           "forced_cleanup": False, "stop_reason": payload["stop_reason"]},
            "final_event_seq": final["event_seq"], "final_accounting": final["accounting"],
            "incomplete_reason": "FACT_INCOMPLETE", "incomplete_detail": payload["stop_detail"],
            **{key: payload[key] for key in ("execution_complete", "cash_complete", "audit_complete", "takeover_required")}}


class EventTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Native tests: cancelled pending POST and bounded cleanup, respectively.
        cls.revaluations = native_revaluations()
        cls.pending = native_stream(745, 755)
        cls.cleanup = native_stream(813, 827)
        cls.decoded = [live.validate_live_event(frame) for frame in cls.cleanup]
        cls.payout_stream = native_stream(780, 783)
        cls.loss_stream = native_stream(1648, 1649)
        cls.two_round = native_stream(1664, 1690)
        # run005's silhouette: a failed runtime account read and its stop.
        cls.runtime_stop = native_stream(96, 98)
        # R13c: the claim is judged before it is booked. The native producer's
        # one-atom-short claim must not close the cash, and the exact claim must
        # clear the wait the reader can observe.
        cls.short_claim = native_stream(
            "claim_wait::the_real_producer_refuses_a_claim_one_atom_short")
        cls.exact_claim = native_stream(
            "claim_wait::the_real_producer_pays_the_claim_and_the_reader_can_follow_it")
        # R13d: the paid claim revokes its own stop, and the window's next
        # order must still be readable afterwards.
        cls.resumed = native_stream(
            "claim_wait::the_real_producer_trades_again_after_the_exact_claim_and_the_reader_follows_it")
        cls.drive_output = subprocess.run(
            ["cargo", "test", "--locked", "--offline", "--lib",
             "bot::runtime::live_control::candidate_tests::contextual_account_failures_stop_safely_without_disclosing_raw_errors",
             "--", "--exact", "--nocapture"], cwd=Path(__file__).resolve().parents[3],
            capture_output=True, timeout=120)
        if cls.drive_output.returncode:
            raise AssertionError(cls.drive_output.stderr.decode() + cls.drive_output.stdout.decode())
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

    def test_legacy_details_default_to_null_but_other_keys_stay_strict(self):
        events = [live.validate_live_event(frame) for frame in self.runtime_stop]
        legacy = copy.deepcopy(events)
        for event in legacy:
            if event["event_kind"] in ("stop", "final"):
                del event["payload"]["stop_detail"]
                self.assertIsNone(live.validate_live_event(event)["payload"]["stop_detail"])
                for change in ("unknown", "required"):
                    bad = copy.deepcopy(event)
                    if change == "unknown":
                        bad["payload"]["unexpected"] = None
                    else:
                        del bad["payload"]["takeover_required"]
                    with self.assertRaises(live.LiveInvalid):
                        live.validate_live_event(bad)
        store = self.store(self.runtime_stop)
        for event in legacy:
            store.append(self.frame(event))
        ended = ended_from_native(events[-1])
        del ended["incomplete_detail"]
        store.finish(ended)
        read = live.read_live_run(store.directory)
        self.assertEqual(read["status"], "INCOMPLETE")
        self.assertNotIn("OUTPUT_INVALID", read["reasons"])
        self.assertIsNone(read["ended"]["incomplete_detail"])
        for change in ("unknown", "required"):
            bad = copy.deepcopy(ended)
            if change == "unknown":
                bad["unexpected"] = None
            else:
                del bad["audit_complete"]
            with self.assertRaises(live.LiveInvalid):
                live.check_ended(bad, store.started, store.relations)

    def test_historical_run005_and_run006_read_only_replay(self):
        from btc5m_control import BOT
        root = BOT.parents[4] / "data/reports"
        for date, suffix, count, detail in (("2026-11-21", "005", 98, None),
                                           ("2026-11-22", "006", 44, "UNKNOWN")):
            directory = root / (date + "-btc5m-controlled-paper/evidence") / ("btc5m-supervised-live-" + suffix)
            names = ("started.json", "public-config.json", "live-context.json", "events.jsonl", "ended.json")
            before = {name: hashlib.sha256((directory / name).read_bytes()).hexdigest() for name in names}
            result = live.read_live_run(directory)
            self.assertEqual(result["status"], "INCOMPLETE")
            self.assertEqual(len(result["events"]), count)
            self.assertEqual(result["reasons"], ["FACT_INCOMPLETE"])
            self.assertEqual(result["ended"]["incomplete_detail"], detail)
            self.assertFalse(result["ended"]["cash_complete"])
            self.assertEqual(before, {name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
                                      for name in names})

    def test_real_drive_logs_and_persisted_protocol_exclude_account_causes(self):
        self.assertNotIn(b"SECRET_SENTINEL", self.drive_output.stdout)
        self.assertNotIn(b"SECRET_SENTINEL", self.drive_output.stderr)
        # Every fault names a warning of its own; a degraded read keeps retrying
        # instead of stopping, so the exact count is a lower bound, never a stop.
        self.assertGreaterEqual(self.drive_output.stdout.count(b"order state check failed"), 6)
        self.assertNotIn(b"OUTPUT_INVALID", self.drive_output.stdout)
        groups, frames = [], []
        for line in self.drive_output.stdout.splitlines():
            if not line.startswith(live.EVENT_PREFIX):
                continue
            frame = line + b"\n"
            frames.append(frame)
            if live.validate_live_event(frame)["event_kind"] == "final":
                groups.append(frames)
                frames = []
        # A fact gap still ends the window with its own code and a final read.
        expected = ("POSITIONS_FACT_INCOMPLETE", "ACCOUNT_LEDGER_MISMATCH", "UNKNOWN")
        self.assertEqual([live.validate_live_event(group[-1])["payload"]["stop_detail"]
                          for group in groups], list(expected))
        for group, detail in zip(groups, expected):
            store = self.store(group)
            for frame in group:
                store.append(frame)
            terminal = live.validate_live_event(group[-1])
            store.finish(ended_from_native(terminal))
            read = live.read_live_run(store.directory)
            self.assertEqual(read["status"], "INCOMPLETE")
            self.assertEqual(read["ended"]["incomplete_detail"], detail)
            stop = next(event for event in read["events"] if event["event_kind"] == "stop")
            self.assertEqual(stop["payload"]["stop_detail"], detail)
            for name in ("events.jsonl", "ended.json"):
                self.assertNotIn(b"SECRET_SENTINEL", (store.directory / name).read_bytes())
        # A transient read failure never ends the window: those runs were cut
        # off while still observing, so their frames carry no stop and no final.
        for frame in frames:
            self.assertNotIn(live.validate_live_event(frame)["event_kind"], ("stop", "final"))

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

    def test_native_nonzero_revaluations_keep_order_ack_and_reader_chain(self):
        for row in self.revaluations:
            with self.subTest(delay_ms=row["delay_ms"]):
                events = copy.deepcopy(self.decoded)
                phases = {"EVALUATE": "evaluate", "PRE_SUBMIT": "pre_submit", "FINAL_SEND": "final_send"}
                for event in events:
                    if event["event_kind"] != "decision":
                        continue
                    original = event["payload"]["decision"]
                    replacement = row[phases[original["phase"]]]
                    # Only diagnostics are substituted in the intact native financial stream.
                    # Its reservation, signed order, ACK, execution and final stay byte-identical.
                    for key in ("decision_id", "candidate_id", "selected_token", "fee_rate_bps"):
                        self.assertEqual(replacement[key], original[key])
                    for side in ("up", "down"):
                        for key in ("token", "limit_price", "gross_shares", "cost"):
                            self.assertEqual(replacement[side][key], original[side][key])
                    event["payload"]["decision"] = copy.deepcopy(replacement)
                frames = [self.frame(event) if event["event_kind"] == "decision" else original
                          for event, original in zip(events, self.cleanup)]
                before = row["evaluate"]
                self.assertEqual(before["p_up"] == row["pre_submit"]["p_up"], row["delay_ms"] == 0)
                store, acks = self.store(frames), []
                for frame in frames:
                    store.append(frame, acks.append)
                store.finish(ended_from_native(events[-1]))
                read = live.read_live_run(store.directory)
                self.assertEqual(read["events"], events)
                self.assertEqual(read["ended"]["final_event_seq"], len(frames))
                self.assertEqual(read["status"], "INCOMPLETE")
                order = next(event for event in events if event["event_kind"] == "order")
                self.assertEqual(acks, [f"A {order['event_seq']} {order['payload']['signed_hash']}\n".encode()])

    def test_static_candidate_facts_reject_before_write_at_each_recheck(self):
        facts = {"decision_id": "999", "candidate_id": "999", "selected_token": "88",
                 "symbol": "ETHUSDT", "prediction_source": "Coinbase", "actual_pair": "BTC-USD",
                 "source_state": "FALLBACK", "source_epoch": 999, "reset_reason": "SOURCE_SWITCH",
                 "settlement_provider": "UNKNOWN", "starts_at": "2026-11-01T00:00:00Z",
                 "ends_at": "2026-11-01T00:05:00Z", "identity_available_at": None,
                 "strike_available_at": None, "strike": 101.0, "spot": 101.0,
                 "decision_at": "2026-11-01T00:00:00Z", "decision_elapsed_ms": 999,
                 "samples": 999, "sigma": 0.01, "fee_rate_bps": 201,
                 "best_yes_bid": 0.29, "best_yes_ask": 0.41}
        changes = [((key,), value) for key, value in facts.items()]
        changes += [((clock, "received_elapsed_ms"), 999)
                    for clock in ("spot_clock", "book_clock")]
        for side in ("up", "down"):
            changes += [((side, key), value) for key, value in
                        (("token", "999"), ("limit_price", 0.5), ("gross_shares", 4.0),
                         ("depth_levels", []), ("depth_complete", False))]
            changes.append(((side,), None))
        for index, event in enumerate(self.decoded):
            if event["event_kind"] != "decision" or event["payload"]["decision"]["phase"] == "EVALUATE":
                continue
            for path, value in changes + [(("market_id",), "999")]:
                with self.subTest(phase=event["payload"]["decision"]["phase"], path=path):
                    store = self.store()
                    for frame in self.cleanup[:index]:
                        store.append(frame)
                    broken = copy.deepcopy(event)
                    target = broken if path == ("market_id",) else broken["payload"]["decision"]
                    for key in path[:-1]:
                        target = target[key]
                    target[path[-1]] = value
                    prior = store.relations
                    with patch.object(store.stream, "write", wraps=store.stream.write) as write:
                        with self.assertRaises(live.LiveInvalid):
                            store.append(self.frame(broken))
                        self.assertEqual(write.call_count, 0)
                    self.assertIs(store.relations, prior)

    def test_final_send_still_requires_ack_unstopped_unlatched_prefix(self):
        index = next(index for index, event in enumerate(self.decoded)
                     if event["event_kind"] == "decision"
                     and event["payload"]["decision"]["phase"] == "FINAL_SEND")
        for fault in ("ack", "stop", "loss", "phase", "sequence"):
            with self.subTest(fault=fault):
                relation = live.LiveRelations(self.decoded[0]["run_id"], self.decoded[0]["risk_window_id"])
                for event in self.decoded[:index]:
                    relation = relation.stage(event)
                candidate = next(iter(relation.candidates.values()))
                if fault == "ack":
                    relation.attempts[candidate["attempt"]]["acked"] = False
                elif fault == "stop":
                    relation.stopped = True
                elif fault == "loss":
                    relation.latched = True
                elif fault == "phase":
                    candidate["phase"] = "EVALUATE"
                else:
                    relation.seq -= 1
                with self.assertRaises(live.LiveInvalid):
                    relation.stage(self.decoded[index])

    def test_metadata_activation_window_cap_is_mode_bound_and_inclusive(self):
        """Exercise the metadata consumer, not merely the shared bound helper."""
        started, public, context = metadata(self.cleanup[0])
        start = live.timestamp(started["started_at"])
        stamp = lambda value: value.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        for mode, cap in (("supervised", 21600), ("unattended", 86400), ("unknown", 0)):
            for seconds in (-1, 0, 1, 21599, 21600, 21601, 86399, 86400, 86401):
                accepted = mode != "unknown" and 0 < seconds <= cap
                with self.subTest(mode=mode, seconds=seconds):
                    candidate = copy.deepcopy(started)
                    candidate_context = copy.deepcopy(context)
                    candidate["operations_mode"] = candidate_context["operations_mode"] = mode
                    candidate["deadline_at"] = candidate_context["deadline_at"] = stamp(
                        start + dt.timedelta(seconds=seconds))
                    if accepted:
                        live.check_metadata(candidate, public, candidate_context)
                    else:
                        with self.assertRaises(live.LiveInvalid):
                            live.check_metadata(candidate, public, candidate_context)

    def test_gas_metadata_requires_the_distinct_fixed_signer(self):
        started, public, context = metadata(self.cleanup[0])
        self.assertNotEqual(started["account_identity"]["trading_address"],
                            started["account_identity"]["signer_address"])
        live.check_metadata(started, public, context)
        for payer in (live.TRADING_ADDRESS, "0x" + "3" * 40, "bad", "", None):
            with self.subTest(payer=payer):
                broken = copy.deepcopy(context)
                if payer is None:
                    del broken["gas_policy"]["payer_address"]
                else:
                    broken["gas_policy"]["payer_address"] = payer
                with self.assertRaises(live.LiveInvalid):
                    live.check_metadata(started, public, broken)

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
                    if (path == ("payload",) and (
                            key == "stop_detail" and event["event_kind"] in ("stop", "final")
                            or key == "diagnostic" and event["event_kind"] == "order")):
                        continue  # Exact historical nullable-key compatibility only.
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
            self.assertEqual(attempt["applied"]["net_cash_atoms"], live.SCALE)
            self.assertEqual(attempt["applied"]["share_fee_atoms"], 25 * 10 ** 15)
            self.assertEqual(attempt["applied"]["net_inventory_atoms"], 2475 * 10 ** 15)
        self.assertTrue(store.relations.cash_pending)
        self.assertEqual(live.read_live_run(store.directory)["status"], "INCOMPLETE")

    def test_a_claim_is_judged_before_it_is_booked_and_the_reader_agrees(self):
        # A claim one atom short of what the confirmed result owes is never
        # booked as cash: the terminal record keeps the cash incomplete, and
        # the reader observes the same pending wait.
        short_store = self.store(self.short_claim)
        for frame in self.short_claim:
            short_store.append(frame)
        short_final = short_store.relations.final
        self.assertFalse(short_final["payload"]["cash_complete"],
                         "a claim one atom short never closes the cash")
        self.assertTrue(short_store.relations.cash_pending,
                        "the reader still sees the cash wait")
        self.assertEqual(live.read_live_run(short_store.directory)["status"], "INCOMPLETE")
        # The exact claim does close it, and the reader observes the cleared
        # wait through the wire's own CASH_CONFIRMED record.
        exact_store = self.store(self.exact_claim)
        for frame in self.exact_claim:
            exact_store.append(frame)
        exact_final = exact_store.relations.final
        self.assertTrue(exact_final["payload"]["cash_complete"],
                        "the exact claim's cash is received, not pending")
        self.assertFalse(exact_store.relations.cash_pending,
                         "the reader must see the wait released")
        paid = [event for event in
                (live.validate_live_event(frame) for frame in self.exact_claim)
                if event["event_kind"] == "payout"
                and event["payload"]["status"] == "CASH_CONFIRMED"]
        self.assertEqual(len(paid), 1, "the paid claim is on the wire")
        self.assertIsNotNone(paid[0]["payload"]["cash_received_atoms"])
        # A failed one-atom proof is not a valid position reconciliation. Its
        # final retains the actual uncredited execution, with a sticky hard stop.
        short_events = [live.validate_live_event(frame) for frame in self.short_claim]
        inventory = next(event["payload"]["facts"]["net_inventory"]["atoms"]
                         for event in short_events if event["event_kind"] == "execution"
                         and event["payload"]["facts"]["net_inventory"]["status"] == "KNOWN")
        self.assertEqual(short_final["payload"]["stop_reason"], "RECONCILIATION_REQUIRED")
        self.assertTrue(short_final["payload"]["takeover_required"])
        self.assertEqual(short_final["payload"]["positions"][0]["net_shares_atoms"], inventory)
        self.assertFalse(any(event["event_kind"] == "payout"
                             and event["payload"]["status"] == "CASH_CONFIRMED"
                             for event in short_events))
        self.assertEqual(exact_store.relations.positions,
                         exact_store.relations.final["payload"]["positions"])

    def test_the_reader_follows_the_run_that_trades_again_after_the_paid_claim(self):
        # The exact claim's CASH_CONFIRMED is the wire fact that revokes the
        # wait it caused. The next round's RESERVE and FINAL_SEND follow it on
        # the same stream: the run is legal, so the reader must not call its
        # own continued ordering OUTPUT_INVALID.
        store = self.store(self.resumed)
        events = [live.validate_live_event(frame) for frame in self.resumed]
        for frame in self.resumed:
            store.append(frame)
        paid = next(index for index, event in enumerate(events)
                    if event["event_kind"] == "payout"
                    and event["payload"]["status"] == "CASH_CONFIRMED")
        self.assertFalse(store.relations.cash_pending)
        self.assertFalse(store.relations.stopped,
                         "the paid claim revokes the stop it caused")
        resumed = [event for event in events[paid + 1:]]
        self.assertTrue(any(event["event_kind"] == "reservation"
                            and event["payload"]["action"] == "RESERVE"
                            for event in resumed),
                        "the resumed window's next order is on the wire")
        self.assertTrue(any(event["event_kind"] == "decision"
                            and event["payload"]["decision"]["phase"] == "FINAL_SEND"
                            for event in resumed),
                        "the resumed window's next send permission is on the wire")
        read = live.read_live_run(store.directory)
        self.assertEqual(read["status"], "INCOMPLETE")
        self.assertNotIn("OUTPUT_INVALID", read["reasons"])

    def test_a_stop_from_another_cause_survives_the_paid_claim(self):
        # Only the wait the missing claim caused is revocable. A run stopped
        # for another reason keeps that stop, so the later order is refused.
        events = [live.validate_live_event(frame) for frame in self.resumed]
        paid = next(index for index, event in enumerate(events)
                    if event["event_kind"] == "payout"
                    and event["payload"]["status"] == "CASH_CONFIRMED")
        relation = live.LiveRelations(events[0]["run_id"], events[0]["risk_window_id"])
        for event in events[:paid]:
            relation = relation.stage(event)
        self.assertTrue(relation.stopped)
        self.assertEqual(relation.stop_reason, "CASH_PENDING")
        relation.stopped, relation.stop_reason = True, "DATA_UNAVAILABLE"
        relation = relation.stage(events[paid])
        self.assertTrue(relation.stopped, "another cause's stop is not the cash wait")
        self.assertEqual(relation.stop_reason, "DATA_UNAVAILABLE")
        self.assertFalse(relation.cash_pending)
        with self.assertRaises(live.LiveInvalid):
            for event in events[paid + 1:]:
                relation = relation.stage(event)

    def test_the_first_stop_cause_outlives_the_settlement_that_follows_it(self):
        # The producer emits its stop record once, so a stop for another cause
        # can precede this settlement's own payout CASH_PENDING. The reader must
        # keep the first cause: CASH_CONFIRMED revokes only a stop the cash wait
        # caused, and the window's next send permission stays shut.
        events = [live.validate_live_event(frame) for frame in self.resumed]
        pending = next(index for index, event in enumerate(events)
                       if event["event_kind"] == "payout"
                       and event["payload"]["status"] == "CASH_PENDING")
        stop = next(index for index, event in enumerate(events) if event["event_kind"] == "stop")
        events[stop]["payload"]["reason"] = "DATA_UNAVAILABLE"
        events[stop]["payload"]["stop_detail"] = "READ_FAILED"
        order = []
        for index, event in enumerate(events):
            if index == pending:
                order.append(events[stop])
            if index != stop:
                order.append(event)
        for position, event in enumerate(order, start=1):
            event["event_seq"] = position
        relation = live.LiveRelations(order[0]["run_id"], order[0]["risk_window_id"])
        stop_seq, accepted, refused = events[stop]["event_seq"], [], None
        for event in order:
            try:
                relation = relation.stage(event)
            except live.LiveInvalid:
                refused = event
                break
            accepted.append(event)
        self.assertTrue(relation.stopped, "the first stop cause is never released")
        self.assertEqual(relation.stop_reason, "DATA_UNAVAILABLE")
        self.assertFalse(relation.cash_pending, "the paid claim ends the wait, not the stop")
        if refused is None:
            self.fail("the stopped window's next order is refused")
        self.assertEqual((refused["event_kind"], refused["payload"]["action"]),
                         ("reservation", "RESERVE"))
        self.assertEqual(refused["event_seq"], relation.seq + 1,
                         "the refused frame is the run's own next legal frame")
        self.assertFalse([event for event in accepted if event["event_kind"] == "decision"
                          and event["payload"]["decision"]["phase"] == "FINAL_SEND"
                          and event["event_seq"] > stop_seq],
                         "no send permission after the first-cause stop is admitted")

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

    def test_a_runtime_failure_code_survives_and_a_malformed_one_degrades(self):
        # The producer's own code reaches the audit unchanged, on both records.
        events = [live.validate_live_event(frame) for frame in self.runtime_stop]
        stop = next(event for event in events if event["event_kind"] == "stop")
        self.assertEqual(stop["payload"]["stop_detail"], "ACCOUNT_LEDGER_MISMATCH")
        self.assertEqual(events[-1]["payload"]["stop_detail"], "ACCOUNT_LEDGER_MISMATCH")
        # A value that is not one plain code degrades to UNKNOWN; the frame is
        # still accepted, never dropped, and the absent value stays null.
        for value in ("not a code", "read failed at https://synthetic.invalid", "x" * 65):
            frame = copy.deepcopy(events[-1])
            frame["payload"]["stop_detail"] = value
            self.assertEqual(live.validate_live_event(frame)["payload"]["stop_detail"], "UNKNOWN")
        frame = copy.deepcopy(events[-1])
        frame["payload"]["stop_detail"] = None
        self.assertIsNone(live.validate_live_event(frame)["payload"]["stop_detail"])

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
