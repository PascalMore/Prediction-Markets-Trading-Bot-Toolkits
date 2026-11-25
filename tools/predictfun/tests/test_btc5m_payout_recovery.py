"""Real frozen audit prefix + explicitly synthetic fresh account; no external I/O."""
import copy
import datetime as dt
import hashlib
import json
import shutil
from pathlib import Path

import pytest

from test_btc5m_launch import native_main
import btc5m_control as control
import btc5m_launch as launch
import btc5m_live_events as live


REAL_ROOT = control.BOT.parents[4]
OLD_RUN = "btc5m-live-60ad1be52eb643f0"
NEW_RUN = "btc5m-unattended-live-012"
PRIOR = 94488600000000000000
SPEND = 1945900000000000000
NEXT = PRIOR - SPEND


@pytest.fixture
def payout_recovery(native_main, monkeypatch):
    """Copy original immutable audit; mock only DB/account/child boundaries."""
    env = native_main
    source = REAL_ROOT / "data/reports/2026-11-25-btc5m-controlled-paper/evidence"
    previous = env.root / OLD_RUN
    shutil.copytree(source / OLD_RUN, previous)
    seed_dir = env.root / "launch-attempts" / OLD_RUN
    shutil.copytree(source / "launch-attempts" / OLD_RUN, seed_dir)
    started = json.loads((previous / "started.json").read_text())
    env.previous, env.seed_dir = previous, seed_dir
    env.old.update({key: started[key] for key in ("run_id", "risk_window_id", "authorization_ref")})
    env.old["candidate_digest"] = live.candidate_digest(started)
    env.collection.record = copy.deepcopy(env.old)
    activation = json.loads(env.activation.read_text())
    for key in ("authorization_ref", "risk_window_id", "candidate_digest"):
        activation[key] = env.old[key]
    env.activation.write_text(json.dumps(activation))
    env.old_bytes = env.activation.read_bytes()
    seed = json.loads((seed_dir / "attempt-reserved.json").read_text())
    events = [json.loads(line) for line in (previous / "events.jsonl").read_text().splitlines()]
    executions = {event["payload"]["attempt_id"]: event for event in events
                  if event["event_kind"] == "execution"}
    assert len(executions) == 2
    env.orders = seed["declared_orders"] + [event["payload"]["signed_hash"]
                                            for event in executions.values()]
    # Synthetic settled account derived from the two frozen, complete executions.
    # This is test data, not an assertion that a real provider returned it.
    fresh = json.loads((seed_dir / "reconciliation.json").read_text())
    fresh["reconciliation"]["declared_orders"] = env.orders
    for event in executions.values():
        market, token = event["market_id"], event["token"]
        record = {"market_id": int(market), "token": token, "oracle_status": "RESOLVED",
                  "trading_status": "CLOSED"}
        shares = event["payload"]["facts"]["net_inventory"]["atoms"]
        if market == "4767590":
            fresh["account"]["settled_zero"].append(
                {**record, "outcome_status": "LOST", "payout_millionths": 0})
        else:
            assert market == "4767663"
            fresh["account"]["claimed_win"].append({**record, "outcome_status": "WON",
                "payout_millionths": 1000000, "claim_atoms": shares, "inventory_atoms": shares})
    fresh["account"]["positions"] = []
    fresh["account"]["collateral_atoms"] = str(NEXT + sum(
        int(record["claim_atoms"]) for record in fresh["account"]["claimed_win"]))
    env.projections = [copy.deepcopy(fresh), copy.deepcopy(fresh)]
    env.reads, env.controllers = [], []
    env.native_exit = [0, 0]
    env.audit_bytes = {path: path.read_bytes() for directory in (previous, seed_dir)
                       for path in directory.iterdir() if path.is_file()}
    env.argv = ["--config", str(env.config), "--auto-prepare", "--run-id", NEW_RUN,
                "--authorization-ref", "offline-new-authorization", "--evidence-root", str(env.root)]

    def reconcile(config, rpc_url, orders, output, *, environment):
        index = len(env.reads)
        assert index < 2
        data = json.loads(Path(config).read_text())
        assert control.parameters(data)["budget"] == NEXT
        assert orders == env.orders
        env.reads.append((data, list(orders)))
        env.calls.append(f"read-{index + 1}")
        value = copy.deepcopy(env.projections[index])
        value["observation"]["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        Path(output).write_text(json.dumps(value))
        return {"exit_code": env.native_exit[index], "projection": value}

    def controller(argv, *, environment, evidence_root):
        assert env.collection.record is None
        assert json.loads(env.activation.read_text())["authorization_ref"] == "offline-new-authorization"
        assert [argv[index + 1] for index, key in enumerate(argv)
                if key == "--predict-reconciled-order"] == env.orders
        env.calls.append("controller")
        env.controllers.append(argv)
        return {"launcher_exit": 0}

    original_replace = launch.os.replace

    def replace(source, target):
        assert Path(target) == env.activation
        env.calls.append("activation")
        return original_replace(source, target)

    monkeypatch.setattr(control.local_signer, "reconcile_account", reconcile)
    monkeypatch.setattr(control, "run", controller)
    monkeypatch.setattr(launch.os, "replace", replace)
    return env


def assert_no_handoff(env):
    assert env.collection.record == env.old
    assert env.activation.read_bytes() == env.old_bytes
    assert not env.controllers
    assert not any(call in env.calls for call in ("activation", "delete", "arm", "controller"))


def test_original_payout_audit_prefix_prepares_only_new_window(payout_recovery):
    import btc5m_payout_recovery as recovery
    env = payout_recovery
    # The old ACK protocol proof binds the historical binary, not the selector
    # of the independently built current candidate.
    started = json.loads((env.previous / "started.json").read_text())
    assert started["candidate_identity"]["binary_sha256"] == recovery.LEGACY_BINARY_SHA256
    assert control.LIVE_BINARY_SHA256 != recovery.LEGACY_BINARY_SHA256
    assert launch.main(env.argv) == 0
    assert env.calls == ["read-1", "read-2", "activation", "delete", "controller"]
    assert len(env.controllers) == 1
    attempt = env.root / "launch-attempts" / NEW_RUN
    proof = json.loads((attempt / "payout-audit-recovery.json").read_text())
    assert proof["prior_budget_atoms"] == str(PRIOR)
    assert proof["next_budget_atoms"] == str(NEXT)
    assert proof["gross_spend_atoms"] == str(SPEND)
    assert proof["derived_token"] is True
    assert proof["old_audit_complete"] is False
    assert proof["prefix_event_seq"] == 532
    assert len(proof["fresh_read_sha256"]) == 2
    assert len(proof["economic_orders"]) == 2
    assert len(env.orders) == 9
    assert all(path.read_bytes() == raw for path, raw in env.audit_bytes.items())
    assert json.loads((env.previous / "ended.json").read_text())["audit_complete"] is False


@pytest.mark.parametrize("damage", [
    "first_exit", "second_exit", "first_incomplete", "cash_atom", "second_cash_atom",
    "missing_positions", "null_positions", "open", "identity", "history_extra", "history_missing",
    "history_duplicate", "history_field", "seed_hash", "seed_budget", "stop_truncated",
    "stop_stream", "stop_kind", "stop_seq", "stop_bytes", "stop_marker", "forced", "other_stop",
    "no_stop_output", "prefix_truncated", "prefix_no_newline", "prefix_noncanonical",
    "native_positions", "loss_threshold", "guard_drift",
])
def test_payout_recovery_refuses_without_business_writes(payout_recovery, monkeypatch, damage):
    env = payout_recovery
    first, second = env.projections
    if damage in ("first_exit", "second_exit"):
        env.native_exit[damage == "second_exit"] = 4
    elif damage == "first_incomplete":
        first["reconciliation"]["complete"] = False
    elif damage in ("cash_atom", "second_cash_atom"):
        value = second if damage == "second_cash_atom" else first
        value["account"]["collateral_atoms"] = str(int(value["account"]["collateral_atoms"]) + 1)
    elif damage == "missing_positions":
        first["account"].pop("positions")
    elif damage == "null_positions":
        first["account"]["positions"] = None
    elif damage == "open":
        first["account"]["open_orders"] = [env.orders[-1]]
    elif damage == "identity":
        first["account_identity"]["trading_address"] = "0x" + "1" * 40
    elif damage.startswith("history_"):
        records = first["account"]["claimed_win"]
        if damage == "history_missing":
            records.pop()
        elif damage == "history_extra":
            records.append({**records[-1], "market_id": 42})
        elif damage == "history_duplicate":
            records.append(copy.deepcopy(records[-1]))
        else:
            records[-1]["inventory_atoms"] = str(int(records[-1]["inventory_atoms"]) + 1)
    elif damage in ("seed_hash", "seed_budget"):
        path = env.seed_dir / "attempt-reserved.json"
        value = json.loads(path.read_text())
        key = "reconciliation_sha256" if damage == "seed_hash" else "committed_budget_atoms"
        value[key] = "a" * 64 if damage == "seed_hash" else PRIOR + 1
        path.write_text(json.dumps(value))
    elif damage.startswith("stop_"):
        path = env.previous / "stop-output.json"
        value = json.loads(path.read_text())
        if damage == "stop_truncated":
            value["truncated"] = True
        elif damage == "stop_stream":
            value["stream"] = "stderr"
        elif damage == "stop_kind":
            value["text"] = value["text"].replace('"event_kind":"payout"', '"event_kind":"stop"')
        elif damage == "stop_seq":
            value["text"] = value["text"].replace('"event_seq":533', '"event_seq":534')
        elif damage == "stop_bytes":
            value["bytes"] += 1
        else:
            value["text"] = value["text"].replace("<REDACTED>", "<OTHER>")
        path.write_text(json.dumps(value))
    elif damage in ("forced", "other_stop"):
        path = env.previous / "ended.json"
        value = json.loads(path.read_text())
        value["exit_facts"]["forced_cleanup" if damage == "forced" else "stop_reason"] = (
            True if damage == "forced" else "SIGNAL")
        path.write_text(json.dumps(value))
    elif damage == "no_stop_output":
        (env.previous / "stop-output.json").unlink()
    elif damage == "prefix_truncated":
        path = env.previous / "events.jsonl"
        path.write_bytes(path.read_bytes().rsplit(b"\n", 2)[0] + b"\n")
    elif damage in ("prefix_no_newline", "prefix_noncanonical"):
        path = env.previous / "events.jsonl"
        raw = path.read_bytes()
        path.write_bytes(raw[:-1] if damage == "prefix_no_newline" else b" " + raw)
    elif damage == "native_positions":
        path = env.seed_dir / "reconciliation.json"
        value = json.loads(path.read_text())
        value["account"].pop("positions")
        path.write_text(json.dumps(value))
        seed_path = env.seed_dir / "attempt-reserved.json"
        seed = json.loads(seed_path.read_text())
        seed["reconciliation_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        seed_path.write_text(json.dumps(seed))
    elif damage == "loss_threshold":
        value = json.loads(env.config.read_text())
        value["strategies"]["limits"].update(max_loss_usd=1, max_total_usd=1)
        assert control.parameters(value)["loss"] == 10**18
        env.config.write_text(json.dumps(value))
    else:
        original = env.collection.find_one
        calls = []

        def drift(query, *args):
            calls.append(query)
            if len(calls) == 2:
                env.collection.record["scope"]["host"] = "concurrent-host"
            return original(query, *args)

        monkeypatch.setattr(env.collection, "find_one", drift)
    assert launch.main(env.argv) == 1
    if damage == "guard_drift":
        assert env.activation.read_bytes() == env.old_bytes
        assert not env.controllers
        assert not any(call in env.calls for call in ("activation", "delete", "controller"))
    else:
        assert_no_handoff(env)


@pytest.mark.parametrize("damage", [
    "unknown", "once_unknown_then_unsent", "unclosed", "remaining", "execution_missing",
    "execution_unproven", "generation", "fee_atom", "multiple_owner", "loss_latched",
    "other_hard_stop", "legacy_controller", "legacy_reader", "legacy_binary", "old_loss_equal",
    "initial_cash", "stop_before_prefix", "stop_after_window", "failure_after_capture",
])
def test_payout_qualification_preserves_risk_and_protocol_proofs(payout_recovery, damage):
    import btc5m_payout_recovery as recovery
    env = payout_recovery
    started = json.loads((env.previous / "started.json").read_text())
    ended = json.loads((env.previous / "ended.json").read_text())
    events = [json.loads(line) for line in (env.previous / "events.jsonl").read_text().splitlines()]
    relations = live.LiveRelations(started["run_id"], started["risk_window_id"],
                                   started["public_config_identity"])
    for event in events:
        relations = relations.stage(event)
    owner = next(item for item in relations.attempts.values() if item["submission"] == "ACCEPTED")
    unsent = next(item for item in relations.attempts.values() if relations._zero_risk_unsent(item))
    seed = json.loads((env.seed_dir / "attempt-reserved.json").read_text())
    native = json.loads((env.seed_dir / "reconciliation.json").read_text())
    checked = launch.preflight(env.previous / "public-config.json", env.seed_dir / "reconciliation.json",
                               now=launch._parse_time(started["started_at"]),
                               declared_orders=seed["declared_orders"])
    record = json.loads((env.previous / "stop-output.json").read_text())
    params = checked["params"]
    initial = copy.deepcopy(events[0])
    if damage == "unknown":
        unsent["submission"] = "UNKNOWN"
    elif damage == "once_unknown_then_unsent":
        unsent["send_observed"] = True
    elif damage == "unclosed":
        unsent["phase"] = "PREPARED"
    elif damage == "remaining":
        owner["remaining"] = 1
    elif damage == "execution_missing":
        owner["execution"] = None
    elif damage == "execution_unproven":
        owner["execution"]["closure"] = "UNPROVEN"
    elif damage == "generation":
        owner["execution"]["generation"] += 1
    elif damage == "fee_atom":
        owner["applied"]["share_fee_atoms"] += 1
    elif damage == "multiple_owner":
        other = next(item for item in relations.attempts.values()
                     if item is not owner and item["submission"] == "ACCEPTED")
        other["identity"] = owner["identity"]
    elif damage == "loss_latched":
        relations.latched = True
    elif damage == "other_hard_stop":
        relations.stop_reason = "FACT_INCOMPLETE"
    elif damage in ("legacy_controller", "legacy_reader"):
        leaf = "btc5m_control.py" if damage == "legacy_controller" else "btc5m_live_events.py"
        started["candidate_identity"]["files"]["tools/predictfun/" + leaf] = "0" * 64
    elif damage == "legacy_binary":
        started["candidate_identity"]["binary_sha256"] = "0" * 64
    elif damage == "old_loss_equal":
        params["loss"] = SPEND
    elif damage == "initial_cash":
        initial["payload"]["collateral_atoms"] = str(int(initial["payload"]["collateral_atoms"]) + 1)
    elif damage == "stop_before_prefix":
        record["occurred_at"] = started["started_at"]
    elif damage == "stop_after_window":
        record["occurred_at"] = "2026-12-01T00:00:00.000Z"
    else:
        record["occurred_at"] = events[-1]["event_time"]
    with pytest.raises(live.LiveInvalid):
        recovery.qualify(started=started, ended=ended, relations=relations, last=events[-1], record=record,
                         old_native=native, old_params=params, seed=seed, initial=initial)
    assert env.calls == []
    assert_no_handoff(env)


def test_payout_recovery_accepts_reordered_complete_histories(payout_recovery):
    env = payout_recovery
    for key in ("claimed_win", "settled_zero"):
        env.projections[1]["account"][key].reverse()
    assert launch.main(env.argv) == 0
    assert env.calls == ["read-1", "read-2", "activation", "delete", "controller"]


def test_payout_recovery_accepts_original_unredacted_stop_token(payout_recovery):
    env = payout_recovery
    path = env.previous / "stop-output.json"
    record = json.loads(path.read_text())
    token = env.projections[0]["account"]["claimed_win"][-1]["token"]
    record["text"] = record["text"].replace('<REDACTED>', json.dumps(token))
    assert len(record["text"].encode()) == record["bytes"]
    path.write_text(json.dumps(record))
    assert launch.main(env.argv) == 0
    proof = json.loads((env.root / "launch-attempts" / NEW_RUN / "payout-audit-recovery.json").read_text())
    assert proof["derived_token"] is False
