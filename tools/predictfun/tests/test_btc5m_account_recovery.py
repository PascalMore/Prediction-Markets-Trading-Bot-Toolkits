"""Immutable real253 audit; fresh account/DB/child are explicitly synthetic."""
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

OLD_RUN = "btc5m-live-253cedc2d9f845ac"
NEW_RUN = "btc5m-unattended-live-012"


@pytest.fixture
def account_recovery(native_main, monkeypatch, request):
    """Reuse the real launcher; replace only external reads, database and child."""
    env = native_main
    old_run = getattr(request, "param", OLD_RUN)
    source = control.BOT.parents[4] / "data/reports/2026-11-25-btc5m-controlled-paper/evidence"
    env.previous = env.root / old_run
    env.seed_dir = env.root / "launch-attempts" / old_run
    shutil.copytree(source / old_run, env.previous)
    shutil.copytree(source / "launch-attempts" / old_run, env.seed_dir)
    shutil.copyfile(env.previous / "public-config.json", env.config)
    started = json.loads((env.previous / "started.json").read_text())
    env.old.update({key: started[key] for key in ("run_id", "risk_window_id", "authorization_ref")})
    env.old["candidate_digest"] = live.candidate_digest(started)
    env.collection.record = copy.deepcopy(env.old)
    activation = json.loads(env.activation.read_text())
    activation.update({key: env.old[key] for key in ("risk_window_id", "authorization_ref", "candidate_digest")})
    env.activation.write_text(json.dumps(activation))
    env.old_bytes = env.activation.read_bytes()
    env.seed = json.loads((env.seed_dir / "attempt-reserved.json").read_text())
    env.events = [json.loads(line) for line in (env.previous / "events.jsonl").read_text().splitlines()]
    executions = {event["payload"]["attempt_id"]: event for event in env.events
                  if event["event_kind"] == "execution"}
    env.prior = env.seed["committed_budget_atoms"]
    env.spent = sum(int(event["payload"]["facts"]["net_cash"]["atoms"]) for event in executions.values())
    env.next_budget = env.prior - env.spent
    env.orders = env.seed["declared_orders"] + [event["payload"]["signed_hash"] for event in executions.values()]
    fresh = json.loads((env.seed_dir / "reconciliation.json").read_text())
    fresh["reconciliation"]["declared_orders"] = env.orders
    payouts = {(event["market_id"], event["token"]): event["payload"] for event in env.events
               if event["event_kind"] == "payout" and event["payload"]["status"] in ("CONFIRMED", "CASH_CONFIRMED")}
    for (market, token), payout in payouts.items():
        base = {"market_id": int(market), "token": token, "oracle_status": "RESOLVED", "trading_status": "CLOSED"}
        if int(payout["claim_value_atoms"]):
            fresh["account"]["claimed_win"].append({**base, "outcome_status": "WON", "payout_millionths": 1000000,
                "inventory_atoms": payout["position_net_atoms"], "claim_atoms": payout["claim_value_atoms"]})
        else:
            fresh["account"]["settled_zero"].append({**base, "outcome_status": "LOST", "payout_millionths": 0})
    fresh["account"]["positions"] = []
    fresh["account"]["collateral_atoms"] = str(env.next_budget + sum(int(item["claim_atoms"]) for item in fresh["account"]["claimed_win"]))
    env.projections = [copy.deepcopy(fresh), copy.deepcopy(fresh)]
    env.native_exit = [0, 0]
    env.reads, env.controllers = [], []
    env.audit_bytes = {path: path.read_bytes() for directory in (env.previous, env.seed_dir)
                       for path in directory.iterdir() if path.is_file()}
    env.argv = ["--config", str(env.config), "--auto-prepare", "--run-id", NEW_RUN,
                "--authorization-ref", "offline-new-authorization", "--evidence-root", str(env.root)]

    def reconcile(config, rpc_url, orders, output, *, environment):
        index = len(env.reads)
        assert index < 2
        data = json.loads(Path(config).read_text())
        assert control.parameters(data)["budget"] == env.next_budget
        assert orders == env.orders
        env.reads.append(list(orders))
        env.calls.append(f"read-{index + 1}")
        value = copy.deepcopy(env.projections[index])
        value["observation"]["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        Path(output).write_text(json.dumps(value))
        return {"exit_code": env.native_exit[index], "projection": value}

    def controller(argv, *, environment, evidence_root):
        assert env.collection.record is None
        assert json.loads(env.activation.read_text())["authorization_ref"] == "offline-new-authorization"
        assert [argv[index + 1] for index, key in enumerate(argv) if key == "--predict-reconciled-order"] == env.orders
        env.calls.append("controller")
        env.controllers.append(argv)
        return {"launcher_exit": 0}

    replace = launch.os.replace

    def activation_replace(source, target):
        assert Path(target) == env.activation
        env.calls.append("activation")
        return replace(source, target)

    env.original_controller = control.run
    monkeypatch.setattr(control.local_signer, "reconcile_account", reconcile)
    monkeypatch.setattr(control, "run", controller)
    monkeypatch.setattr(launch.os, "replace", activation_replace)
    return env


R20_RUN = "btc5m-live-6baf1ef8d6724dfb"


@pytest.fixture
def r20_main(native_main, monkeypatch):
    """Exact old audit; model future native success only at the read boundary."""
    from types import SimpleNamespace

    env = native_main
    bot = Path(__file__).resolve().parents[3]
    # A verified git archive has no .git or build products. The staging parent
    # holds frozen public fixtures; the binary is an input read, never executed.
    fixture_root = bot.parent
    reports = fixture_root / "data/reports"
    old = reports / "2026-11-27-btc5m-controlled-paper/evidence"
    env.previous = env.root / R20_RUN
    env.seed_dir = env.root / "launch-attempts" / R20_RUN
    shutil.copytree(old / R20_RUN, env.previous)
    shutil.copytree(old / "launch-attempts" / R20_RUN, env.seed_dir)
    shutil.copyfile(env.previous / "public-config.json", env.config)
    started = live.strict_json((env.previous / "started.json").read_bytes())
    env.old.update({key: started[key] for key in ("run_id", "risk_window_id", "authorization_ref")})
    env.old["candidate_digest"] = live.candidate_digest(started)
    env.collection.record = copy.deepcopy(env.old)
    activation = json.loads(env.activation.read_bytes())
    activation.update({key: env.old[key] for key in ("risk_window_id", "authorization_ref", "candidate_digest")})
    env.activation.write_text(json.dumps(activation))
    env.old_bytes = env.activation.read_bytes()
    env.audit_bytes = {path: path.read_bytes() for directory in (env.previous, env.seed_dir)
                       for path in directory.iterdir() if path.is_file()}
    diagnostic = reports / ("2026-11-27-btc5m-fact-incomplete-stop/evidence/"
                            "account-readonly-20261127T021307265594Z/reconciliation-pass-1.json")
    raw = diagnostic.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == "f841f5ab4e242adb170005363cc515611f796ae6ae877fc960ff44cfd1d36489"
    fresh = live.strict_json(raw)
    assert fresh["reconciliation"]["complete"] is False
    env.orders = fresh["reconciliation"]["declared_orders"]
    # Only independent account facts are borrowed. Completion, freshness,
    # positions explanation and exit are the explicitly synthetic boundary.
    fresh["reconciliation"].update(complete=True, positions_match_history=True)
    fresh["account"]["positions_explained"] = True
    env.projections = [copy.deepcopy(fresh), copy.deepcopy(fresh)]
    env.reads, env.controllers = [], []
    env.native_exit = [0, 0]
    env.argv = ["--config", str(env.config), "--auto-prepare", "--run-id", NEW_RUN,
                "--authorization-ref", "offline-new-authorization", "--evidence-root", str(env.root)]

    # Real candidate file hashing and binary pin; simulate only absent git
    # metadata when testing an export, and never run the artifact itself.
    if not (bot / ".git").exists():
        import os

        project = Path(os.environ["BTC5M_TEST_PROJECT_ROOT"])
        monkeypatch.setattr(control, "LIVE_BINARY", project / control.LIVE_BINARY.relative_to(bot.parents[4]))
        def git_head(argv, **kwargs):
            assert argv[0] == "/usr/bin/git" and argv[3:] == ["rev-parse", "HEAD"]
            key = "bot_head" if Path(argv[2]) == bot else "main_head"
            return SimpleNamespace(stdout=started["candidate_identity"][key].encode())
        monkeypatch.setattr(control.subprocess, "run", git_head)
    monkeypatch.setattr(control.local_signer, "_read_owned", lambda: pytest.fail("unexpected key read"))

    def reconcile(config, rpc_url, orders, output, *, environment):
        index = len(env.reads)
        assert index < 2
        assert control.parameters(json.loads(Path(config).read_bytes()))["budget"] == 80760100000000000001
        assert orders == env.orders
        env.reads.append(list(orders))
        env.calls.append(f"read-{index + 1}")
        value = copy.deepcopy(env.projections[index])
        value["observation"]["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        Path(output).write_text(json.dumps(value))
        return {"exit_code": env.native_exit[index], "projection": value}

    def controller(argv, *, environment, evidence_root):
        assert env.collection.record is None
        assert json.loads(env.activation.read_bytes())["authorization_ref"] == "offline-new-authorization"
        assert [argv[i + 1] for i, key in enumerate(argv) if key == "--predict-reconciled-order"] == env.orders
        env.calls.append("controller")
        env.controllers.append(argv)
        return {"launcher_exit": 0}

    replace = launch.os.replace
    def publish(source, target):
        assert Path(target) == env.activation
        env.calls.append("activation")
        return replace(source, target)

    monkeypatch.setattr(control.local_signer, "reconcile_account", reconcile)
    monkeypatch.setattr(control, "run", controller)
    monkeypatch.setattr(launch.os, "replace", publish)
    return env


def test_r20_closed_rereads_recover_at_original_main(r20_main, capsys):
    env = r20_main
    result = launch.main(env.argv)
    assert result == 0, capsys.readouterr().out
    assert env.calls == ["read-1", "read-2", "activation", "delete", "controller"]
    proof = json.loads((env.root / "launch-attempts" / NEW_RUN / "account-read-recovery.json").read_bytes())
    assert proof["checkpoint_event_seq"] == 2479 and proof["final_event_seq"] == 2499
    assert proof["prior_budget_atoms"] == "87694700000000000001"
    assert proof["next_budget_atoms"] == "80760100000000000001"
    assert proof["gross_spend_atoms"] == "6934600000000000000"
    assert len(proof["economic_orders"]) == 7 and len(proof["zero_fill_orders"]) == 1
    assert proof["window_order_hashes"] == env.orders[-8:]
    assert len(proof["fresh_read_sha256"]) == 2
    assert all(path.read_bytes() == raw for path, raw in env.audit_bytes.items())


@pytest.mark.parametrize("damage,reads,reason", [
    ("cash_plus", 1, "CASH_BUDGET_MISMATCH"), ("cash_minus", 1, "CASH_BUDGET_MISMATCH"),
    ("second_cash", 2, "RECONCILIATION_DRIFT"), ("first_incomplete", 1, "RECONCILIATION_NOT_COMPLETE"),
    ("second_incomplete", 2, "RECONCILIATION_NOT_COMPLETE"), ("declarations", 1, "RECONCILIATION_FACTS"),
    ("positions_missing", 1, "HISTORY_REQUIRES_TAKEOVER"), ("open_orders", 1, "RECONCILIATION_FACTS"),
    ("native_identity", 1, "RECONCILIATION_FACTS"), ("offset_claims", 1, "HISTORY_REQUIRES_TAKEOVER"),
    ("duplicate_zero", 1, "HISTORY_REQUIRES_TAKEOVER"), ("missing_zero", 1, "HISTORY_REQUIRES_TAKEOVER"),
    ("wrong_official", 1, "HISTORY_REQUIRES_TAKEOVER"), ("second_history", 2, "RECONCILIATION_DRIFT"),
    ("activation_drift", 2, "ACTIVATION_DRIFT"), ("guard_drift", 2, "BARRIER_DRIFT"),
    ("cas_failure", 2, "BARRIER_ROTATION"),
])
def test_r20_fresh_and_handoff_refusals(r20_main, monkeypatch, capsys, damage, reads, reason):
    from types import SimpleNamespace

    env = r20_main
    first, second = env.projections
    account = first["account"]
    if damage.startswith("cash_") or damage == "second_cash":
        affected = second["account"] if damage == "second_cash" else account
        affected["collateral_atoms"] = str(int(affected["collateral_atoms"]) + (-1 if damage == "cash_minus" else 1))
    elif damage.endswith("_incomplete"):
        env.native_exit[damage.startswith("second")] = 4
    elif damage == "declarations":
        first["reconciliation"]["declared_orders"].pop()
    elif damage == "positions_missing":
        account.pop("positions")
    elif damage == "open_orders":
        account["open_orders"].append(env.orders[-1])
    elif damage == "native_identity":
        first["account_identity"]["chain_id"] = 1
    elif damage == "offset_claims":
        for item, delta in zip(account["claimed_win"][:2], (1, -1)):
            item["claim_atoms"] = item["inventory_atoms"] = str(int(item["claim_atoms"]) + delta)
    elif damage == "duplicate_zero":
        account["settled_zero"].append(copy.deepcopy(account["settled_zero"][0]))
    elif damage == "missing_zero":
        account["settled_zero"].pop()
    elif damage in ("wrong_official", "second_history"):
        selected = second["account"] if damage == "second_history" else account
        selected["settled_zero"][0]["oracle_status"] = "UNRESOLVED"
    elif damage == "guard_drift":
        read = env.collection.find_one
        monkeypatch.setattr(env.collection, "find_one", lambda query: (
            {**read(query), "armed_at": "drift"} if len(env.reads) == 2 else read(query)))
    elif damage == "activation_drift":
        read = control.local_signer.reconcile_account
        def drift(*args, **kwargs):
            value = read(*args, **kwargs)
            if len(env.reads) == 2:
                env.activation.write_bytes(env.old_bytes + b"\n")
            return value
        monkeypatch.setattr(control.local_signer, "reconcile_account", drift)
    elif damage == "cas_failure":
        def fail_cas(record):
            assert record == env.old
            assert json.loads(env.activation.read_bytes())["authorization_ref"] == "offline-new-authorization"
            env.calls.append("cas-miss")
            return SimpleNamespace(deleted_count=0)
        monkeypatch.setattr(env.collection, "delete_one", fail_cas)
    assert launch.main(env.argv) == 1
    assert json.loads(capsys.readouterr().out)["reason"] == reason
    expected = [f"read-{index}" for index in range(1, reads + 1)]
    if damage == "cas_failure":
        expected += ["activation", "cas-miss"]
    assert env.calls == expected and env.collection.record == env.old and not env.controllers
    attempt = env.root / "launch-attempts" / NEW_RUN
    assert not (attempt / "launch-dispatch.json").exists()
    if reads == 1 or damage in ("second_history", "second_cash", "second_incomplete"):
        assert not (attempt / "account-read-recovery.json").exists()
    if damage != "cas_failure":
        assert env.activation.read_bytes() == env.old_bytes + (b"\n" if damage == "activation_drift" else b"")


@pytest.mark.parametrize("damage,reason", [
    ("wrong_coverage", "HISTORY_REQUIRES_TAKEOVER"), ("late_fill", "HISTORY_REQUIRES_TAKEOVER"),
    ("wrong_final_fact", "HISTORY_REQUIRES_TAKEOVER"), ("fee", "LAUNCH_PREPARATION_FAILED"),
    ("generation", "LAUNCH_PREPARATION_FAILED"), ("execution_identity", "LAUNCH_PREPARATION_FAILED"),
    ("wrong_final", "LAUNCH_PREPARATION_FAILED"), ("second_hard_stop", "HISTORY_REQUIRES_TAKEOVER"),
    ("off_branch", "HISTORY_REQUIRES_TAKEOVER"), ("seed_hash", "HISTORY_REQUIRES_TAKEOVER"),
    ("old_collection", "HISTORY_REQUIRES_TAKEOVER"), ("checkpoint_atom", "HISTORY_REQUIRES_TAKEOVER"),
    ("metadata", "LAUNCH_PREPARATION_FAILED"), ("binary", "HISTORY_REQUIRES_TAKEOVER"),
    ("files", "HISTORY_REQUIRES_TAKEOVER"), ("unknown_submit", "HISTORY_REQUIRES_TAKEOVER"),
    ("unopened_unknown", "HISTORY_REQUIRES_TAKEOVER"), ("unapplied_final", "HISTORY_REQUIRES_TAKEOVER"),
])
def test_r20_historical_refusals_never_prepare(r20_main, capsys, damage, reason):
    env = r20_main
    if damage == "seed_hash":
        path = env.seed_dir / "attempt-reserved.json"
        seed = live.strict_json(path.read_bytes())
        seed["reconciliation_sha256"] = "0" * 64
        path.write_bytes(live.canonical(seed))
    elif damage == "old_collection":
        path = env.seed_dir / "reconciliation.json"
        value = live.strict_json(path.read_bytes())
        value["account"].pop("settled_zero")
        path.write_bytes(live.canonical(value))
        seed_path = env.seed_dir / "attempt-reserved.json"
        seed = live.strict_json(seed_path.read_bytes())
        seed["reconciliation_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        seed_path.write_bytes(live.canonical(seed))
    elif damage == "metadata":
        path = env.previous / "live-context.json"
        value = live.strict_json(path.read_bytes())
        value["candidate_digest"] = "0" * 64
        path.write_bytes(live.canonical(value))
    elif damage in ("binary", "files"):
        path = env.previous / "started.json"
        value = live.strict_json(path.read_bytes())
        identity = value["candidate_identity"]
        if damage == "binary":
            identity["binary_sha256"] = "0" * 64
        else:
            identity["files"][next(iter(identity["files"]))] = "0" * 64
        path.write_bytes(live.canonical(value))
        digest = live.candidate_digest(value)
        context_path = env.previous / "live-context.json"
        context = live.strict_json(context_path.read_bytes())
        context["candidate_digest"] = digest
        context_path.write_bytes(live.canonical(context))
        env.old["candidate_digest"] = env.collection.record["candidate_digest"] = digest
        activation = json.loads(env.old_bytes)
        activation["candidate_digest"] = digest
        env.old_bytes = live.canonical(activation)
        env.activation.write_bytes(env.old_bytes)
    elif damage == "off_branch":
        path = env.previous / "ended.json"
        value = live.strict_json(path.read_bytes())
        value["incomplete_reason"] = "OUTPUT_INVALID"
        path.write_bytes(live.canonical(value))
    else:
        path = env.previous / "events.jsonl"
        rows = [live.strict_json(line) for line in path.read_bytes().splitlines()]
        degraded = rows[2481]["payload"]
        final = rows[2479]["payload"]
        if damage == "wrong_coverage":
            degraded["facts"]["net_cash"]["coverage_gross_atoms"] = "0"
        elif damage == "late_fill":
            last_unproven = next(row["payload"] for row in reversed(rows[2479:])
                                 if row["event_kind"] == "execution" and row["payload"]["closure"] == "UNPROVEN")
            last_unproven["gross_q_atoms"] = str(int(last_unproven["gross_q_atoms"]) + 1)
        elif damage == "wrong_final_fact":
            last_final = next(row["payload"] for row in reversed(rows[2479:])
                              if row["event_kind"] == "execution" and row["payload"]["closure"] == "FINAL")
            last_final["settlement_status"] = "CONFLICT"
        elif damage == "fee":
            degraded["facts"]["share_fee"]["atoms"] = str(int(degraded["facts"]["share_fee"]["atoms"]) + 1)
        elif damage == "generation":
            degraded["generation"] -= 2
        elif damage == "execution_identity":
            degraded["venue_order_id"] = "1"
        elif damage == "wrong_final":
            final["final_gross_q_atoms"] = "1"
        elif damage == "second_hard_stop":
            rows[2483].update(event_kind="stop", market_id=None, token=None,
                              payload={**rows[2482]["payload"], "reason": "READ_UNAVAILABLE", "stop_detail": None})
        elif damage == "checkpoint_atom":
            value = rows[2478]["payload"]
            value["collateral_atoms"] = str(int(value["collateral_atoms"]) + 1)
        elif damage == "unapplied_final":
            row = next(row for row in reversed(rows[:2478]) if row["event_kind"] == "execution")
            row["payload"]["delta"] = dict.fromkeys(row["payload"]["delta"])
        elif damage in ("unknown_submit", "unopened_unknown"):
            order = next(row["payload"] for row in reversed(rows[:2478]) if row["event_kind"] == "order"
                         and row["payload"]["submission"] == "ACCEPTED")
            order["submission"] = "UNKNOWN"
            if damage == "unopened_unknown":
                order["venue_order_id"] = None
        path.write_bytes(b"".join(live.canonical(row) + b"\n" for row in rows))
    assert launch.main(env.argv) == 1
    assert json.loads(capsys.readouterr().out)["reason"] == reason
    assert env.calls == [] and env.collection.record == env.old
    assert env.activation.read_bytes() == env.old_bytes
    assert not (env.root / "launch-attempts" / NEW_RUN).exists()


def assert_no_handoff(env):
    assert env.activation.read_bytes() == env.old_bytes
    assert not env.controllers
    assert not any(call in env.calls for call in ("activation", "delete", "arm", "controller"))


def test_real_account_stop_prepares_new_window_only_after_both_complete_reads(account_recovery):
    env = account_recovery
    assert launch.main(env.argv) == 0
    assert env.calls == ["read-1", "read-2", "activation", "delete", "controller"]
    proof = json.loads((env.root / "launch-attempts" / NEW_RUN / "account-read-recovery.json").read_text())
    assert proof["prior_budget_atoms"] == str(env.prior)
    assert proof["gross_spend_atoms"] == str(env.spent)
    assert proof["next_budget_atoms"] == str(env.next_budget)
    assert proof["checkpoint_event_seq"] == 666
    assert len(proof["fresh_read_sha256"]) == 2
    assert len(proof["economic_orders"]) == 2
    assert proof["old_stop_detail"] == "UNKNOWN"
    assert all(path.read_bytes() == raw for path, raw in env.audit_bytes.items())
    assert json.loads((env.previous / "ended.json").read_text())["cash_complete"] is False


@pytest.mark.parametrize("damage", ["first_exit", "second_exit", "incomplete", "cash_atom", "second_cash_atom",
    "missing_positions", "null_positions", "positions", "second_missing_positions", "open", "identity",
    "history_missing", "history_extra", "history_duplicate", "history_field", "zero_missing", "zero_duplicate",
    "seed_hash", "seed_budget", "native_positions", "noncanonical", "truncated", "forced", "other_detail",
    "incomplete_audit", "loss_threshold", "gas", "pagination", "guard_drift", "cas_failure"])
def test_refuses_without_handoff(account_recovery, monkeypatch, damage):
    env = account_recovery
    first, second = env.projections
    if damage in ("first_exit", "second_exit"):
        env.native_exit[damage == "second_exit"] = 4
    elif damage == "incomplete": first["reconciliation"]["complete"] = False
    elif damage in ("cash_atom", "second_cash_atom"):
        account = (second if damage == "second_cash_atom" else first)["account"]
        account["collateral_atoms"] = str(int(account["collateral_atoms"]) + 1)
    elif damage in ("missing_positions", "second_missing_positions"):
        (second if damage == "second_missing_positions" else first)["account"].pop("positions")
    elif damage in ("null_positions", "positions"):
        first["account"]["positions"] = None if damage == "null_positions" else [{"token": "unexpected"}]
    elif damage == "open": first["account"]["open_orders"] = [env.orders[-1]]
    elif damage == "identity": first["account_identity"]["trading_address"] = "0x" + "1" * 40
    elif damage.startswith(("history_", "zero_")):
        records = first["account"]["settled_zero" if damage.startswith("zero_") else "claimed_win"]
        if damage.endswith("missing"): records.pop()
        elif damage.endswith("duplicate"): records.append(copy.deepcopy(records[-1]))
        elif damage.endswith("extra"): records.append({**records[-1], "market_id": 42})
        else: records[-1]["inventory_atoms"] = str(int(records[-1]["inventory_atoms"]) + 1)
    elif damage.startswith("seed_"):
        path = env.seed_dir / "attempt-reserved.json"
        seed = json.loads(path.read_text())
        seed["reconciliation_sha256" if damage == "seed_hash" else "committed_budget_atoms"] = (
            "a" * 64 if damage == "seed_hash" else env.prior + 1)
        path.write_text(json.dumps(seed))
    elif damage == "native_positions":
        path = env.seed_dir / "reconciliation.json"
        value = json.loads(path.read_text()); value["account"].pop("positions")
        path.write_text(json.dumps(value))
        seed_path = env.seed_dir / "attempt-reserved.json"
        seed = json.loads(seed_path.read_text()); seed["reconciliation_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        seed_path.write_text(json.dumps(seed))
    elif damage in ("noncanonical", "truncated"):
        path = env.previous / "events.jsonl"; raw = path.read_bytes()
        path.write_bytes(b" " + raw if damage == "noncanonical" else raw.rsplit(b"\n", 2)[0] + b"\n")
    elif damage in ("forced", "other_detail", "incomplete_audit"):
        path = env.previous / "ended.json"; value = json.loads(path.read_text())
        if damage == "forced": value["exit_facts"]["forced_cleanup"] = True
        elif damage == "other_detail": value["incomplete_detail"] = "INVENTORY_MISMATCH"
        else: value["audit_complete"] = False
        path.write_text(json.dumps(value))
    elif damage == "loss_threshold":
        value = json.loads(env.config.read_text()); value["strategies"]["limits"].update(max_loss_usd=1, max_total_usd=1)
        env.config.write_text(json.dumps(value))
    elif damage == "gas": first["account"]["gas_atoms"] = "0"
    elif damage == "pagination": first["account"]["pagination_complete"] = False
    elif damage == "cas_failure":
        from types import SimpleNamespace
        monkeypatch.setattr(env.collection, "delete_one", lambda query: SimpleNamespace(deleted_count=0))
    else:
        original = env.collection.find_one; reads = []
        def drift(query, *args):
            reads.append(query)
            if len(reads) == 2: env.collection.record["scope"]["host"] = "concurrent-host"
            return original(query, *args)
        monkeypatch.setattr(env.collection, "find_one", drift)
    assert launch.main(env.argv) == 1
    if damage == "cas_failure":
        # Original activation-before-CAS ordering is retained. A failed CAS
        # may leave the new activation, but never removes the old guard or starts.
        assert env.collection.record == env.old and not env.controllers
        assert "delete" not in env.calls
    else: assert_no_handoff(env)


def qualification_args(env):
    """Expose the pure proof inputs by independently replaying the real prefix."""
    started = json.loads((env.previous / "started.json").read_text())
    relations = live.LiveRelations(started["run_id"], started["risk_window_id"], started["public_config_identity"])
    checkpoint = checkpoint_event = initial = None
    tail = []
    for event in env.events:
        relations = relations.stage(event)
        if event["event_kind"] == "reconciliation":
            if initial is None: initial = event
            if event["payload"]["scope"] == "RUNTIME" and event["payload"]["complete"]:
                checkpoint, checkpoint_event, tail = relations, event, []
                continue
        if checkpoint is not None: tail.append(event)
    checked = launch.preflight(env.previous / "public-config.json", env.seed_dir / "reconciliation.json",
        now=launch._parse_time(started["started_at"]), declared_orders=env.seed["declared_orders"])
    return dict(started=started, ended=json.loads((env.previous / "ended.json").read_text()), relations=relations,
        checkpoint=checkpoint, checkpoint_event=checkpoint_event, tail=tail,
        old_native=json.loads((env.seed_dir / "reconciliation.json").read_text()), old_params=checked["params"],
        seed=copy.deepcopy(env.seed), initial=initial)


@pytest.mark.parametrize("damage", ["legacy_binary", "legacy_files", "unknown", "sent_then_unsent", "unclosed",
    "remaining", "missing_execution", "pages", "coverage", "fee_atom", "generation", "owner", "missing_claim",
    "unpaid_claim", "loss", "stopped", "initial_cash", "checkpoint_cash", "loss_equal", "checkpoint_open",
    "tail_delta", "tail_generation", "tail_fact", "tail_order", "tail_candidate", "tail_claim", "final_generation",
    "final_claims", "final_attempt", "other_stop", "checkpoint_seq"])
def test_qualification_refuses_changed_or_incomplete_proofs(account_recovery, damage):
    import btc5m_account_recovery as recovery
    env = account_recovery; args = qualification_args(env)
    checkpoint, final = args["checkpoint"], args["relations"]
    key = next(k for k, item in checkpoint.attempts.items() if item["submission"] == "ACCEPTED")
    owner = checkpoint.attempts[key]; execution = owner["execution"]
    tail_execution = next(event["payload"] for event in args["tail"] if event["event_kind"] == "execution")
    if damage == "legacy_binary": args["started"]["candidate_identity"]["binary_sha256"] = "0" * 64
    elif damage == "legacy_files": args["started"]["candidate_identity"]["files"]["src/service/predict_fun/account.rs"] = "0" * 64
    elif damage == "unknown": owner["submission"] = "UNKNOWN"
    elif damage == "sent_then_unsent":
        next(item for item in checkpoint.attempts.values() if item["submission"] == "NOT_SUBMITTED")["send_observed"] = True
    elif damage == "unclosed": execution["closure"] = "UNPROVEN"
    elif damage == "remaining": owner["remaining"] = 1
    elif damage == "missing_execution": owner["execution"] = None
    elif damage == "pages": execution["pages_complete"] = False
    elif damage == "coverage": execution["facts"]["net_cash"]["coverage_gross_atoms"] = "0"
    elif damage == "fee_atom": owner["applied"]["share_fee_atoms"] += 1
    elif damage == "generation": checkpoint.reconciled_generations[key] += 1
    elif damage == "owner":
        next(item for k, item in checkpoint.attempts.items() if k != key and item["submission"] == "ACCEPTED")["identity"] = owner["identity"]
    elif damage == "missing_claim": checkpoint.claims.pop(key)
    elif damage == "unpaid_claim":
        for claim in checkpoint.claims.values():
            if claim["value"][1]: claim["confirmed"] = False
    elif damage == "loss": checkpoint.latched = True
    elif damage == "stopped": checkpoint.stopped = True
    elif damage == "initial_cash": args["initial"]["accounting"]["cash"] = str(env.prior + 1)
    elif damage == "checkpoint_cash": args["checkpoint_event"]["accounting"]["cash"] = "1"
    elif damage == "loss_equal": args["old_params"]["loss"] = env.spent
    elif damage == "checkpoint_open": checkpoint.reconciled_open_hashes = [env.orders[-1]]
    elif damage == "tail_delta": tail_execution["delta"]["net_cash_atoms"] = "1"
    elif damage == "tail_generation": tail_execution["generation"] = 0
    elif damage == "tail_fact": tail_execution["facts"]["net_cash"]["atoms"] = "1"
    elif damage in ("tail_order", "tail_claim"): args["tail"][0]["event_kind"] = "order" if damage == "tail_order" else "payout"
    elif damage == "tail_candidate": args["tail"][0]["payload"]["decision"]["candidate_id"] = "999"
    elif damage == "final_generation": final.attempts[key]["execution"]["generation"] += 1
    elif damage == "final_claims": final.claims = {}
    elif damage == "final_attempt": final.attempts["999"] = copy.deepcopy(owner)
    elif damage == "other_stop": next(event for event in args["tail"] if event["event_kind"] == "stop")["payload"]["stop_detail"] = "OTHER"
    else: args["checkpoint_event"]["event_seq"] -= 1
    with pytest.raises(live.LiveInvalid): recovery.qualify(**args)
    assert env.calls == []
    assert_no_handoff(env)


def test_reordered_complete_fresh_histories_are_not_drift(account_recovery):
    env = account_recovery
    for key in ("claimed_win", "settled_zero"): env.projections[1]["account"][key].reverse()
    assert launch.main(env.argv) == 0
    assert env.calls == ["read-1", "read-2", "activation", "delete", "controller"]
