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
