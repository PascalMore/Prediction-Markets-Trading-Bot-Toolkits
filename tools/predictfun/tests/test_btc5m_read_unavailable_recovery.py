"""Real 00767 prefix is immutable; fresh reads, DB, secrets and child are fake."""
import copy
import datetime as dt
import hashlib
import json
import shutil
from pathlib import Path

import pytest

from test_btc5m_launch import native_main
from test_btc5m_account_recovery import (
    assert_no_handoff, qualification_args, test_refuses_without_handoff as refuse_fresh_case,
)
import btc5m_control as control
import btc5m_launch as launch
import btc5m_live_events as live
import btc5m_account_recovery as recovery

OLD_RUN = "btc5m-live-00767cdfab8a4567"
NEW_RUN = "btc5m-unattended-live-012"


@pytest.fixture
def read_unavailable(native_main, monkeypatch):
    """Keep old IDs/clocks/frames intact, adapt only the new run and fresh clock."""
    env = native_main
    source = control.BOT.parents[4] / "data/reports/2026-11-26-btc5m-controlled-paper/evidence"
    env.previous = env.root / OLD_RUN
    env.seed_dir = env.root / "launch-attempts" / OLD_RUN
    shutil.copytree(source / OLD_RUN, env.previous)
    shutil.copytree(source / "launch-attempts" / OLD_RUN, env.seed_dir)
    shutil.copyfile(env.previous / "public-config.json", env.config)
    env.audit_bytes = {path: path.read_bytes() for directory in (env.previous, env.seed_dir)
                       for path in directory.iterdir() if path.is_file()}
    assert hashlib.sha256((env.previous / "events.jsonl").read_bytes()).hexdigest() == (
        "02d51b53bbd6d983807a57e231ee58c23a4e520b87a5f682d0177266ed1a1494")
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
    env.prior = env.seed["committed_budget_atoms"]
    env.orders = list(env.seed["declared_orders"])
    for frame in env.events:
        if frame["event_kind"] == "order" and frame["payload"]["submission"] == "ACCEPTED":
            order = frame["payload"]["signed_hash"]
            if order not in env.orders:
                env.orders.append(order)
    fresh = json.loads((env.seed_dir / "reconciliation.json").read_text())
    fresh["reconciliation"]["declared_orders"] = list(env.orders)
    # No balance fitting or synthetic healthy old final: zero window economics
    # means the original native account history/collateral are unchanged.
    env.projections = [copy.deepcopy(fresh), copy.deepcopy(fresh)]
    env.native_exit = [0, 0]
    env.reads, env.controllers = [], []
    env.argv = ["--config", str(env.config), "--auto-prepare", "--run-id", NEW_RUN,
                "--authorization-ref", "offline-new-authorization", "--evidence-root", str(env.root)]

    def reconcile(config, rpc_url, orders, output, *, environment):
        index = len(env.reads)
        assert index < 2
        assert control.parameters(json.loads(Path(config).read_text()))["budget"] == env.prior
        assert orders == env.orders
        env.reads.append(list(orders))
        env.calls.append(f"read-{index + 1}")
        value = copy.deepcopy(env.projections[index])
        value["observation"]["finished_at"] = getattr(
            env, "fresh_time", dt.datetime.now(dt.timezone.utc).isoformat())
        Path(output).write_text(json.dumps(value))
        return {"exit_code": env.native_exit[index], "projection": value}

    replace = launch.os.replace

    def publish(source, target):
        assert Path(target) == env.activation
        env.calls.append("activation")
        return replace(source, target)

    env.qualified = 0
    qualify = recovery.qualify_read_unavailable

    def qualify_once(**kwargs):
        env.qualified += 1
        return qualify(**kwargs)

    monkeypatch.setattr(recovery, "qualify_read_unavailable", qualify_once)
    monkeypatch.setattr(control.local_signer, "reconcile_account", reconcile)
    monkeypatch.setattr(launch.os, "replace", publish)
    return env


def test_real_prefix_qualifies_without_rewriting_submitted_orders(read_unavailable):
    env = read_unavailable
    stopped = launch._stopped_window(env.old, env.account)
    assert stopped["kind"] == "ACCOUNT_READ"
    assert stopped["orders"] == env.orders
    assert stopped["budget_atoms"] == env.prior
    assert stopped["recovery"]["proof_kind"] == "READ_UNAVAILABLE_ZERO_FILL_PLUS_FRESH_NATIVE_REQUIRED"
    assert len(stopped["recovery"]["zero_fill_orders"]) == 2
    assert stopped["recovery"]["economic_orders"] == []
    assert stopped["recovery"]["checkpoint_event_seq"] == 490
    assert all(path.read_bytes() == raw for path, raw in env.audit_bytes.items())


def test_original_parser_main_dispatch_and_fixed_manifest(read_unavailable):
    env = read_unavailable
    assert launch.main(env.argv) == 0
    assert env.calls == ["read-1", "read-2", "activation", "delete", "arm", "child"]
    assert len(env.children) == 1
    command = env.children[0][0]
    assert [command[index + 1] for index, option in enumerate(command)
            if option == "--predict-reconciled-order"] == env.orders
    started = json.loads((env.root / NEW_RUN / "started.json").read_text())
    assert started["candidate_identity"] == control.public_identity({}, live=True)
    proof = json.loads((env.root / "launch-attempts" / NEW_RUN / "account-read-recovery.json").read_text())
    assert proof["prior_budget_atoms"] == proof["next_budget_atoms"] == str(env.prior)
    assert proof["gross_spend_atoms"] == proof["net_loss_atoms"] == "0"
    assert len(proof["fresh_read_sha256"]) == 2
    assert all(path.read_bytes() == raw for path, raw in env.audit_bytes.items())
    assert json.loads((env.previous / "ended.json").read_text())["final_accounting"]["cash"] is None


@pytest.mark.parametrize("damage", [
    "first_exit", "second_exit", "incomplete", "cash_atom", "second_cash_atom",
    "missing_positions", "null_positions", "positions", "second_missing_positions", "open", "identity",
    "history_missing", "history_extra", "history_duplicate", "history_field", "zero_missing", "zero_duplicate",
    "seed_hash", "seed_budget", "native_positions", "noncanonical", "truncated", "forced", "other_detail",
    "incomplete_audit", "gas", "pagination", "guard_drift", "cas_failure",
])
def test_original_main_refuses_without_child(read_unavailable, monkeypatch, damage):
    env = read_unavailable
    refuse_fresh_case(env, monkeypatch, damage)
    assert env.children == []
    if damage == "guard_drift":
        expected = copy.deepcopy(env.old)
        expected["scope"]["host"] = "concurrent-host"
        assert env.collection.record == expected
    elif damage != "cas_failure":
        assert env.collection.record == env.old
    if damage not in {"seed_hash", "seed_budget", "native_positions", "noncanonical", "truncated",
                      "forced", "other_detail", "incomplete_audit"}:
        assert env.qualified == 1  # Rejection is downstream of this new qualification.


@pytest.mark.parametrize("pass_index", [0, 1])
@pytest.mark.parametrize("damage", ["incomplete", "pagination", "history_hash", "cash_minus_atom",
    "identity", "gas", "positions", "open", "history_offset", "history_field", "history_missing"])
def test_each_fresh_pass_is_complete_exact_and_history_preserving(read_unavailable, pass_index, damage):
    env = read_unavailable
    value = env.projections[pass_index]
    if damage == "incomplete":
        value["reconciliation"]["complete"] = False
    elif damage == "pagination":
        value["account"]["pagination_complete"] = False
    elif damage == "history_hash":
        value["reconciliation"]["declared_orders"][-1] = "0x" + "a" * 64
    elif damage == "cash_minus_atom":
        value["account"]["collateral_atoms"] = str(int(value["account"]["collateral_atoms"]) - 1)
    elif damage == "identity":
        value["account_identity"]["signer_address"] = "0x" + "1" * 40
    elif damage == "gas":
        value["account"]["gas_atoms"] = "0"
    elif damage == "positions":
        value["account"]["positions"] = [{"token": "unexplained"}]
    elif damage == "open":
        value["account"]["open_orders"] = [env.orders[-1]]
    elif damage == "history_offset":
        # Same net claim/cash but different members must not cancel each other.
        for projection in env.projections:
            records = projection["account"]["claimed_win"]
            for record, delta in zip(records, (1, -1)):
                record["claim_atoms"] = str(int(record["claim_atoms"]) + delta)
                record["inventory_atoms"] = record["claim_atoms"]
    elif damage == "history_field":
        value["account"]["settled_zero"][-1]["market_id"] += 1
    else:
        value["account"]["claimed_win"].pop()
    assert launch.main(env.argv) == 1
    assert env.qualified == 1
    assert_no_handoff(env)
    assert env.children == [] and env.collection.record == env.old


@pytest.mark.parametrize("seconds", [-1000, 1000])
def test_fresh_clock_refuses_past_and_future(read_unavailable, seconds):
    env = read_unavailable
    env.fresh_time = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=seconds)).isoformat()
    assert launch.main(env.argv) == 1
    assert env.qualified == 1
    assert_no_handoff(env)
    assert env.children == [] and env.collection.record == env.old


@pytest.mark.parametrize("damage", ["binary", "source"])
def test_source_pinning_rejects_valid_metadata_drift(read_unavailable, damage):
    env = read_unavailable
    path = env.previous / "started.json"
    started = json.loads(path.read_text())
    if damage == "binary":
        started["candidate_identity"]["binary_sha256"] = control.LIVE_BINARY_SHA256
    else:
        started["candidate_identity"]["files"]["tools/predictfun/btc5m_account_recovery.py"] = "0" * 64
    path.write_bytes(live.canonical(started) + b"\n")
    env.old["candidate_digest"] = live.candidate_digest(started)
    context_path = env.previous / "live-context.json"
    context = json.loads(context_path.read_text())
    context["candidate_digest"] = env.old["candidate_digest"]
    context_path.write_bytes(live.canonical(context) + b"\n")
    env.collection.record = copy.deepcopy(env.old)
    activation = json.loads(env.activation.read_text())
    activation["candidate_digest"] = env.old["candidate_digest"]
    env.activation.write_text(json.dumps(activation))
    env.old_bytes = env.activation.read_bytes()
    assert launch.main(env.argv) == 1
    assert env.qualified == 1 and env.calls == []
    assert_no_handoff(env)
    assert env.children == [] and env.collection.record == env.old


@pytest.mark.parametrize("damage", ["unknown", "unclosed", "missing_execution", "quantity", "fee",
    "generation", "hash", "venue", "remaining", "claim", "unpaid_claim", "loss", "stopped",
    "initial_cash", "checkpoint_cash", "checkpoint_collateral", "checkpoint_open", "tail_delta",
    "tail_generation", "tail_fact", "tail_order", "tail_candidate", "final_generation", "final_claims",
    "final_attempt", "other_stop", "checkpoint_seq", "prefix_missing", "early_unknown",
    "early_nonzero", "early_fee", "early_payout", "early_loss", "early_cash", "early_inventory"])
def test_qualification_rejects_unproven_prefix_checkpoint_and_tail(read_unavailable, damage):
    env = read_unavailable
    args = qualification_args(env)
    args["prefix"] = copy.deepcopy(env.events)
    checkpoint, final = args["checkpoint"], args["relations"]
    owner = checkpoint.attempts["1"]
    execution = owner["execution"]
    tail_execution = next(frame["payload"] for frame in args["tail"] if frame["event_kind"] == "execution")
    early = next(frame for frame in args["prefix"] if frame["event_kind"] == "execution")
    if damage == "unknown": owner["submission"] = "UNKNOWN"
    elif damage == "unclosed": execution["closure"] = "UNPROVEN"
    elif damage == "missing_execution": owner["execution"] = None
    elif damage == "quantity": execution["gross_q_atoms"] = "1"
    elif damage == "fee": owner["applied"]["share_fee_atoms"] = 1
    elif damage == "generation": checkpoint.reconciled_generations["1"] += 1
    elif damage == "hash": execution["signed_hash"] = "0x" + "a" * 64
    elif damage == "venue": execution["venue_order_id"] = "other-order"
    elif damage == "remaining": owner["remaining"] = 1
    elif damage in ("claim", "unpaid_claim"):
        checkpoint.claims["1"] = {"value": (1, 1, 1.0), "confirmed": damage == "claim"}
    elif damage == "loss": checkpoint.latched = True
    elif damage == "stopped": checkpoint.stopped = True
    elif damage == "initial_cash": args["initial"]["accounting"]["cash"] = str(env.prior + 1)
    elif damage == "checkpoint_cash": args["checkpoint_event"]["accounting"]["cash"] = str(env.prior - 1)
    elif damage == "checkpoint_collateral": args["checkpoint_event"]["payload"]["collateral_atoms"] = "1"
    elif damage == "checkpoint_open": checkpoint.reconciled_open_hashes = [env.orders[-1]]
    elif damage == "tail_delta": tail_execution["delta"]["net_cash_atoms"] = "1"
    elif damage == "tail_generation": tail_execution["generation"] = 0
    elif damage == "tail_fact": tail_execution["facts"]["net_cash"]["atoms"] = "1"
    elif damage == "tail_order": args["tail"][0]["event_kind"] = "order"
    elif damage == "tail_candidate": args["tail"][0]["payload"]["decision"]["candidate_id"] = "9000"
    elif damage == "final_generation": final.attempts["1"]["execution"]["generation"] += 1
    elif damage == "final_claims": final.claims["1"] = {"value": (1, 1, 1.0), "confirmed": False}
    elif damage == "final_attempt": final.attempts["999"] = copy.deepcopy(owner)
    elif damage == "other_stop": next(frame for frame in args["tail"] if frame["event_kind"] == "stop")["payload"]["reason"] = "OTHER"
    elif damage == "checkpoint_seq": args["checkpoint_event"]["event_seq"] -= 1
    elif damage == "prefix_missing": args["prefix"].pop(1)
    elif damage == "early_unknown": early["payload"]["facts"]["net_cash"]["status"] = "UNKNOWN"
    elif damage == "early_nonzero": early["payload"]["gross_q_atoms"] = "1"
    elif damage == "early_fee": early["payload"]["facts"]["share_fee"]["atoms"] = "1"
    elif damage == "early_payout": early["event_kind"] = "payout"
    elif damage == "early_loss": early["accounting"]["loss_latched"] = True
    elif damage == "early_cash": early["accounting"]["cash"] = str(env.prior + 1)
    else: early["payload"]["facts"]["net_inventory"]["atoms"] = "1"
    with pytest.raises(live.LiveInvalid):
        recovery.qualify_read_unavailable(**args)
    assert env.qualified == 1 and env.calls == []
    assert_no_handoff(env)
    assert env.children == [] and env.collection.record == env.old
