"""Real4549 immutable audit; account observations and handoff are synthetic."""
import copy
import json

import pytest

from test_btc5m_launch import native_main
from test_btc5m_account_recovery import (
    account_recovery,
    qualification_args,
    test_refuses_without_handoff as refuse_fresh_case,
)
import btc5m_launch as launch
import btc5m_live_events as live
import btc5m_account_recovery as recovery
import btc5m_control as control

OLD_RUN = "btc5m-live-4549aad3a86c4bdb"


@pytest.mark.parametrize("account_recovery", [OLD_RUN], indirect=True)
def test_real_settled_inventory_stop_reaches_both_reads_and_one_handoff(account_recovery):
    env = account_recovery
    stopped = launch._stopped_window(env.old, env.account)
    assert stopped["kind"] == "ACCOUNT_READ"
    assert stopped["orders"] == env.orders
    assert launch.main(env.argv) == 0
    assert env.calls == ["read-1", "read-2", "activation", "delete", "controller"]
    proof = json.loads((env.root / "launch-attempts/btc5m-unattended-live-012/account-read-recovery.json").read_text())
    assert proof["old_stop_detail"] == "INVENTORY_MISMATCH"
    assert proof["prior_budget_atoms"] == "90652900000000000000"
    assert proof["gross_spend_atoms"] == "1958800000000000000"
    assert proof["next_budget_atoms"] == "88694100000000000000"
    assert proof["checkpoint_event_seq"] == 1037
    assert len(proof["economic_orders"]) == 2
    assert len(proof["zero_fill_orders"]) == 1
    assert proof["zero_fill_orders"][0]["signed_hash"] == env.orders[-2]
    assert len(proof["fresh_read_sha256"]) == 2
    assert all(path.read_bytes() == raw for path, raw in env.audit_bytes.items())
    assert json.loads((env.previous / "ended.json").read_text())["cash_complete"] is False


@pytest.mark.parametrize("account_recovery", [OLD_RUN], indirect=True)
def test_real_controller_receives_all_three_terminal_orders_once(account_recovery, monkeypatch):
    env = account_recovery
    monkeypatch.setattr(control, "run", env.original_controller)
    assert launch.main(env.argv) == 0
    assert env.calls == ["read-1", "read-2", "activation", "delete", "arm", "child"]
    assert len(env.children) == 1
    command = env.children[0][0]
    assert [command[index + 1] for index, option in enumerate(command)
            if option == "--predict-reconciled-order"] == env.orders
    assert all(path.read_bytes() == raw for path, raw in env.audit_bytes.items())


@pytest.mark.parametrize("account_recovery", [OLD_RUN], indirect=True)
@pytest.mark.parametrize("damage", ["first_exit", "second_exit", "incomplete", "cash_atom", "second_cash_atom",
    "missing_positions", "null_positions", "positions", "second_missing_positions", "open", "identity",
    "history_missing", "history_extra", "history_duplicate", "history_field", "zero_missing", "zero_duplicate",
    "seed_hash", "seed_budget", "native_positions", "noncanonical", "truncated", "forced",
    "incomplete_audit", "loss_threshold", "gas", "pagination", "guard_drift", "cas_failure"])
def test_new_branch_refuses_incomplete_fresh_or_history(account_recovery, monkeypatch, damage):
    refuse_fresh_case(account_recovery, monkeypatch, damage)


@pytest.mark.parametrize("account_recovery", [OLD_RUN], indirect=True)
@pytest.mark.parametrize("damage", ["unknown", "unclosed", "quantity", "fee", "generation", "hash", "venue",
                                   "missing_claim", "unpaid_claim", "tail_delta", "tail_order", "candidate",
                                   "loss", "identity", "files", "other_stop", "draw", "claim_on_zero"])
def test_new_branch_refuses_unproven_owners_or_tail(account_recovery, damage):
    args = qualification_args(account_recovery)
    checkpoint = args["checkpoint"]
    zero = checkpoint.attempts["2"]
    execution = zero["execution"]
    if damage == "unknown": zero["submission"] = "UNKNOWN"
    elif damage == "unclosed": execution["closure"] = "UNPROVEN"
    elif damage == "quantity": execution["gross_q_atoms"] = "1"
    elif damage == "fee": zero["applied"]["share_fee_atoms"] = 1
    elif damage == "generation": checkpoint.reconciled_generations["2"] += 1
    elif damage == "hash": execution["signed_hash"] = "0x" + "a" * 64
    elif damage == "venue": execution["venue_order_id"] = "other-order"
    elif damage == "missing_claim": checkpoint.claims.pop("3")
    elif damage == "unpaid_claim": checkpoint.claims["1"]["confirmed"] = False
    elif damage == "tail_delta":
        next(frame for frame in args["tail"] if frame["event_kind"] == "execution")["payload"]["delta"]["gross_q_atoms"] = "1"
    elif damage == "tail_order": args["tail"][0]["event_kind"] = "order"
    elif damage == "candidate": args["tail"][0]["payload"]["decision"]["candidate_id"] = "9000"
    elif damage == "loss": checkpoint.latched = True
    elif damage == "identity": args["started"]["candidate_identity"]["binary_sha256"] = "0" * 64
    elif damage == "files": args["started"]["candidate_identity"]["files"]["src/service/predict_fun/account.rs"] = "0" * 64
    elif damage == "other_stop": args["ended"]["incomplete_detail"] = "UNKNOWN"
    elif damage == "draw":
        shares, _, payout = checkpoint.claims["1"]["value"]
        checkpoint.claims["1"]["value"] = (shares, shares // 2, payout)
    else: checkpoint.claims["2"] = copy.deepcopy(checkpoint.claims["3"])
    with pytest.raises(live.LiveInvalid):
        recovery.qualify(**args)
