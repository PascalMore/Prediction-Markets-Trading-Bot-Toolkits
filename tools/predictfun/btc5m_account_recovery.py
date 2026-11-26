"""Pure qualification of settled orders after recognized account/inventory stops.

No network, signing, processes or persistence. A new proof never repairs the
old incomplete final and never establishes current account availability.
"""
import hashlib
from typing import Any

import btc5m_live_events as live

LEGACY_BINARY_SHA256 = "1d693ed2d00795f9c958eb22d43410b0c4d1f1ad46fe88ce61efc8d49107b792"
LEGACY_FILES_SHA256 = "6de472f32a0076c26ba1888844b5c91c2468f0aa092144d769bc9d5e0e820238"
SETTLED_BINARY_SHA256 = "9e437aa8fc047bbeccc60d5895e9d3418848819e7be9b16bf080fb2b4fbd78e3"
SETTLED_FILES_SHA256 = "83b07c5e21547216526189db6d111b82a8b7140c4a5dd17b9394a7e9e745d960"


READ_UNAVAILABLE_BINARY_SHA256 = "bee8cbc4ab916133dcf2bd86306c65d0d0923d1009f2300880601c506e652dd1"
READ_UNAVAILABLE_FILES_SHA256 = "c43ab1fcfa475880520420aa5422e3c7bdc376c88f7aee0c6485f00ff31fd662"


def known_read_unavailable(ended: dict) -> bool:
    """Recognize only the audited, normal-exit, null-detail read stop envelope."""
    return (ended.get("audit_complete") is True and ended.get("cash_complete") is False
            and ended.get("execution_complete") is False and ended.get("takeover_required") is True
            and ended.get("incomplete_reason") == "FACT_INCOMPLETE"
            and "incomplete_detail" in ended and ended["incomplete_detail"] is None
            and ended.get("exit_facts") == {"child_returncode": 0, "child_signal": None,
                "forced_cleanup": False, "launcher_exit": 0, "stop_reason": "READ_UNAVAILABLE"})


def known_ended(ended: dict) -> bool:
    """Recognize supported envelopes; qualify still binds each to exact sources."""
    return (ended.get("audit_complete") is True
            and ended.get("cash_complete") is False
            and ended.get("execution_complete") is False
            and ended.get("takeover_required") is True
            and ended.get("incomplete_reason") == "FACT_INCOMPLETE"
            and ended.get("incomplete_detail") in ("UNKNOWN", "INVENTORY_MISMATCH")
            and ended.get("exit_facts") == {"child_returncode": 0, "child_signal": None,
                "forced_cleanup": False, "launcher_exit": 0, "stop_reason": "RECONCILIATION_REQUIRED"})


def _execution_facts(execution: dict) -> dict:
    """Project all original execution fields except receipt time and generation."""
    return {key: value for key, value in execution.items() if key not in ("observed_at", "generation")}


def _check_tail(checkpoint: Any, event: dict, tail: list[dict], relations: Any,
                stop_detail: str | None = "UNKNOWN",
                stop_reason: str = "RECONCILIATION_REQUIRED") -> None:
    """Require every event after reconciliation to carry no new economic fact."""
    live.require(bool(tail) and tail[-1] == relations.final
                 and all(frame["event_seq"] == checkpoint.seq + index for index, frame in enumerate(tail, 1))
                 and tail[-1]["event_seq"] == relations.seq
                 and set(relations.attempts) == set(checkpoint.attempts)
                 and relations.claims == checkpoint.claims)
    generations = {key: item["execution"]["generation"] for key, item in checkpoint.attempts.items()
                   if item["execution"] is not None}
    stops = 0
    for frame in tail:
        kind, payload, accounting = frame["event_kind"], frame["payload"], frame["accounting"]
        live.require(not accounting["loss_latched"] and accounting["reserved"] == "0"
                     and accounting["cost"] == "0" and accounting["pending_claim_value"] is None)
        if accounting["status"] == "COMPLETE":
            live.require({k: v for k, v in accounting.items() if k != "mark_time"}
                         == {k: v for k, v in event["accounting"].items() if k != "mark_time"})
        else:
            live.require(accounting["status"] == "INCOMPLETE" and accounting["reason"] == "FACT_INCOMPLETE")
        if kind == "execution":
            key = payload["attempt_id"]
            live.require(key in generations and all(int(value) == 0 for value in payload["delta"].values())
                         and _execution_facts(payload) == _execution_facts(checkpoint.attempts[key]["execution"])
                         and payload["generation"] >= generations[key])
            generations[key] = payload["generation"]
        elif kind == "decision":
            decision = payload["decision"]
            live.require(stops == 0 and decision["phase"] == "EVALUATE"
                         and decision["candidate_id"] is None and decision["selected_token"] is None)
        elif kind == "stop":
            live.require(payload["reason"] == stop_reason
                         and payload["stop_detail"] == stop_detail and stops == 0)
            stops += 1
        elif kind == "recovery":
            live.require(stops == 1 and payload == {"policy": "COLD_START_REFUSE",
                         "reason": stop_reason, "state": "HANDOFF"})
        elif kind == "final":
            live.require(stops == 1 and frame == tail[-1])
        else:
            live.require(False)
    live.require(stops == 1)
    for key, before in checkpoint.attempts.items():
        after = relations.attempts[key]
        live.require({k: v for k, v in before.items() if k != "execution"}
                     == {k: v for k, v in after.items() if k != "execution"})
        if before["execution"] is not None:
            live.require(_execution_facts(before["execution"]) == _execution_facts(after["execution"])
                         and after["execution"]["generation"] == generations[key])


def qualify_read_unavailable(*, started: dict, ended: dict, relations: Any, checkpoint: Any,
            checkpoint_event: dict, tail: list[dict], old_native: dict,
            old_params: dict, seed: dict, initial: dict | None, prefix: list[dict]) -> dict:
    """Qualify the pinned zero-fill read stop, never a repaired historical final.

    The caller has replayed the entire canonical prefix through LiveRelations
    and bound native/seed/public C0. Only fresh complete reads may admit a run.
    """
    candidate = started["candidate_identity"]
    live.require(known_read_unavailable(ended)
                 and candidate["binary_sha256"] == READ_UNAVAILABLE_BINARY_SHA256
                 and hashlib.sha256(live.canonical(candidate["files"])).hexdigest() == READ_UNAVAILABLE_FILES_SHA256
                 and checkpoint is not None and initial is not None and relations.final is not None)
    live.require(relations.final["payload"] == {
        "audit_complete": True, "cash_complete": False, "execution_complete": False,
        "positions": [], "run_status": "INCOMPLETE", "stop_detail": None,
        "stop_reason": "READ_UNAVAILABLE", "takeover_required": True, "unresolved": []}
        and not relations.latched and not checkpoint.latched and not checkpoint.stopped
        and not checkpoint.cash_pending and not relations.cash_pending
        and checkpoint.claims == relations.claims == {}
        and checkpoint.positions == relations.positions == []
        and checkpoint.reconciled_open_hashes == relations.reconciled_open_hashes == [])
    budget, account = old_params["budget"], old_native["account"]
    live.require(type(seed.get("committed_budget_atoms")) is int
                 and seed["committed_budget_atoms"] == budget and budget > 0
                 and account.get("positions") == []
                 and all(type(account.get(key)) is list for key in ("claimed_win", "settled_zero"))
                 and initial["event_kind"] == "reconciliation"
                 and initial["payload"]["scope"] == "STARTUP"
                 and initial["payload"]["complete"] is True
                 and not initial["payload"]["foreign_risk"]
                 and initial["payload"]["positions"] == initial["payload"]["open_hashes"] == []
                 and initial["payload"]["collateral_atoms"] == account["collateral_atoms"]
                 and initial["accounting"]["cash"] == str(budget))
    snapshot, accounting = checkpoint_event["payload"], checkpoint_event["accounting"]
    live.require(checkpoint_event["event_kind"] == "reconciliation"
                 and checkpoint_event["event_seq"] == checkpoint.seq
                 and snapshot["scope"] == "RUNTIME" and snapshot["complete"] is True
                 and not snapshot["foreign_risk"] and snapshot["open_hashes"] == snapshot["positions"] == []
                 and snapshot["collateral_atoms"] == account["collateral_atoms"]
                 and accounting["status"] == "COMPLETE" and accounting["reason"] == "NONE"
                 and all(accounting[key] == str(budget) for key in ("cash", "nav", "available"))
                 and all(accounting[key] == "0" for key in ("cost", "reserved", "realized", "unrealized", "net_pnl")))
    _check_tail(checkpoint, checkpoint_event, tail, relations, None, "READ_UNAVAILABLE")
    # Never let a healthy last checkpoint erase earlier unknown execution,
    # fees, inventory or payouts. Transient MAY_HAVE_SUBMITTED/UNKNOWN is the
    # producer's send intent; every such owner must end as ACCEPTED + Final ZERO.
    live.require(len(prefix) == relations.seq and prefix[-1] == relations.final
                 and all(frame["event_seq"] == index for index, frame in enumerate(prefix, 1)))
    for frame in prefix:
        payload, book, kind = frame["payload"], frame["accounting"], frame["event_kind"]
        live.require(not book["loss_latched"] and book["cost"] == "0"
                     and book["pending_claim_value"] is None and kind != "payout")
        for key in ("cash", "nav"):
            live.require(book[key] is None or book[key] == str(budget))
        for key in ("realized", "unrealized", "net_pnl"):
            live.require(book[key] in (None, "0"))
        if kind == "execution":
            live.require(payload["quantity_class"] == "ZERO" and payload["pages_complete"] is True
                         and payload["gross_q_atoms"] == payload["accounting_gross_q_atoms"] == "0"
                         and payload["final_gross_q_atoms"] in (None, "0")
                         and all(value == "0" for value in payload["delta"].values())
                         and all(fact == {"atoms": "0", "coverage_gross_atoms": "0",
                                          "reason": "NONE", "status": "KNOWN"}
                                 for name, fact in payload["facts"].items() if name != "vwap"))
        elif kind == "reconciliation" and payload["complete"]:
            live.require(not payload["foreign_risk"] and payload["positions"] == []
                         and payload["collateral_atoms"] == account["collateral_atoms"])
    zero_orders = []
    for key, attempt in checkpoint.attempts.items():
        if checkpoint._zero_risk_unsent(attempt):
            live.require(attempt["hash"] not in seed["declared_orders"])
            continue
        execution = attempt["execution"]
        live.require(attempt["submission"] == "ACCEPTED" and attempt["hash"] is not None
                     and attempt["venue"] is not None and attempt["remaining"] == 0
                     and attempt["hash"] not in seed["declared_orders"]
                     and execution is not None and checkpoint._execution_closed(execution)
                     and execution["quantity_class"] == "ZERO"
                     and execution["gross_q_atoms"] == execution["final_gross_q_atoms"] == "0"
                     and execution["signed_hash"] == attempt["hash"]
                     and execution["venue_order_id"] == attempt["venue"]
                     and checkpoint.reconciled_generations.get(key) == execution["generation"]
                     and all(value == 0 for value in attempt["applied"].values()))
        zero_orders.append({"signed_hash": attempt["hash"], "venue_order_id": attempt["venue"],
            "market_id": attempt["identity"][0], "token": attempt["identity"][1],
            "checkpoint_generation": execution["generation"],
            "final_generation": relations.attempts[key]["execution"]["generation"]})
    live.require(bool(zero_orders))
    return {"proof_kind": "READ_UNAVAILABLE_ZERO_FILL_PLUS_FRESH_NATIVE_REQUIRED",
            "old_run_id": started["run_id"], "old_stop_detail": None, "old_final_preserved": True,
            "checkpoint_event_seq": checkpoint.seq, "final_event_seq": relations.seq,
            "prior_budget_atoms": str(budget), "next_budget_atoms": str(budget),
            "gross_spend_atoms": "0", "net_loss_atoms": "0", "confirmed_window_claim_atoms": "0",
            "economic_orders": [], "zero_fill_orders": zero_orders,
            "window_order_hashes": [item["signed_hash"] for item in zero_orders],
            "expected_history": {key: list(account[key]) for key in ("claimed_win", "settled_zero")}}


def qualify(*, started: dict, ended: dict, relations: Any, checkpoint: Any,
            checkpoint_event: dict, tail: list[dict], old_native: dict,
            old_params: dict, seed: dict, initial: dict | None) -> dict:
    """Derive C0 and exact expected history from fully replayed settled owners.

    Caller binds metadata/guard, canonical prefix, seed/native hash and the old
    preflight. All fresh-account facts must still be proved twice by the caller.
    """
    candidate = started["candidate_identity"]
    stop_detail = ended.get("incomplete_detail")
    supported = {"UNKNOWN": (LEGACY_BINARY_SHA256, LEGACY_FILES_SHA256),
                 "INVENTORY_MISMATCH": (SETTLED_BINARY_SHA256, SETTLED_FILES_SHA256)}
    live.require(known_ended(ended) and supported.get(stop_detail) == (
                     candidate["binary_sha256"], hashlib.sha256(live.canonical(candidate["files"])).hexdigest())
                 and checkpoint is not None and initial is not None and relations.final is not None)
    final = relations.final["payload"]
    live.require(final == {"audit_complete": True, "cash_complete": False, "execution_complete": False,
                 "positions": [], "run_status": "INCOMPLETE", "stop_detail": stop_detail,
                 "stop_reason": "RECONCILIATION_REQUIRED", "takeover_required": True, "unresolved": []}
                 and not relations.latched and not checkpoint.latched and not checkpoint.stopped
                 and not checkpoint.cash_pending and checkpoint.positions == []
                 and checkpoint.reconciled_open_hashes == [])
    snapshot, accounting = checkpoint_event["payload"], checkpoint_event["accounting"]
    live.require(checkpoint_event["event_seq"] == checkpoint.seq
                 and checkpoint_event["event_kind"] == "reconciliation" and snapshot["scope"] == "RUNTIME"
                 and snapshot["complete"] is True and snapshot["foreign_risk"] is False
                 and snapshot["open_hashes"] == [] and snapshot["positions"] == []
                 and accounting["status"] == "COMPLETE" and accounting["reason"] == "NONE"
                 and accounting["cost"] == "0" and accounting["reserved"] == "0"
                 and accounting["pending_claim_value"] is None and not accounting["loss_latched"])
    _check_tail(checkpoint, checkpoint_event, tail, relations, stop_detail)
    account, budget = old_native["account"], old_params["budget"]
    live.require(account.get("positions") == [] and all(type(account.get(k)) is list for k in ("claimed_win", "settled_zero"))
                 and initial["payload"]["scope"] == "STARTUP" and initial["payload"]["complete"] is True
                 and initial["payload"]["foreign_risk"] is False and initial["payload"]["positions"] == []
                 and initial["payload"]["open_hashes"] == []
                 and initial["payload"]["collateral_atoms"] == account["collateral_atoms"]
                 and initial["accounting"]["cash"] == str(budget)
                 and type(seed.get("committed_budget_atoms")) is int and seed["committed_budget_atoms"] == budget)
    owners = {}
    zero_fill_orders = []
    window_order_hashes = []
    for key, attempt in checkpoint.attempts.items():
        if checkpoint._zero_risk_unsent(attempt):
            live.require(attempt["hash"] not in seed["declared_orders"])
            continue
        execution, applied = attempt["execution"], attempt["applied"]
        live.require(attempt["submission"] == "ACCEPTED" and attempt["hash"] is not None
                     and attempt["hash"] not in seed["declared_orders"] and attempt["venue"] is not None
                     and attempt["remaining"] == 0 and execution is not None and checkpoint._execution_closed(execution)
                     and execution["signed_hash"] == attempt["hash"] and execution["venue_order_id"] == attempt["venue"]
                     and checkpoint.reconciled_generations.get(key) == execution["generation"]
                     and all(int(execution["facts"][name]["atoms"]) == applied[name + "_atoms"]
                             for name in ("gross_cash", "collateral_fee", "share_fee", "net_cash", "net_inventory")))
        window_order_hashes.append(attempt["hash"])
        if stop_detail == "INVENTORY_MISMATCH" and execution["quantity_class"] == "ZERO":
            live.require(execution["gross_q_atoms"] == execution["final_gross_q_atoms"] == "0"
                         and all(value == 0 for value in applied.values())
                         and key not in checkpoint.claims)
            zero_fill_orders.append({"signed_hash": attempt["hash"], "venue_order_id": attempt["venue"],
                "market_id": attempt["identity"][0], "token": attempt["identity"][1],
                "checkpoint_generation": execution["generation"],
                "final_generation": relations.attempts[key]["execution"]["generation"]})
            continue
        live.require(applied["net_cash_atoms"] > 0 and applied["net_inventory_atoms"] > 0)
        owners[key] = attempt
    live.require(bool(owners) and set(checkpoint.claims) == set(owners)
                 and len({item["identity"] for item in owners.values()}) == len(owners))
    history = {key: list(account[key]) for key in ("claimed_win", "settled_zero")}
    orders, received = [], 0
    for key, attempt in owners.items():
        market, token = attempt["identity"]
        shares, claim, _ = checkpoint.claims[key]["value"]
        live.require(shares == attempt["applied"]["net_inventory_atoms"] and claim in (0, shares)
                     and (claim == 0 or checkpoint.claims[key]["confirmed"])
                     and not any(item.get("token") == token for records in history.values() for item in records))
        base = {"market_id": int(market), "token": token, "oracle_status": "RESOLVED", "trading_status": "CLOSED"}
        if claim:
            history["claimed_win"].append({**base, "outcome_status": "WON", "payout_millionths": 1000000,
                "inventory_atoms": str(shares), "claim_atoms": str(claim)})
            received += claim
        else:
            history["settled_zero"].append({**base, "outcome_status": "LOST", "payout_millionths": 0})
        orders.append({"signed_hash": attempt["hash"], "venue_order_id": attempt["venue"],
            "checkpoint_generation": attempt["execution"]["generation"],
            "final_generation": relations.attempts[key]["execution"]["generation"],
            "net_cost_atoms": str(attempt["applied"]["net_cash_atoms"]),
            "net_inventory_atoms": str(shares), "claim_atoms": str(claim)})
    spent = sum(item["applied"]["net_cash_atoms"] for item in owners.values())
    live.require(0 < spent < old_params["loss"] and budget > spent
                 and accounting["cash"] == str(budget - spent + received)
                 and accounting["net_pnl"] == str(received - spent)
                 and accounting["realized"] == accounting["net_pnl"]
                 and accounting["nav"] == accounting["cash"] and accounting["available"] == accounting["cash"]
                 and accounting["unrealized"] == "0")
    result = {"proof_kind": "LEGACY_FINAL_ACCOUNT_READ_PLUS_FRESH_NATIVE_REQUIRED",
            "old_run_id": started["run_id"], "old_stop_detail": stop_detail, "old_final_preserved": True,
            "checkpoint_event_seq": checkpoint_event["event_seq"], "final_event_seq": relations.seq,
            "prior_budget_atoms": str(budget), "next_budget_atoms": str(budget - spent),
            "gross_spend_atoms": str(spent), "net_loss_atoms": str(spent),
            "confirmed_window_claim_atoms": str(received), "economic_orders": orders,
            "expected_history": history}
    if stop_detail == "INVENTORY_MISMATCH":
        result.update(proof_kind="SETTLED_INVENTORY_PLUS_FRESH_NATIVE_REQUIRED",
                      zero_fill_orders=zero_fill_orders, window_order_hashes=window_order_hashes)
    return result
