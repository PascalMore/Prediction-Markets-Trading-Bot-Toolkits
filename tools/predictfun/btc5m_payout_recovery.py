"""Pure qualification of the known legacy payout-owner audit failure.

No account reads, signing, persistence or process control. A derived stop frame
never becomes the old run's missing final; fresh native reads remain mandatory.
"""
from collections import Counter
from typing import Any

import btc5m_live_events as live


LEGACY_CONTROLLER_SHA256 = "0d08e75e383a3b0194d8a65cf6b27d9c0ed239799e63871aac09ad2460a8653d"
LEGACY_READER_SHA256 = "74656123900ee64a93bcdb1b2b5ab6061c7cdfb62dce2ec81e29daea5cb56c45"
LEGACY_BINARY_SHA256 = "1d693ed2d00795f9c958eb22d43410b0c4d1f1ad46fe88ce61efc8d49107b792"


def known_ended(ended: dict[str, Any]) -> bool:
    """Recognize only the bounded normal-child-exit OUTPUT_INVALID shape."""
    expected = {"schema_version": "btc5m-controlled-live.v1", "run_id": ended.get("run_id"),
                "audit_complete": False, "cash_complete": False, "execution_complete": False,
                "takeover_required": True, "final_accounting": None, "final_event_seq": None,
                "incomplete_detail": None, "incomplete_reason": "AUDIT_UNAVAILABLE",
                "exit_facts": {"child_returncode": 0, "child_signal": None, "forced_cleanup": False,
                               "launcher_exit": 1, "stop_reason": "OUTPUT_INVALID"}}
    return live.canonical(ended) == live.canonical(expected)


def _stopping_frame(record: dict, started: dict, relations: Any,
                    owners: dict[str, dict]) -> tuple[Any, bool, str]:
    """Stage a separately labelled derived frame, not a historical wire repair."""
    live.exact(record, "schema_version run_id bytes child_returncode newline_terminated "
               "occurred_at stream text truncated")
    live.require(record["schema_version"] == "btc5m-stop-output/1"
                 and record["run_id"] == started["run_id"] and record["stream"] == "stdout"
                 and record["newline_terminated"] is True and record["truncated"] is False
                 and type(record["child_returncode"]) is int and record["child_returncode"] == 0
                 and type(record["bytes"]) is int and 0 < record["bytes"] <= live.MAX_LINE_BYTES)
    raw = record["text"]
    live.require(type(raw) is str and raw.startswith("BTC5M_EVENT ")
                 and raw.endswith("\n") and raw.count("\n") == 1 and "\r" not in raw)
    derived = '<REDACTED>' in raw
    if derived:
        live.require(raw.count('<REDACTED>') == 1 and raw.count('"token":<REDACTED>') == 1)
        tentative = live.strict_json(raw[len("BTC5M_EVENT "):].replace('"token":<REDACTED>', '"token":null'))
        matches = [item for item in owners.values() if item["identity"][0] == tentative.get("market_id")]
        live.require(len(matches) == 1)
        raw = raw.replace('"token":<REDACTED>', '"token":' + live.canonical(matches[0]["identity"][1]).decode())
    live.require(len(raw.encode("utf-8")) == record["bytes"])
    event = live.validate_live_event(raw.encode(), started["public_config_identity"])
    live.require(event["event_kind"] == "payout" and event["payload"]["status"] == "CASH_PENDING"
                 and event["payload"]["reason"] == "CASH_PENDING"
                 and event["payload"]["cash_received_atoms"] is None
                 and event["payload"]["tx_hash"] is None)
    identity = (event["market_id"], event["token"])
    economic = [item for item in owners.values() if item["identity"] == identity]
    non_economic = [item for item in relations.attempts.values()
                    if item["identity"] == identity and relations._zero_risk_unsent(item)]
    live.require(len(economic) == 1 and bool(non_economic))
    # The recognized legacy _payout failed uniqueness here. All its other
    # validations, including sequence, shares, rate and accounting, must pass.
    return relations.stage(event), derived, event["event_time"]


def qualify(*, started: dict, ended: dict, relations: Any, last: dict,
            record: dict, old_native: dict, old_params: dict, seed: dict,
            initial: dict | None) -> dict[str, Any]:
    """Derive a bounded new-window basis from a fully replayed legacy prefix.

    Caller must first bind metadata/guard, seed/native SHA and the old preflight.
    The returned expected history is not a claim that cash has already arrived.
    """
    candidate = started["candidate_identity"]
    files = candidate["files"]
    live.require(known_ended(ended) and relations.final is None and relations.seq > 0
                 and not relations.latched and relations.stop_reason in (None, "CASH_PENDING")
                 and candidate["binary_sha256"] == LEGACY_BINARY_SHA256
                 and files.get("tools/predictfun/btc5m_control.py") == LEGACY_CONTROLLER_SHA256
                 and files.get("tools/predictfun/btc5m_live_events.py") == LEGACY_READER_SHA256)
    live.require(last["event_kind"] == "reconciliation" and last["payload"]["complete"] is True
                 and last["payload"]["foreign_risk"] is False
                 and last["payload"]["open_hashes"] == [] and last["payload"]["positions"] == []
                 and relations.positions == [] and relations.reconciled_open_hashes == []
                 and last["accounting"]["reserved"] == "0")
    account = old_native["account"]
    live.require(account.get("positions") == [] and initial is not None
                 and initial["payload"]["scope"] == "STARTUP"
                 and initial["payload"]["complete"] is True
                 and initial["payload"]["foreign_risk"] is False
                 and initial["payload"]["collateral_atoms"] == account["collateral_atoms"]
                 and initial["payload"]["open_hashes"] == [] and initial["payload"]["positions"] == []
                 and all(type(account.get(key)) is list for key in ("claimed_win", "settled_zero")))
    owners = {}
    for key, attempt in relations.attempts.items():
        if relations._zero_risk_unsent(attempt):
            live.require(attempt["hash"] not in seed["declared_orders"])
            continue
        execution = attempt["execution"]
        applied = attempt["applied"]
        live.require(attempt["submission"] == "ACCEPTED" and attempt["hash"] is not None
                     and attempt["venue"] is not None and attempt["remaining"] == 0
                     and execution is not None and relations._execution_closed(execution)
                     and execution["signed_hash"] == attempt["hash"]
                     and execution["venue_order_id"] == attempt["venue"]
                     and relations.reconciled_generations.get(key) == execution["generation"]
                     and applied["net_cash_atoms"] > 0 and applied["net_inventory_atoms"] > 0
                     and attempt["hash"] not in seed["declared_orders"])
        live.require(all(int(execution["facts"][name]["atoms"]) == applied[name + "_atoms"]
                         for name in ("gross_cash", "collateral_fee", "share_fee", "net_cash", "net_inventory")))
        owners[key] = attempt
    live.require(bool(owners) and len({item["identity"] for item in owners.values()}) == len(owners))
    spent = sum(item["applied"]["net_cash_atoms"] for item in owners.values())
    budget = old_params["budget"]
    live.require(type(seed.get("committed_budget_atoms")) is int
                 and seed["committed_budget_atoms"] == budget and 0 < spent < old_params["loss"]
                 and budget > spent and last["accounting"]["cash"] == str(budget - spent))
    staged, derived, failure_time = _stopping_frame(record, started, relations, owners)
    # The frame's own time is validated by stage. Capture time must also be a
    # valid timestamp inside the original authorized window, after the prefix.
    capture = live.timestamp(record["occurred_at"])
    live.require(live.timestamp(started["started_at"]) <= live.timestamp(last["event_time"])
                 <= live.timestamp(failure_time) <= capture <= live.timestamp(started["deadline_at"]))
    live.require(set(staged.claims) == set(owners))
    history = {key: list(account[key]) for key in ("claimed_win", "settled_zero")}
    orders = []
    for key, attempt in owners.items():
        market, token = attempt["identity"]
        live.require(not any(item.get("token") == token for records in history.values() for item in records))
        shares, claim, _payout = staged.claims[key]["value"]
        live.require(claim in (0, shares) and shares == attempt["applied"]["net_inventory_atoms"])
        base = {"market_id": int(market), "token": token,
                "oracle_status": "RESOLVED", "trading_status": "CLOSED"}
        if claim:
            history["claimed_win"].append({**base, "outcome_status": "WON", "payout_millionths": 1000000,
                "claim_atoms": str(claim), "inventory_atoms": str(shares)})
        else:
            history["settled_zero"].append({**base, "outcome_status": "LOST", "payout_millionths": 0})
        orders.append({"signed_hash": attempt["hash"], "venue_order_id": attempt["venue"],
                       "generation": attempt["execution"]["generation"],
                       "net_cost_atoms": str(attempt["applied"]["net_cash_atoms"]),
                       "net_inventory_atoms": str(shares), "claim_atoms": str(claim)})
    return {"old_run_id": started["run_id"], "old_audit_complete": False,
            "native_final_missing": True, "derived_token": derived, "prefix_event_seq": relations.seq,
            "prior_budget_atoms": str(budget), "next_budget_atoms": str(budget - spent),
            "gross_spend_atoms": str(spent), "net_loss_atoms": str(spent),
            "economic_orders": orders, "expected_history": history,
            "proof_kind": "LEGACY_PAYOUT_OWNER_PREFIX_PLUS_FRESH_NATIVE_REQUIRED"}


def fresh_history_matches(basis: dict, projection: dict) -> bool:
    """Require explicit empty inventory and exact complete history multisets."""
    account = projection["account"]
    if account.get("positions") != []:
        return False
    for key in ("claimed_win", "settled_zero"):
        records = account.get(key)
        if type(records) is not list or any(type(item) is not dict for item in records):
            return False
        if Counter(live.canonical(item) for item in records) != Counter(
                live.canonical(item) for item in basis["expected_history"][key]):
            return False
    return True
