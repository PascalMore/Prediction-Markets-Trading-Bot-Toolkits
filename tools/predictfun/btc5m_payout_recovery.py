"""Qualification of two specifically recognized legacy output-audit failures.

No account reads, signing, persistence or process control. A derived stop frame
never becomes the old run's missing final; fresh native reads remain mandatory.
"""
from collections import Counter
import hashlib
from pathlib import Path
from typing import Any

import btc5m_live_events as live


LEGACY_CONTROLLER_SHA256 = "0d08e75e383a3b0194d8a65cf6b27d9c0ed239799e63871aac09ad2460a8653d"
LEGACY_READER_SHA256 = "74656123900ee64a93bcdb1b2b5ab6061c7cdfb62dce2ec81e29daea5cb56c45"
LEGACY_BINARY_SHA256 = "1d693ed2d00795f9c958eb22d43410b0c4d1f1ad46fe88ce61efc8d49107b792"

EXECUTION_RUN = "btc5m-live-4d633f8696984a73"
_EXECUTION_SOURCE = {
    "started.json": "4698e858af2a5d35f140d91a7b2cb154190b8e19fd19c890b35d695c5aff5a1a",
    "ended.json": "4c19bdb4d6ba3761059d869e798b7e51b1baa611c330d86a3bcef7d57426f6cf",
    "public-config.json": "d91f12bf1fa02e0d68f917677b6fd3d21e555101153b7b5583f61e2b756a306e",
    "live-context.json": "1a645124cc123cb2cc620f3cb86a129aad6a9caca50cd22739d29f191466bd0e",
    "events.jsonl": "8c845ccc74384aefeb04fb06b9da2281cb1e01f72b8d965b8806ed42bd999dce",
    "stop-output.json": "e241b013d050b154f119bf0e3c01cd043eec0fa7006b3a3438603e7ef7009c26",
    "seed": "8e28b70d458d6f67368dd576f6df402f7b426224502277021fea09fb30e306d1",
    "native": "4503ef48efdb480498047126e5c49d36ae88e3dcc8c4015338d632bf824d1a0b",
}
_EXECUTION_AUTHORITY = {
    "chain-orderfilled.json": "7eea29e8916e5a678ce5f2855a67ba0ae46f3e5633586a8b49fcf68c071841f2",
    "venue-order-matches.json": "7d00a30ba6a01809722b9762786bbf20745fcd6363dadb41ccd81ef61985b731",
    "root-cause-arithmetic.json": "256a871164e88f18300f9c38535079046ec3c9e6b6f916030201de3f284e9049",
}


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


def _execution_stop(record: dict, started: dict, relations: Any, last: dict,
                    owner: dict) -> dict:
    """Replay the one preserved diagnostic, never install it into the old audit."""
    market, token = owner["identity"]
    text = record["text"]
    # Only this pinned capture, with its sole redacted public token, is eligible.
    live.require(record["run_id"] == EXECUTION_RUN and record["stream"] == "stdout"
                 and record["child_returncode"] == 0 and record["newline_terminated"] is True
                 and record["truncated"] is False and text.count("<REDACTED>") == 1
                 and text.count('"token":<REDACTED>') == 1)
    wire = text.replace('"token":<REDACTED>', '"token":' + live.canonical(token).decode()).encode()
    live.require(len(wire) == record["bytes"])
    event = live.validate_live_event(wire, started["public_config_identity"])
    facts = event["payload"]
    before = last["payload"]
    live.require(event["event_kind"] == "execution" and (event["market_id"], event["token"]) == (market, token)
                 and all(facts[key] == before[key] for key in ("attempt_id", "signed_hash", "venue_order_id"))
                 and live.timestamp(started["started_at"]) <= live.timestamp(last["event_time"])
                 <= live.timestamp(event["event_time"]) <= live.timestamp(record["occurred_at"])
                 <= live.timestamp(started["deadline_at"])
                 and facts["generation"] == before["generation"] + 1
                 and facts["quantity_class"] == "PARTIAL" and facts["closure"] == "UNPROVEN"
                 and facts["reason"] == "FACT_INCOMPLETE" and facts["pages_complete"] is False
                 and before["accounting_gross_q_atoms"] == "0" and facts["accounting_gross_q_atoms"] is None
                 and all(fact["status"] == "UNKNOWN" for fact in facts["facts"].values())
                 and all(value is None for value in facts["delta"].values())
                 and event["accounting"]["reserved"] == str(owner["remaining"]))
    staged = relations.stage(event)
    live.require(staged.seq == 52 and staged.final is None and not staged.latched
                 and staged.attempts[facts["attempt_id"]]["applied"] == owner["applied"])
    return facts


def _execution_cost(root: Path, owner: dict, facts: dict) -> tuple[int, int]:
    """Cross-bind the pinned complete venue list to receipt maker debits and share fees."""
    records = []
    directory = root / "data/reports/2026-11-26-btc5m-history-not-closed-root-cause/evidence"
    for name, expected in _EXECUTION_AUTHORITY.items():
        raw = live._read_file(directory / name, live.MAX_LINE_BYTES)
        live.require(hashlib.sha256(raw).hexdigest() == expected)
        records.append(live.strict_json(raw))
    chain, venue, arithmetic = records
    market, token = owner["identity"]
    live.require(chain["our_order_hash"] == venue["order_hash"] == owner["hash"]
                 and str(venue["market"]["id"]) == market
                 and venue["market"]["status"] == "RESOLVED"
                 and venue["market"]["tradingStatus"] == "CLOSED")
    matches = venue["matches"]
    legs = {leg["tx"]: leg for leg in matches["legs"]}
    rows = chain["rows"]
    live.require(matches["cursor_present"] is False and len(rows) == matches["count"] == len(legs) == 2
                 and {row["tx"] for row in rows} == set(legs))
    cash = gross = fees = 0
    for row in rows:
        leg = legs[row["tx"]]
        live.require(row["status"] == "0x1" and row["makerAssetId"] == "0" and row["takerAssetId"] == token
                     and leg["quoteType"] == "Bid" and leg["fee"]["type"] == "SHARES"
                     and leg["amount"] == row["takerAmountFilled"] and leg["fee"]["amount"] == row["fee"])
        cash += int(row["makerAmountFilled"])
        gross += int(row["takerAmountFilled"])
        fees += int(row["fee"])
    net = gross - fees
    requested, maximum_cash = map(int, owner["bound"])
    live.require(0 < gross < requested and gross == int(facts["gross_q_atoms"])
                 and 0 < cash <= maximum_cash <= owner["remaining"] and 0 <= fees < gross
                 and net >= int(owner["min_net_shares_atoms"])
                 and chain["aggregate"] == {"authoritative_cash_atoms": str(cash),
                     "authoritative_shares_atoms": str(gross), "fee_atoms": str(fees),
                     "net_inventory_atoms": str(net)}
                 and arithmetic["chain_authoritative_cash_atoms"] == str(cash)
                 and arithmetic["net_inventory_atoms"] == str(net))
    return cash, net


def qualify_execution(*, started: dict, ended: dict, relations: Any, last: dict,
                      record: dict, old_native: dict, old_params: dict, seed: dict,
                      initial: dict, order_events: list[dict], source_sha256: dict,
                      root: Path) -> dict[str, Any]:
    """Admit only the audited one-order prefix to *fresh* read-only closure checks."""
    # started binds the original binary/controller/reader identities together.
    # Those exact versions require a durable PREPARED before ACK; the atomic
    # handoff consumes that ACK once. The controller stops ACK on first refusal.
    live.require(started["run_id"] == EXECUTION_RUN and source_sha256 == _EXECUTION_SOURCE
                 and known_ended(ended) and relations.seq == 51 and relations.final is None
                 and not relations.latched and not relations.stopped and not relations.claims
                 and relations.positions == [] and relations.reconciled_open_hashes == []
                 and len(relations.attempts) == 1)
    owner, = relations.attempts.values()
    live.require([(item["attempt_phase"], item["submission"]) for item in order_events]
                 == [("PREPARED", "NOT_SUBMITTED"), ("MAY_HAVE_SUBMITTED", "UNKNOWN"),
                     ("SUBMITTED", "ACCEPTED")]
                 and all(item["signed_hash"] == owner["hash"] for item in order_events)
                 and order_events[-1]["venue_order_id"] == owner["venue"] is not None
                 and owner["acked"] and owner["send_observed"] and owner["remaining"] > 0
                 and not any(owner["applied"].values())
                 and last["event_kind"] == "execution" and last["payload"] == owner["execution"]
                 and last["payload"]["closure"] == "OPEN" and last["payload"]["gross_q_atoms"] == "0"
                 and last["payload"]["pages_complete"] is True
                 and last["accounting"]["reserved"] == str(owner["remaining"]))
    account, prior = old_native["account"], old_params["budget"]
    declarations = seed["declared_orders"]
    live.require(len(declarations) == len(set(declarations)) == 16 and owner["hash"] not in declarations
                 and seed["committed_budget_atoms"] == prior
                 and initial is not None and initial["payload"]["scope"] == "STARTUP"
                 and initial["payload"]["complete"] is True and initial["payload"]["foreign_risk"] is False
                 and initial["payload"]["collateral_atoms"] == account["collateral_atoms"]
                 and initial["payload"]["open_hashes"] == [] and initial["payload"]["positions"] == []
                 and account.get("positions") == [])
    history = {key: list(account[key]) for key in ("claimed_win", "settled_zero")}
    market, token = owner["identity"]
    live.require(not any(item["token"] == token for group in history.values() for item in group))
    facts = _execution_stop(record, started, relations, last, owner)
    cost, inventory = _execution_cost(root, owner, facts)
    live.require(0 < cost < min(prior, old_params["loss"]))
    # Expected zero is a condition to prove on BOTH complete, fresh reads,
    # not a payout or Final inferred from the diagnostic or archived balance.
    history["settled_zero"].append({"market_id": int(market), "token": token,
        "oracle_status": "RESOLVED", "trading_status": "CLOSED", "outcome_status": "LOST",
        "payout_millionths": 0})
    return {"old_run_id": EXECUTION_RUN, "old_audit_complete": False, "native_final_missing": True,
            "derived_token": True, "prefix_event_seq": relations.seq, "failure_event_seq": 52,
            "prior_budget_atoms": str(prior), "next_budget_atoms": str(prior - cost),
            "gross_spend_atoms": str(cost), "net_loss_atoms": str(cost), "expected_history": history,
            "economic_orders": [{"signed_hash": owner["hash"], "venue_order_id": owner["venue"],
                                 "net_cost_atoms": str(cost), "net_inventory_atoms": str(inventory)}],
            "proof_kind": "LEGACY_PARTIAL_EXECUTION_PLUS_FRESH_NATIVE_REQUIRED",
            "authority_sha256": dict(_EXECUTION_AUTHORITY)}


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
