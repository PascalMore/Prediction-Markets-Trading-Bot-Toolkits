"""Strict 07-007 audit consumer. No credentials, trading or recoverable balance state."""
from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
from typing import Any, BinaryIO, Callable

from btc5m_control import candidate_input_error

SCHEMA_VERSION = "btc5m-controlled-live.v1"
EVENT_PREFIX = b"BTC5M_EVENT "
MAX_LINE_BYTES = 65536
U256_MAX = (1 << 256) - 1
SCALE = 10 ** 18
REASONS = tuple("NONE CONTEXT_MISSING SOURCE_NOT_ALLOWED INPUT_CHANGED SIGNED_BOUND_UNPROVEN "
                "ALREADY_RUNNING RECOVERY_GUARD_UNAVAILABLE RECONCILIATION_REQUIRED "
                "SUBMISSION_UNKNOWN FACT_INCOMPLETE FACT_CONFLICT ACCOUNT_MISMATCH READ_UNAVAILABLE "
                "DATA_UNAVAILABLE OUTPUT_INVALID AUDIT_UNAVAILABLE KEYRING_UNAVAILABLE LOSS_LATCH "
                "DEADLINE PARENT_LOST CONTROL_CHANNEL MANUAL_STOP PROCESS_FAILED CASH_PENDING "
                "CASH_READBACK_UNAVAILABLE GAS_UNAVAILABLE GAS_BUDGET NOT_APPLICABLE".split())
PUBLIC_ID = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z", re.ASCII)
DETAIL = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z", re.ASCII)
ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z", re.ASCII)
DIGEST = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
TRADING_ADDRESS = "0xfc9eb66e6f22f96608058c0a711ed048eed8c2e2"
SIGNER_ADDRESS = "0x5129330159f4e6a99ef873f86901e3cafa7f9a7d"


class LiveInvalid(ValueError):
    """A fixed public error code; input/exception contents never escape."""

    def __init__(self, reason: str = "OUTPUT_INVALID") -> None:
        super().__init__(reason)
        self.reason = reason


def detail_code(value: Any) -> str | None:
    """Keep the producer's own failure code; anything else degrades, never drops.

    An unrecognised token or one that is too long becomes ``UNKNOWN``: the
    frame is still accepted, it just stops claiming to name the cause.
    """
    if value is None:
        return None
    return value if type(value) is str and DETAIL.fullmatch(value) is not None else "UNKNOWN"


def require(condition: bool, reason: str = "OUTPUT_INVALID") -> None:
    """Refuse invalid audit evidence without repairing it."""
    if not condition:
        raise LiveInvalid(reason)


def exact(value: Any, keys: str) -> None:
    """Require all and only the declared keys, including nullable fields."""
    require(type(value) is dict and set(value) == set(keys.split()))


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def _constant(_value):
    raise LiveInvalid()


def strict_json(raw: bytes | str) -> Any:
    """Decode strict UTF-8 JSON before duplicate-key information is lost."""
    try:
        return json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw,
                          object_pairs_hook=_pairs, parse_constant=_constant)
    except (ValueError, UnicodeError, RecursionError, TypeError):
        raise LiveInvalid() from None


def canonical(value: Any) -> bytes:
    """Encode only validated public evidence in the frozen digest representation."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False).encode("utf-8")


def timestamp(value: Any) -> dt.datetime:
    """Validate RFC3339 with explicit offset and at most millisecond precision."""
    require(type(value) is str and re.fullmatch(
        r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,3})?(?:Z|[+-]\d\d:\d\d)", value) is not None)
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise LiveInvalid() from None


def _scalar(value: Any, kind: str) -> None:
    if kind.endswith("?") and value is None:
        return
    kind = kind.removesuffix("?")
    if kind in ("atoms", "signed"):
        require(type(value) is str)
        pattern = r"(?:0|[1-9][0-9]{0,77})" if kind == "atoms" else r"(?:0|-?[1-9][0-9]{0,77})"
        require(re.fullmatch(pattern, value, re.ASCII) is not None)
        require(abs(int(value)) <= U256_MAX)
    elif kind == "id":
        require(type(value) is str and re.fullmatch(r"[0-9]{1,78}", value, re.ASCII) is not None)
    elif kind == "public":
        require(type(value) is str and PUBLIC_ID.fullmatch(value) is not None)
    elif kind == "hash":
        require(type(value) is str and re.fullmatch(r"0x[0-9a-fA-F]{64}", value, re.ASCII) is not None)
    elif kind == "orderref":
        require(type(value) is str and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value, re.ASCII) is not None)
    elif kind == "bool":
        require(type(value) is bool)
    elif kind in ("integer", "positive", "u32"):
        require(type(value) is int and 0 <= value <= (2 ** 32 - 1 if kind == "u32" else 2 ** 64 - 1))
        require(kind != "positive" or value > 0)
    elif kind == "number":
        require(type(value) in (int, float))
        try:
            require(math.isfinite(value))
        except OverflowError:
            raise LiveInvalid() from None
    elif kind == "text":
        require(type(value) is str and 0 < len(value) <= 128)
    elif kind == "detail":
        # The value degrades to UNKNOWN later; here only its type is fixed.
        require(type(value) is str)
    elif kind == "time":
        timestamp(value)
    elif kind == "reason":
        require(type(value) is str and value in REASONS)
    elif kind in OBJECTS:
        fields(value, OBJECTS[kind])
    elif kind.startswith("["):
        require(type(value) is list and len(value) <= 1000)
        for item in value:
            _scalar(item, kind[1:-1])
    else:
        require(type(value) is str and value in kind.split("|"))


def fields(value: Any, schema: str) -> None:
    """Validate exact nested objects with independent Python key/type tables."""
    tokens = [field.split(":", 1) for field in schema.split()]
    exact(value, " ".join(key for key, _ in tokens))
    for key, kind in tokens:
        _scalar(value[key], kind)


SIDE = ("token:id fair:number? limit_price:number? gross_shares:number? "
        "fee_per_gross_share:number? net_shares:number? cost:number? ev:number? edge:number? "
        "roi:number? breakeven_fair:number? qualifies:bool? depth_levels:[level]? "
        "depth_complete:bool? walk_vwap:number? execution_net_shares:number? "
        "execution_cost:number? execution_ev:number? execution_edge:number?")
DECISION = (
    "decision_id:id candidate_id:id? phase:EVALUATE|PRE_SUBMIT|FINAL_SEND "
    "strategy_contract:phase1_net_shares "
    "reason:RUN_STOPPED|ALREADY_ENTERED|MARKET_INVALID|MISSING_INPUT|INPUT_INVALID|CLOCK_INVALID|"
    "REQUEST_TOO_SLOW|SPOT_STALE|BOOK_STALE|ROUND_TOO_LATE|INPUT_CHANGED|WARMUP|NET_SHARES_INVALID|"
    "EDGE_BELOW_MIN|DIAGNOSTIC_LIMIT|BROKER_BLOCKED|CONTEXT_MISSING|ELIGIBLE "
    "symbol:text prediction_source:Binance|Coinbase? actual_pair:text? "
    "source_state:PRIMARY|DEGRADED|UNAVAILABLE source_epoch:integer? "
    "reset_reason:NONE|FIRST_SAMPLE|SOURCE_SWITCH|LONG_GAP|CLOCK_INVALID "
    "settlement_provider:CHAINLINK|UNKNOWN starts_at:time? ends_at:time? "
    "identity_available_at:time? strike_available_at:time? strike:number? spot:number? "
    "spot_clock:clock? book_clock:clock? decision_at:time decision_elapsed_ms:integer "
    "checked_at:time checked_elapsed_ms:integer secs_left:number? samples:u32 sigma:number? "
    "p_up:number? best_yes_bid:number? best_yes_ask:number? fee_rate_bps:u32? "
    "selected_token:id? up:side? down:side?"
)
CLOSURE = "OPEN|REMOVING|UNPROVEN|FINAL"
FACT_NAMES = "gross_cash collateral_fee share_fee net_cash net_inventory vwap".split()
DELTA_NAMES = "gross_q_atoms gross_cash_atoms collateral_fee_atoms share_fee_atoms net_cash_atoms net_inventory_atoms".split()
ACCOUNTING = (
    "cash:atoms? available:atoms? reserved:atoms? cost:atoms? realized:signed? unrealized:signed? "
    "nav:signed? net_pnl:signed? loss_latched:bool status:COMPLETE|INCOMPLETE|CONFLICT "
    "reason:reason mark_time:time? pending_claim_value:atoms? "
    "gas_status:NOT_MEASURED|KNOWN|UNAVAILABLE gas_paid_atoms:atoms?"
)
OBJECTS = {
    "level": "price:number size:number",
    "side": SIDE, "decision": DECISION, "accounting": ACCOUNTING,
    "clock": ("source_at:time? source_time_status:MISSING|PRESENT "
              "freshness_basis:RECEIPT_BOUND|SOURCE_AND_RECEIPT request_at:time received_at:time "
              "request_elapsed_ms:integer received_elapsed_ms:integer"),
    "fact": "status:KNOWN|UNKNOWN|NOT_APPLICABLE atoms:atoms? coverage_gross_atoms:atoms? reason:reason",
    "facts": " ".join(name + ":fact" for name in FACT_NAMES),
    "delta": " ".join(name + ":atoms?" for name in DELTA_NAMES),
    "position": "market_id:id token:id net_shares_atoms:atoms cost_atoms:atoms?",
    "unresolved": "attempt_id:id signed_hash:hash? closure:" + CLOSURE + " reserved_atoms:atoms reason:reason",
}
PAYLOADS = {
    "decision": "decision:decision",
    "reservation": "candidate_id:id attempt_id:id action:RESERVE|RELEASE worst_cash_atoms:atoms remaining_atoms:atoms reason:reason",
    "order": ("candidate_id:id attempt_id:id signed_hash:hash? venue_order_id:orderref? "
              "attempt_phase:PREPARED|MAY_HAVE_SUBMITTED|SUBMITTED|CLOSED "
              "submission:NOT_SUBMITTED|REJECTED_FINAL|ACCEPTED|UNKNOWN request_gross_atoms:atoms "
              "max_cash_atoms:atoms min_net_shares_atoms:atoms? expires_at:time? "
              "removal_locked_until:time? reason:reason"),
    "execution": ("attempt_id:id signed_hash:hash venue_order_id:orderref? generation:positive "
                  "observed_at:time pages_complete:bool quantity_class:UNKNOWN|ZERO|PARTIAL|FULL "
                  "gross_q_atoms:atoms? accounting_gross_q_atoms:atoms? closure:" + CLOSURE +
                  " final_gross_q_atoms:atoms? facts:facts delta:delta tx_hash:hash? "
                  "settlement_status:UNVERIFIED|MATCHED|CHAIN_SETTLED|CONFLICT settlement_checked_at:time? reason:reason"),
    "reconciliation": ("scope:STARTUP|RUNTIME|HANDOFF checked_at:time complete:bool "
                       "collateral_atoms:atoms? gas_balance_atoms:atoms? positions:[position] "
                       "open_hashes:[hash] foreign_risk:bool reason:reason"),
    "loss_latch": "latched:bool trigger_net_pnl_atoms:signed? reason:reason",
    "stop": ("stop_new_risk:bool reason:reason takeover_required:bool "
             "alert_status:LOCAL_RECORDED|DELIVERED|UNAVAILABLE stop_detail:detail?"),
    "recovery": "policy:COLD_START_REFUSE state:ARMED|RECONCILIATION_REQUIRED|HANDOFF reason:reason",
    "payout": ("status:UNRESOLVED|CONFIRMED|CONFLICT|CASH_PENDING|CASH_CONFIRMED payout_yes:number? "
               "position_net_atoms:atoms claim_value_atoms:atoms? cash_received_atoms:atoms? "
               "tx_hash:hash? observed_at:time reason:reason"),
    "final": ("run_status:COMPLETED|STOPPED|INCOMPLETE stop_reason:reason execution_complete:bool "
              "cash_complete:bool audit_complete:bool takeover_required:bool "
              "unresolved:[unresolved] positions:[position] stop_detail:detail?"),
}


def check_accounting(accounting: dict) -> None:
    """Check integer identities without creating a spendable balance."""
    fields(accounting, ACCOUNTING)
    if accounting["status"] == "COMPLETE":
        names = "cash available reserved cost realized unrealized nav net_pnl".split()
        require(all(accounting[name] is not None for name in names))
        amounts = {name: int(accounting[name]) for name in names}
        require(amounts["cash"] == 100 * SCALE + amounts["realized"] - amounts["cost"])
        require(amounts["net_pnl"] == amounts["realized"] + amounts["unrealized"])
        require(amounts["nav"] == 100 * SCALE + amounts["net_pnl"])
        require(amounts["available"] <= max(0, amounts["cash"] - amounts["reserved"]))
        require(amounts["net_pnl"] > -20 * SCALE or accounting["loss_latched"])
    require((accounting["gas_status"] == "KNOWN") == (accounting["gas_paid_atoms"] is not None))


def validate_live_event(raw: bytes | str | dict) -> dict:
    """Validate a complete wire frame (or an already decoded audit object)."""
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    if isinstance(raw, bytes):
        require(len(raw) <= MAX_LINE_BYTES and raw.startswith(EVENT_PREFIX) and raw.endswith(b"\n"))
        require(b"\n" not in raw[:-1] and b"\r" not in raw)
        event = strict_json(raw[len(EVENT_PREFIX):-1])
    else:
        event = copy.deepcopy(raw)
        require(len(canonical(event)) + len(EVENT_PREFIX) + 1 <= MAX_LINE_BYTES)
    exact(event, "schema_version run_id risk_window_id event_seq event_kind event_time market_id token accounting payload")
    for key, kind in (("schema_version", SCHEMA_VERSION), ("run_id", "public"),
                      ("risk_window_id", "public"), ("event_seq", "positive"),
                      ("event_time", "time"), ("market_id", "id?"), ("token", "id?")):
        _scalar(event[key], kind)
    kind = event["event_kind"]
    require(type(kind) is str and kind in PAYLOADS)
    fields(event["payload"], PAYLOADS[kind])
    check_accounting(event["accounting"])
    payload = event["payload"]
    if kind in ("stop", "final"):
        payload["stop_detail"] = detail_code(payload["stop_detail"])
    if kind in ("decision", "reservation", "order", "execution", "payout"):
        require(event["market_id"] is not None)
    if kind in ("reservation", "order", "execution", "payout"):
        require(event["token"] is not None)
    if kind == "loss_latch":
        require(payload["latched"] and event["accounting"]["loss_latched"])
    if kind == "stop":
        require(payload["stop_new_risk"])
    if kind == "order" and payload["attempt_phase"] in ("MAY_HAVE_SUBMITTED", "SUBMITTED"):
        require(payload["signed_hash"] is not None and payload["min_net_shares_atoms"] not in (None, "0"))
    if kind == "execution":
        gross = payload["gross_q_atoms"]
        require((payload["quantity_class"] == "UNKNOWN") == (gross is None))
        if gross is not None:
            require((payload["quantity_class"] == "ZERO") == (gross == "0"))
        require((payload["closure"] == "FINAL") == (payload["final_gross_q_atoms"] is not None))
        if payload["closure"] == "FINAL":
            require(payload["final_gross_q_atoms"] == gross)
        for name, fact in payload["facts"].items():
            if fact["status"] == "KNOWN":
                require(fact["atoms"] is not None and fact["coverage_gross_atoms"] is not None)
            else:
                require(fact["atoms"] is None)
            if fact["status"] == "NOT_APPLICABLE":
                require(name == "vwap" and gross == "0" and fact["coverage_gross_atoms"] == "0")
        if payload["settlement_status"] == "CHAIN_SETTLED":
            require(payload["tx_hash"] is not None and payload["settlement_checked_at"] is not None)
    if kind == "reconciliation" and payload["complete"]:
        require(payload["collateral_atoms"] is not None and payload["gas_balance_atoms"] is not None
                and not payload["foreign_risk"])
    if kind == "payout":
        require(payload["payout_yes"] is None or 0 <= payload["payout_yes"] <= 1)
        if payload["status"] in ("CONFIRMED", "CASH_PENDING", "CASH_CONFIRMED"):
            require(payload["payout_yes"] is not None)
        if payload["status"] == "CASH_CONFIRMED":
            require(payload["cash_received_atoms"] is not None and payload["tx_hash"] is not None)
    return event


class LiveRelations:
    """Staged audit relationships and observed watermarks, never an execution ledger."""

    def __init__(self, run_id: str, risk_window_id: str) -> None:
        _scalar(run_id, "public")
        _scalar(risk_window_id, "public")
        self.run_id, self.risk_window_id = run_id, risk_window_id
        self.seq = 0
        self.final = None
        self.candidates = {}
        self.attempts = {}
        self.hashes = {}
        self.latched = False
        self.stopped = False
        self.cash_pending = False
        self.positions = None
        self.previous_accounting = None
        self.previous_cash = None

    def stage(self, event: dict) -> LiveRelations:
        """Return a validated next index; callers commit only after durable readback."""
        event = validate_live_event(event)
        require(event["run_id"] == self.run_id and event["risk_window_id"] == self.risk_window_id)
        require(self.final is None and event["event_seq"] == self.seq + 1)
        staged = copy.deepcopy(self)
        staged._apply(event)
        staged.seq = event["event_seq"]
        return staged

    def _apply(self, event: dict) -> None:
        kind, payload, accounting = event["event_kind"], event["payload"], event["accounting"]
        require(not self.latched or accounting["loss_latched"])
        self.latched |= accounting["loss_latched"]
        market_token = (event["market_id"], event["token"])
        if kind == "decision":
            decision = payload["decision"]
            candidate = decision["candidate_id"]
            if candidate is not None:
                prior = self.candidates.get(candidate)
                if decision["phase"] == "EVALUATE":
                    require(prior is None)
                    self.candidates[candidate] = {"frozen": decision, "market": event["market_id"],
                                                  "phase": "EVALUATE", "attempt": None}
                else:
                    require(prior is not None)
                    require(candidate_input_error(prior["frozen"], decision, event["market_id"],
                                                  prior["market"]) is None)
                    if decision["phase"] == "PRE_SUBMIT":
                        require(prior["phase"] == "EVALUATE")
                    else:
                        require(prior["phase"] == "PRE_SUBMIT" and prior["attempt"] is not None)
                        attempt = self.attempts[prior["attempt"]]
                        require(attempt.get("hash") is not None and attempt.get("acked"))
                        require(not self.stopped and not self.latched)
                    prior["phase"] = decision["phase"]
                    prior["eligible"] = decision["reason"] == "ELIGIBLE"
        elif kind == "reservation":
            candidate, attempt_id = payload["candidate_id"], payload["attempt_id"]
            if payload["action"] == "RESERVE":
                prior = self.candidates.get(candidate)
                require(prior is not None and prior["phase"] == "PRE_SUBMIT"
                        and prior["attempt"] is None and attempt_id not in self.attempts
                        and not self.stopped and not self.latched)
                require(event["market_id"] == prior["market"]
                        and event["token"] == prior["frozen"]["selected_token"])
                require(payload["remaining_atoms"] == payload["worst_cash_atoms"])
                prior["attempt"] = attempt_id
                self.attempts[attempt_id] = {"candidate": candidate, "identity": market_token,
                    "worst": int(payload["worst_cash_atoms"]), "remaining": int(payload["remaining_atoms"]),
                    "hash": None, "venue": None, "phase": None, "submission": None,
                    "execution": None, "applied": {key: 0 for key in DELTA_NAMES}}
            else:
                attempt = self._attempt(payload, market_token)
                require(candidate == attempt["candidate"]
                        and int(payload["worst_cash_atoms"]) == attempt["worst"])
                require(self._closed(attempt))
                require(int(payload["remaining_atoms"]) == 0)
                attempt["remaining"] = 0
        elif kind == "order":
            attempt = self._attempt(payload, market_token)
            require(payload["candidate_id"] == attempt["candidate"])
            self._bind(attempt, payload)
            bound = tuple(payload[key] for key in ("request_gross_atoms", "max_cash_atoms"))
            require("bound" not in attempt or attempt["bound"] == bound)
            attempt["bound"] = bound
            require(int(payload["max_cash_atoms"]) <= attempt["worst"])
            for key in ("min_net_shares_atoms", "expires_at"):
                if attempt.get(key) is not None:
                    require(payload[key] == attempt[key])
                attempt[key] = payload[key]
            phase, previous = payload["attempt_phase"], attempt["phase"]
            transitions = {None: {"PREPARED", "CLOSED"}, "PREPARED": {"PREPARED", "MAY_HAVE_SUBMITTED", "CLOSED"},
                           "MAY_HAVE_SUBMITTED": {"SUBMITTED", "CLOSED"}, "SUBMITTED": {"SUBMITTED", "CLOSED"}, "CLOSED": {"CLOSED"}}
            require(phase in transitions[previous])
            if phase == "MAY_HAVE_SUBMITTED":
                candidate = self.candidates[attempt["candidate"]]
                require(candidate["phase"] == "FINAL_SEND" and candidate["eligible"]
                        and attempt.get("acked") and not self.stopped and not self.latched)
            if phase == "SUBMITTED":
                require(previous in ("MAY_HAVE_SUBMITTED", "SUBMITTED"))
            if phase == "CLOSED" and previous in ("MAY_HAVE_SUBMITTED", "SUBMITTED"):
                require(attempt["execution"] is not None and self._execution_closed(attempt["execution"]))
            attempt["phase"], attempt["submission"] = phase, payload["submission"]
            if phase == "CLOSED" and payload["submission"] in ("NOT_SUBMITTED", "REJECTED_FINAL"):
                attempt["remaining"] = 0
            if phase == "PREPARED" and payload["signed_hash"] is not None and payload["reason"] == "NONE":
                require(not attempt.get("acked") and payload["min_net_shares_atoms"] not in (None, "0"))
                attempt["acked"] = True
        elif kind == "execution":
            attempt = self._attempt(payload, market_token)
            self._bind(attempt, payload)
            require(attempt["phase"] in ("MAY_HAVE_SUBMITTED", "SUBMITTED", "CLOSED"))
            self._execution(attempt, payload)
        elif kind == "reconciliation":
            if payload["complete"]:
                self.positions = payload["positions"]
        elif kind == "payout":
            require(any(attempt["identity"] == market_token for attempt in self.attempts.values()))
            if payload["status"] == "CASH_PENDING":
                self.cash_pending = True
                self.stopped = True
            if payload["cash_received_atoms"] is None and self.seq and accounting["cash"] is not None:
                # Payout/claim is not a cash receipt. Compare only known audit projections.
                require(self.previous_cash is None or accounting["cash"] == self.previous_cash)
        elif kind == "stop":
            self.stopped = True
        elif kind == "final":
            unresolved = {item["attempt_id"]: item for item in payload["unresolved"]}
            require(len(unresolved) == len(payload["unresolved"]))
            expected = {key for key, attempt in self.attempts.items() if not self._closed(attempt)}
            require(set(unresolved) == expected)
            for key, item in unresolved.items():
                attempt = self.attempts[key]
                require(item["signed_hash"] == attempt["hash"])
                require(int(item["reserved_atoms"]) >= attempt["remaining"])
                require(item["closure"] != "FINAL")
            require(not payload["execution_complete"] or not expected)
            require(not payload["cash_complete"] or (not self.cash_pending and not expected
                    and all(position["cost_atoms"] == "0" for position in payload["positions"])))
            if self.positions is not None:
                require(payload["positions"] == self.positions)
            require(payload["run_status"] != "COMPLETED" or
                    (payload["execution_complete"] and payload["audit_complete"]))
            self.final = event
        if self.attempts:
            require(accounting["reserved"] is not None)
            floor = sum(attempt["remaining"] for attempt in self.attempts.values())
            require(int(accounting["reserved"]) >= floor)
            if accounting["status"] == "COMPLETE":
                require(int(accounting["reserved"]) == floor)
        if kind == "execution" and self.previous_accounting is not None:
            delta = payload["delta"]["net_cash_atoms"]
            if delta is not None:
                before = self.previous_accounting
                if before["cost"] is not None and accounting["cost"] is not None:
                    require(int(accounting["cost"]) == int(before["cost"]) + int(delta))
                if before["cash"] is not None and accounting["cash"] is not None:
                    require(int(accounting["cash"]) == int(before["cash"]) - int(delta))
        self.previous_accounting = copy.deepcopy(accounting)
        self.previous_cash = accounting["cash"]

    def _attempt(self, payload: dict, identity: tuple) -> dict:
        attempt = self.attempts.get(payload["attempt_id"])
        require(attempt is not None and attempt["identity"] == identity)
        return attempt

    def _bind(self, attempt: dict, payload: dict) -> None:
        for source, target in (("signed_hash", "hash"), ("venue_order_id", "venue")):
            value = payload[source]
            require(attempt[target] is None or value == attempt[target])
            if value is not None:
                if target == "hash":
                    owner = self.hashes.setdefault(value, payload["attempt_id"])
                    require(owner == payload["attempt_id"])
                attempt[target] = value

    @staticmethod
    def _execution_closed(payload: dict) -> bool:
        gross = payload["gross_q_atoms"]
        return (payload["closure"] == "FINAL" and payload["pages_complete"]
                and gross is not None and payload["accounting_gross_q_atoms"] == gross
                and all(fact["coverage_gross_atoms"] == gross and fact["status"] != "UNKNOWN"
                        for fact in payload["facts"].values()))

    def _closed(self, attempt: dict) -> bool:
        return ((attempt["phase"] == "CLOSED" and attempt["submission"] in ("NOT_SUBMITTED", "REJECTED_FINAL"))
                or (attempt["execution"] is not None and self._execution_closed(attempt["execution"])))

    def _execution(self, attempt: dict, payload: dict) -> None:
        previous = attempt["execution"]
        if previous:
            require(payload["generation"] >= previous["generation"])
            if payload["generation"] == previous["generation"]:
                require(all(payload[key] == previous[key] for key in payload if key != "delta"))
            for key in ("gross_q_atoms", "accounting_gross_q_atoms"):
                if previous[key] is not None:
                    require(payload[key] is not None and int(payload[key]) >= int(previous[key]))
            for name in FACT_NAMES:
                before, after = previous["facts"][name], payload["facts"][name]
                if before["coverage_gross_atoms"] is not None:
                    require(after["coverage_gross_atoms"] is not None and
                            int(after["coverage_gross_atoms"]) >= int(before["coverage_gross_atoms"]))
                if before["status"] == "KNOWN":
                    require(after["status"] == "KNOWN")
                    if after["coverage_gross_atoms"] == before["coverage_gross_atoms"]:
                        require(after["atoms"] == before["atoms"])
                    elif name != "vwap":
                        require(int(after["atoms"]) >= int(before["atoms"]))
        facts = payload["facts"]
        complete = (payload["gross_q_atoms"] is not None and
                    payload["accounting_gross_q_atoms"] == payload["gross_q_atoms"] and
                    all(fact["coverage_gross_atoms"] == payload["gross_q_atoms"] and
                        fact["status"] != "UNKNOWN" for fact in facts.values()))
        values = {"gross_q_atoms": payload["gross_q_atoms"]}
        values.update({name + "_atoms": facts[name]["atoms"] for name in FACT_NAMES if name != "vwap"})
        if complete:
            require(int(facts["net_cash"]["atoms"]) == int(facts["gross_cash"]["atoms"]) + int(facts["collateral_fee"]["atoms"]))
            require(int(facts["net_inventory"]["atoms"]) + int(facts["share_fee"]["atoms"]) == int(payload["gross_q_atoms"]))
        deltas = payload["delta"]
        if any(value is not None and int(value) > 0 for value in deltas.values()):
            require(complete and all(value is not None for value in deltas.values()))
        for key, value in deltas.items():
            if value is not None:
                require(values[key] is not None and int(values[key]) - attempt["applied"][key] == int(value))
                attempt["applied"][key] = int(values[key])
        if complete:
            cash = attempt["applied"]["net_cash_atoms"]
            attempt["remaining"] = 0 if self._execution_closed(payload) else max(attempt["worst"], cash) - cash
        attempt["execution"] = copy.deepcopy(payload)


ACCOUNT_ID_KEYS = "venue chain_id account_mode trading_address signer_address"
START_KEYS = ("schema_version strategy_contract operations_mode risk_window_id account_identity authorization_ref "
              "recovery_policy cash_recovery_policy asset_units run_id started_at deadline_at candidate_identity public_config_identity")
CONTEXT_KEYS = ("schema_version run_id risk_window_id authorization_ref operations_mode account_identity started_at "
                "deadline_at candidate_digest asset_units gas_policy recovery_policy cash_recovery_policy")
ENDED_KEYS = ("schema_version run_id exit_facts final_event_seq final_accounting incomplete_reason "
              "incomplete_detail execution_complete cash_complete audit_complete takeover_required")


def check_account_identity(identity: dict) -> None:
    """Check syntax before comparing the one approved public account identity."""
    exact(identity, ACCOUNT_ID_KEYS)
    require(identity["venue"] == "PREDICT_FUN" and type(identity["chain_id"]) is int
            and identity["chain_id"] == 56 and identity["account_mode"] == "predict-account")
    for key, expected in (("trading_address", TRADING_ADDRESS), ("signer_address", SIGNER_ADDRESS)):
        value = identity[key]
        require(type(value) is str and ADDRESS.fullmatch(value) is not None)
        require(value.lower() == expected)


def candidate_digest(started: dict) -> str:
    """Digest the two frozen public identity objects, never an environment."""
    return hashlib.sha256(canonical({key: started[key] for key in
                                    ("candidate_identity", "public_config_identity")})).hexdigest()


def check_metadata(started: dict, public_config: dict, context: dict) -> None:
    """Validate the frozen startup files and cross-file public identity bindings."""
    exact(started, START_KEYS)
    exact(context, CONTEXT_KEYS)
    require(started["schema_version"] == SCHEMA_VERSION and context["schema_version"] == "btc5m-live-context/1")
    for key in ("run_id", "risk_window_id", "authorization_ref"):
        _scalar(started[key], "public")
    require(started["strategy_contract"] == "phase1_net_shares")
    require(started["operations_mode"] in ("supervised", "unattended"))
    require(started["recovery_policy"] == "COLD_START_REFUSE" and started["cash_recovery_policy"] == "MANUAL_PAUSE")
    check_account_identity(started["account_identity"])
    units = started["asset_units"]
    fields(units, "collateral_symbol:USDT collateral_decimals:integer share_decimals:integer gas_symbol:BNB gas_decimals:integer")
    require(all(units[key] == 18 for key in ("collateral_decimals", "share_decimals", "gas_decimals")))
    duration = timestamp(started["deadline_at"]) - timestamp(started["started_at"])
    require(dt.timedelta(0) < duration <= dt.timedelta(hours=6))
    identity = started["candidate_identity"]
    exact(identity, "main_head bot_head binary_sha256 files")
    for key in ("main_head", "bot_head"):
        require(type(identity[key]) is str and re.fullmatch(r"[0-9a-f]{40}", identity[key]) is not None)
    require(type(identity["binary_sha256"]) is str and DIGEST.fullmatch(identity["binary_sha256"]) is not None)
    require(type(identity["files"]) is dict and 0 < len(identity["files"]) <= 1000)
    for name, digest in identity["files"].items():
        require(type(name) is str and not Path(name).is_absolute() and ".." not in Path(name).parts
                and re.fullmatch(r"[A-Za-z0-9_./-]+", name) is not None)
        require(type(digest) is str and DIGEST.fullmatch(digest) is not None)
    public = started["public_config_identity"]
    exact(public, "sha256 initial_cash max_order_usd max_market_usd max_total_usd max_loss_usd")
    require(public["sha256"] == hashlib.sha256(canonical(public_config)).hexdigest())
    for key, value in zip("initial_cash max_order_usd max_market_usd max_total_usd max_loss_usd".split(), (100, 2, 2, 10, 20)):
        require(type(public[key]) in (int, float) and public[key] == value)
    require(context["candidate_digest"] == candidate_digest(started))
    for key in set(context) & set(started) - {"schema_version"}:
        require(context[key] == started[key])
    fields(context["gas_policy"], "payer_address:text minimum_balance_atoms:atoms maximum_consumption_atoms:atoms")
    require(context["gas_policy"]["payer_address"] == started["account_identity"]["signer_address"])
    require(all(int(context["gas_policy"][key]) > 0 for key in ("minimum_balance_atoms", "maximum_consumption_atoms")))


def _read_file(path: Path, limit: int) -> bytes:
    for part in (path, *path.parents):
        require(not part.is_symlink(), "AUDIT_UNAVAILABLE")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), "AUDIT_UNAVAILABLE")
        raw = stream.read(limit + 1)
        require(len(raw) <= limit)
        return raw


def _sync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _create_file(path: Path, raw: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        require(stream.write(raw) == len(raw), "AUDIT_UNAVAILABLE")
        stream.flush()
        os.fsync(stream.fileno())
    _sync_directory(path.parent)
    require(_read_file(path, len(raw)) == raw, "AUDIT_UNAVAILABLE")


class LiveEventStore:
    """Exclusive new-run evidence; no append retry after an ambiguous I/O failure."""

    def __init__(self, directory: Path, started: dict, public_config: dict, context: dict) -> None:
        check_metadata(started, public_config, context)
        self.directory = Path(directory)
        self.started = copy.deepcopy(started)
        self.relations = LiveRelations(started["run_id"], started["risk_window_id"])
        self.failed = False
        self.stream: BinaryIO | None = None
        try:
            # The caller owns the existing parent. Never replace an old run directory.
            require(not any(part.is_symlink() for part in (self.directory.parent, *self.directory.parents)))
            self.directory.mkdir(mode=0o700)
            _sync_directory(self.directory.parent)
            for name, value in (("started.json", started), ("public-config.json", public_config), ("live-context.json", context)):
                _create_file(self.directory / name, canonical(value) + b"\n")
            _create_file(self.directory / "events.jsonl", b"")
            self.stream = open(self.directory / "events.jsonl", "r+b")
        except (OSError, LiveInvalid):
            self.failed = True
            raise LiveInvalid("AUDIT_UNAVAILABLE") from None

    def append(self, raw: bytes | str, ack: Callable[[bytes], None] | None = None) -> dict:
        """Validate, write once, flush/fsync/readback, commit, then acknowledge an order."""
        require(not self.failed and self.stream is not None, "AUDIT_UNAVAILABLE")
        try:
            event = validate_live_event(raw)
            staged = self.relations.stage(event)
            line = canonical(event) + b"\n"
        except (LiveInvalid, ValueError, TypeError, RecursionError):
            self.failed = True
            raise LiveInvalid("OUTPUT_INVALID") from None
        try:
            offset = self.stream.tell()
            require(self.stream.write(line) == len(line), "AUDIT_UNAVAILABLE")
            self.stream.flush()
            os.fsync(self.stream.fileno())
            require(os.fstat(self.stream.fileno()).st_ino == (self.directory / "events.jsonl").lstat().st_ino,
                    "AUDIT_UNAVAILABLE")
            with open(self.directory / "events.jsonl", "rb") as readback:
                readback.seek(offset)
                require(readback.read(len(line) + 1) == line, "AUDIT_UNAVAILABLE")
            self.relations = staged
            payload = event["payload"]
            if (event["event_kind"] == "order" and payload["attempt_phase"] == "PREPARED"
                    and payload["signed_hash"] is not None and payload["reason"] == "NONE" and ack is not None):
                ack(f"A {event['event_seq']} {payload['signed_hash']}\n".encode("ascii"))
            return event
        except (OSError, ValueError, LiveInvalid):
            self.failed = True
            raise LiveInvalid("AUDIT_UNAVAILABLE") from None

    def finish(self, ended: dict) -> None:
        """Persist supplied native exit facts; never invent a missing final event."""
        require(not self.failed, "AUDIT_UNAVAILABLE")
        check_ended(ended, self.started, self.relations)
        try:
            _create_file(self.directory / "ended.json", canonical(ended) + b"\n")
        except (OSError, LiveInvalid):
            self.failed = True
            raise LiveInvalid("AUDIT_UNAVAILABLE") from None

    def close(self) -> None:
        """Close local audit handles, not a recovery barrier."""
        if self.stream is not None:
            self.stream.close()
            self.stream = None


def check_ended(ended: dict, started: dict, relations: LiveRelations) -> None:
    """Cross-check native result dimensions against the actual final evidence."""
    exact(ended, ENDED_KEYS)
    require(ended["schema_version"] == SCHEMA_VERSION and ended["run_id"] == started["run_id"])
    exit_facts = ended["exit_facts"]
    exact(exit_facts, "launcher_exit child_returncode child_signal forced_cleanup stop_reason")
    for key in ("launcher_exit", "child_returncode"):
        require(exit_facts[key] is None or (type(exit_facts[key]) is int and -255 <= exit_facts[key] <= 255))
    _scalar(exit_facts["child_signal"], "positive?")
    _scalar(exit_facts["forced_cleanup"], "bool")
    _scalar(exit_facts["stop_reason"], "reason")
    _scalar(ended["incomplete_reason"], "reason?")
    require(ended["incomplete_detail"] is None or
            (type(ended["incomplete_detail"]) is str and DETAIL.fullmatch(ended["incomplete_detail"]) is not None))
    _scalar(ended["final_event_seq"], "positive?")
    for key in ("execution_complete", "cash_complete", "audit_complete", "takeover_required"):
        _scalar(ended[key], "bool")
    final = relations.final
    if final is None:
        require(ended["final_event_seq"] is None and ended["final_accounting"] is None
                and not ended["audit_complete"] and ended["incomplete_reason"] is not None
                and ended["incomplete_detail"] is None)
    else:
        check_accounting(ended["final_accounting"])
        require(ended["final_event_seq"] == final["event_seq"] and ended["final_accounting"] == final["accounting"])
        require(exit_facts["stop_reason"] == final["payload"]["stop_reason"])
        require(ended["incomplete_detail"] == final["payload"]["stop_detail"])
        for key in ("execution_complete", "cash_complete", "audit_complete", "takeover_required"):
            require(ended[key] == final["payload"][key])
    unhealthy = (final is None or exit_facts["forced_cleanup"] or exit_facts["launcher_exit"] != 0
                 or exit_facts["child_returncode"] != 0 or exit_facts["child_signal"] is not None
                 or not all(ended[key] for key in ("execution_complete", "cash_complete", "audit_complete")))
    require(not unhealthy or ended["incomplete_reason"] not in (None, "NONE"))


def read_live_run(directory: Path) -> dict:
    """Read and revalidate disk evidence only; missing/bad tails never become complete."""
    result = {"schema_version": "btc5m-live-read/1", "run_id": None, "status": "UNAVAILABLE",
              "events": [], "ended": None, "reasons": []}
    reasons = set()
    directory = Path(directory)
    try:
        started = strict_json(_read_file(directory / "started.json", MAX_LINE_BYTES))
        if type(started) is not dict or started.get("schema_version") != SCHEMA_VERSION:
            reasons.add("FACT_INCOMPLETE")
            return {**result, "reasons": ["FACT_INCOMPLETE"]}
        result["status"] = "INCOMPLETE"
        public = strict_json(_read_file(directory / "public-config.json", MAX_LINE_BYTES))
        context = strict_json(_read_file(directory / "live-context.json", MAX_LINE_BYTES))
        check_metadata(started, public, context)
        result["run_id"] = started["run_id"]
        relations = LiveRelations(started["run_id"], started["risk_window_id"])
        raw = _read_file(directory / "events.jsonl", 64 * 1024 * 1024)
        for line in raw.splitlines(keepends=True):
            require(line.endswith(b"\n") and len(line) + len(EVENT_PREFIX) <= MAX_LINE_BYTES)
            event = validate_live_event(EVENT_PREFIX + line)
            relations = relations.stage(event)
            result["events"].append(event)
        ended = strict_json(_read_file(directory / "ended.json", MAX_LINE_BYTES))
        check_ended(ended, started, relations)
        result["ended"] = ended
        if relations.final is None:
            reasons.add("AUDIT_UNAVAILABLE")
        if ended["incomplete_reason"] not in (None, "NONE"):
            reasons.add(ended["incomplete_reason"])
        if not all(ended[key] for key in ("execution_complete", "cash_complete", "audit_complete")):
            reasons.add("FACT_INCOMPLETE")
        if relations.final and relations.final["payload"]["run_status"] == "INCOMPLETE":
            reasons.add("FACT_INCOMPLETE")
        native = ended["exit_facts"]
        if native["forced_cleanup"] or native["launcher_exit"] != 0 or native["child_returncode"] != 0 or native["child_signal"] is not None:
            reasons.add("PROCESS_FAILED")
        if not reasons:
            result["status"] = "COMPLETE"
    except OSError:
        reasons.add("AUDIT_UNAVAILABLE")
    except (LiveInvalid, ValueError, TypeError, KeyError, RecursionError):
        reasons.add("OUTPUT_INVALID")
    result["reasons"] = [reason for reason in REASONS if reason in reasons]
    return result
