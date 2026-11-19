"""The consumer of the post-entry observation channel.

The launcher hands this module a dedicated datagram socket. It reads the
frames the run's observer produced, validates each one against a strict
allowlist, relates it to the run's own trading evidence, and only then writes
it to the run's evidence directory.

The rules it follows are narrow on purpose:

* the trading stream comes first. An observation frame is read only after the
  original stream has been drained, never before it, and never in place of it;
* nothing is written before it has been validated. A frame that does not
  validate is dropped and counted, never repaired;
* the observation is related to the trading evidence as of the sequence the
  producer saw, so a frame that arrives before its fill is not written before
  the fill it belongs to;
* a failure here only makes the observation incomplete. It never changes the
  trading facts, the exit code or the run's own verdict.
"""
from __future__ import annotations

import datetime
import json
import re
import socket
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

WIRE_VERSION = "btc5m-post-entry-wire/1"
RECORD_SCHEMA_VERSION = "btc5m-post-entry-observer/1"
COVERAGE_SCHEMA_VERSION = "btc5m-post-entry-coverage/1"
READ_SCHEMA_VERSION = "btc5m-post-entry-read/1"

MAX_FRAME_BYTES = 64 * 1024
PAPER72_END_BYTES = 256 * 1024
PAPER72_ENV = "BTC5M_POST_ENTRY_RUN_SECONDS"
MAX_LEVELS = 1000
MAX_SERIES = 1000
MAX_BUFFERED_FRAMES = 16
MAX_BUFFERED_BYTES = 1024 * 1024

OBSERVATIONS_FILE = "post-entry-observations.jsonl"
COVERAGE_FILE = "post-entry-coverage.json"

RUN_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
DECIMAL_ID_PATTERN = re.compile(r"^[0-9]{1,78}$")
TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,9}(?:Z|[+-]\d{2}:\d{2})$"
)

SAMPLE_STATES = {"ACTIVE", "ROUND_ENDED", "SETTLED", "RUN_STOPPED"}
MODEL_REASONS = {
    "OK", "MISSING_INPUT", "MARKET_INVALID", "INPUT_INVALID", "CLOCK_INVALID",
    "REQUEST_TOO_SLOW", "SPOT_STALE", "INPUT_CHANGED", "WARMUP", "ROUND_ENDED",
    "SETTLED", "RUN_STOPPED",
}
EXIT_REASONS = {
    "OK", "NO_POSITION", "MARKET_INVALID", "MISSING_BOOK", "INPUT_INVALID",
    "CLOCK_INVALID", "REQUEST_TOO_SLOW", "BOOK_STALE", "NO_BID",
    "INSUFFICIENT_DEPTH", "OBSERVATION_LIMIT", "ROUND_ENDED", "SETTLED",
    "RUN_STOPPED",
}
FEE_BASES = {"PAPER_CASH_FEE", "UNKNOWN"}
DOWN_QUOTE_BASES = {"MIRRORED_YES_BOOK"}
SOURCE_STATES = {"PRIMARY", "DEGRADED", "UNAVAILABLE"}
RESET_REASONS = {"NONE", "FIRST_SAMPLE", "SOURCE_SWITCH", "LONG_GAP", "CLOCK_INVALID"}
SETTLEMENT_PROVIDERS = {"CHAINLINK", "UNKNOWN"}
PREDICTION_SOURCES = {"Binance", "Coinbase"}
ACTUAL_PAIRS = {"BTCUSDT", "BTC-USD"}

CLOCK_SCHEMA = {
    "source_at": "time",
    "source_time_status": "source_time_status",
    "freshness_basis": "freshness_basis",
    "request_at": "time",
    "received_at": "time",
    "request_elapsed_ms": "elapsed",
    "received_elapsed_ms": "elapsed",
}
LEVEL_SCHEMA = {"price": "number", "size": "number"}

RECORD_SCHEMA = {
    "schema_version": "record_version",
    "run_id": "run_id",
    "observation_seq": "positive",
    "market_id": "decimal_id",
    "token": "decimal_id",
    "sample_state": "sample_state",
    "observed_at": "time",
    "observed_elapsed_ms": "elapsed",
    "starts_at": "time",
    "ends_at": "time",
    "secs_left": "number",
    "strike": "number",
    "position_net_shares": "number",
    "position_cost": "number",
    "prediction_source": "prediction_source",
    "actual_pair": "actual_pair",
    "source_state": "source_state",
    "source_epoch": "epoch",
    "reset_reason": "reset_reason",
    "settlement_provider": "settlement_provider",
    "spot": "number",
    "spot_clock": CLOCK_SCHEMA,
    "book_clock": CLOCK_SCHEMA,
    "samples": "samples",
    "sigma_raw": "number",
    "sigma": "number",
    "p_up": "number",
    "p_down": "number",
    "model_reason": "model_reason",
    "up_bid": "number",
    "up_ask": "number",
    "down_bid": "number",
    "down_ask": "number",
    "down_quote_basis": "down_quote_basis",
    "exit_bid_levels": [LEVEL_SCHEMA],
    "exit_covered_shares": "number",
    "exit_depth_complete": "bool",
    "exit_vwap": "number",
    "exit_gross_income": "number",
    "exit_reason": "exit_reason",
    "fee_basis": "fee_basis",
    "fee_rate_bps": "bps",
    "exit_fee": "number",
    "exit_net_income": "number",
    "exit_pnl_vs_cost": "number",
    "model_hold_value": "number",
    "model_hold_surplus": "number",
}

RECORD_ENUMS = {
    "record_version": {RECORD_SCHEMA_VERSION},
    "sample_state": SAMPLE_STATES,
    "model_reason": MODEL_REASONS,
    "exit_reason": EXIT_REASONS,
    "fee_basis": FEE_BASES,
    "down_quote_basis": DOWN_QUOTE_BASES,
    "source_state": SOURCE_STATES,
    "reset_reason": RESET_REASONS,
    "settlement_provider": SETTLEMENT_PROVIDERS,
    "prediction_source": PREDICTION_SOURCES,
    "actual_pair": ACTUAL_PAIRS,
    "source_time_status": {"MISSING", "PRESENT"},
    "freshness_basis": {"RECEIPT_BOUND", "SOURCE_AND_RECEIPT"},
}

SERIES_SCHEMA = {
    "market_id": "decimal_id",
    "token": "decimal_id",
    "terminal_state": "sample_state",
    "terminal_observation_seq": "positive",
}

PRODUCER_SCHEMA = {
    "attempted_samples": "elapsed",
    "encoded_samples": "elapsed",
    "sent_samples": "elapsed",
    "dropped_samples": "elapsed",
    "clock_unavailable_samples": "elapsed",
    "encode_failed_samples": "elapsed",
    "send_failed_samples": "elapsed",
    "last_observation_seq": "elapsed",
    "disabled_reason": "disabled_reason",
    "series": [SERIES_SCHEMA],
}
DISABLED_REASONS = {
    "TRANSPORT_INVALID", "TRANSPORT_FAILED", "OBSERVATION_LIMIT", "ENCODING_FAILED",
}

COVERAGE_SCHEMA = {
    "schema_version": "coverage_version",
    "run_id": "run_id",
    "enabled": "bool",
    "transport": "transport",
    "status": "coverage_status",
    "ready_received": "bool",
    "end_received": "bool",
    "received_samples": "elapsed",
    "written_samples": "elapsed",
    "rejected_samples": "elapsed",
    "unwritten_samples": "elapsed",
    "sequence_gaps": "elapsed",
    "producer": "producer",
    "reasons": "reasons",
}
COVERAGE_ENUMS = {
    "coverage_version": {COVERAGE_SCHEMA_VERSION},
    "transport": {"UNIX_DGRAM"},
    "coverage_status": {"COMPLETE", "INCOMPLETE"},
    "disabled_reason": DISABLED_REASONS,
}

COVERAGE_REASONS = {
    "TRANSPORT_SETUP_FAILED", "READY_MISSING", "END_MISSING", "INVALID_FRAME",
    "SEQUENCE_INVALID", "SEQUENCE_GAP", "RELATION_INVALID", "RELATION_UNRESOLVED",
    "RELATION_BUFFER_LIMIT", "UNSUPPORTED_POSITION_TRANSITION", "WRITE_FAILED",
    "COVERAGE_WRITE_FAILED", "PRODUCER_DROPPED", "CLOCK_UNAVAILABLE",
    "FIRST_SAMPLE_MISSING", "TERMINAL_MISSING", "OBSERVATION_LIMIT",
    "TRADING_EVIDENCE_INVALID", "TRUNCATED_OUTPUT",
}


def sample_envelope(record: dict[str, Any], watermark: int, run_id: str) -> dict[str, Any]:
    """The strict rebuild of the SAMPLE envelope one record was written with.

    The envelope carries the watermark the record was related to. It is the
    producer's own after_event_seq, carried through unchanged: nothing here
    invents, rounds or reconstructs it.
    """
    return {
        "wire_version": WIRE_VERSION,
        "kind": "SAMPLE",
        "run_id": run_id,
        "after_event_seq": watermark,
        "record": record,
    }


def _is_flat_record(data: Any) -> bool:
    """Whether a line is a legacy flat record rather than a SAMPLE envelope.

    A legacy line carries the record's own keys and nothing else. It has no
    watermark, so it cannot prove its own as-of relation: the reader refuses
    it as unresolved evidence instead of reconstructing a watermark for it.
    """
    return isinstance(data, dict) and "schema_version" in data and "record" not in data


class ObserverRefused(Exception):
    """A fixed refusal of one observation frame: a code, never a value."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _unique_object(pairs):
    """Refuse a duplicate key rather than letting the last one win."""
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate key")
        result[name] = value
    return result


def _reject_constant(_value):
    raise ValueError("non-finite number")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _finite(value: Any) -> bool:
    if not _is_number(value):
        return False
    try:
        return float(value) == float(value) and abs(float(value)) != float("inf")
    except OverflowError:
        return False


def _timestamp(value: Any) -> bool:
    if not isinstance(value, str) or len(value) > 40:
        return False
    if not TIMESTAMP_PATTERN.fullmatch(value):
        return False
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.utcoffset() == datetime.timedelta(0)


def _rebuild(value: Any, schema: Any, table: dict[str, Any]) -> Any:
    """Rebuild one value through a strict, type-specific allowlist."""
    if value is None:
        return None
    if isinstance(schema, dict):
        if not isinstance(value, dict) or set(value) != set(schema):
            raise ObserverRefused("FIELDS")
        return {name: _rebuild(item, schema[name], table) for name, item in value.items()}
    if isinstance(schema, list):
        if not isinstance(value, list) or len(value) > 1000:
            raise ObserverRefused("LIST")
        return [_rebuild(item, schema[0], table) for item in value]
    if schema == "producer":
        if not isinstance(value, dict):
            raise ObserverRefused("FIELDS")
        return _rebuild(value, PRODUCER_SCHEMA, table)
    if schema == "reasons":
        if not isinstance(value, list):
            raise ObserverRefused("REASONS")
        for item in value:
            if item not in COVERAGE_REASONS:
                raise ObserverRefused("REASONS")
        return list(value)
    if schema == "number" and _finite(value):
        return value
    if schema == "bool" and isinstance(value, bool):
        return value
    if schema == "elapsed" and type(value) is int and 0 <= value <= 2**63 - 1:
        return value
    if schema == "positive" and type(value) is int and value > 0:
        return value
    if schema == "samples" and type(value) is int and value >= 0:
        return value
    if schema == "epoch" and type(value) is int and value > 0:
        return value
    if schema == "bps" and type(value) is int and 0 <= value <= 10000:
        return value
    if schema == "decimal_id" and isinstance(value, str) and DECIMAL_ID_PATTERN.fullmatch(value):
        return value
    if schema == "run_id" and isinstance(value, str) and RUN_ID_PATTERN.fullmatch(value):
        return value
    if schema == "time" and _timestamp(value):
        return value
    if schema in table and isinstance(value, str) and value in table[schema]:
        return value
    raise ObserverRefused("TYPE")


def validate_record(record: Any, run_id: str) -> dict[str, Any]:
    """Rebuild one observation record through the strict allowlist."""
    if not isinstance(record, dict):
        raise ObserverRefused("FIELDS")
    rebuilt = _rebuild(record, RECORD_SCHEMA, RECORD_ENUMS)
    if rebuilt["run_id"] != run_id:
        raise ObserverRefused("RUN_ID")
    required = ("schema_version", "run_id", "observation_seq", "market_id", "token",
                "sample_state", "observed_at", "observed_elapsed_ms", "source_state",
                "reset_reason", "settlement_provider", "samples", "model_reason",
                "down_quote_basis", "exit_reason", "fee_basis")
    if any(rebuilt[name] is None for name in required):
        raise ObserverRefused("NULL")
    for name in ("spot_clock", "book_clock"):
        if rebuilt[name] is not None and any(rebuilt[name][key] is None for key in CLOCK_SCHEMA if key != "source_at"):
            raise ObserverRefused("CLOCK_NULL")
    _check_record_formulas(rebuilt)
    return rebuilt


def _time(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _wire_millis(value: str) -> datetime.datetime:
    """One instant on the wire's own millisecond basis.

    A fill carries nanoseconds; an observation carries milliseconds. The two
    cannot be compared on their raw instants: the observation's own value is
    the millisecond its instant falls in, so comparing it with a nanosecond
    fill compares a truncated value with a precise one and makes a fill that
    happened in the same millisecond look later than it was.

    Truncating the precise side to the millisecond it falls in is not a
    tolerance. It is the same instant, expressed on the basis the wire
    itself states. Two instants in different milliseconds still order
    exactly as before.
    """
    moment = _time(value)
    return moment.replace(microsecond=(moment.microsecond // 1000) * 1000)


def _equal(actual: Any, expected: Any) -> None:
    if actual is None or expected is None:
        if actual is not expected:
            raise ObserverRefused("FORMULA")
    elif abs(Decimal(str(actual)) - Decimal(str(expected))) > Decimal("0.0000001"):
        raise ObserverRefused("FORMULA")


def _clock_reason(clock: Any, record: dict[str, Any], stale: str) -> str:
    if clock is None:
        return "MISSING_INPUT"
    if any(clock[name] is None for name in CLOCK_SCHEMA if name != "source_at"):
        raise ObserverRefused("CLOCK_NULL")
    present = clock["source_at"] is not None
    if (clock["source_time_status"] != ("PRESENT" if present else "MISSING")
            or clock["freshness_basis"] != ("SOURCE_AND_RECEIPT" if present else "RECEIPT_BOUND")):
        return "CLOCK_INVALID"
    request, receive, now = map(_time, (clock["request_at"], clock["received_at"], record["observed_at"]))
    span = clock["received_elapsed_ms"] - clock["request_elapsed_ms"]
    age = record["observed_elapsed_ms"] - clock["request_elapsed_ms"]
    if (receive < request or receive > now or span < 0 or age < 0
            or (present and _time(clock["source_at"]) > receive)):
        return "CLOCK_INVALID"
    if max(span, (receive - request).total_seconds() * 1000) > 5000:
        return "REQUEST_TOO_SLOW"
    if (max(age, (now - request).total_seconds() * 1000) > 10000
            or (present and (now - _time(clock["source_at"])).total_seconds() * 1000 > 10000)):
        return stale
    return "OK"


def _check_record_formulas(record: dict[str, Any]) -> None:
    """Validate domains independently; Decimal checks original walk rounding."""
    shares, cost = record["position_net_shares"], record["position_cost"]
    if (shares is None) != (cost is None) or (shares is not None and (shares <= 0 or cost < 0)):
        raise ObserverRefused("POSITION")
    for name in ("strike", "spot", "sigma"):
        if record[name] is not None and record[name] <= 0:
            raise ObserverRefused("DOMAIN")
    for name in ("sigma_raw", "exit_covered_shares", "exit_fee", "model_hold_value"):
        if record[name] is not None and record[name] < 0:
            raise ObserverRefused("DOMAIN")
    for name in ("up_bid", "up_ask", "down_bid", "down_ask", "exit_vwap"):
        if record[name] is not None and not 0 < record[name] < 1:
            raise ObserverRefused("PRICE")
    if record["ends_at"] is not None:
        _equal(record["secs_left"], (_time(record["ends_at"]) - _time(record["observed_at"])).total_seconds())
    elif record["secs_left"] is not None:
        raise ObserverRefused("TIME")
    if (record["prediction_source"], record["actual_pair"]) not in {
            (None, None), ("Binance", "BTCUSDT"), ("Coinbase", "BTC-USD")}:
        raise ObserverRefused("SOURCE")
    expected_state = {None: "UNAVAILABLE", "Binance": "PRIMARY", "Coinbase": "DEGRADED"}[record["prediction_source"]]
    if record["source_state"] != expected_state:
        raise ObserverRefused("SOURCE")
    for original, mirrored in (("up_bid", "down_ask"), ("up_ask", "down_bid")):
        if record[original] is not None and record[mirrored] is not None:
            _equal(record[mirrored], round(1 - record[original], 9))
    terminal = record["sample_state"] != "ACTIVE"
    model_fields = ("sigma_raw", "sigma", "p_up", "p_down", "model_hold_value", "model_hold_surplus")
    exit_fields = ("exit_vwap", "exit_gross_income", "exit_fee", "exit_net_income", "exit_pnl_vs_cost")
    if terminal:
        if record["model_reason"] != record["sample_state"] or record["exit_reason"] != record["sample_state"]:
            raise ObserverRefused("TERMINAL")
        if any(record[name] is not None for name in model_fields + exit_fields + ("exit_bid_levels", "exit_covered_shares", "exit_depth_complete")):
            raise ObserverRefused("TERMINAL_VALUE")
    elif shares is None:
        raise ObserverRefused("POSITION")
    if record["model_reason"] == "OK":
        if (any(record[name] is None for name in model_fields) or record["samples"] < 20
                or record["source_epoch"] is None or record["spot"] is None
                or record["strike"] is None or record["secs_left"] is None
                or record["secs_left"] <= 0 or record["prediction_source"] is None):
            raise ObserverRefused("MODEL_INCOMPLETE")
        if not 0 <= record["p_up"] <= 1 or not 0 <= record["p_down"] <= 1:
            raise ObserverRefused("PROBABILITY_RANGE")
        _equal(record["p_down"], 1 - record["p_up"])
        _equal(record["model_hold_surplus"], record["model_hold_value"] - cost)
        if _clock_reason(record["spot_clock"], record, "SPOT_STALE") != "OK":
            raise ObserverRefused("MODEL_CLOCK")
    elif any(record[name] is not None for name in model_fields):
        raise ObserverRefused("MODEL_REASON")
    levels, covered, complete = (record[name] for name in ("exit_bid_levels", "exit_covered_shares", "exit_depth_complete"))
    if levels is not None:
        if shares is None or complete is None or covered is None:
            raise ObserverRefused("DEPTH")
        remaining, value = Decimal(str(shares)), Decimal(0)
        binary_remaining, binary_value = float(shares), 0.0
        previous = Decimal(1)
        for level in levels:
            if not isinstance(level, dict) or any(level[name] is None for name in LEVEL_SCHEMA):
                raise ObserverRefused("LEVEL")
            price, size = Decimal(str(level["price"])), Decimal(str(level["size"]))
            if not 0 < price < 1 or size <= 0 or price > previous or binary_remaining <= 1e-9:
                raise ObserverRefused("LEVEL")
            take = min(remaining, size)
            remaining -= take
            value += take * price
            binary_take = min(binary_remaining, float(size))
            binary_remaining -= binary_take
            binary_value += binary_take * float(price)
            previous = price
        _equal(covered, min(Decimal(str(shares)), Decimal(str(shares)) - remaining))
        if complete != (binary_remaining <= 1e-9):
            raise ObserverRefused("DEPTH")
        if record["exit_reason"] != ("OK" if complete else "INSUFFICIENT_DEPTH" if levels else "NO_BID"):
            raise ObserverRefused("DEPTH_REASON")
        if complete:
            # runtime::walk divides by original Q then clean()s to 1e-6.
            # Preserve IEEE-754 EPS/half-way branch decisions of runtime::walk;
            # independently bound the unrounded cash calculation with Decimal.
            expected = Decimal(str(binary_value / shares * 1e6)).quantize(Decimal(1), rounding=ROUND_HALF_UP) / 1000000
            if abs(expected - value / Decimal(str(shares))) > Decimal("0.000000500001"):
                raise ObserverRefused("VWAP_ROUNDING")
            _equal(record["exit_vwap"], expected)
    elif covered is not None or complete is not None:
        raise ObserverRefused("DEPTH")
    if record["exit_reason"] == "OK":
        if complete is not True or record["exit_vwap"] is None:
            raise ObserverRefused("EXIT_INCOMPLETE")
        if _clock_reason(record["book_clock"], record, "BOOK_STALE") != "OK":
            raise ObserverRefused("BOOK_CLOCK")
        _equal(record["exit_gross_income"], Decimal(str(shares)) * Decimal(str(record["exit_vwap"])))
    elif any(record[name] is not None for name in exit_fields):
        raise ObserverRefused("EXIT_REASON")
    if record["fee_basis"] == "UNKNOWN":
        if any(record[name] is not None for name in ("fee_rate_bps", "exit_fee", "exit_net_income", "exit_pnl_vs_cost")):
            raise ObserverRefused("FEE_UNKNOWN")
    else:
        bps, vwap = record["fee_rate_bps"], record["exit_vwap"]
        if bps is None or vwap is None:
            raise ObserverRefused("FEE_INPUT")
        expected = Decimal(str(shares)) * Decimal(bps) / 10000 * min(Decimal(str(vwap)), 1 - Decimal(str(vwap)))
        _equal(record["exit_fee"], expected)
        _equal(record["exit_net_income"], Decimal(str(record["exit_gross_income"])) - expected)
        _equal(record["exit_pnl_vs_cost"], Decimal(str(record["exit_net_income"])) - Decimal(str(cost)))


@dataclass
class TradeIndex:
    """What the trading evidence says, as of a sequence number.

    It is a read-only index of the run's own events. It is not a second cash
    ledger: it holds the identity of each fill and each settlement, nothing
    else.
    """

    fills: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    settlements: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    markets: dict[str, dict[str, Any]] = field(default_factory=dict)
    transitions: dict[tuple[str, str], int] = field(default_factory=dict)
    last_seq: int = 0

    def accept(self, event: dict[str, Any]) -> None:
        """Record one trading event."""
        kind = event.get("event_kind")
        seq = event.get("event_seq")
        if type(seq) is not int:
            return
        source = event.get("source") or {}
        market_id = event.get("market_id")
        token = event.get("token")
        if kind == "fill" and source.get("side") == "buy" and source.get("net_shares", 0) > 0 and market_id and token:
            if (market_id, token) in self.fills:
                self.transitions[(market_id, token)] = seq
                self.last_seq = seq
                return
            self.fills[(market_id, token)] = {
                "seq": seq,
                "net_shares": source.get("net_shares"),
                "cost": source.get("cost"),
                "fee": source.get("fee"),
                "candidate_id": source.get("candidate_id"),
                "time": event.get("event_time"),
            }
        elif kind == "fill" and market_id and token:
            self.transitions[(market_id, token)] = seq
        elif kind == "settlement" and market_id and token:
            self.settlements[(market_id, token)] = {"seq": seq, "time": event.get("event_time")}
        elif kind == "summary":
            decision = source.get("decision") if isinstance(source, dict) else None
            if (isinstance(decision, dict) and decision.get("strike") is not None
                    and not any(pair[0] == market_id for pair in self.fills)):
                self.markets[market_id] = {
                    "strike": decision.get("strike"),
                    "starts_at": decision.get("starts_at"),
                    "ends_at": decision.get("ends_at"),
                    "seq": seq,
                    "yes_token": (decision.get("up") or {}).get("token"),
                    "no_token": (decision.get("down") or {}).get("token"),
                    "fee_rate_bps": decision.get("fee_rate_bps"),
                }
        self.last_seq = max(self.last_seq, seq)

    def fill_for(self, market_id: str, token: str, watermark: int) -> dict[str, Any] | None:
        """The fill of this position, as of the given sequence."""
        fill = self.fills.get((market_id, token))
        if fill is None or fill["seq"] > watermark:
            return None
        return fill

    def settled_before(self, market_id: str, token: str, watermark: int) -> bool:
        """Whether this position was officially settled as of the sequence."""
        settlement = self.settlements.get((market_id, token))
        return settlement is not None and settlement["seq"] <= watermark


@dataclass
class ObservationStore:
    """What the receiver saw, and what it wrote."""

    run_id: str
    directory: Path
    ready_received: bool = False
    end_received: bool = False
    received_samples: int = 0
    written_samples: int = 0
    rejected_samples: int = 0
    unwritten_samples: int = 0
    sequence_gaps: int | None = None
    producer: dict[str, Any] | None = None
    reasons: list[str] = field(default_factory=list)
    records: list[dict[str, Any]] = field(default_factory=list)
    sequences: set[int] = field(default_factory=set)
    last_sequence: int = 0
    fill_series: set[tuple[str, str]] = field(default_factory=set)
    handle: Any = None

    def reason(self, name: str) -> None:
        """Record a fixed reason, once."""
        if name not in self.reasons:
            self.reasons.append(name)

    def open(self) -> None:
        """Create the observations file, exclusively."""
        path = self.directory / OBSERVATIONS_FILE
        self.handle = open(path, "x", encoding="utf-8")

    def write(self, record: dict[str, Any], watermark: int) -> bool:
        """Append one validated sample envelope and flush it.

        The envelope is the strict rebuild of the SAMPLE the producer sent:
        the same five keys, the same record and the same watermark the
        relation was checked against. The watermark is carried, never
        invented: it is the one the receiver already related the record to.
        """
        if self.handle is None:
            self.reason("WRITE_FAILED")
            self.unwritten_samples += 1
            return False
        envelope = sample_envelope(record, watermark, self.run_id)
        try:
            text = json.dumps(envelope, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n"
            if len(text.encode()) > MAX_FRAME_BYTES or self.handle.write(text) != len(text):
                raise OSError("PARTIAL_OBSERVATION_WRITE")
            self.handle.flush()
        except (OSError, ValueError, TypeError):
            self.reason("WRITE_FAILED")
            self.unwritten_samples += 1
            return False
        self.written_samples += 1
        return True

    def close(self) -> None:
        """Close the observations file."""
        if self.handle is not None:
            try:
                self.handle.close()
            except (OSError, ValueError):
                self.reason("WRITE_FAILED")
            self.handle = None


class ObserverReceiver:
    """The read-only consumer of the observation channel.

    It never reads the trading stream: the launcher hands it the frames it
    has already validated, one at a time.
    """

    def __init__(self, store: ObservationStore, index: TradeIndex,
                 terminal_bytes: int = MAX_FRAME_BYTES) -> None:
        self.store = store
        self.index = index
        if terminal_bytes not in (MAX_FRAME_BYTES, PAPER72_END_BYTES):
            raise ValueError("TERMINAL_LIMIT")
        self.terminal_bytes = terminal_bytes
        self.buffer: list[tuple[int, dict[str, Any], int]] = []
        self.buffer_bytes = 0

    def note_trade_event(self, event: dict[str, Any]) -> None:
        """Record one trading event and advance the waiting frames."""
        self.index.accept(event)
        self.store.fill_series = set(self.index.fills)
        self.drain()

    def receive(self, frame: dict[str, Any], frame_bytes: int | None = None) -> None:
        """Take one frame from the channel."""
        kind = frame.get("kind") if isinstance(frame, dict) else None
        if kind == "SAMPLE":
            self.store.received_samples += 1
        size = frame_bytes if frame_bytes is not None else len(json.dumps(frame, ensure_ascii=True, allow_nan=False).encode()) + 1
        if size > (self.terminal_bytes if kind == "END" else MAX_FRAME_BYTES):
            self.store.reason("TRUNCATED_OUTPUT")
            if kind == "SAMPLE":
                self.store.rejected_samples += 1
            return
        keys = {"wire_version", "kind", "run_id"}
        expected = keys | ({"record", "after_event_seq"} if kind == "SAMPLE"
                           else {"producer"} if kind == "END" else set())
        if (kind not in {"READY", "SAMPLE", "END"} or set(frame) != expected
                or frame["wire_version"] != WIRE_VERSION or frame["run_id"] != self.store.run_id
                or self.store.end_received):
            self.store.reason("INVALID_FRAME")
            if kind == "SAMPLE":
                self.store.rejected_samples += 1
            return
        if kind == "READY":
            if self.store.ready_received:
                self.store.reason("INVALID_FRAME")
                return
            self.store.ready_received = True
            self.drain()
            return
        if kind == "END":
            self.receive_end(frame)
            return
        if kind != "SAMPLE":
            self.store.reason("INVALID_FRAME")
            return
        record = frame.get("record")
        try:
            validated = validate_record(record, self.store.run_id)
        except ObserverRefused:
            self.store.rejected_samples += 1
            self.store.reason("RELATION_INVALID")
            return
        watermark = frame.get("after_event_seq")
        if type(watermark) is not int or watermark < 0:
            self.store.rejected_samples += 1
            self.store.reason("RELATION_INVALID")
            return
        seq = validated["observation_seq"]
        self.store.sequences.add(seq)
        if seq <= self.store.last_sequence:
            self.store.rejected_samples += 1
            self.store.reason("SEQUENCE_INVALID")
            return
        self.store.last_sequence = seq
        if len(self.buffer) >= MAX_BUFFERED_FRAMES or self.buffer_bytes + size > MAX_BUFFERED_BYTES:
            self.store.unwritten_samples += 1
            self.store.reason("RELATION_BUFFER_LIMIT")
            return
        self.buffer.append((watermark, validated, size))
        self.buffer_bytes += size
        self.drain()

    def receive_end(self, frame: dict[str, Any]) -> None:
        """Take the producer's closing report."""
        if self.store.end_received:
            self.store.reason("INVALID_FRAME")
            return
        producer = frame.get("producer")
        try:
            rebuilt = validate_producer(producer)
        except ObserverRefused:
            self.store.reason("INVALID_FRAME")
            self.store.producer = None
            self.drain()
            return
        self.store.end_received = True
        self.store.producer = rebuilt
        counters = rebuilt
        if (counters["dropped_samples"] > 0
                or counters["disabled_reason"] is not None):
            self.store.reason("PRODUCER_DROPPED")
        self.drain()

    def drain(self) -> None:
        """Write the frames whose watermark the trading stream has reached."""
        while self.buffer:
            watermark, record, size = self.buffer[0]
            if watermark > self.index.last_seq:
                break
            self.buffer.pop(0)
            self.buffer_bytes -= size
            if not self.relate(record, watermark):
                self.store.rejected_samples += 1
                self.store.reason("RELATION_UNRESOLVED")
                continue
            if self.store.write(record, watermark):
                self.store.records.append(record)

    def finish(self) -> None:
        """Reject unresolved frames without waiting or fabricating a watermark."""
        self.drain()
        if self.buffer:
            self.store.rejected_samples += len(self.buffer)
            self.store.reason("RELATION_UNRESOLVED")
            self.buffer.clear()
            self.buffer_bytes = 0
        self.store.close()

    def relate(self, record: dict[str, Any], watermark: int) -> bool:
        """Check the observation against the trading evidence of that moment."""
        market_id = record["market_id"]
        token = record["token"]
        fill = self.index.fill_for(market_id, token, watermark)
        if fill is None:
            return False
        transition = self.index.transitions.get((market_id, token))
        if transition is not None and transition <= watermark:
            self.store.reason("UNSUPPORTED_POSITION_TRANSITION")
            return False
        market = self.index.markets.get(market_id)
        if market is None or market["seq"] > watermark or token not in {market["yes_token"], market["no_token"]}:
            return False
        if any(record[name] != market[name] for name in ("strike", "starts_at", "ends_at")):
            return False
        if (market["starts_at"] is None or market["ends_at"] is None
                or (_time(market["ends_at"]) - _time(market["starts_at"])).total_seconds() != 300):
            return False
        if record["fee_basis"] == "PAPER_CASH_FEE" and record["fee_rate_bps"] != market["fee_rate_bps"]:
            return False
        levels = record["exit_bid_levels"]
        if levels:
            touch = record["up_bid"] if token == market["yes_token"] else record["down_bid"]
            if touch is None or abs(levels[0]["price"] - touch) > 1e-7:
                return False
        settled = self.index.settled_before(market_id, token, watermark)
        shares = record["position_net_shares"]
        if shares is None:
            return settled and record["sample_state"] == "SETTLED" and record["position_cost"] is None
        net = fill.get("net_shares")
        if (net is None or abs(shares - net) > 1e-7 or fill["cost"] is None
                or abs(record["position_cost"] - fill["cost"]) > 1e-7 or settled):
            return False
        if record["sample_state"] == "SETTLED":
            return False
        if record["model_reason"] == "OK":
            fair = record["p_up"] if token == market["yes_token"] else record["p_down"]
            try:
                _equal(record["model_hold_value"], shares * fair)
            except ObserverRefused:
                return False
        return True


def read_observations(run_directory: Path) -> dict[str, Any]:
    """Read a run's observations, strictly and without writing anything.

    It never creates a file, never repairs a missing value and never
    interpolates: a record it cannot validate is not a record.
    """
    directory = Path(run_directory)
    observations = directory / OBSERVATIONS_FILE
    coverage_path = directory / COVERAGE_FILE
    if not observations.exists() and not coverage_path.exists():
        return {
            "schema_version": READ_SCHEMA_VERSION,
            "run_id": None,
            "status": "OBSERVATIONS_UNAVAILABLE",
            "records": [],
            "coverage": None,
            "reasons": ["OBSERVATIONS_UNAVAILABLE"],
        }
    reasons: list[str] = []
    coverage = None
    if coverage_path.exists():
        try:
            raw = coverage_path.read_text(encoding="utf-8")
            data = json.loads(raw, object_pairs_hook=_unique_object,
                              parse_constant=_reject_constant)
            coverage = validate_coverage(data)
        except (OSError, ValueError, TypeError, RecursionError, ObserverRefused):
            reasons.append("COVERAGE_WRITE_FAILED")
    else:
        reasons.append("END_MISSING")
    run_id = coverage["run_id"] if coverage else None
    index = TradeIndex()
    trading_valid = False
    try:
        run_id, index = _read_trading(directory, run_id)
        trading_valid = True
    except (OSError, ValueError, TypeError, KeyError, ObserverRefused):
        reasons.append("TRADING_EVIDENCE_INVALID")
    records: list[dict[str, Any]] = []
    if observations.exists():
        try:
            text = observations.read_text(encoding="utf-8")
        except OSError:
            text = ""
            reasons.append("TRUNCATED_OUTPUT")
        if text and not text.endswith("\n"):
            reasons.append("TRUNCATED_OUTPUT")
        for line in text.splitlines():
            if not line.strip():
                reasons.append("INVALID_FRAME")
                continue
            try:
                data = json.loads(line, object_pairs_hook=_unique_object,
                                  parse_constant=_reject_constant)
            except (ValueError, TypeError, RecursionError):
                reasons.append("INVALID_FRAME")
                continue
            if run_id is None and isinstance(data, dict):
                candidate = data.get("run_id")
                if isinstance(candidate, str):
                    run_id = candidate
            if not isinstance(run_id, str):
                reasons.append("INVALID_FRAME")
                continue
            try:
                if len(line.encode()) + 1 > MAX_FRAME_BYTES:
                    raise ObserverRefused("LIMIT")
                # Each line is the strict SAMPLE envelope the producer sent.
                # The watermark it carries is the only causal evidence the
                # reader gets: a legacy flat-only line has none, and cannot
                # prove its own as-of relation.
                if not isinstance(data, dict) or set(data) != {"wire_version", "kind",
                                                               "run_id", "after_event_seq", "record"}:
                    raise ObserverRefused("LEGACY" if _is_flat_record(data) else "ENVELOPE")
                if (data["wire_version"] != WIRE_VERSION or data["kind"] != "SAMPLE"
                        or not isinstance(data["run_id"], str)
                        or data["run_id"] != run_id):
                    raise ObserverRefused("ENVELOPE")
                watermark = data["after_event_seq"]
                if type(watermark) is not int or watermark < 0:
                    raise ObserverRefused("WATERMARK")
                if trading_valid and watermark > index.last_seq:
                    raise ObserverRefused("BEYOND")
                validated = validate_record(data["record"], run_id)
                if records and validated["observation_seq"] <= records[-1]["observation_seq"]:
                    raise ObserverRefused("SEQUENCE")
                if trading_valid:
                    pair = (validated["market_id"], validated["token"])
                    fill = index.fills.get(pair)
                    # The watermark is the causal evidence: the fill must
                    # already exist inside it. Millisecond times only reject
                    # a fill that is provably in a later millisecond; they
                    # never prove the order inside one millisecond.
                    if fill is None or fill["seq"] > watermark:
                        raise ObserverRefused("FILL")
                    settlement = index.settlements.get(pair)
                    if (validated["sample_state"] == "SETTLED" and (settlement is None
                            or settlement["seq"] > watermark)):
                        raise ObserverRefused("SETTLEMENT")
                    # A fill in a later millisecond is a future event. Inside
                    # one millisecond the order is not provable, and the
                    # watermark is what proves it.
                    if (fill["time"] is not None
                            and _wire_millis(fill["time"]) > _wire_millis(validated["observed_at"])):
                        raise ObserverRefused("FILL")
                    temporary = ObservationStore(run_id, directory)
                    if not ObserverReceiver(temporary, index).relate(validated, watermark):
                        raise ObserverRefused("RELATION")
                    records.append(validated)
            except ObserverRefused as refused:
                # A legacy flat-only line has no causal watermark. It stays
                # unresolved evidence: the watermark is never back-filled.
                # A frame that does not parse is invalid; a frame that parses
                # but cannot be related is unresolved. The two are never
                # confused with each other.
                if refused.reason == "LEGACY":
                    reasons.append("RELATION_UNRESOLVED")
                elif refused.reason in ("FILL", "SETTLEMENT", "RELATION"):
                    reasons.append("RELATION_UNRESOLVED")
                else:
                    reasons.append("INVALID_FRAME")
    else:
        reasons.append("TRUNCATED_OUTPUT")
    if coverage:
        reasons.extend(coverage["reasons"])
        check = ObservationStore(run_id, directory, ready_received=coverage["ready_received"],
                                 end_received=coverage["end_received"], producer=coverage["producer"],
                                 received_samples=coverage["received_samples"], written_samples=len(records),
                                 rejected_samples=coverage["rejected_samples"], unwritten_samples=coverage["unwritten_samples"],
                                 records=records, fill_series=set(index.fills),
                                 sequences={r["observation_seq"] for r in records})
        recomputed = coverage_payload(check)
        reasons.extend(recomputed["reasons"])
        if (coverage["written_samples"] != len(records)
                or coverage["sequence_gaps"] != recomputed["sequence_gaps"]
                or (coverage["status"] == "COMPLETE" and recomputed["status"] != "COMPLETE")):
            reasons.append("RELATION_INVALID")
    status = "COMPLETE" if coverage and coverage["status"] == "COMPLETE" and not reasons else "INCOMPLETE"
    return {
        "schema_version": READ_SCHEMA_VERSION,
        "run_id": run_id,
        "status": status,
        "records": records,
        "coverage": coverage,
        "reasons": sorted(set(reasons)),
    }


def _read_trading(directory: Path, run_id: str | None) -> tuple[str, TradeIndex]:
    """Reuse original validation/relations and adjacent accounting, read-only."""
    import hashlib
    import btc5m_control as control

    def load(name):
        return json.loads((directory / name).read_bytes(), object_pairs_hook=_unique_object,
                          parse_constant=_reject_constant)

    started, ended, config = load("started.json"), load("ended.json"), load("public-config.json")
    identifier = started["run_id"]
    if (not RUN_ID_PATTERN.fullmatch(identifier) or run_id not in (None, identifier)
            or started["mode"] != control.MODE_UNSIGNED_PAPER
            or started["schema_version"] != control.SCHEMA_VERSION_V2
            or started["strategy_contract"] != control.STRATEGY_CONTRACT
            or ended["run_id"] != identifier or ended["schema_version"] != control.SCHEMA_VERSION_V2
            or ended["strategy_contract"] != control.STRATEGY_CONTRACT
            or started["public_config_identity"]["sha256"] != hashlib.sha256((directory / "public-config.json").read_bytes()).hexdigest()):
        raise ValueError("IDENTITY")
    try:
        control.check_fixed_values(config, control.MODE_UNSIGNED_PAPER)
    except control.Refused:
        raise ValueError("PUBLIC_CONFIG") from None
    relations, index, previous = control._EventRelations(), TradeIndex(), None
    text = (directory / "events.jsonl").read_bytes()
    if not text.endswith(b"\n"):
        raise ValueError("TRUNCATED")
    for line in text.splitlines(keepends=True):
        if previous is not None and previous["event_kind"] == "final":
            raise ValueError("AFTER_FINAL")
        event = control.validate_event(line, identifier, control.MODE_UNSIGNED_PAPER)
        if previous is not None:
            source = event["source"] or {}
            delta = dict(cash=0, cost=0, realized=0, fees_in_cash=0)
            if event["event_kind"] == "fill":
                if source["side"] == "buy":
                    delta.update(cash=-source["cost"], cost=source["cost"], fees_in_cash=source["fee"])
                else:
                    delta.update(cash=source["net_cash"], cost=-source["basis"], realized=source["pnl"], fees_in_cash=source["fee"])
            elif event["event_kind"] == "settlement":
                delta.update(cash=source["income"], cost=-source["basis"], realized=source["income"] - source["basis"])
            if any(abs(event["accounting"][name] - previous["accounting"][name] - change) > 1e-7 for name, change in delta.items()):
                raise ValueError("EVENT_TRANSITION")
        relations.accept(event, previous)
        index.accept(event)
        previous = event
    if (previous is None or previous["event_kind"] != "final"
            or ended["final_accounting"] != previous["accounting"]
            or ended["settlement_status"] != previous["source"]):
        raise ValueError("FINAL")
    return identifier, index


def create_socketpair() -> tuple[socket.socket, socket.socket]:
    """The pair the launcher and the run share.

    It is a local, anonymous datagram pair: no address, no network, no
    service.
    """
    return socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)


def child_fd_environment(fd: int) -> dict[str, str]:
    """The one variable the child needs to find its end of the channel."""
    return {"BTC5M_POST_ENTRY_FD": str(fd)}


def coverage_payload(store: ObservationStore) -> dict[str, Any]:
    """The coverage object of one run."""
    if not store.ready_received:
        store.reason("READY_MISSING")
    if not store.end_received or store.producer is None:
        store.reason("END_MISSING")
        store.sequence_gaps = None
    if store.received_samples != store.written_samples + store.rejected_samples + store.unwritten_samples:
        store.reason("RELATION_UNRESOLVED")
    if store.producer is not None:
        try:
            producer = validate_producer(store.producer)
        except ObserverRefused:
            store.producer = None
            store.end_received = False
            store.reason("INVALID_FRAME")
            producer = None
        if producer is not None:
            last = producer["last_observation_seq"]
            store.sequence_gaps = last - len({seq for seq in store.sequences if seq <= last})
            if any(seq > last for seq in store.sequences):
                store.reason("SEQUENCE_INVALID")
            if store.sequence_gaps:
                store.reason("SEQUENCE_GAP")
            if producer["dropped_samples"] or producer["disabled_reason"]:
                store.reason("PRODUCER_DROPPED")
            if producer["clock_unavailable_samples"]:
                store.reason("CLOCK_UNAVAILABLE")
            if producer["sent_samples"] != store.received_samples or producer["sent_samples"] != store.written_samples:
                store.reason("SEQUENCE_GAP")
            by_pair = {}
            for record in store.records:
                by_pair.setdefault((record["market_id"], record["token"]), []).append(record)
            reported = {(item["market_id"], item["token"]): item for item in producer["series"]}
            if set(reported) != store.fill_series or set(by_pair) != store.fill_series:
                store.reason("FIRST_SAMPLE_MISSING")
            for pair, item in reported.items():
                samples = by_pair.get(pair, [])
                terminal = [r for r in samples if r["sample_state"] != "ACTIVE"]
                if (not terminal or len(terminal) != 1 or terminal[-1] != samples[-1]
                        or terminal[0]["sample_state"] != item["terminal_state"]
                        or terminal[0]["observation_seq"] != item["terminal_observation_seq"]):
                    store.reason("TERMINAL_MISSING")
    status = "COMPLETE"
    if (not store.ready_received or not store.end_received
            or store.producer is None or store.reasons
            or (store.sequence_gaps or 0) > 0
            or store.rejected_samples or store.unwritten_samples):
        status = "INCOMPLETE"
    return {
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "run_id": store.run_id,
        "enabled": True,
        "transport": "UNIX_DGRAM",
        "status": status,
        "ready_received": store.ready_received,
        "end_received": store.end_received,
        "received_samples": store.received_samples,
        "written_samples": store.written_samples,
        "rejected_samples": store.rejected_samples,
        "unwritten_samples": store.unwritten_samples,
        "sequence_gaps": store.sequence_gaps,
        "producer": store.producer,
        "reasons": sorted(set(store.reasons)),
    }


def write_coverage(directory: Path, store: ObservationStore) -> bool:
    """Write the coverage object, once, exclusively."""
    path = Path(directory) / COVERAGE_FILE
    payload = coverage_payload(store)
    try:
        with open(path, "x", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, ensure_ascii=True, allow_nan=False)
            handle.write("\n")
    except (OSError, ValueError, TypeError):
        store.reason("COVERAGE_WRITE_FAILED")
        return False
    return True


def close_channel(first: Any, second: Any, store: ObservationStore) -> None:
    """Close only observer descriptors; never propagate into trading cleanup."""
    for endpoint in (first, second):
        if endpoint is not None:
            try:
                endpoint.close()
            except (OSError, ValueError):
                store.reason("INVALID_FRAME")


def validate_producer(value: Any) -> dict[str, Any]:
    """Check the complete END arithmetic and unique terminal series."""
    result = _rebuild(value, PRODUCER_SCHEMA, COVERAGE_ENUMS | RECORD_ENUMS)
    if result is None or any(result[name] is None for name in PRODUCER_SCHEMA if name != "disabled_reason"):
        raise ObserverRefused("PRODUCER_NULL")
    if (result["last_observation_seq"] != result["attempted_samples"]
            or result["dropped_samples"] != sum(result[name] for name in ("clock_unavailable_samples", "encode_failed_samples", "send_failed_samples"))
            or result["encoded_samples"] != result["sent_samples"] + result["send_failed_samples"]
            or result["attempted_samples"] != result["sent_samples"] + result["dropped_samples"]):
        raise ObserverRefused("PRODUCER_COUNTS")
    identities = set()
    for item in result["series"]:
        if not isinstance(item, dict) or item["market_id"] is None or item["token"] is None:
            raise ObserverRefused("SERIES")
        pair, state, seq = (item["market_id"], item["token"]), item["terminal_state"], item["terminal_observation_seq"]
        if (pair in identities or state == "ACTIVE" or (state is None and seq is not None)
                or (seq is not None and seq > result["last_observation_seq"])):
            raise ObserverRefused("SERIES")
        identities.add(pair)
    return result


def validate_coverage(value: Any) -> dict[str, Any]:
    """Rebuild coverage without trusting its verdict or nullable counters."""
    result = _rebuild(value, COVERAGE_SCHEMA, COVERAGE_ENUMS | RECORD_ENUMS)
    if result is None or any(result[name] is None for name in COVERAGE_SCHEMA if name not in {"producer", "sequence_gaps"}):
        raise ObserverRefused("COVERAGE_NULL")
    if (result["enabled"] is not True or len(result["reasons"]) != len(set(result["reasons"]))
            or result["received_samples"] != result["written_samples"] + result["rejected_samples"] + result["unwritten_samples"]):
        raise ObserverRefused("COVERAGE_COUNTS")
    if result["producer"] is not None:
        validate_producer(result["producer"])
    if result["end_received"] != (result["producer"] is not None):
        raise ObserverRefused("COVERAGE_END")
    if result["end_received"] != (result["sequence_gaps"] is not None):
        raise ObserverRefused("COVERAGE_SEQUENCE")
    return result
