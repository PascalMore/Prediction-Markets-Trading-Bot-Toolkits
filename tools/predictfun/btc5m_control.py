"""Fixed entry point for the controlled BTC5m unsigned paper run.

The launcher owns the run: it validates the public inputs, freezes them into
this run's own snapshot, starts exactly one bot child, watches it, and writes
the run's evidence. It is not a command proxy: the mode, the config and the
run id are the only inputs, and there is no way to point it at another
executable, another path or another venue.

The unsigned paper mode reads no keyring, builds no signer and sends no
authenticated request. Only the market data API key is passed on, and only
through the existing environment source. When the parent environment has no
market key at all, exactly one key may be read from the fixed project file;
nothing else from that file is imported and the parent environment is never
changed.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
import selectors
import signal
import subprocess
import sys
import time
from contextlib import nullcontext
from decimal import Decimal
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import local_signer
from local_signer import BOT, Failure, INHERIT_ENV

SCHEMA_VERSION = "btc5m-controlled-paper.v1"
# The wire version of the unsigned phase1 protocol. The signed run keeps v1:
# it has no real accounting to report.
SCHEMA_VERSION_V2 = "btc5m-controlled-paper.v2"
# The economics the unsigned phase1 run decides under.
STRATEGY_CONTRACT = "phase1_net_shares"
MODE_UNSIGNED_PAPER = "unsigned-paper"
MODE_SIGNED_FUTURE_LIVE = "signed-future-live"
RUN_SECONDS = 21_600
# A launcher computes a frozen window's deadline from the moment it read its
# script; the controller's own clock starts a little later. The configuration's
# duration owns that window's start, and the clock may agree with it only this
# closely: an older deadline would start a window that is already partly gone.
EFFECTIVE_START_SKEW_SECONDS = 120
PAPER72_SECONDS = 259_200
PAPER72_FINAL_BYTES = 1024 * 1024
PAPER72_BINARY = BOT.parents[4] / "data/reports/2026-11-19-btc5m-live-parity-and-operations/evidence/candidate-v2/polymarket-toolkits"
LIVE_BINARY = (BOT.parents[4] / "data/reports/2026-11-27-btc5m-fact-incomplete-stop/evidence"
               / "context-root-fixed-candidate/polymarket-toolkits")
LIVE_BINARY_SHA256 = "66f1286dae49bfc9b4ce2d7e66897c42d1034d20cccd341bfeac8f65d3482f21"
LIVE_PATHS = (
    "src/config/controlled_live.rs", "src/bot/runtime/taker.rs",
    "src/bot/runtime/phase1_control.rs", "src/bot/runtime/live.rs",
    "src/bot/runtime/timing.rs",
    "src/bot/runtime/live_control.rs", "src/bot/runtime/live_control/channel.rs",
    "src/bot/runtime/live_control/tests.rs", "src/bot/runtime/live_accounting.rs",
    "src/bot/runtime/live_control/tests/live003_regression.rs",
    "src/bot/runtime/live_control/tests/live004_regression.rs",
    "src/bot/runtime/live_events.rs", "src/bot/runtime/live_events/types.rs",
    "src/bot/runtime/live_events/tests.rs", "src/service/predict_fun/request.rs",
    "src/service/predict_fun/controlled.rs", "src/service/predict_fun/fills.rs",
    "src/service/predict_fun/account.rs", "src/service/predict_fun/account/controlled.rs",
    "tools/predictfun/btc5m_live_events.py", "tools/predictfun/btc5m_live_guard.py",
    "tools/predictfun/tests/test_btc5m_live_events.py", "tools/predictfun/tests/test_btc5m_live_guard.py",
    "tools/predictfun/local_signer.py", "tools/predictfun/README.md", "docs/LIVE-TRADING.md",
    # Run-parameter surface: the parser, the generic launcher and their tests are
    # part of what a live run's frozen identity must bind.
    "src/config/run_parameters.rs", "src/config/run_parameters/tests.rs",
    "tools/predictfun/btc5m_launch.py", "tools/predictfun/tests/test_btc5m_launch.py",
    "tools/predictfun/btc5m_pre_send_exit_recovery.py",
    "tools/predictfun/tests/test_btc5m_pre_send_exit_recovery.py",
    "tools/predictfun/tests/test_btc5m_takeover.py",
    "tools/predictfun/btc5m_payout_recovery.py",
    "tools/predictfun/tests/test_btc5m_payout_recovery.py",
    "tools/predictfun/btc5m_account_recovery.py",
    "tools/predictfun/tests/test_btc5m_account_recovery.py",
    "tools/predictfun/tests/test_btc5m_settled_inventory_recovery.py",
    "tools/predictfun/tests/test_btc5m_read_unavailable_recovery.py",
    "tools/predictfun/tests/test_btc5m_parameterization.py",
    # R15/R16 loaded recovery, claim, diagnostics and regression modules.
    "src/bot/runtime/live/claims.rs", "src/bot/runtime/live/closed_execution.rs",
    "src/bot/runtime/live_control/cash_wait.rs",
    "src/bot/runtime/live/order_wait.rs", "src/bot/runtime/live_control/tests/order_wait.rs",
    "src/service/predict_fun/fills/order_wait_tests.rs",
    "src/bot/runtime/live_control/diagnostics.rs", "src/bot/runtime/live_events/order.rs",
    "src/bot/runtime/live_control/tests/claim_wait.rs",
    "src/bot/runtime/live_control/tests/degrade_recovery.rs",
    "src/bot/runtime/live_control/tests/live012_regression.rs",
    "src/bot/runtime/live_control/tests/send_timing_bench.rs",
    "src/bot/runtime/live_control/tests/r16_cash_drive.rs",
    "src/bot/runtime/live_control/tests/r16_diagnostics.rs",
    "src/service/predict_fun/account/runtime_claims.rs",
    "src/service/predict_fun/account/r16_tests.rs",
    "src/service/predict_fun/controlled/r16_fixture.rs",
    "tools/predictfun/tests/test_btc5m_r16_integrated.py",
    # R18 startup-only history owner and original-consumer regressions.
    "src/service/predict_fun.rs", "src/service/venue.rs",
    "src/service/predict_fun/account/startup_history.rs",
    "src/service/predict_fun/account/startup_history_tests.rs",
    "src/service/predict_fun/discovery.rs",
    "src/bot/runtime/live_control/tests/startup_history.rs",
    "tools/predictfun/tests/test_btc5m_startup_history.py",
    "Cargo.toml", "Cargo.lock", "src/bin/btc5m_reconcile.rs",
)
CLEANUP_SECONDS = 30
TAIL_SECONDS = 600
MAX_LINE_BYTES = 64 * 1024
# The observation channel's own bounds: a frame, and the number of frames the
# launcher will read after the child is gone.
MAX_OBSERVER_FRAME_BYTES = 64 * 1024
MAX_OBSERVER_DRAIN = 64
EVENT_PREFIX = "BTC5M_EVENT "
RUN_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
# The single environment key the unsigned path may take from the fixed file.
API_SOURCE_ENV = "YQUANT_PREDICT_FUN_TOKEN"
# The fixed project file; it is derived from BOT, never from the working directory.
API_SOURCE_FILE = BOT.parents[4] / "skills/.env"
# The environment a paper run must not see, checked by presence only: the
# values are never read.
FORBIDDEN_ENV = (
    "PM_PRIVATE_KEY",
    "PM_FUNDER_ADDRESS",
    "PM_SIGNATURE_TYPE",
    "PM_API_KEY",
    "PM_API_SECRET",
    "PM_API_PASSPHRASE",
    "PM_PREDICT_ACCOUNT",
    "PM_JWT",
    "PM_PREDICT_JWT",
    "JWT",
    "PRIVATE_KEY",
    "MNEMONIC",
    "SEED",
)


class Refused(Exception):
    """A fixed public refusal: a code, a step and a reason, never a value."""

    def __init__(self, code: str, step: str, reason: str) -> None:
        super().__init__(code)
        self.code = code
        self.step = step
        self.reason = reason

    @property
    def result(self) -> dict[str, str]:
        return {"code": self.code, "step": self.step, "reason": self.reason}

    @property
    def exit_code(self) -> int:
        return 2 if self.code in ("PUBLIC_CONFIG", "CONTROLLED_MODE", "ENV_CONFLICT",
                                  "API_SOURCE", "API_CONFLICT", "DUPLICATE_RUN") else 1


class _Parser(argparse.ArgumentParser):
    """The parser refuses rather than guessing."""

    def error(self, message: str):
        raise Refused("CONTROLLED_MODE", "args", "BAD_ARGUMENTS")


class _Duration(argparse.Action):
    """Preserve the old parsed default while retaining explicit-option provenance."""

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)
        namespace.duration_supplied = True


def build_parser() -> argparse.ArgumentParser:
    """Define the fixed public interface: a mode, a config, a run id and the
    operator's own declarations about history they have explained."""
    parser = _Parser(prog="btc5m_control", allow_abbrev=False)
    parser.add_argument("--mode", choices=(MODE_UNSIGNED_PAPER, MODE_SIGNED_FUTURE_LIVE),
                        required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--predict-budget-confirmation")
    # A historical order hash a human says they explained. Repeatable, and
    # valid only inside a signed live run: it is the child's own input.
    parser.add_argument("--predict-reconciled-order", action="append", metavar="HASH",
                        default=None)
    parser.add_argument("--predict-rpc-url")
    parser.add_argument("--post-entry-observer", action="store_true")
    parser.set_defaults(duration_supplied=False)
    parser.add_argument("--duration-hours", type=int, choices=(6, 72), default=6, action=_Duration)
    parser.add_argument("--live-contract", choices=(STRATEGY_CONTRACT,))
    parser.add_argument("--operations-mode", choices=("supervised", "unattended"))
    parser.add_argument("--risk-window-id")
    parser.add_argument("--authorization-ref")
    parser.add_argument("--deadline-at")
    return parser


def _unique_object(pairs):
    """Refuse a duplicate key rather than letting the last one win."""
    result = {}
    for name, value in pairs:
        if name in result or name.lower() in {
                *(field.lower() for field in FORBIDDEN_ENV),
                "credentials", "private_key", "jwt", "seed", "mnemonic", "api_key",
                "api_secret", "api_passphrase", "predict_api_key", "predict_jwt",
                "yquant_predict_fun_token", "pm_predict_api_key", "signature", "password"}:
            raise ValueError("duplicate or forbidden key")
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


def read_public_config(path: Path) -> dict[str, Any]:
    """Read the public configuration strictly: no duplicates, no secrets."""
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw, object_pairs_hook=_unique_object,
                          parse_constant=_reject_constant)
    except (OSError, ValueError, TypeError, RecursionError):
        raise Refused("PUBLIC_CONFIG", "config", "PUBLIC_CONFIG") from None
    if not isinstance(data, dict):
        raise Refused("PUBLIC_CONFIG", "config", "PUBLIC_CONFIG")
    return data


def check_fixed_values(data: dict[str, Any], mode: str) -> None:
    """Check the values this mode fixes, before anything else happens."""
    # Mirror AppConfig's required fields and serde scalar shapes before any secret read.
    groups = {
        "bot": {"wallets_to_track": "strings", "enable_trading": "bool", "mock_trading": "bool"},
        "site": {name: "str" for name in ("gamma_api_base", "data_api_base", "clob_api_base",
                                         "clob_wss_url", "polygon_rpc_url", "polygon_ws_url")},
        "strategy": {name: "number" for name in ("copy_size", "trade_multiplier", "min_order_size_usd",
                     "max_order_size_usd", "min_whale_shares_to_copy", "adaptive_threshold_usd",
                     "adaptive_min_percent", "adaptive_max_percent")},
        "trading": {"rate_limit": "u32", "rate_window_secs": "u64", "poll_interval_secs": "u64",
                    "price_buffer": "number", "order_expiration_secs": "u64"},
        "risk": {"large_trade_shares": "number", "consecutive_trigger": "u32",
                 "sequence_window_secs": "u64", "min_depth_usd": "number", "trip_duration_secs": "u64"},
        "exchange": {name: "str" for name in ("ctf_exchange_address", "neg_risk_exchange_address",
                                             "domain_name", "domain_version")},
        "predict_fun": {"network": "str", "wallets_to_track": "strings"},
        "filters": {name: "strings" for name in ("slug_allow", "slug_block", "categories_allow",
                                                 "categories_block", "tags_allow", "tags_block")},
        "tp_sl": {"enabled": "bool", "default_take_profit_pct": "number", "default_stop_loss_pct": "number",
                  "per_category_tp_pct": "numbers", "per_category_sl_pct": "numbers", "poll_interval_secs": "u64"},
        "strategies": {"poll_secs": "u64", "rescan_secs": "u64", "quote_ttl_secs": "u64"},
    }
    groups["strategy"]["copy_strategy"] = "str"
    groups["exchange"].update(chain_id="u64", exchange_v3_address="str")
    groups["filters"].update(per_category_max_open_usd="numbers", per_tag_max_open_usd="numbers")
    optional_strategies = {
        "cross_arb": {"min_edge": "number", "order_usd": "number", "polymarket_fee_bps": "u32", "markets_per_tick": "u64"},
        "directional_arb": {"max_basket_cost": "number", "tilt_threshold_cents": "number", "max_tilt_ratio": "number",
                            "order_usd": "number", "max_markets": "u64", "min_secs_left": "i64"},
        "spread_farming": {"min_spread": "number", "order_usd": "number", "max_markets": "u64",
                           "max_hold_secs": "i64", "min_secs_left": "i64"},
        "sports": {"include_props": "bool", "list_limit": "u64"},
        "resolution_sniper": {"min_price": "number", "max_price": "number", "min_net_edge": "number",
                              "order_usd": "number", "max_secs_to_end": "i64"},
        "orderbook_imbalance": {name: "number" for name in ("threshold", "max_spread", "order_usd", "take_profit",
                                                            "stop_loss", "min_price", "max_price")},
        "market_making": {"half_spread": "number", "order_usd": "number", "skew_per_usd": "number",
                          "requote_distance": "number", "max_markets": "u64", "min_secs_left": "i64"},
        "whale_signal": {name: "number" for name in ("min_trade_usd", "order_usd", "per_wallet_max_usd", "max_price")},
    }
    optional_strategies["orderbook_imbalance"].update(levels="u64", fade="bool", max_hold_secs="i64",
                                                       max_markets="u64", max_positions="u64")
    for name, fields in optional_strategies.items():
        groups["strategies." + name] = fields
    optional_fields = {"bot": {"enable_trading", "mock_trading"}, "exchange": {"exchange_v3_address"}}
    required_sections = {"bot", "site", "strategy", "trading", "risk", "exchange", "predict_fun", "strategies"}
    for section, fields in groups.items():
        parent = data
        name = section
        if section.startswith("strategies."):
            parent = data.get("strategies", {})
            name = section.split(".")[1]
            if not isinstance(parent, dict):
                raise Refused("PUBLIC_CONFIG", "config", "APP_CONFIG")
        if name not in parent and section not in required_sections:
            continue
        node = parent.get(name)
        if not isinstance(node, dict):
            raise Refused("PUBLIC_CONFIG", "config", "APP_CONFIG")
        for name, kind in fields.items():
            if name not in node and (section in {"filters", "tp_sl"} or section.startswith("strategies.")
                                     or name in optional_fields.get(section, set())
                                     or (section == "predict_fun" and name == "wallets_to_track")):
                continue
            value = node.get(name)
            valid = ((kind == "str" and isinstance(value, str))
                     or (kind == "bool" and type(value) is bool)
                     or (kind == "number" and _finite(value))
                     or (kind == "i64" and type(value) is int and -(2 ** 63) <= value < 2 ** 63)
                     or (kind in {"u32", "u64"} and type(value) is int
                         and 0 <= value < 2 ** (32 if kind == "u32" else 64))
                     or (kind == "strings" and isinstance(value, list)
                         and all(isinstance(item, str) for item in value))
                     or (kind == "numbers" and isinstance(value, dict)
                         and all(_finite(item) for item in value.values())))
            if not valid:
                raise Refused("PUBLIC_CONFIG", "config", "APP_CONFIG")
    if data["strategy"]["copy_strategy"] not in {"PERCENTAGE", "FIXED", "ADAPTIVE"}:
        raise Refused("PUBLIC_CONFIG", "config", "APP_CONFIG")
    for name, expected in (("poll_secs", 5), ("rescan_secs", 300), ("quote_ttl_secs", 300)):
        if data["strategies"][name] != expected:
            raise Refused("PUBLIC_CONFIG", "config", "PARAMETERS")
    if data.get("venue") != "predict_fun":
        raise Refused("PUBLIC_CONFIG", "config", "VENUE")
    network = data.get("predict_fun")
    if not isinstance(network, dict) or network.get("network") != "mainnet":
        raise Refused("PUBLIC_CONFIG", "config", "NETWORK")
    bot = data.get("bot")
    if not isinstance(bot, dict):
        raise Refused("PUBLIC_CONFIG", "config", "BOT")
    unsigned = mode == MODE_UNSIGNED_PAPER
    if bot.get("enable_trading") is not (not unsigned) or bot.get("mock_trading") is not unsigned:
        raise Refused("PUBLIC_CONFIG", "config", "TRADING_SWITCH")
    paper = data.get("controlled_paper")
    if mode == MODE_UNSIGNED_PAPER:
        if not isinstance(paper, dict):
            raise Refused("PUBLIC_CONFIG", "config", "CONTROLLED_PAPER")
        if paper.get("initial_cash") != 100 or isinstance(paper.get("initial_cash"), bool):
            raise Refused("PUBLIC_CONFIG", "config", "INITIAL_CASH")
        if paper.get("fee_model") != "paper_net_shares":
            raise Refused("PUBLIC_CONFIG", "config", "FEE_MODEL")
    elif paper is not None:
        raise Refused("PUBLIC_CONFIG", "config", "CONTROLLED_PAPER")
    strategies = data.get("strategies")
    if not isinstance(strategies, dict):
        raise Refused("PUBLIC_CONFIG", "config", "STRATEGIES")
    limits = strategies.get("limits")
    if not isinstance(limits, dict):
        raise Refused("PUBLIC_CONFIG", "config", "LIMITS")
    caps = []
    for name in ("max_order_usd", "max_market_usd", "max_total_usd"):
        caps.append(_positive(limits.get(name), "LIMITS"))
    _positive(limits.get("max_loss_usd"), "LIMITS")
    settings = data.get("controlled_run")
    if settings is not None and type(settings) is not dict:
        raise Refused("PUBLIC_CONFIG", "config", "CONTROLLED_RUN")
    if settings is not None and mode == MODE_UNSIGNED_PAPER:
        # The parameterized entry is a signed live run, never a paper one.
        raise Refused("PUBLIC_CONFIG", "config", "CONTROLLED_RUN")
    if mode == MODE_UNSIGNED_PAPER:
        # The controlled unsigned-paper run keeps its historical values.
        for name, expected in (("max_order_usd", 1.0), ("max_market_usd", 1.0),
                               ("max_total_usd", 10.0), ("max_loss_usd", 20.0)):
            if float(limits[name]) != expected:
                raise Refused("PUBLIC_CONFIG", "config", "LIMITS")
    btc = strategies.get("btc_arb")
    if not isinstance(btc, dict):
        raise Refused("PUBLIC_CONFIG", "config", "BTC_ARB")
    order = _positive(btc.get("order_usd"), "PARAMETERS")
    if mode == MODE_UNSIGNED_PAPER and float(order) != 1.0:
        raise Refused("PUBLIC_CONFIG", "config", "PARAMETERS")
    if not float(order) <= caps[0] <= caps[1] <= caps[2]:
        raise Refused("PUBLIC_CONFIG", "config", "MONEY_RELATION")
    if settings is not None:
        parameters(data)
    if btc.get("symbols") != ["BTCUSDT"]:
        raise Refused("PUBLIC_CONFIG", "config", "SYMBOLS")
    if type(btc.get("round_interval_secs")) is not int or btc.get("round_interval_secs") != 300:
        raise Refused("PUBLIC_CONFIG", "config", "ROUND_INTERVAL")
    for name, expected in (("min_edge", 0.03), ("vol_floor", 0.00005)):
        value = btc.get(name)
        if not _finite(value) or float(value) != expected:
            raise Refused("PUBLIC_CONFIG", "config", "PARAMETERS")
    if type(btc.get("min_secs_left")) is not int or btc.get("min_secs_left") != 30:
        raise Refused("PUBLIC_CONFIG", "config", "PARAMETERS")


def _positive(value: Any, reason: str) -> float:
    """One strictly positive, finite public number, or a refusal."""
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not _finite(value):
        raise Refused("PUBLIC_CONFIG", "config", reason)
    if value <= 0:
        raise Refused("PUBLIC_CONFIG", "config", reason)
    return float(value)


def _money_atoms(value: Any) -> int:
    """The exact atoms of a public money value, or a refusal.

    Only the shortest decimal text is read, so no float multiply decides a
    spendable amount; more than six decimals or a non-positive value refuse.
    """
    if not _finite(value):
        raise Refused("PUBLIC_CONFIG", "config", "MONEY_PRECISION")
    try:
        amount = Decimal(str(value))
    except (ValueError, ArithmeticError):
        raise Refused("PUBLIC_CONFIG", "config", "MONEY_PRECISION") from None
    if amount <= 0 or amount.as_tuple().exponent < -6 or amount > Decimal(10) ** 9:
        raise Refused("PUBLIC_CONFIG", "config", "MONEY_PRECISION")
    return int(amount * (Decimal(10) ** 18))


def _budget_atoms(value: Any) -> int:
    """The budget's exact atoms: a decimal string (≤18 places) or a number (≤6).

    A string is the only form that can state a budget at atom precision, for
    example a post-claim balance of ``98.148096491228070176``; it is read as a
    plain decimal with ``Decimal``, never through a float. A string with a sign,
    an exponent, a trailing dot or more than eighteen fractional digits is a
    refusal, never a truncation or a rounding.
    """
    if not isinstance(value, str):
        return _money_atoms(value)
    whole, dot, fraction = value.partition(".")
    # Only ASCII digits are a decimal budget: `str.isdigit` also accepts other
    # Unicode digit forms, and `Decimal` would read them as numbers. A budget
    # is a plain decimal, so anything else is a refusal, never a conversion.
    if (re.fullmatch(r"[0-9]+", whole) is None or not whole or len(fraction) > 18
            or (dot and re.fullmatch(r"[0-9]+", fraction) is None)):
        raise Refused("PUBLIC_CONFIG", "config", "MONEY_PRECISION")
    try:
        amount = Decimal(value)
    except ArithmeticError:
        raise Refused("PUBLIC_CONFIG", "config", "MONEY_PRECISION") from None
    if amount <= 0 or amount > Decimal(10) ** 9:
        raise Refused("PUBLIC_CONFIG", "config", "MONEY_PRECISION")
    return int(amount * (Decimal(10) ** 18))


def parameters(data: dict[str, Any]) -> dict[str, Any] | None:
    """The frozen money and duration this public configuration declares.

    A legacy file declares none and keeps the historical values; the
    parameterized entry declares all of them, and its relation
    `target<=order<=market<=total<=budget` must hold exactly.
    """
    settings = data.get("controlled_run")
    if settings is None:
        return None
    if not isinstance(settings, dict) or set(settings) != {"budget_usdt", "duration_seconds",
                                                          "operations_mode"}:
        raise Refused("PUBLIC_CONFIG", "config", "CONTROLLED_RUN")
    mode = settings["operations_mode"]
    seconds = settings["duration_seconds"]
    if mode not in {"supervised", "unattended"} or type(seconds) is not int or isinstance(seconds, bool):
        raise Refused("PUBLIC_CONFIG", "config", "CONTROLLED_RUN")
    if not 0 < seconds <= (21_600 if mode == "supervised" else 86_400):
        raise Refused("PUBLIC_CONFIG", "config", "DURATION")
    strategies = data.get("strategies")
    if not isinstance(strategies, dict):
        raise Refused("PUBLIC_CONFIG", "config", "STRATEGIES")
    try:
        limits = strategies["limits"]
        btc = strategies["btc_arb"]
        result = {"budget": _budget_atoms(settings["budget_usdt"]),
                  "target": _money_atoms(btc["order_usd"]),
                  "order": _money_atoms(limits["max_order_usd"]),
                  "market": _money_atoms(limits["max_market_usd"]),
                  "total": _money_atoms(limits["max_total_usd"]),
                  "loss": _money_atoms(limits["max_loss_usd"]),
                  "duration": seconds, "operations_mode": mode}
    except (KeyError, TypeError):
        raise Refused("PUBLIC_CONFIG", "config", "CONTROLLED_RUN") from None
    if not (result["target"] <= result["order"] <= result["market"]
            <= result["total"] <= result["budget"]):
        raise Refused("PUBLIC_CONFIG", "config", "MONEY_RELATION")
    return result


def _parse_api_source(text: str) -> str:
    """Extract exactly one target assignment from a fixed single-line syntax.

    Only ``KEY=value`` lines count, with an optional ``export``, optional
    whitespace around the key and the equals sign, and an optionally quoted
    value. The value is literal: no escape, variable or command substitution.
    A trailing comment is not part of the value, so it can never be mistaken
    for a line continuation.
    """
    found: list[str] = []
    for raw in text.splitlines():
        line = raw.strip(" \t")
        if not line or line.startswith("#"):
            continue
        if line.startswith("export") and len(line) > len("export") and line[len("export")] in " \t":
            line = line[len("export"):].lstrip(" \t")
        key, separator, rest = line.partition("=")
        if not separator or key.strip(" \t") != API_SOURCE_ENV:
            continue  # A near-miss name is not the target key.
        found.append(rest.lstrip(" \t"))
    if len(found) != 1:
        raise Refused("API_SOURCE", "env", "API_SOURCE")  # Missing or duplicated.
    rest = found[0]
    if rest[:1] in {'"', "'"}:
        quote = rest[0]
        end = rest.find(quote, 1)
        if end < 0:
            raise Refused("API_SOURCE", "env", "API_SOURCE")  # No multi-line quotes.
        value = rest[1:end]
        tail = rest[end + 1:].strip(" \t")
        if tail and not tail.startswith("#"):
            raise Refused("API_SOURCE", "env", "API_SOURCE")  # Illegal trailing text.
    else:
        cut = len(rest)
        for index, character in enumerate(rest):
            if character == "#" and (index == 0 or rest[index - 1] in " \t"):
                cut = index
                break
        value = rest[:cut].rstrip(" \t")
        if value.endswith("\\"):
            raise Refused("API_SOURCE", "env", "API_SOURCE")  # No line continuation.
    if not value.strip():
        raise Refused("API_SOURCE", "env", "API_SOURCE")
    return value


def read_api_source_file(path: Path | None = None) -> str:
    """Read the one permitted key from the fixed project file."""
    target = API_SOURCE_FILE if path is None else path
    try:
        text = target.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        raise Refused("API_SOURCE", "env", "API_SOURCE") from None
    return _parse_api_source(text)


def check_environment(environment: dict[str, str]) -> str:
    """Refuse a wallet or a token in the environment, and find the API key."""
    for name in FORBIDDEN_ENV:
        if name in environment:
            raise Refused("ENV_CONFLICT", "env", "SECRET_PRESENT")
    source = environment
    if API_SOURCE_ENV not in environment:
        # Only a completely absent source key falls back to the fixed file;
        # the parent environment and os.environ stay untouched.
        source = {**environment, API_SOURCE_ENV: read_api_source_file(API_SOURCE_FILE)}
    try:
        return local_signer._api_source(source)
    except Failure as error:
        raise Refused("ENV_CONFLICT" if error.result["reason"] == "API_CONFLICT"
                      else "API_SOURCE", "env", "API_SOURCE") from None


def child_environment(environment: dict[str, str], api_key: str) -> dict[str, str]:
    """Build the child's environment: the OS basics and the market API key."""
    child = {name: environment[name] for name in INHERIT_ENV if name in environment}
    child.update(PATH="/usr/bin:/bin", RUST_LOG="warn", PM_PREDICT_API_KEY=api_key)
    return child


def check_paths(config: Path, paper72: bool = False) -> tuple[Path, Path]:
    """Find the fixed child and refuse a credentials file that exists."""
    binary = PAPER72_BINARY if paper72 else BOT / "target/debug/polymarket-toolkits"
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise Refused("CONTROLLED_MODE", "paths", "CHILD_PATH")
    credentials = BOT / "tools/predictfun/.keyring-no-credentials"
    if os.path.lexists(credentials):
        raise Refused("CONTROLLED_MODE", "paths", "CREDENTIAL_PATH_PRESENT")
    return config, binary


@dataclass
class Evidence:
    """The run's public evidence, written before anything is started."""

    directory: Path
    run_id: str
    started_at: str
    deadline_at: str
    mode: str
    files: list[Path] = field(default_factory=list)

    def write(self, name: str, payload: dict[str, Any]) -> None:
        """Create one evidence file, exclusively."""
        path = self.directory / name
        with open(path, "x", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, ensure_ascii=True)
            handle.write("\n")
        self.files.append(path)


def public_identity(environment: dict[str, str], paper72: bool = False, *, live: bool = False) -> dict[str, Any]:
    """Resolve fixed Git identities and actual public candidate bytes."""
    paths = ("src/config.rs", "src/main.rs", "src/bot/runtime.rs", "src/bot/runtime/paper.rs",
             "src/bot/arbitrage.rs", "src/bot/arbitrage/replay/driver.rs", "src/service/venue.rs",
             "src/service/predict_fun.rs", "tools/predictfun/cli.py", "tools/predictfun/sdk_ops.py",
             "tools/predictfun/btc5m_control.py", "tools/predictfun/tests/test_sdk_ops.py",
             "tools/predictfun/tests/test_btc5m_control.py", "tools/predictfun/btc5m-controlled-paper.json",
             "src/service/spot.rs", "src/service/observation.rs", "src/service/mod.rs",
             "src/service/predict_fun/books.rs", "src/bot/quoting.rs",
             "src/bot/arbitrage/phase1.rs", "src/bot/arbitrage/phase1/tests.rs",
             "src/bot/runtime/paper/tests.rs", "src/bot/arbitrage/replay.rs",
             "src/bot/arbitrage/replay/types.rs", "src/bot/arbitrage/replay/tests.rs",
             "src/bot/arbitrage/observer.rs", "src/bot/arbitrage/observer/tests.rs",
             "src/bot/runtime/observer.rs", "tools/predictfun/btc5m_observer.py",
             "tools/predictfun/tests/test_btc5m_observer.py",
             "tools/predictfun/tests/test_btc5m_paper72.py",
             "src/bin/btc_arb_replay.rs")
    root = BOT.parents[4]
    identity: dict[str, Any] = {}
    try:
        for label, path in (("main", root), ("bot", BOT)):
            result = subprocess.run(["/usr/bin/git", "-C", str(path), "rev-parse", "HEAD"],
                                    env={"PATH": "/usr/bin:/bin"}, capture_output=True,
                                    timeout=5, check=True)
            head = result.stdout.decode("ascii").strip()
            if re.fullmatch("[0-9a-f]{40}", head) is None:
                raise ValueError("HEAD")
            identity[f"{label}_head"] = head
        binary = LIVE_BINARY if live else (PAPER72_BINARY if paper72 else BOT / "target/debug/polymarket-toolkits")
        identity["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
        if live:
            if identity["binary_sha256"] != LIVE_BINARY_SHA256 or not os.access(binary, os.X_OK):
                raise ValueError("BINARY_IDENTITY")
            paths = tuple(sorted(set(paths) | set(LIVE_PATHS)))
        identity["files"] = {name: hashlib.sha256((BOT / name).read_bytes()).hexdigest()
                             for name in paths}
    except (OSError, ValueError, subprocess.SubprocessError):
        raise Refused("CONTROLLED_MODE", "identity", "CANDIDATE_IDENTITY") from None
    return identity


def start_evidence(directory: Path, run_id: str, started_at: str, deadline_at: str,
                   mode: str, config: dict[str, Any], config_sha: str,
                   identity: dict[str, Any]) -> Evidence:
    """Create the run's evidence directory and its start record."""
    directory.mkdir(parents=True, exist_ok=False)
    evidence = Evidence(directory=directory, run_id=run_id, started_at=started_at,
                        deadline_at=deadline_at, mode=mode)
    evidence.write("public-config.json", config)
    config_sha = hashlib.sha256((evidence.directory / "public-config.json").read_bytes()).hexdigest()
    started: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION if mode == MODE_SIGNED_FUTURE_LIVE else SCHEMA_VERSION_V2,
        "run_id": run_id,
        "started_at": started_at,
        "deadline_at": deadline_at,
        "mode": mode,
        "candidate_identity": identity,
        "public_config_identity": {
            "sha256": config_sha,
            # The paper window is funded from its own simulated cash (100); the
            # signed window's budget (98) is written by run_live below.
            "initial_cash": 100 if mode == MODE_UNSIGNED_PAPER else None,
            "max_order_usd": 1,
            "max_market_usd": 1,
            "max_total_usd": 10,
            "max_loss_usd": 20,
        },
    }
    if mode == MODE_UNSIGNED_PAPER:
        # The unsigned run speaks the phase1 protocol: the wire version and
        # the economics it decides under are part of its identity.
        started["strategy_contract"] = STRATEGY_CONTRACT
    evidence.write("started.json", started)
    return evidence


def child_arguments(binary: Path, config_path: Path, run_id: str, started_at: str,
                    deadline_at: str) -> list[str]:
    """The one fixed child command line."""
    return [
        str(binary),
        "--config", str(config_path),
        "--controlled-mode", MODE_UNSIGNED_PAPER,
        "--controlled-run-id", run_id,
        "--controlled-started-at", started_at,
        "--controlled-deadline-at", deadline_at,
        "run", "btc-arb",
    ]


def boottime() -> float:
    """Include suspend time in the parent's elapsed budget."""
    return time.clock_gettime(time.CLOCK_BOOTTIME)


def candidate_input_error(frozen: dict[str, Any], decision: dict[str, Any],
                          market_id: Any, original_market_id: Any, *,
                          allow_diagnostic_limit: bool = False) -> str | None:
    """Compare fixed candidate facts, allowing clock-derived valuations to change.

    Inputs have already passed the caller's schema validator. Only paper's
    diagnostic-limit path may omit depth facts; live retains strict depth checks.
    """
    fields = ("decision_id", "candidate_id", "selected_token", "symbol",
        "prediction_source", "actual_pair", "source_state", "source_epoch",
        "reset_reason", "settlement_provider", "starts_at", "ends_at",
        "identity_available_at", "strike_available_at", "strike", "spot",
        "spot_clock", "book_clock", "decision_at", "decision_elapsed_ms",
        "samples", "sigma", "fee_rate_bps", "best_yes_bid", "best_yes_ask")
    if (market_id != original_market_id
            or any(decision[name] != frozen[name] for name in fields)):
        return "CANDIDATE_INPUTS"
    for side in ("up", "down"):
        if decision[side] is None or frozen[side] is None:
            if decision[side] != frozen[side]:
                return "CANDIDATE_SIDE"
            continue
        for name in ("token", "limit_price", "gross_shares", "depth_levels", "depth_complete"):
            if (decision[side][name] != frozen[side][name]
                    and not (allow_diagnostic_limit and decision["reason"] == "DIAGNOSTIC_LIMIT"
                             and name.startswith("depth_") and decision[side][name] is None)):
                return "CANDIDATE_SIDE"
    return None


class _EventRelations:
    """Validate v2 correlations, not a second cash or execution ledger."""

    def __init__(self) -> None:
        self.last_seq = 0
        self.evaluated: set[str] = set()
        self.candidates: dict[str, dict[str, Any]] = {}
        self.fills: dict[tuple[str, str], dict[str, Any]] = {}
        self.settled: set[tuple[str, str]] = set()

    def accept(self, event: dict[str, Any], previous: dict[str, Any] | None) -> None:
        """Commit indices only after this event and all transitions validate."""
        if event["event_seq"] != self.last_seq + 1:
            raise ValueError("EVENT_SEQUENCE")
        kind, details = event["event_kind"], event["source"] or {}
        pair = (event["market_id"], event["token"])
        decision = details.get("decision")
        candidate_id = details.get("candidate_id")
        candidate_update = fill_update = evaluation = settlement = None
        reserved_delta = 0.0
        if decision is not None:
            identifier = decision["decision_id"]
            candidate_id = decision["candidate_id"]
            if decision["phase"] == "EVALUATE":
                if identifier in self.evaluated or int(identifier) != len(self.evaluated) + 1:
                    raise ValueError("REPEATED_EVALUATE")
                evaluation = identifier
                if candidate_id is not None:
                    candidate_update = {"initial": decision, "last": decision,
                        "market_id": event["market_id"], "stage": "EVALUATE"}
            else:
                original = self.candidates.get(candidate_id)
                expected = "EVALUATE" if decision["phase"] == "PRE_SUBMIT" else "RESERVED"
                if original is None or original["stage"] != expected:
                    raise ValueError("CANDIDATE_PHASE")
                frozen = original["initial"]
                error = candidate_input_error(frozen, decision, event["market_id"],
                                              original["market_id"], allow_diagnostic_limit=True)
                if error is not None:
                    raise ValueError(error)
                candidate_update = {**original, "stage": decision["phase"], "last": decision}
        elif kind in {"reservation", "release", "blocked"} or (kind == "fill" and details["side"] == "buy"):
            original = self.candidates.get(candidate_id) if candidate_id is not None else None
            if original is None or original["stage"] == "TERMINAL":
                raise ValueError("UNKNOWN_OR_TERMINAL_CANDIDATE")
            frozen, last = original["initial"], original["last"]
            if (pair != (original["market_id"], frozen["selected_token"])
                    or details["decision_id"] != frozen["decision_id"]):
                raise ValueError("CANDIDATE_IDENTITY")
            side = next(item for item in (frozen["up"], frozen["down"])
                        if item is not None and item["token"] == frozen["selected_token"])
            amount = side["cost"]
            if kind == "reservation":
                if (original["stage"] != "PRE_SUBMIT" or last["reason"] != "ELIGIBLE"
                        or details["diagnostic_reason"] != "ELIGIBLE"):
                    raise ValueError("RESERVATION_PHASE")
                stage, reserved_delta = "RESERVED", amount
            elif kind == "release":
                if original["stage"] not in {"RESERVED", "PRE_APPLY"}:
                    raise ValueError("RELEASE_PHASE")
                stage, reserved_delta = "RELEASED", -amount
            elif kind == "blocked":
                if original["stage"] not in {"EVALUATE", "PRE_SUBMIT", "RELEASED"}:
                    raise ValueError("BLOCKED_WITH_RESERVATION")
                stage = "TERMINAL"
            else:
                chosen = next(item for item in (last["up"], last["down"])
                              if item is not None and item["token"] == frozen["selected_token"])
                if (original["stage"] != "PRE_APPLY" or last["reason"] != "ELIGIBLE"
                        or details["diagnostic_reason"] != "ELIGIBLE"
                        or details["gross_shares"] != side["gross_shares"]
                        or details["price"] > side["limit_price"]
                        or details["fee_rate_bps"] != frozen["fee_rate_bps"]
                        or details.get("price_source") != {"name": frozen["prediction_source"],
                                                           "clock": frozen["spot_clock"]}
                        or not chosen["depth_complete"] or chosen["execution_edge"] is None
                        or any(chosen[expected] is None
                               or abs(details[actual] - chosen[expected]) > 1e-7
                               for actual, expected in (("price", "walk_vwap"),
                                   ("cost", "execution_cost"), ("net_shares", "execution_net_shares")))
                        or chosen["execution_edge"] < .03 or details["net_shares"] <= 0
                        or pair in self.fills):
                    raise ValueError("BUY_CANDIDATE")
                stage, reserved_delta = "TERMINAL", -amount
                fill_update = (pair, {"candidate_id": candidate_id,
                                      "shares": details["net_shares"], "cost": details["cost"]})
            candidate_update = {**original, "stage": stage}
        elif kind == "fill":  # Shared SELL accounting remains legal; no invented sell candidate.
            bought = self.fills.get(pair)
            if bought is None or pair in self.settled or details["shares"] > bought["shares"]:
                raise ValueError("SELL_WITHOUT_BUY")
            fill_update = (pair, {**bought, "shares": bought["shares"] - details["shares"],
                                 "cost": bought["cost"] - details["basis"]})
        elif kind == "settlement":
            bought = self.fills.get(pair)
            if (bought is None or pair in self.settled
                    or bought["candidate_id"] != candidate_id
                    or abs(details["shares"] - bought["shares"]) > 1e-7
                    or abs(details["basis"] - bought["cost"]) > 1e-7):
                raise ValueError("SETTLEMENT_LINK")
            settlement = pair
        elif kind == "final":
            if any(item["stage"] != "TERMINAL" for item in self.candidates.values()):
                raise ValueError("UNFINISHED_CANDIDATE")
            if event["accounting"]["reserved"] != 0:
                raise ValueError("UNRELEASED_RESERVATION")
        before = previous["accounting"]["reserved"] if previous is not None else 0.0
        if abs(event["accounting"]["reserved"] - before - reserved_delta) > 1e-7:
            raise ValueError("RESERVATION_TRANSITION")
        # All checks are complete. A subsequent output failure stops the run, never retries it.
        if evaluation is not None:
            self.evaluated.add(evaluation)
        if candidate_update is not None and candidate_id is not None:
            self.candidates[candidate_id] = candidate_update
        if fill_update is not None:
            self.fills[fill_update[0]] = fill_update[1]
        if settlement is not None:
            self.settled.add(settlement)
        self.last_seq = event["event_seq"]


def supervise(process, evidence, started_at, started_boot, observer=None, *, run_seconds=None):
    """Keep the sole control writer alive while draining both bounded streams."""
    facts = {"launcher_exit": 0, "child_returncode": None, "stop_reason": None,
             "forced_cleanup": False, "events": 0, "discarded": 0}
    deadline_sent = False
    cleanup_at = None
    final = None
    previous = None
    buffers = {}
    dropping = set()
    selector = selectors.DefaultSelector()
    old_signals = {}
    # The relations the v2 protocol needs to check. They are not a ledger:
    # they say which decision each event belongs to and whether that
    # decision has already reached its terminal event.
    relations = _EventRelations()
    run_seconds = RUN_SECONDS if run_seconds is None else run_seconds
    from btc5m_live_events import LiveEventStore, LiveInvalid
    live = isinstance(evidence, LiveEventStore)
    paper72 = not live and evidence.mode == MODE_UNSIGNED_PAPER and run_seconds == PAPER72_SECONDS
    line_limit = PAPER72_FINAL_BYTES if paper72 else MAX_LINE_BYTES
    # The observation channel, when the run opted into it. It is read only
    # after the original stream, and a failure in it never stops the run.
    observer_socket = None
    if observer is not None:
        observer_socket = observer.get("socket")
        if observer_socket is not None:
            try:
                os.set_blocking(observer_socket.fileno(), False)
            except (OSError, ValueError):
                observer["receiver"].store.reason("TRANSPORT_SETUP_FAILED")
                observer_socket = None

    def stop(signum=None, _frame=None, *, reason="OUTPUT_INVALID"):
        nonlocal cleanup_at, final
        if signum is None:
            final = None
        facts["launcher_exit"] = 128 + signum if signum else 1
        cause = "SIGNAL" if signum else reason
        # The cause that stopped the run stands: output that only arrives after
        # it is a consequence of the stop, never a cause of it, so an ambiguous
        # audit failure can never be rewritten as OUTPUT_INVALID (or the other
        # way round) by a later stop.
        if facts["stop_reason"] is None:
            facts["stop_reason"] = cause
            # This run stopped on its own output only when that output is the
            # one cause. The marker is the single write-through condition and
            # cannot be set or moved by anything that follows it.
            if cause == "OUTPUT_INVALID":
                facts["output_invalid_origin"] = True
        proposed = boottime() + CLEANUP_SECONDS
        cleanup_at = min(cleanup_at, proposed) if cleanup_at is not None else proposed
        if process.stdin and not process.stdin.closed:
            process.stdin.close()
        if process.poll() is None:
            process.send_signal(signal.SIGINT)

    # The first fragment that stops a live run is the only output that
    # explains the stop, and the one piece that never reaches the evidence by
    # itself. It is remembered here, redacted at write time, and written
    # before the run's terminal record; later fragments are its consequences.
    stopped: dict[str, Any] | None = None
    # How much of this run's own child log is already written.
    log_budget = [0]

    def keep_stopped(stream, raw):
        """Keep the first stopping fragment of a live run, before it is dropped."""
        nonlocal stopped
        if stopped is not None or not live:
            return
        if facts["stop_reason"] not in (None, "OUTPUT_INVALID"):
            # The run already stopped for another cause: this fragment is one
            # consequence of that stop, never the fragment that explains it.
            return
        stopped = {"stream": "stdout" if stream is process.stdout else "stderr",
                   "raw": bytes(raw),
                   "newline_terminated": bytes(raw).endswith(b"\n"),
                   "occurred_at": datetime.datetime.now(datetime.timezone.utc)
                   .isoformat(timespec="milliseconds").replace("+00:00", "Z")}

    def drain_observer():
        """Read at most one observation frame, after the trading stream.

        The frame is validated and related before it is written; a frame that
        fails is dropped and counted, never repaired and never retried.
        """
        if observer is None or observer_socket is None:
            return
        receiver = observer.get("receiver")
        if receiver is None:
            return
        try:
            chunk = observer_socket.recv(observer["max_bytes"] + 1)
        except (BlockingIOError, InterruptedError):
            return
        except OSError:
            receiver.store.reason("INVALID_FRAME")
            return
        if not chunk:
            return
        if len(chunk) > observer["max_bytes"]:
            receiver.store.reason("TRUNCATED_OUTPUT")
            return
        if not chunk.endswith(b"\n") or b"\n" in chunk[:-1] or b"\r" in chunk:
            receiver.store.reason("INVALID_FRAME")
            return
        try:
            frame = json.loads(chunk.decode("utf-8"),
                               object_pairs_hook=_unique_object,
                               parse_constant=_reject_constant)
        except (ValueError, TypeError, UnicodeError, RecursionError):
            receiver.store.reason("INVALID_FRAME")
            return
        if not isinstance(frame, dict):
            receiver.store.reason("INVALID_FRAME")
            return
        try:
            receiver.receive(frame, len(chunk))
        except Exception:
            receiver.store.reason("INVALID_FRAME")

    def acknowledge(raw):
        # No ACK after local stop/deadline, even if persistence itself was slow.
        elapsed = max(boottime() - started_boot,
                      (datetime.datetime.now(datetime.timezone.utc) - started_at).total_seconds())
        if facts["stop_reason"] is not None or elapsed >= run_seconds:
            raise ValueError("ACK_REFUSED")
        if not process.stdin or process.stdin.closed:
            raise ValueError("CONTROL_CHANNEL")
        if process.stdin.write(raw) != len(raw):
            raise ValueError("CONTROL_CHANNEL")
        process.stdin.flush()

    try:
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            old_signals[signum] = signal.signal(signum, stop)
        for stream in (process.stdout, process.stderr):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ)
            buffers[stream] = bytearray()
        output = nullcontext(None) if live else open(evidence.directory / "events.jsonl", "x", encoding="utf-8")
        with output as events:
            while selector.get_map() or process.poll() is None:
                elapsed = max(boottime() - started_boot,
                              (datetime.datetime.now(datetime.timezone.utc) - started_at).total_seconds())
                if not deadline_sent and elapsed >= run_seconds and cleanup_at is None:
                    deadline_sent = True
                    facts["stop_reason"] = "DEADLINE"
                    process.stdin.write(b"D")
                    process.stdin.flush()
                    cleanup_at = boottime() + CLEANUP_SECONDS + TAIL_SECONDS
                # Reserve part of the SAME cleanup window for kill/reap, not a new timeout.
                if cleanup_at is not None and boottime() >= cleanup_at - min(1, CLEANUP_SECONDS / 4):
                    if process.poll() is None:
                        facts["forced_cleanup"] = True
                        facts["launcher_exit"] = 1
                        process.kill()
                    break
                for key, _mask in selector.select(timeout=0.05):
                    stream = key.fileobj
                    chunk = os.read(stream.fileno(), 8192)
                    if not chunk:
                        selector.unregister(stream)
                        if buffers[stream] or stream in dropping:
                            residue = bytes(buffers[stream])
                            buffers[stream].clear()
                            if live and stream is not process.stdout:
                                # The child's stderr residue is evidence, not a
                                # cause: it is kept in this run's own log and
                                # the run goes on.
                                append_run_log(evidence.directory, "stderr", residue, log_budget)
                            else:
                                keep_stopped(stream, residue)
                                stop()
                        continue
                    for fragment in chunk.splitlines(keepends=True):
                        if stream not in dropping:
                            buffers[stream].extend(fragment)
                            limit = (line_limit if stream is process.stdout
                                     and buffers[stream].startswith(EVENT_PREFIX.encode()) else MAX_LINE_BYTES)
                            if len(buffers[stream]) > limit:
                                dropping.add(stream)
                                residue = bytes(buffers[stream])
                                buffers[stream].clear()
                                if live and stream is not process.stdout:
                                    # An oversize stderr line is evidence, not a
                                    # cause: it is kept in this run's log and the
                                    # run goes on.
                                    append_run_log(evidence.directory, "stderr", residue, log_budget)
                                else:
                                    keep_stopped(stream, residue)
                                    stop()
                        if fragment.endswith(b"\n"):
                            if stream in dropping:
                                dropping.remove(stream)
                                continue
                            raw = bytes(buffers[stream])
                            buffers[stream].clear()
                            if not raw.startswith(EVENT_PREFIX.encode()):
                                facts["discarded"] += 1
                                # A non-event line is evidence, not a cause: it
                                # is kept in this run's own bounded log and the
                                # run goes on.
                                if live:
                                    append_run_log(evidence.directory,
                                                   "stdout" if stream is process.stdout else "stderr",
                                                   raw, log_budget)
                                continue
                            try:
                                if facts["stop_reason"] == "OUTPUT_INVALID":
                                    raise ValueError("OUTPUT_INVALID")
                                if live:
                                    if stream is not process.stdout:
                                        # The child's stderr carries no event.
                                        # It is evidence for a later diagnosis,
                                        # not a reason to stop the run.
                                        append_run_log(evidence.directory, "stderr", raw,
                                                       log_budget)
                                        facts["discarded"] += 1
                                        continue
                                    try:
                                        event = evidence.append(raw, acknowledge)
                                    except LiveInvalid as error:
                                        # Only the store's refusal of this line
                                        # makes it the reason the run stops.
                                        if error.reason == "OUTPUT_INVALID":
                                            keep_stopped(stream, raw)
                                        raise
                                    facts["events"] += 1
                                    if event["event_kind"] == "final":
                                        final = event
                                    continue
                                event = validate_event(raw[len(EVENT_PREFIX):], evidence.run_id, evidence.mode)
                                if len(raw) > MAX_LINE_BYTES and not (paper72 and event["event_kind"] == "final"):
                                    raise ValueError("OUTPUT_INVALID")
                                if event["event_kind"] == "final" and event["source"]["cleanup_timed_out"]:
                                    facts["launcher_exit"] = 1
                                if final is not None:
                                    raise ValueError("AFTER_FINAL")
                                # Only compare adjacent protocol snapshots; no independent ledger.
                                if previous is not None and evidence.mode == MODE_UNSIGNED_PAPER:
                                    before, after = previous["accounting"], event["accounting"]
                                    details = event["source"] or {}
                                    deltas = dict(cash=0, cost=0, realized=0, fees_in_cash=0)
                                    if event["event_kind"] == "fill":
                                        if details["side"] == "buy":
                                            deltas.update(cash=-details["cost"], cost=details["cost"], fees_in_cash=details["fee"])
                                        else:
                                            deltas.update(cash=details["net_cash"], cost=-details["basis"],
                                                          realized=details["pnl"], fees_in_cash=details["fee"])
                                    elif event["event_kind"] == "settlement":
                                        deltas.update(cash=details["income"], cost=-details["basis"],
                                                      realized=details["income"] - details["basis"])
                                    if any(abs(after[name] - before[name] - delta) > 1e-7
                                           for name, delta in deltas.items()):
                                        raise ValueError("EVENT_TRANSITION")
                                if evidence.mode == MODE_UNSIGNED_PAPER:
                                    relations.accept(event, previous)
                                previous = event
                                if event["event_kind"] == "final":
                                    final = event
                                events.write(json.dumps(event, sort_keys=True, allow_nan=False) + "\n")
                                events.flush()
                                facts["events"] += 1
                                # The observation is related to the evidence that
                                # has just been written, and only then.
                                if observer is not None and observer.get("receiver") is not None:
                                    try:
                                        observer["receiver"].note_trade_event(event)
                                    except Exception:
                                        observer["receiver"].store.reason("RELATION_INVALID")
                            except (ValueError, TypeError, UnicodeError, RecursionError, OverflowError) as error:
                                stop(reason=getattr(error, "reason", "OUTPUT_INVALID") if live else "OUTPUT_INVALID")
                # The trading stream has had its turn; the observation may now
                # be read, at most one frame per turn.
                drain_observer()
                # A dead child must not keep the parent waiting for unrelated inherited output FDs.
                if process.poll() is not None and cleanup_at is None:
                    cleanup_at = boottime() + CLEANUP_SECONDS
    except BaseException:
        stop()
    finally:
        if process.stdin and not process.stdin.closed:
            process.stdin.close()
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        cleanup_at = cleanup_at if cleanup_at is not None else boottime() + CLEANUP_SECONDS
        try:
            process.wait(timeout=max(0, cleanup_at - boottime()))
        except subprocess.TimeoutExpired:
            facts.update(forced_cleanup=True, launcher_exit=1)
            process.kill()
            try:
                process.wait(timeout=max(0, cleanup_at - boottime()))
            except subprocess.TimeoutExpired:
                facts["stop_reason"] = "REAP_TIMEOUT"
        facts["child_returncode"] = process.returncode
        facts["child_signal"] = -process.returncode if process.returncode is not None and process.returncode < 0 else None
        if stopped is not None:
            # The fragment is written first: the terminal record that says the
            # run stopped on its own output must be able to say what it was.
            # A failed write never becomes the reason the run stopped.
            try:
                write_stop_output(evidence, stopped, facts)
                facts["stop_output"] = "WRITTEN"
            except (OSError, ValueError):
                facts.update(launcher_exit=1, stop_output="WRITE_FAILED")
        for stream in (process.stdout, process.stderr):
            stream.close()
        selector.close()
        for signum, handler in old_signals.items():
            signal.signal(signum, handler)
        # A bounded drain of the observation channel: the frames that are
        # already there, and nothing more.
        if observer is not None and observer.get("receiver") is not None:
            for _ in range(MAX_OBSERVER_DRAIN):
                if boottime() >= cleanup_at:
                    break
                drain_observer()
            try:
                observer["receiver"].finish()
            except Exception:
                observer["receiver"].store.reason("WRITE_FAILED")
    if process.returncode != 0 or final is None:
        facts["launcher_exit"] = facts["launcher_exit"] or 1
    facts["elapsed_secs"] = boottime() - started_boot
    return facts, final


# --- The phase1 diagnostic schema ---------------------------------------------
#
# The v2 protocol adds one object: the decision snapshot. It is rebuilt
# through an explicit allowlist, field by field, so a key that the contract
# does not name can never reach the evidence file.
#
# Every listed key must be present. A value that could not be computed is
# null, never zero and never omitted.

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

SIDE_SCHEMA = {
    "token": "id",
    "fair": "number",
    "limit_price": "number",
    "gross_shares": "number",
    "fee_per_gross_share": "number",
    "net_shares": "number",
    "cost": "number",
    "ev": "number",
    "edge": "number",
    "roi": "number",
    "breakeven_fair": "number",
    "qualifies": "bool",
    "depth_levels": [LEVEL_SCHEMA],
    "depth_complete": "bool",
    "walk_vwap": "number",
    "execution_net_shares": "number",
    "execution_cost": "number",
    "execution_ev": "number",
    "execution_edge": "number",
}

DECISION_SCHEMA = {
    "decision_id": "id",
    "candidate_id": "id",
    "phase": "phase",
    "strategy_contract": "strategy_contract",
    "reason": "diagnostic_reason",
    "symbol": "symbol",
    "prediction_source": "prediction_source",
    "actual_pair": "actual_pair",
    "source_state": "source_state",
    "source_epoch": "epoch",
    "reset_reason": "reset_reason",
    "settlement_provider": "settlement_provider",
    "starts_at": "time",
    "ends_at": "time",
    "identity_available_at": "time",
    "strike_available_at": "time",
    "strike": "number",
    "spot": "number",
    "spot_clock": CLOCK_SCHEMA,
    "book_clock": CLOCK_SCHEMA,
    "decision_at": "time",
    "decision_elapsed_ms": "elapsed",
    "checked_at": "time",
    "checked_elapsed_ms": "elapsed",
    "secs_left": "number",
    "samples": "samples",
    "sigma": "number",
    "p_up": "number",
    "best_yes_bid": "number",
    "best_yes_ask": "number",
    "fee_rate_bps": "bps",
    "selected_token": "id",
    "up": SIDE_SCHEMA,
    "down": SIDE_SCHEMA,
}

V2_ENUMS = {
    "phase": {"EVALUATE", "PRE_SUBMIT", "PRE_APPLY"},
    "strategy_contract": {STRATEGY_CONTRACT},
    "diagnostic_reason": {
        "RUN_STOPPED", "ALREADY_ENTERED", "MARKET_INVALID", "MISSING_INPUT",
        "INPUT_INVALID", "CLOCK_INVALID", "REQUEST_TOO_SLOW", "SPOT_STALE",
        "BOOK_STALE", "ROUND_TOO_LATE", "INPUT_CHANGED", "WARMUP",
        "NET_SHARES_INVALID", "EDGE_BELOW_MIN", "DIAGNOSTIC_LIMIT",
        "BROKER_BLOCKED", "CONTEXT_MISSING", "ELIGIBLE",
    },
    "symbol": {"BTCUSDT"},
    "prediction_source": {"Binance", "Coinbase"},
    "actual_pair": {"BTCUSDT", "BTC-USD"},
    "source_state": {"PRIMARY", "DEGRADED", "UNAVAILABLE"},
    "reset_reason": {"NONE", "FIRST_SAMPLE", "SOURCE_SWITCH", "LONG_GAP", "CLOCK_INVALID"},
    "settlement_provider": {"CHAINLINK", "UNKNOWN"},
    "source_time_status": {"MISSING", "PRESENT"},
    "freshness_basis": {"RECEIPT_BOUND", "SOURCE_AND_RECEIPT"},
}


def validate_event(raw: bytes, run_id: str, mode: str = MODE_UNSIGNED_PAPER) -> dict[str, Any]:
    """Rebuild an event through recursive type-specific allowlists before writing.

    The wire version decides the path: the unsigned phase1 run speaks v2 and
    the signed run keeps v1. An unsigned run never accepts a v1 event, and a
    signed run never accepts a v2 one: the two protocols carry different
    facts.
    """
    event = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object,
                       parse_constant=_reject_constant)
    if not isinstance(event, dict) or event.get("run_id") != run_id:
        raise ValueError("RUN_ID")
    version = event.get("schema_version")
    if mode == MODE_UNSIGNED_PAPER:
        if version != SCHEMA_VERSION_V2:
            raise ValueError("SCHEMA_VERSION")
        return validate_event_v2(event, run_id)
    if version is not None:
        raise ValueError("SCHEMA_VERSION")
    return validate_event_v1(event, run_id, mode)


def read_legacy_event(raw: bytes, run_id: str) -> dict[str, Any]:
    """Parse historical v1 bytes without writes, clock invention, or run admission."""
    event = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object,
                       parse_constant=_reject_constant)
    if (not isinstance(event, dict) or event.get("run_id") != run_id
            or "schema_version" in event or "event_seq" in event):
        raise ValueError("LEGACY_VERSION_OR_RUN")
    return {"event": validate_event_v1(event, run_id, MODE_UNSIGNED_PAPER),
            "diagnostics_status": "DIAGNOSTICS_UNAVAILABLE"}


def validate_event_v1(event: dict[str, Any], run_id: str, mode: str) -> dict[str, Any]:
    """Rebuild a v1 event: the old shape, unchanged."""
    account = {name: "number" for name in (
        "cash", "available", "reserved", "cost", "realized", "unrealized", "nav", "net_pnl", "fees_in_cash")}
    account.update(loss_latched="bool", holdings="number")
    holding = {"token": "id", "shares": "number", "cost": "number", "mark": "number",
               "mark_valid": "bool", "mark_time": "time"}
    market = {"market_id": "id", "yes_token": "id", "no_token": "id", "status": "status",
              "holdings": [holding], "payout_yes": "number"}
    source = {name: "number" for name in ("gross_shares", "net_shares", "shares", "price", "cost",
              "fee", "fee_shares", "fee_rate_bps", "net_cash", "basis", "pnl", "payout", "income")}
    source.update(side="side", fee_direction="direction", provider="provider", reason="reason",
                  cash_unit="cash_unit", share_unit="share_unit",
                  price_source={"name": "spot", "observed_at": "time"})
    enums = {"reason": {"Halted", "BelowMinimum", "OrderCap", "MarketCap", "TotalCap", "NothingHeld",
                        "WouldCross", "ThinBook", "Deadline", "LossLatch", "DataIncomplete", "InsufficientCash",
                        "MarketRulesUnknown", "SettlementConflict", "ControlledRestUnsupported"},
             "side": {"buy", "sell"}, "direction": {"shares", "cash"},
             "spot": {"Binance", "Coinbase"}, "provider": {"CHAINLINK", "UNKNOWN"},
             "cash_unit": {"USDT"}, "share_unit": {"shares"},
             "accounting_reason": {"LIVE_ACCOUNTING_UNAVAILABLE"},
             "stop_reason": {"DEADLINE", "PARENT_LOST", "CONTROL_CHANNEL", "SETTLEMENT_CONFLICT"},
             "status": {"PENDING", "CONFIRMED", "CONFLICT"}}

    def rebuild(value: Any, schema: Any) -> Any:
        if value is None:
            return None
        if isinstance(schema, dict):
            if not isinstance(value, dict) or not set(value) <= set(schema):
                raise ValueError("FIELDS")
            return {name: rebuild(item, schema[name]) for name, item in value.items()}
        if isinstance(schema, list):
            if not isinstance(value, list) or len(value) > 1000:
                raise ValueError("LIST")
            return [rebuild(item, schema[0]) for item in value]
        if schema == "number" and _finite(value):
            return value
        if schema == "bool" and isinstance(value, bool):
            return value
        if schema == "id" and isinstance(value, (str, int)) and not isinstance(value, bool):
            if re.fullmatch(r"[0-9]{1,78}", str(value)):
                return str(value)
        if schema == "time" and isinstance(value, str) and len(value) <= 40:
            if re.fullmatch(r"[0-9T:.+Z-]+", value):
                timestamp = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
                if timestamp.tzinfo is not None:
                    return value
        if schema in enums and value in enums[schema]:
            return value
        raise ValueError("TYPE")

    kind = event.get("event_kind")
    if kind not in {"fill", "settlement", "loss_latch", "summary", "final", "reservation", "release", "blocked"}:
        raise ValueError("KIND")
    schema = {"market_id": "id", "token": "id", "accounting": account,
              "source": {"markets": [market], "finalized": "bool", "stop_reason": "stop_reason",
                         "cleanup_timed_out": "bool", "accounting_reason": "accounting_reason"}
                        if kind == "final" else source,
              "event_time": "time"}
    rebuilt = rebuild({name: value for name, value in event.items()
                       if name not in {"run_id", "event_kind"}}, schema)
    if set(rebuilt) != set(schema) or not isinstance(rebuilt["accounting"], dict):
        raise ValueError("INCOMPLETE")
    if not (set(account) - {"holdings"}) <= set(rebuilt["accounting"]):
        raise ValueError("ACCOUNTING")
    accounting = rebuilt["accounting"]
    details = rebuilt["source"]
    if rebuilt["event_time"] is None:
        raise ValueError("TIME")
    if mode == MODE_SIGNED_FUTURE_LIVE:
        if (kind != "final" or any(value is not None for value in accounting.values())
                or not isinstance(details, dict) or details.get("markets", []) is not None
                or details.get("finalized") is not False
                or details.get("accounting_reason") != "LIVE_ACCOUNTING_UNAVAILABLE"):
            raise ValueError("SIGNED_ACCOUNTING")
    else:
        for name in ("cash", "available", "reserved", "cost", "realized", "fees_in_cash"):
            if accounting[name] is None:
                raise ValueError("ACCOUNTING_NULL")
        if type(accounting["loss_latched"]) is not bool:
            raise ValueError("LOSS_LATCH")
        def equal(actual, expected):
            if actual is None or abs(actual - expected) > 1e-7:
                raise ValueError("ACCOUNTING_IDENTITY")
        equal(accounting["cash"], 100 + accounting["realized"] - accounting["cost"])
        equal(accounting["available"], accounting["cash"] - accounting["reserved"])
        marked = [accounting[name] for name in ("unrealized", "nav", "net_pnl")]
        if any(value is None for value in marked):
            if not all(value is None for value in marked):
                raise ValueError("PARTIAL_MARK")
        else:
            equal(accounting["net_pnl"], accounting["realized"] + accounting["unrealized"])
            equal(accounting["nav"], 100 + accounting["net_pnl"])
        if kind in {"fill", "settlement", "reservation", "release", "blocked"}:
            if rebuilt["market_id"] is None or rebuilt["token"] is None:
                raise ValueError("IDENTITY")
        required = {"blocked": {"reason"}, "settlement": {"shares", "payout", "income", "basis", "provider",
                                                               "cash_unit", "share_unit"}}
        if kind == "fill":
            if not isinstance(details, dict):
                raise ValueError("FILL")
            required[kind] = ({"gross_shares", "net_shares", "price", "cost", "fee", "fee_shares",
                               "fee_rate_bps", "side", "fee_direction", "cash_unit", "share_unit"}
                              if details.get("side") == "buy" else
                              {"shares", "price", "net_cash", "basis", "pnl", "fee", "fee_rate_bps", "side",
                               "fee_direction", "cash_unit", "share_unit"})
        if kind in required and (not isinstance(details, dict)
                                  or not required[kind] <= set(details)
                                  or any(details[name] is None for name in required[kind])):
            raise ValueError("SOURCE_INCOMPLETE")
        if kind in {"summary", "loss_latch", "reservation", "release"} and details is not None:
            raise ValueError("SOURCE_KIND")
        if details is None:
            details = {}
        if kind in required:
            allowed = required[kind] | ({"price_source"} if kind == "fill" and details["side"] == "buy" else set())
            if set(details) - allowed:
                raise ValueError("SOURCE_KIND")
            price_source = details.get("price_source")
            if price_source is not None and (set(price_source) != {"name", "observed_at"}
                                             or any(value is None for value in price_source.values())):
                raise ValueError("PRICE_SOURCE")
        if kind == "fill":
            buy = details["side"] == "buy"
            quantity = details["gross_shares" if buy else "shares"]
            price, rate = details["price"], details["fee_rate_bps"]
            if (quantity <= 0 or not 0 < price < 1 or type(rate) is not int
                    or not 0 <= rate <= 10000 or details["fee"] < 0
                    or details["fee_direction"] != ("shares" if buy else "cash")):
                raise ValueError("FILL_RANGE_OR_DIRECTION")
            gross = quantity * price
            equal(details["fee"], rate / 10000 * min(price, 1 - price) * quantity)
            if buy:
                if details["net_shares"] < 0 or details["fee_shares"] < 0:
                    raise ValueError("FILL_RANGE")
                equal(details["cost"], gross)
                equal(details["fee_shares"] * price, details["fee"])
                equal(details["net_shares"], quantity - details["fee_shares"])
            else:
                if details["basis"] < 0 or details["net_cash"] < 0:
                    raise ValueError("FILL_RANGE")
                equal(details["net_cash"], gross - details["fee"])
                equal(details["pnl"], details["net_cash"] - details["basis"])
        if kind == "settlement":
            equal(details["income"], details["shares"] * details["payout"])
    if kind == "final":
        if (not isinstance(details, dict) or not {"markets", "finalized", "stop_reason", "cleanup_timed_out"} <= set(details)
                or type(details["finalized"]) is not bool or type(details["cleanup_timed_out"]) is not bool):
            raise ValueError("FINAL")
        if mode == MODE_UNSIGNED_PAPER:
            if not isinstance(details["markets"], list):
                raise ValueError("MARKETS")
            seen = set()
            for item in details["markets"]:
                if (not isinstance(item, dict) or set(item) != set(market)
                        or item["market_id"] in seen or item["market_id"] is None
                        or item["yes_token"] is None or item["no_token"] is None
                        or item["yes_token"] == item["no_token"] or item["status"] is None
                        or not isinstance(item["holdings"], list)):
                    raise ValueError("MARKET")
                seen.add(item["market_id"])
                for position in item["holdings"]:
                    if (set(position) != set(holding) or any(position[name] is None
                            for name in ("token", "shares", "cost", "mark_valid"))):
                        raise ValueError("HOLDING")
                if item["status"] == "CONFIRMED" and (item["payout_yes"] not in (0, .5, 1) or item["holdings"]):
                    raise ValueError("SETTLEMENT")
            if details["finalized"] != all(item["status"] == "CONFIRMED" for item in details["markets"]):
                raise ValueError("FINALIZED")
    return {"run_id": run_id, "event_kind": kind, **rebuilt}


def validate_event_v2(event: dict[str, Any], run_id: str) -> dict[str, Any]:
    """Rebuild a v2 event: the old shape plus the phase1 envelope.

    The v2 envelope is exactly two fields — the wire version and the event
    sequence — and one object: the decision snapshot, which may only travel
    in a summary.
    """
    kind = event.get("event_kind")
    if kind not in {"fill", "settlement", "loss_latch", "summary", "final",
                    "reservation", "release", "blocked"}:
        raise ValueError("KIND")
    seq = event.get("event_seq")
    if type(seq) is not int or seq < 1:
        raise ValueError("EVENT_SEQ")
    account = {name: "number" for name in (
        "cash", "available", "reserved", "cost", "realized", "unrealized", "nav", "net_pnl", "fees_in_cash")}
    account.update(loss_latched="bool", holdings="number")
    holding = {"token": "id", "shares": "number", "cost": "number", "mark": "number",
               "mark_valid": "bool", "mark_time": "time"}
    market = {"market_id": "id", "yes_token": "id", "no_token": "id", "status": "status",
              "holdings": [holding], "payout_yes": "number"}
    source = {name: "number" for name in ("gross_shares", "net_shares", "shares", "price", "cost",
              "fee", "fee_shares", "fee_rate_bps", "net_cash", "basis", "pnl", "payout", "income")}
    source.update(side="side", fee_direction="direction", provider="provider", reason="reason",
                  cash_unit="cash_unit", share_unit="share_unit",
                  decision_id="id", candidate_id="id", diagnostic_reason="diagnostic_reason")
    source["price_source"] = {"name": "spot", "clock": CLOCK_SCHEMA}
    enums = {"reason": {"Halted", "BelowMinimum", "OrderCap", "MarketCap", "TotalCap", "NothingHeld",
                        "WouldCross", "ThinBook", "Deadline", "LossLatch", "DataIncomplete", "InsufficientCash",
                        "MarketRulesUnknown", "SettlementConflict", "ControlledRestUnsupported"},
             "side": {"buy", "sell"}, "direction": {"shares", "cash"},
             "spot": {"Binance", "Coinbase"}, "provider": {"CHAINLINK", "UNKNOWN"},
             "cash_unit": {"USDT"}, "share_unit": {"shares"},
             "accounting_reason": {"LIVE_ACCOUNTING_UNAVAILABLE"},
             "stop_reason": {"DEADLINE", "PARENT_LOST", "CONTROL_CHANNEL", "SETTLEMENT_CONFLICT"},
             "status": {"PENDING", "CONFIRMED", "CONFLICT"}}
    # The diagnostic snapshot's own enums live in the same table, so one
    # rebuild serves the event and the snapshot inside it.
    enums.update(V2_ENUMS)

    def rebuild(value: Any, schema: Any, table: dict[str, Any]) -> Any:
        if value is None:
            return None
        if isinstance(schema, dict):
            if not isinstance(value, dict) or not set(value) <= set(schema):
                raise ValueError("FIELDS")
            nonnull = None
            if schema is DECISION_SCHEMA:
                nonnull = {"decision_id", "phase", "strategy_contract", "reason", "symbol",
                           "source_state", "reset_reason", "settlement_provider", "decision_at",
                           "decision_elapsed_ms", "checked_at", "checked_elapsed_ms", "samples"}
            elif schema is CLOCK_SCHEMA:
                nonnull = set(schema) - {"source_at"}
            elif schema is SIDE_SCHEMA:
                nonnull = {"token"}
            elif schema is LEVEL_SCHEMA:
                nonnull = set(schema)
            if nonnull is not None:
                if (set(value) != set(schema)
                        or any(value[name] is None for name in nonnull)):
                    raise ValueError("REQUIRED_FIELDS")
                for name, field_type in schema.items():
                    if field_type == "time" and value[name] is not None and not re.fullmatch(
                            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,9}(?:Z|[+-]\d{2}:\d{2})",
                            value[name] if isinstance(value[name], str) else ""):
                        raise ValueError("MILLISECOND_TIME")
            return {name: rebuild(item, schema[name], table) for name, item in value.items()}
        if isinstance(schema, list):
            if not isinstance(value, list) or len(value) > 1000:
                raise ValueError("LIST")
            return [rebuild(item, schema[0], table) for item in value]
        if schema == "number" and _finite(value):
            return value
        if schema == "bool" and isinstance(value, bool):
            return value
        if schema == "id" and isinstance(value, (str, int)) and not isinstance(value, bool):
            if re.fullmatch(r"[0-9]{1,78}", str(value)):
                return str(value)
        if schema == "time" and isinstance(value, str) and len(value) <= 40:
            if re.fullmatch(r"[0-9T:.+Z-]+", value):
                timestamp = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
                if timestamp.tzinfo is not None:
                    return value
        if schema == "elapsed" and type(value) is int and value >= 0:
            return value
        if schema == "epoch" and type(value) is int and value >= 0:
            return value
        if schema == "samples" and type(value) is int and value >= 0:
            return value
        if schema == "bps" and type(value) is int and 0 <= value <= 10000:
            return value
        if schema in table and value in table[schema]:
            return value
        raise ValueError("TYPE")

    raw_source = event.get("source")
    is_diagnostic = isinstance(raw_source, dict) and "decision" in raw_source
    schema = {"market_id": "id", "token": "id", "accounting": account,
              "source": {"markets": [market], "finalized": "bool", "stop_reason": "stop_reason",
                         "cleanup_timed_out": "bool", "accounting_reason": "accounting_reason"}
                        if kind == "final" else
                        ({"decision": DECISION_SCHEMA} if is_diagnostic else source),
              "event_time": "time"}
    payload = {name: value for name, value in event.items()
               if name not in {"run_id", "event_kind", "schema_version", "event_seq"}}
    rebuilt = rebuild(payload, schema, enums)
    if set(rebuilt) != set(schema) or not isinstance(rebuilt["accounting"], dict):
        raise ValueError("INCOMPLETE")
    if not (set(account) - {"holdings"}) <= set(rebuilt["accounting"]):
        raise ValueError("ACCOUNTING")
    accounting = rebuilt["accounting"]
    details = rebuilt["source"]
    if rebuilt["event_time"] is None:
        raise ValueError("TIME")
    for name in ("cash", "available", "reserved", "cost", "realized", "fees_in_cash"):
        if accounting[name] is None:
            raise ValueError("ACCOUNTING_NULL")
    if type(accounting["loss_latched"]) is not bool:
        raise ValueError("LOSS_LATCH")

    def equal(actual, expected):
        if actual is None or abs(actual - expected) > 1e-7:
            raise ValueError("ACCOUNTING_IDENTITY")
    equal(accounting["cash"], 100 + accounting["realized"] - accounting["cost"])
    equal(accounting["available"], accounting["cash"] - accounting["reserved"])
    marked = [accounting[name] for name in ("unrealized", "nav", "net_pnl")]
    if any(value is None for value in marked):
        if not all(value is None for value in marked):
            raise ValueError("PARTIAL_MARK")
    else:
        equal(accounting["net_pnl"], accounting["realized"] + accounting["unrealized"])
        equal(accounting["nav"], 100 + accounting["net_pnl"])

    # The diagnostic object may only travel in a summary, and only as the
    # whole of the source: it is an evaluation, not a trade.
    if is_diagnostic:
        if kind != "summary":
            raise ValueError("DECISION_KIND")
        if set(raw_source) != {"decision"}:
            raise ValueError("DECISION_SOURCE")
        if rebuilt["market_id"] is None or rebuilt["token"] is not None:
            raise ValueError("DECISION_IDENTITY")
        decision = rebuilt["source"]["decision"]
        if set(decision) != set(DECISION_SCHEMA):
            raise ValueError("DECISION_INCOMPLETE")
        if decision["strategy_contract"] != STRATEGY_CONTRACT:
            raise ValueError("STRATEGY_CONTRACT")
        if decision["decision_id"] is None:
            raise ValueError("DECISION_ID")
        if decision["candidate_id"] is not None and decision["candidate_id"] != decision["decision_id"]:
            raise ValueError("CANDIDATE_ID")
        if decision["phase"] == "EVALUATE":
            if (decision["reason"] == "ELIGIBLE") != (decision["candidate_id"] is not None):
                raise ValueError("CANDIDATE_REASON")
        elif decision["candidate_id"] is None:
            raise ValueError("CANDIDATE_ID")
        _check_decision_formulas(decision)
    elif kind == "summary" and isinstance(raw_source, dict) and raw_source:
        raise ValueError("DECISION_SOURCE")

    if kind in {"fill", "settlement", "reservation", "release", "blocked"}:
        if rebuilt["market_id"] is None or rebuilt["token"] is None:
            raise ValueError("IDENTITY")
    required = {"blocked": {"reason"}, "settlement": {"shares", "payout", "income", "basis", "provider",
                                                       "cash_unit", "share_unit", "candidate_id"}}
    if kind == "fill":
        if not isinstance(details, dict):
            raise ValueError("FILL")
        required[kind] = ({"gross_shares", "net_shares", "price", "cost", "fee", "fee_shares",
                           "fee_rate_bps", "side", "fee_direction", "cash_unit", "share_unit"}
                          if details.get("side") == "buy" else
                          {"shares", "price", "net_cash", "basis", "pnl", "fee", "fee_rate_bps", "side",
                           "fee_direction", "cash_unit", "share_unit"})
    if kind in required and (not isinstance(details, dict)
                              or not required[kind] <= set(details)
                              or any(details[name] is None for name in required[kind])):
        raise ValueError("SOURCE_INCOMPLETE")
    if kind == "loss_latch" and details is not None:
        raise ValueError("SOURCE_KIND")
    if kind == "summary" and not is_diagnostic and details is not None:
        raise ValueError("SOURCE_KIND")
    if kind in {"reservation", "release"}:
        # The reservation and its release carry the relation and nothing
        # else: they are the two halves of one candidate's attempt.
        if not isinstance(details, dict) or set(details) != {"decision_id", "candidate_id",
                                                             "diagnostic_reason"}:
            raise ValueError("SOURCE_KIND")
    if details is None:
        details = {}
    if kind in required:
        allowed = required[kind] | ({"price_source"} if kind == "fill" and details["side"] == "buy" else set())
        if kind in {"blocked"} or (kind == "fill" and details["side"] == "buy"):
            allowed |= {"decision_id", "candidate_id", "diagnostic_reason"}
        if set(details) - allowed:
            raise ValueError("SOURCE_KIND")
        price_source = details.get("price_source")
        if price_source is not None:
            if set(price_source) != {"name", "clock"}:
                raise ValueError("PRICE_SOURCE")
            rebuilt_clock = rebuild(price_source["clock"], CLOCK_SCHEMA, V2_ENUMS)
            if set(rebuilt_clock) != set(CLOCK_SCHEMA):
                raise ValueError("PRICE_SOURCE")
    # The candidate's execution events carry the relation, and the relation
    # is mandatory: an event without it cannot be followed to its decision.
    if kind in {"reservation", "release", "blocked"} or (kind == "fill" and details.get("side") == "buy"):
        for name in ("decision_id", "candidate_id", "diagnostic_reason"):
            if details.get(name) is None:
                raise ValueError("LINKAGE")
        if details["candidate_id"] != details["decision_id"]:
            raise ValueError("LINKAGE")
        if details["diagnostic_reason"] not in V2_ENUMS["diagnostic_reason"]:
            raise ValueError("LINKAGE")
    if kind == "fill":
        buy = details["side"] == "buy"
        quantity = details["gross_shares" if buy else "shares"]
        price, rate = details["price"], details["fee_rate_bps"]
        if (quantity <= 0 or not 0 < price < 1 or type(rate) is not int
                or not 0 <= rate <= 10000 or details["fee"] < 0
                or details["fee_direction"] != ("shares" if buy else "cash")):
            raise ValueError("FILL_RANGE_OR_DIRECTION")
        gross = quantity * price
        equal(details["fee"], rate / 10000 * min(price, 1 - price) * quantity)
        if buy:
            if details["net_shares"] < 0 or details["fee_shares"] < 0:
                raise ValueError("FILL_RANGE")
            equal(details["cost"], gross)
            equal(details["fee_shares"] * price, details["fee"])
            equal(details["net_shares"], quantity - details["fee_shares"])
        else:
            if details["basis"] < 0 or details["net_cash"] < 0:
                raise ValueError("FILL_RANGE")
            equal(details["net_cash"], gross - details["fee"])
            equal(details["pnl"], details["net_cash"] - details["basis"])
    if kind == "settlement":
        equal(details["income"], details["shares"] * details["payout"])
    if kind == "final":
        if (not isinstance(details, dict) or not {"markets", "finalized", "stop_reason", "cleanup_timed_out"} <= set(details)
                or type(details["finalized"]) is not bool or type(details["cleanup_timed_out"]) is not bool):
            raise ValueError("FINAL")
        if not isinstance(details["markets"], list):
            raise ValueError("MARKETS")
        seen = set()
        for item in details["markets"]:
            if (not isinstance(item, dict) or set(item) != set(market)
                    or item["market_id"] in seen or item["market_id"] is None
                    or item["yes_token"] is None or item["no_token"] is None
                    or item["yes_token"] == item["no_token"] or item["status"] is None
                    or not isinstance(item["holdings"], list)):
                raise ValueError("MARKET")
            seen.add(item["market_id"])
            for position in item["holdings"]:
                if (set(position) != set(holding) or any(position[name] is None
                        for name in ("token", "shares", "cost", "mark_valid"))):
                    raise ValueError("HOLDING")
            if item["status"] == "CONFIRMED" and (item["payout_yes"] not in (0, .5, 1) or item["holdings"]):
                raise ValueError("SETTLEMENT")
        if details["finalized"] != all(item["status"] == "CONFIRMED" for item in details["markets"]):
            raise ValueError("FINALIZED")
    return {"run_id": run_id, "event_kind": kind, "schema_version": SCHEMA_VERSION_V2,
            "event_seq": seq, **rebuilt}


def _check_decision_formulas(decision: dict[str, Any]) -> None:
    """Recompute the diagnostic's own arithmetic.

    The check is a recomputation of the protocol's formulas, not a second
    model: a number that does not follow from the others is a broken event.
    """
    eligible = decision["reason"] == "ELIGIBLE"
    if eligible:
        needed = ("p_up", "spot", "strike", "sigma", "source_epoch", "selected_token",
                  "spot_clock", "book_clock", "secs_left", "starts_at", "ends_at",
                  "identity_available_at", "strike_available_at", "fee_rate_bps", "up", "down")
        if (any(decision[name] is None for name in needed) or decision["samples"] < 20
                or min(decision[name] for name in ("spot", "strike", "sigma", "source_epoch")) <= 0
                or not 0 <= decision["p_up"] <= 1 or decision["secs_left"] < 30
                or (decision["prediction_source"], decision["actual_pair"], decision["source_state"])
                   not in {("Binance", "BTCUSDT", "PRIMARY"), ("Coinbase", "BTC-USD", "DEGRADED")}):
            raise ValueError("ELIGIBLE_INPUTS")
        if (decision["up"]["token"] == decision["down"]["token"]
                or decision["selected_token"] not in {decision["up"]["token"], decision["down"]["token"]}):
            raise ValueError("ELIGIBLE_TOKEN")
        def timestamp(value: str) -> datetime.datetime:
            return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        initial, checked = timestamp(decision["decision_at"]), timestamp(decision["checked_at"])
        elapsed = decision["checked_elapsed_ms"] - decision["decision_elapsed_ms"]
        if checked < initial or elapsed < 0:
            raise ValueError("ELIGIBLE_CLOCK")
        effective = max(checked, initial + datetime.timedelta(milliseconds=elapsed))
        start, end = timestamp(decision["starts_at"]), timestamp(decision["ends_at"])
        if (end - start != datetime.timedelta(seconds=300) or start > initial
                or timestamp(decision["identity_available_at"]) > initial
                or timestamp(decision["strike_available_at"]) > initial
                or end - effective < datetime.timedelta(seconds=30)):
            raise ValueError("ELIGIBLE_ROUND")
        for name in ("spot_clock", "book_clock"):
            clock = decision[name]
            request, received = timestamp(clock["request_at"]), timestamp(clock["received_at"])
            request_elapsed, received_elapsed = clock["request_elapsed_ms"], clock["received_elapsed_ms"]
            missing_source = clock["source_at"] is None
            if ((clock["source_time_status"] == "MISSING") != missing_source
                    or (clock["freshness_basis"] == "RECEIPT_BOUND") != missing_source
                    or not request <= received <= checked
                    or not request_elapsed <= received_elapsed <= decision["checked_elapsed_ms"]
                    or max((received - request).total_seconds(),
                           (received_elapsed - request_elapsed) / 1000) > 5
                    or max((checked - request).total_seconds(),
                           (decision["checked_elapsed_ms"] - request_elapsed) / 1000) > 10):
                raise ValueError("ELIGIBLE_ACQUISITION")
            if not missing_source:
                source = timestamp(clock["source_at"])
                if source > received or (checked - source).total_seconds() > 10:
                    raise ValueError("ELIGIBLE_SOURCE_TIME")
    for side_name in ("up", "down"):
        side = decision[side_name]
        if side is None:
            continue
        if eligible:
            numbers = ("fair", "limit_price", "gross_shares", "fee_per_gross_share", "net_shares",
                       "cost", "ev", "edge", "roi", "breakeven_fair", "qualifies")
            if (any(side[name] is None for name in numbers)
                    or side["fair"] != (decision["p_up"] if side_name == "up" else 1 - decision["p_up"])):
                raise ValueError("ELIGIBLE_SIDE")
            if side["token"] == decision["selected_token"] and (
                    side["edge"] < .03 or side["net_shares"] <= 0):
                raise ValueError("ELIGIBLE_EDGE")
        price = side["limit_price"]
        if price is None or not 0 < price < 1:
            continue
        bps = decision["fee_rate_bps"]
        if bps is None:
            raise ValueError("FEE_RATE")
        fee_per_gross = bps / 10000 * min(price, 1 - price)
        if side["fee_per_gross_share"] is not None and abs(side["fee_per_gross_share"] - fee_per_gross) > 1e-7:
            raise ValueError("FEE_FORMULA")
        shares = side["gross_shares"]
        if shares is None:
            continue
        net = side["net_shares"]
        if net is not None:
            expected = shares - fee_per_gross / price * shares
            if abs(net - expected) > 1e-7:
                raise ValueError("NET_FORMULA")
        fair = side["fair"]
        if fair is None and any(side[name] is not None for name in
                                ("ev", "edge", "roi", "qualifies", "execution_ev", "execution_edge")):
            raise ValueError("UNAVAILABLE_FAIR")
        cost = side["cost"]
        ev = side["ev"]
        edge = side["edge"]
        if cost is not None and abs(cost - shares * price) > 1e-7:
            raise ValueError("COST_FORMULA")
        if None not in (fair, cost, ev, net) and abs(ev - (net * fair - cost)) > 1e-7:
            raise ValueError("EV_FORMULA")
        if None not in (ev, shares) and edge is not None and abs(edge - ev / shares) > 1e-7:
            raise ValueError("EDGE_FORMULA")
        if edge is not None and side["qualifies"] is not None:
            if side["qualifies"] != (edge >= 0.03):
                raise ValueError("QUALIFIES")
        if shares <= 0:
            raise ValueError("GROSS_SHARES")

        def check(name: str, expected: float) -> None:
            if side[name] is None or abs(side[name] - expected) > 1e-7:
                raise ValueError("SIDE_FORMULA")

        if ev is not None and cost is not None and cost > 0:
            check("roi", ev / cost)
        if net is not None and net > 0:
            check("breakeven_fair", price / (net / shares))
        levels = side["depth_levels"]
        vwap = side["walk_vwap"]
        if levels is not None:
            remaining, total, prior_price = shares, 0.0, 0.0
            for level in levels:
                if (remaining <= 1e-9 or not 0 < level["price"] < 1 or level["size"] <= 0
                        or level["price"] < prior_price or level["price"] > price + 1e-9):
                    raise ValueError("DEPTH_PREFIX")
                used = min(remaining, level["size"])
                total += used * level["price"]
                remaining -= used
                prior_price = level["price"]
            complete = remaining <= 1e-9
            if side["depth_complete"] is not complete:
                raise ValueError("DEPTH_COMPLETE")
            if complete:
                check("walk_vwap", total / shares)
            elif any(side[name] is not None for name in
                     ("walk_vwap", "execution_net_shares", "execution_cost",
                      "execution_ev", "execution_edge")):
                raise ValueError("INCOMPLETE_WALK")
        if vwap is not None:
            if not 0 < vwap < 1 or vwap > price + 1e-9:
                raise ValueError("WALK_LIMIT")
            executed_net = shares * (1 - bps / 10000 * min(vwap, 1 - vwap) / vwap)
            check("execution_net_shares", executed_net)
            check("execution_cost", shares * vwap)
            if fair is not None:
                executed_ev = executed_net * fair - shares * vwap
                check("execution_ev", executed_ev)
                check("execution_edge", executed_ev / shares)


# --- The fragment that stopped the run ---------------------------------------
# A stopped run's evidence says it stopped; the fragment it stopped on is the
# one piece of child output that never reaches that evidence. It is kept, but
# only redacted and only to a bound: the launcher never writes raw child bytes.
STOP_OUTPUT_SCHEMA = "btc5m-stop-output/1"
STOP_OUTPUT_LIMIT = 2048
REDACTED = "<REDACTED>"
# The names this project already refuses in public input, plus the usual token
# spellings: a value that follows one of them is never kept.
_SECRET_NAMES = tuple(sorted({name.lower() for name in FORBIDDEN_ENV} | {
    "api_key", "api_passphrase", "api_secret", "authorization", "credentials", "jwt",
    "mnemonic", "password", "predict_api_key", "predict_jwt", "private_key", "seed",
    "signature", "token"}))
_SECRET_PAIR = re.compile(
    r"(?i)\b(" + "|".join(re.escape(name) for name in _SECRET_NAMES) + r")\b"
    r"(\"?\s*[:=]\s*)(?:bearer\s+)?(\"[^\"]*\"|'[^']*'|[^\s,;}\]\"']+)")
_SECRET_URI = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*://[^\s\"'<>]+")
_SECRET_BEARER = re.compile(r"(?i)\bbearer\b\s+\S+")
# A bare 64-hex token is a private key; a 0x-prefixed hash is public and stays.
_SECRET_HEX = re.compile(r"(?<![0-9A-Za-z])[0-9a-fA-F]{64}(?![0-9A-Za-z])")


def redact_output(text: str) -> str:
    """Replace every secret-shaped token before a fragment is kept."""
    text = _SECRET_URI.sub(REDACTED, text)
    text = _SECRET_PAIR.sub(lambda match: match.group(1) + match.group(2) + REDACTED, text)
    text = _SECRET_BEARER.sub("Bearer " + REDACTED, text)
    return _SECRET_HEX.sub(REDACTED, text)


def stop_output_record(fragment: dict[str, Any], facts: dict[str, Any],
                       run_id: str) -> dict[str, Any]:
    """Describe one stopped fragment: its stream, its size and what it said."""
    text = redact_output(fragment["raw"].decode("utf-8", "backslashreplace"))
    return {"schema_version": STOP_OUTPUT_SCHEMA, "run_id": run_id,
            "stream": fragment["stream"], "bytes": len(fragment["raw"]),
            "newline_terminated": fragment["newline_terminated"],
            "truncated": len(text) > STOP_OUTPUT_LIMIT, "text": text[:STOP_OUTPUT_LIMIT],
            "occurred_at": fragment["occurred_at"], "child_returncode": facts["child_returncode"]}


def write_stop_output(store: Any, fragment: dict[str, Any], facts: dict[str, Any]) -> None:
    """Create this run's stop-output record once, before its terminal record."""
    import btc5m_live_events as live
    record = stop_output_record(fragment, facts, store.started["run_id"])
    live._create_file(store.directory / "stop-output.json", live.canonical(record) + b"\n")


# --- The child's non-event output --------------------------------------------
# A live child's stderr and the non-event lines of its stdout are evidence for
# a later diagnosis, not a reason to stop. They are written to this run's own
# bounded log, redacted, so that a run that ends for another cause can still
# be explained afterwards.
RUN_LOG_NAME = "run-log.txt"
# The whole log of one run stays inside this bound, like the stopped fragment.
RUN_LOG_LIMIT = 64 * 1024


def append_run_log(directory: Path, stream: str, raw: bytes, budget: list[int]) -> None:
    """Append one redacted, bounded child line to this run's own log.

    A failed write is swallowed: the log is evidence, never a cause, so it may
    not change the run's own stop.
    """
    try:
        text = redact_output(raw.decode("utf-8", "backslashreplace"))
        line = f"[{stream}] {text[:STOP_OUTPUT_LIMIT]}\n"
        size = len(line.encode("utf-8"))
        if budget[0] + size > RUN_LOG_LIMIT:
            return
        with open(directory / RUN_LOG_NAME, "a", encoding="utf-8") as handle:
            handle.write(line)
        budget[0] += size
    except (OSError, ValueError):
        return


def finish_live_evidence(store: Any, facts: dict[str, Any], final: dict[str, Any] | None) -> None:
    """Record native exits and the producer's independent completeness flags."""
    import btc5m_live_events as live
    native = {key: facts[key] for key in
              ("launcher_exit", "child_returncode", "child_signal", "forced_cleanup", "stop_reason")}
    payload = final["payload"] if final else {}
    if final:
        # The strict wire's stop reason is the producer's final reason; native
        # return code/signal and forced cleanup remain independent facts.
        native["stop_reason"] = payload["stop_reason"]
    if native["stop_reason"] is None:
        native["stop_reason"] = "PROCESS_FAILED"
    if native["stop_reason"] not in live.REASONS:
        native["stop_reason"] = "PROCESS_FAILED"
    flags = {key: payload.get(key, key == "takeover_required") for key in
             ("execution_complete", "cash_complete", "audit_complete", "takeover_required")}
    reason = None
    if not final:
        reason = "AUDIT_UNAVAILABLE"
    elif (native["launcher_exit"] != 0 or native["child_returncode"] != 0
          or native["forced_cleanup"] or native["child_signal"] is not None):
        reason = "PROCESS_FAILED"
    elif not all(flags[key] for key in ("execution_complete", "cash_complete", "audit_complete")):
        reason = "FACT_INCOMPLETE"
    ended = {"schema_version": live.SCHEMA_VERSION, "run_id": store.started["run_id"],
             "exit_facts": native, "final_event_seq": final["event_seq"] if final else None,
             "final_accounting": final["accounting"] if final else None,
             "incomplete_reason": reason,
             # The producer's own failure code, when it stopped with one.
             "incomplete_detail": payload.get("stop_detail"), **flags}
    if store.failed and not facts.get("output_invalid_origin"):
        # An ambiguous audit write is never repaired into a healthy ending.
        raise live.LiveInvalid("AUDIT_UNAVAILABLE")
    if store.failed:
        # Only a refused output line failed this store: that line was never
        # appended, so the tail is intact and the same record is written
        # through the store's own checks; its guard is left failing.
        live.check_ended(ended, store.started, store.relations)
        live._create_file(store.directory / "ended.json", live.canonical(ended) + b"\n")
        return
    store.finish(ended)


def run_live(args: argparse.Namespace, data: dict[str, Any], environment: dict[str, str],
             started_at: datetime.datetime, started_boot: float,
             evidence_root: Path | None, popen: Any) -> dict[str, Any]:
    """Thin explicit 07-007 lifecycle; the original supervisor owns the sole child."""
    import btc5m_live_events as live
    import btc5m_live_guard as guard
    store = None
    lock = None
    process = None
    facts = {"launcher_exit": 1, "child_returncode": None, "child_signal": None,
             "forced_cleanup": False, "stop_reason": "PROCESS_FAILED"}
    final = None
    try:
        deadline = live.timestamp(args.deadline_at)
        frozen = parameters(data)
        if frozen is not None:
            # The configuration owns the window: a supplied duration or a
            # different mode is a conflict, never an override.
            if args.duration_supplied or args.operations_mode != frozen["operations_mode"]:
                raise Refused("CONTROLLED_MODE", "args", "LIVE_CONTEXT")
            # The launcher can only compute the deadline from the moment it read
            # its script, so this process's clock always starts a little after
            # the frozen window did. The declared duration therefore fixes the
            # start, and the clock must agree with it to within the accepted
            # skew; any larger difference is a stale deadline, still refused.
            effective_start = deadline - datetime.timedelta(seconds=frozen["duration"])
            if abs((started_at - effective_start).total_seconds()) > EFFECTIVE_START_SKEW_SECONDS:
                raise Refused("CONTROLLED_MODE", "args", "DEADLINE")
            started_at = effective_start
        if not live.activation_window_allowed(args.operations_mode, deadline - started_at):
            raise Refused("CONTROLLED_MODE", "args", "DEADLINE")
        for value in (args.risk_window_id, args.authorization_ref):
            if RUN_ID_PATTERN.fullmatch(value) is None:
                raise Refused("CONTROLLED_MODE", "args", "LIVE_CONTEXT")
        # Each declared hash is the child's own form, checked with the consumer's
        # own hash rule: one bad declaration refuses the run, never half of it.
        for value in args.predict_reconciled_order or ():
            try:
                live._scalar(value, "hash")
            except live.LiveInvalid:
                raise Refused("CONTROLLED_MODE", "args", "BAD_ARGUMENTS") from None
        local_signer._check_environment(environment)
        identity = public_identity(environment, live=True)
        if os.path.lexists(BOT / "tools/predictfun/.keyring-no-credentials"):
            raise Refused("CONTROLLED_MODE", "paths", "CREDENTIAL_PATH_PRESENT")
        activation = live.strict_json(live._read_file(guard.ACTIVATION_PATH, MAX_LINE_BYTES))
        started_text = started_at.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        deadline_text = args.deadline_at
        limits = data["strategies"]["limits"]
        # The identity is read from the same configuration the child will read;
        # a legacy file keeps the historical values it always wrote.
        frozen_public = {"initial_cash": 98, "max_order_usd": 1, "max_market_usd": 1,
                         "max_total_usd": 10, "max_loss_usd": 20}
        if frozen is not None:
            frozen_public.update(initial_cash=data["controlled_run"]["budget_usdt"],
                                 max_order_usd=limits["max_order_usd"],
                                 max_market_usd=limits["max_market_usd"],
                                 max_total_usd=limits["max_total_usd"],
                                 max_loss_usd=limits["max_loss_usd"])
        started = {"schema_version": live.SCHEMA_VERSION, "strategy_contract": args.live_contract,
                   "operations_mode": args.operations_mode, "risk_window_id": args.risk_window_id,
                   "account_identity": activation["account_identity"], "authorization_ref": args.authorization_ref,
                   "recovery_policy": "COLD_START_REFUSE", "cash_recovery_policy": "MANUAL_PAUSE",
                   "asset_units": {"collateral_symbol": "USDT", "collateral_decimals": 18,
                                   "share_decimals": 18, "gas_symbol": "BNB", "gas_decimals": 18},
                   "run_id": args.run_id, "started_at": started_text, "deadline_at": deadline_text,
                   "candidate_identity": identity,
                   "public_config_identity": {"sha256": hashlib.sha256(live.canonical(data)).hexdigest(),
                                              **frozen_public}}
        context = {key: started[key] for key in set(live.CONTEXT_KEYS.split()) & set(started)}
        context.update(schema_version="btc5m-live-context/1", candidate_digest=live.candidate_digest(started),
                       gas_policy=activation["gas_policy"])
        live.check_metadata(started, data, context)
        guard.validate_activation(activation, context, datetime.datetime.now(datetime.timezone.utc))
        date = started_at.astimezone(datetime.timezone(datetime.timedelta(hours=8))).date().isoformat()
        root = evidence_root or BOT.parents[4] / f"data/reports/{date}-btc5m-controlled-paper/evidence"
        directory = root / args.run_id
        if directory.exists() or directory.is_symlink():
            raise Refused("DUPLICATE_RUN", "evidence", "DUPLICATE_RUN")
        lock = guard.AccountLock(started["account_identity"], args.mode).acquire()
        guard.arm_live_guard(lock, context)
        root.mkdir(parents=True, exist_ok=True)
        store = live.LiveEventStore(directory, started, data, context)
        # All actual trading-secret reads follow the persistent barrier and durable metadata.
        child_env = local_signer.build_child_env(local_signer._read_owned(), environment)
        command = child_arguments(LIVE_BINARY, directory / "public-config.json", args.run_id,
                                  started_text, deadline_text)
        command[command.index("--controlled-mode") + 1] = args.mode
        command[1:1] = ["--predict-budget-confirmation", args.predict_budget_confirmation,
                        "--predict-rpc-url", args.predict_rpc_url,
                        "--controlled-live-contract", args.live_contract,
                        "--controlled-operations-mode", args.operations_mode,
                        "--controlled-risk-window-id", args.risk_window_id,
                        "--controlled-authorization-ref", args.authorization_ref,
                        "--controlled-live-manifest", str(directory / "live-context.json")]
        # The operator's declarations travel to the child verbatim and in the
        # order they were given; with none, the command line is unchanged.
        if args.predict_reconciled_order:
            command[1:1] = [item for value in args.predict_reconciled_order
                            for item in ("--predict-reconciled-order", value)]
        seconds = (deadline - started_at).total_seconds()
        if max(boottime() - started_boot,
               (datetime.datetime.now(datetime.timezone.utc) - started_at).total_seconds()) >= seconds:
            facts["stop_reason"] = "DEADLINE"
        else:
            process = popen(command, cwd=BOT, env=child_env, shell=False, close_fds=True,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            pass_fds=())
            facts, final = supervise(process, store, started_at, started_boot, run_seconds=seconds)
    except Refused:
        raise
    except guard.GuardRefused as error:
        raise Refused("CONTROLLED_MODE", "guard", error.reason) from None
    except Failure:
        facts["stop_reason"] = "KEYRING_UNAVAILABLE"
    except (live.LiveInvalid, OSError, KeyError, TypeError):
        if store is None:
            raise Refused("CONTROLLED_MODE", "context", "RECOVERY_GUARD_UNAVAILABLE") from None
        facts["launcher_exit"] = 1
    finally:
        try:
            if store is not None:
                try:
                    # Every route that stops the run still gets its ending; a
                    # corrupt/ambiguous audit write is never repaired into one.
                    finish_live_evidence(store, facts, final)
                finally:
                    store.close()
        except (OSError, ValueError):
            facts["launcher_exit"] = 1
        finally:
            if lock is not None and (process is None or process.poll() is not None):
                lock.close()
    return facts


def run(argv: list[str] | None = None, *, environment: dict[str, str] | None = None,
        evidence_root: Path | None = None, popen: Any = subprocess.Popen) -> dict[str, Any]:
    """Validate, freeze and supervise one fixed child; never automatically restart."""
    started_at = datetime.datetime.now(datetime.timezone.utc)
    started_boot = boottime()
    args = build_parser().parse_args(argv if argv is not None else sys.argv[1:])
    environment = dict(os.environ if environment is None else environment)
    if RUN_ID_PATTERN.fullmatch(args.run_id) is None:
        raise Refused("CONTROLLED_MODE", "args", "RUN_ID")
    unsigned = args.mode == MODE_UNSIGNED_PAPER
    live_options = (args.live_contract, args.operations_mode, args.risk_window_id,
                    args.authorization_ref, args.deadline_at)
    new_live = any(value is not None for value in live_options)
    if new_live and (unsigned or not all(value is not None for value in live_options)
                     or args.duration_supplied or args.post_entry_observer):
        raise Refused("CONTROLLED_MODE", "args", "LIVE_CONTEXT")
    # Declaring an explained historical order is a live-only act: a paper
    # ledger has no such history, and the declaration needs the live run whose
    # account it explains. Silence is never reinterpreted as a declaration.
    if args.predict_reconciled_order:
        if unsigned:
            raise Refused("CONTROLLED_MODE", "args", "LIVE_OPTION_REFUSED")
        if not new_live:
            raise Refused("CONTROLLED_MODE", "args", "LIVE_CONTEXT")
    paper72 = args.duration_hours == 72
    if paper72 and not unsigned:
        raise Refused("CONTROLLED_MODE", "args", "DURATION_REFUSED")
    run_seconds = PAPER72_SECONDS if paper72 else RUN_SECONDS
    if unsigned and (args.predict_budget_confirmation is not None or args.predict_rpc_url is not None):
        raise Refused("CONTROLLED_MODE", "args", "LIVE_OPTION_REFUSED")
    # The post-entry observer is an unsigned-paper opt-in only: a signed run
    # has no position that may be read.
    if not unsigned and args.post_entry_observer:
        raise Refused("CONTROLLED_MODE", "args", "OBSERVER_REFUSED")
    if not unsigned:
        from urllib.parse import urlsplit
        expected = f"56:{local_signer.TRADING_ADDRESS.lower()}:predict-account"
        rpc = urlsplit(args.predict_rpc_url or "")
        if ((args.predict_budget_confirmation or "").lower() != expected
                or rpc.scheme not in {"http", "https"} or not rpc.hostname
                or rpc.username or rpc.password or rpc.query or rpc.fragment):
            raise Refused("CONTROLLED_MODE", "args", "LIVE_CONFIRMATION")
    data = read_public_config(Path(args.config))
    check_fixed_values(data, args.mode)
    if new_live:
        return run_live(args, data, environment, started_at, started_boot, evidence_root, popen)
    if unsigned:
        api_key = check_environment(environment)
    else:
        try:
            local_signer._check_environment(environment)
            api_key = local_signer._api_source(environment)
        except Failure:
            raise Refused("ENV_CONFLICT", "env", "ENV_CONFLICT") from None
    _config, binary = check_paths(Path(args.config), True) if paper72 else check_paths(Path(args.config))
    identity = public_identity(environment, True) if paper72 else public_identity(environment)
    deadline_at = started_at + datetime.timedelta(seconds=run_seconds)
    started_text = started_at.isoformat().replace("+00:00", "Z")
    deadline_text = deadline_at.isoformat().replace("+00:00", "Z")
    date = started_at.astimezone(datetime.timezone(datetime.timedelta(hours=8))).date().isoformat()
    root = evidence_root or (BOT.parents[4] / f"data/reports/{date}-btc5m-controlled-paper/evidence")
    try:
        evidence = start_evidence(root / args.run_id, args.run_id, started_text, deadline_text,
                                  args.mode, data, "", identity)
    except FileExistsError:
        raise Refused("DUPLICATE_RUN", "evidence", "DUPLICATE_RUN") from None
    child_env = child_environment(environment, api_key)
    facts = {"launcher_exit": 1, "child_returncode": None, "stop_reason": "STARTUP_FAILED",
             "forced_cleanup": False}
    final = None
    observer = None
    parent_socket = None
    child_socket = None
    observer_module = None
    try:
        if not unsigned:
            child_env = local_signer.build_child_env(local_signer._read_owned(), environment)
        if args.post_entry_observer:
            import btc5m_observer as observer_module
            store = observer_module.ObservationStore(args.run_id, evidence.directory)
            terminal_bytes = observer_module.PAPER72_END_BYTES if paper72 else MAX_OBSERVER_FRAME_BYTES
            receiver = observer_module.ObserverReceiver(store, observer_module.TradeIndex(), terminal_bytes)
            observer = {
                "socket": None,
                "receiver": receiver,
                "run_id": args.run_id,
                "wire_version": observer_module.WIRE_VERSION,
                "max_bytes": terminal_bytes,
            }
            try:
                store.open()
                parent_socket, child_socket = observer_module.create_socketpair()
                if paper72:
                    child_socket.setsockopt(observer_module.socket.SOL_SOCKET,
                                            observer_module.socket.SO_SNDBUF, terminal_bytes)
                    child_env[observer_module.PAPER72_ENV] = str(PAPER72_SECONDS)
                parent_socket.setblocking(False)
                child_socket.setblocking(False)
                os.set_inheritable(child_socket.fileno(), True)
                observer["socket"] = parent_socket
            except (OSError, ValueError):
                store.reason("TRANSPORT_SETUP_FAILED")
                observer_module.close_channel(parent_socket, child_socket, store)
                parent_socket = child_socket = None
        command = child_arguments(binary, evidence.directory / "public-config.json", args.run_id,
                                  started_text, deadline_text)
        command[command.index("--controlled-mode") + 1] = args.mode
        if not unsigned:
            command[1:1] = ["--predict-budget-confirmation", args.predict_budget_confirmation,
                            "--predict-rpc-url", args.predict_rpc_url]
        if max(boottime() - started_boot,
               (datetime.datetime.now(datetime.timezone.utc) - started_at).total_seconds()) >= run_seconds:
            facts["stop_reason"] = "DEADLINE"
        else:
            pass_fds = ()
            if child_socket is not None and observer_module is not None:
                # The child gets this one descriptor and nothing else: the
                # parent environment is never inherited wholesale.
                child_env.update(observer_module.child_fd_environment(child_socket.fileno()))
                pass_fds = (child_socket.fileno(),)
            process = popen(command, cwd=BOT, env=child_env, shell=False, close_fds=True,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            pass_fds=pass_fds)
            # The parent's own end of the channel is closed before the run
            # starts: only the child writes to it.
            if child_socket is not None:
                observer_module.close_channel(child_socket, None, store)
                child_socket = None
            facts, final = supervise(process, evidence, started_at, started_boot, observer,
                                     run_seconds=run_seconds)
    except (Exception, KeyboardInterrupt):
        facts["launcher_exit"] = 1
    finally:
        if observer is not None and observer_module is not None:
            observer_module.close_channel(child_socket, parent_socket, store)
            store.close()
            try:
                observer_module.write_coverage(evidence.directory, store)
            except Exception:
                store.reason("COVERAGE_WRITE_FAILED")
        ended: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION if not unsigned else SCHEMA_VERSION_V2,
            "run_id": args.run_id, "exit_facts": facts,
            "final_accounting": final["accounting"] if final else None,
            "settlement_status": final["source"] if final else None,
            "incomplete_reason": None if final else "FINAL_UNAVAILABLE",
            "limitation": (["paper", "gas NOT_INCURRED", "gas NOT_MEASURED",
                            "paper settlement is not live redeem; no queue/partial-fill/Gas simulation"]
                           if unsigned else ["signed-future-live", "gas NOT_MEASURED",
                                             "LIVE_ACCOUNTING_UNAVAILABLE"]) + ["not evidence of sustained alpha"],
        }
        if unsigned:
            ended["strategy_contract"] = STRATEGY_CONTRACT
        evidence.write("ended.json", ended)
    return facts


def main(argv: list[str] | None = None) -> int:
    """Emit the fixed public result; a refusal is a nonzero exit."""
    try:
        facts = run(argv)
    except Refused as error:
        print(json.dumps({"error": error.result}, sort_keys=True), file=sys.stderr)
        return error.exit_code
    except Exception:
        print(json.dumps({"error": {"code": "CONTROLLED_MODE", "step": "launcher",
                                    "reason": "LAUNCH_FAILED"}}), file=sys.stderr)
        return 1
    print(json.dumps(facts, sort_keys=True))
    return facts["launcher_exit"]


if __name__ == "__main__":
    raise SystemExit(main())
