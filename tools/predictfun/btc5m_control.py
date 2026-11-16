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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import local_signer
from local_signer import BOT, Failure, INHERIT_ENV

SCHEMA_VERSION = "btc5m-controlled-paper.v1"
MODE_UNSIGNED_PAPER = "unsigned-paper"
MODE_SIGNED_FUTURE_LIVE = "signed-future-live"
RUN_SECONDS = 21_600
CLEANUP_SECONDS = 30
TAIL_SECONDS = 600
MAX_LINE_BYTES = 64 * 1024
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


def build_parser() -> argparse.ArgumentParser:
    """Define the fixed public interface: a mode, a config and a run id."""
    parser = _Parser(prog="btc5m_control", allow_abbrev=False)
    parser.add_argument("--mode", choices=(MODE_UNSIGNED_PAPER, MODE_SIGNED_FUTURE_LIVE),
                        required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--predict-budget-confirmation")
    parser.add_argument("--predict-rpc-url")
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
    for name, expected in (("max_order_usd", 2.0), ("max_market_usd", 2.0),
                           ("max_total_usd", 10.0), ("max_loss_usd", 20.0)):
        value = limits.get(name)
        if not _finite(value) or float(value) != expected:
            raise Refused("PUBLIC_CONFIG", "config", "LIMITS")
    btc = strategies.get("btc_arb")
    if not isinstance(btc, dict):
        raise Refused("PUBLIC_CONFIG", "config", "BTC_ARB")
    if btc.get("symbols") != ["BTCUSDT"]:
        raise Refused("PUBLIC_CONFIG", "config", "SYMBOLS")
    if type(btc.get("round_interval_secs")) is not int or btc.get("round_interval_secs") != 300:
        raise Refused("PUBLIC_CONFIG", "config", "ROUND_INTERVAL")
    for name, expected in (("min_edge", 0.03), ("vol_floor", 0.00005), ("order_usd", 2.0)):
        value = btc.get(name)
        if not _finite(value) or float(value) != expected:
            raise Refused("PUBLIC_CONFIG", "config", "PARAMETERS")
    if type(btc.get("min_secs_left")) is not int or btc.get("min_secs_left") != 30:
        raise Refused("PUBLIC_CONFIG", "config", "PARAMETERS")


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


def check_paths(config: Path) -> tuple[Path, Path]:
    """Find the fixed child and refuse a credentials file that exists."""
    binary = BOT / "target/debug/polymarket-toolkits"
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


def public_identity(environment: dict[str, str]) -> dict[str, Any]:
    """Resolve fixed Git identities and actual public candidate bytes."""
    paths = ("src/config.rs", "src/main.rs", "src/bot/runtime.rs", "src/bot/runtime/paper.rs",
             "src/bot/arbitrage.rs", "src/bot/arbitrage/replay/driver.rs", "src/service/venue.rs",
             "src/service/predict_fun.rs", "tools/predictfun/cli.py", "tools/predictfun/sdk_ops.py",
             "tools/predictfun/btc5m_control.py", "tools/predictfun/tests/test_sdk_ops.py",
             "tools/predictfun/tests/test_btc5m_control.py", "tools/predictfun/btc5m-controlled-paper.json")
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
        identity["binary_sha256"] = hashlib.sha256(
            (BOT / "target/debug/polymarket-toolkits").read_bytes()).hexdigest()
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
    evidence.write("started.json", {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "started_at": started_at,
        "deadline_at": deadline_at,
        "mode": mode,
        "candidate_identity": identity,
        "public_config_identity": {
            "sha256": config_sha,
            "initial_cash": 100 if mode == MODE_UNSIGNED_PAPER else None,
            "max_order_usd": 2,
            "max_market_usd": 2,
            "max_total_usd": 10,
            "max_loss_usd": 20,
        },
    })
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


def supervise(process, evidence, started_at, started_boot):
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

    def stop(signum=None, _frame=None):
        nonlocal cleanup_at, final
        if signum is None:
            final = None
        facts["launcher_exit"] = 128 + signum if signum else 1
        facts["stop_reason"] = "SIGNAL" if signum else "OUTPUT_INVALID"
        proposed = boottime() + CLEANUP_SECONDS
        cleanup_at = min(cleanup_at, proposed) if cleanup_at is not None else proposed
        if process.stdin and not process.stdin.closed:
            process.stdin.close()
        if process.poll() is None:
            process.send_signal(signal.SIGINT)

    try:
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            old_signals[signum] = signal.signal(signum, stop)
        for stream in (process.stdout, process.stderr):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ)
            buffers[stream] = bytearray()
        with open(evidence.directory / "events.jsonl", "x", encoding="utf-8") as events:
            while selector.get_map() or process.poll() is None:
                elapsed = max(boottime() - started_boot,
                              (datetime.datetime.now(datetime.timezone.utc) - started_at).total_seconds())
                if not deadline_sent and elapsed >= RUN_SECONDS and cleanup_at is None:
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
                            stop()
                        continue
                    for fragment in chunk.splitlines(keepends=True):
                        if stream not in dropping:
                            buffers[stream].extend(fragment)
                            if len(buffers[stream]) > MAX_LINE_BYTES:
                                dropping.add(stream)
                                buffers[stream].clear()
                                stop()
                        if fragment.endswith(b"\n"):
                            if stream in dropping:
                                dropping.remove(stream)
                                continue
                            raw = bytes(buffers[stream])
                            buffers[stream].clear()
                            if not raw.startswith(EVENT_PREFIX.encode()):
                                facts["discarded"] += 1
                                continue
                            try:
                                if facts["stop_reason"] == "OUTPUT_INVALID":
                                    raise ValueError("OUTPUT_INVALID")
                                event = validate_event(raw[len(EVENT_PREFIX):], evidence.run_id, evidence.mode)
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
                                previous = event
                                if event["event_kind"] == "final":
                                    final = event
                                events.write(json.dumps(event, sort_keys=True, allow_nan=False) + "\n")
                                events.flush()
                                facts["events"] += 1
                            except (ValueError, TypeError, UnicodeError, RecursionError, OverflowError):
                                stop()
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
        for stream in (process.stdout, process.stderr):
            stream.close()
        selector.close()
        for signum, handler in old_signals.items():
            signal.signal(signum, handler)
    if process.returncode != 0 or final is None:
        facts["launcher_exit"] = facts["launcher_exit"] or 1
    facts["elapsed_secs"] = boottime() - started_boot
    return facts, final


def validate_event(raw: bytes, run_id: str, mode: str = MODE_UNSIGNED_PAPER) -> dict[str, Any]:
    """Rebuild an event through recursive type-specific allowlists before writing."""
    event = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object,
                       parse_constant=_reject_constant)
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

    if not isinstance(event, dict) or event.get("run_id") != run_id:
        raise ValueError("RUN_ID")
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
    if unsigned and (args.predict_budget_confirmation is not None or args.predict_rpc_url is not None):
        raise Refused("CONTROLLED_MODE", "args", "LIVE_OPTION_REFUSED")
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
    if unsigned:
        api_key = check_environment(environment)
    else:
        try:
            local_signer._check_environment(environment)
            api_key = local_signer._api_source(environment)
        except Failure:
            raise Refused("ENV_CONFLICT", "env", "ENV_CONFLICT") from None
    _config, binary = check_paths(Path(args.config))
    identity = public_identity(environment)
    deadline_at = started_at + datetime.timedelta(seconds=RUN_SECONDS)
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
    try:
        if not unsigned:
            child_env = local_signer.build_child_env(local_signer._read_owned(), environment)
        command = child_arguments(binary, evidence.directory / "public-config.json", args.run_id,
                                  started_text, deadline_text)
        command[command.index("--controlled-mode") + 1] = args.mode
        if not unsigned:
            command[1:1] = ["--predict-budget-confirmation", args.predict_budget_confirmation,
                            "--predict-rpc-url", args.predict_rpc_url]
        if max(boottime() - started_boot,
               (datetime.datetime.now(datetime.timezone.utc) - started_at).total_seconds()) >= RUN_SECONDS:
            facts["stop_reason"] = "DEADLINE"
        else:
            process = popen(command, cwd=BOT, env=child_env, shell=False, close_fds=True,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            facts, final = supervise(process, evidence, started_at, started_boot)
    except (Exception, KeyboardInterrupt):
        facts["launcher_exit"] = 1
    finally:
        evidence.write("ended.json", {
            "schema_version": SCHEMA_VERSION, "run_id": args.run_id, "exit_facts": facts,
            "final_accounting": final["accounting"] if final else None,
            "settlement_status": final["source"] if final else None,
            "incomplete_reason": None if final else "FINAL_UNAVAILABLE",
            "limitation": (["paper", "gas NOT_INCURRED", "gas NOT_MEASURED",
                            "paper settlement is not live redeem; no queue/partial-fill/Gas simulation"]
                           if unsigned else ["signed-future-live", "gas NOT_MEASURED",
                                             "LIVE_ACCOUNTING_UNAVAILABLE"]) + ["not evidence of sustained alpha"],
        })
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
