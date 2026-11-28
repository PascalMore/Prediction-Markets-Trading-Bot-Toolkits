"""One public launcher for a parameterized BTC5m live run.

This is the operational entry point that used to be a single-run scratch
script: every checkable precondition first, then the activation write, exactly
one stale ARMED rotation, and exactly one controller invocation. It carries no
amount of its own: the budget, the caps and the duration come from the public
configuration, and the run identity and history declarations are explicit
inputs.

Importing this module does nothing: no file, no network, no secret, no child.
``preflight`` is read-only, and only ``launch`` performs the three durable
actions, in that order, at most once. Every collaborator is injected so that a
test can prove the ordering with fakes and no real storage or funds.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Callable, NoReturn

BOT = Path(__file__).resolve().parents[2]
# The project root is four levels above the bot directory. It is not four
# levels above this file: that would be `auto-trading`, which has neither
# `skills/.env` nor `data/reports`.
ROOT = BOT.parents[4]
TOOLS = Path(__file__).resolve().parent
MAX_RECONCILIATION_AGE_SECONDS = 900
MIN_GAS_ATOMS = 2_000_000_000_000_000
# The operator's own limits. The launcher may only narrow them, never widen
# them: an order or a market cap above these values is refused, not accepted.
MAX_ORDER_ATOMS = 1 * 10 ** 18
MAX_MARKET_ATOMS = 1 * 10 ** 18
MAX_TOTAL_ATOMS = 10 * 10 ** 18
MAX_LOSS_ATOMS = 20 * 10 ** 18
ORDER_HASH = re.compile(r"0x[0-9a-fA-F]{64}", re.ASCII)


class Refused(Exception):
    """A fixed public refusal: a code and a reason, never a value."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise Refused("BAD_ARGUMENTS")


def build_parser() -> argparse.ArgumentParser:
    """The fixed public interface: where the inputs are, never what they are."""
    parser = _Parser(prog="btc5m_launch", allow_abbrev=False)
    parser.add_argument("--config", default=str(TOOLS / "btc5m-controlled-paper.json"))
    parser.add_argument("--auto-prepare", action="store_true",
                        help="Refresh account facts and prepare a new window using the fixed reader")
    parser.add_argument("--reconciliation")
    parser.add_argument("--run-id")
    parser.add_argument("--authorization-ref", required=True)
    parser.add_argument("--risk-window-id")
    parser.add_argument("--operations-mode", choices=("supervised", "unattended"),
                        default="unattended")
    parser.add_argument("--declared-order", action="append", default=None, metavar="HASH")
    parser.add_argument("--rpc-url", default="https://bsc-dataseed.bnbchain.org")
    parser.add_argument("--evidence-root")
    parser.add_argument("--old-run")
    parser.add_argument("--old-risk-window")
    parser.add_argument("--old-authorization-ref")
    parser.add_argument("--old-digest")
    parser.add_argument("--deployment-id")
    parser.add_argument("--collection", default="07_trading_btc5m_live_guard")
    parser.add_argument("--takeover-ref")
    return parser


def _parse_time(value: Any) -> dt.datetime:
    if type(value) is not str:
        raise Refused("RECONCILIATION_FACTS")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise Refused("RECONCILIATION_FACTS") from None
    if parsed.tzinfo is None:
        raise Refused("RECONCILIATION_FACTS")
    return parsed


def _atoms(value: Any) -> int:
    if type(value) is not str or re.fullmatch(r"[0-9]+", value) is None:
        raise Refused("RECONCILIATION_FACTS")
    return int(value)


def _budget_atoms(value: Any) -> int:
    """The committed budget in atoms, whether the audit states it as text or a number."""
    if type(value) is str:
        return _atoms(value)
    if type(value) is int and value > 0:
        return value
    raise Refused("RECONCILIATION_FACTS")


def _atoms_text(atoms: int) -> str:
    """One budget's exact decimal USDT text, so a public config states it to the atom."""
    whole, fraction = divmod(atoms, 10 ** 18)
    return f"{whole}.{fraction:018d}".rstrip("0").rstrip(".")


def _anchor_budget_atoms(public_config: Any) -> int | None:
    """The stopped window's own declared C0, read through the canonical public parser.

    The window's public configuration is covered by its candidate digest, so its
    budget is an anchor the operator cannot change alone. A window that declares
    no budget (a legacy fixture) has no anchor and keeps its old behaviour.
    """
    import btc5m_control as control
    if not isinstance(public_config, dict):
        raise Refused("HISTORY_IDENTITY")
    if not isinstance(public_config.get("controlled_run"), dict):
        return None
    try:
        params = control.parameters(public_config)
    except control.Refused:
        raise Refused("HISTORY_IDENTITY") from None
    if params is None:
        return None
    return params["budget"]


def preflight(config_path: Path, reconciliation_path: Path, *,
              now: dt.datetime,
              declared_orders: list[str] | None = None) -> dict[str, Any]:
    """Read-only: the configuration and one fresh reconciliation must agree.

    The controlled window's C0 is exactly the cash it accounts for: the observed
    collateral must equal the configured budget atom-for-atom. A different
    balance is never an automatic budget change; it is refused before any order
    so the window cannot trade into an identity it can never close.

    The reconciliation is the account's own read, so it also carries the
    operator's declared history. The launcher's declaration must be that same
    list, verbatim and in order: a reconciliation that explained a different
    history, or none at all, is refused instead of being papered over.
    """
    import btc5m_control as control

    data = control.read_public_config(Path(config_path))
    control.check_fixed_values(data, control.MODE_SIGNED_FUTURE_LIVE)
    params = control.parameters(data)
    if params is None:
        raise Refused("PARAMETERIZED_CONFIG_REQUIRED")
    # The operator's own limits are a ceiling, not a suggestion: a window that
    # asks for more is refused, never quietly narrowed.
    if (params["order"] > MAX_ORDER_ATOMS or params["market"] > MAX_MARKET_ATOMS
            or params["total"] > MAX_TOTAL_ATOMS or params["loss"] > MAX_LOSS_ATOMS):
        raise Refused("LIMITS_EXCEEDED")
    try:
        reconciliation = json.loads(Path(reconciliation_path).read_text(encoding="utf-8"))
        observed = _parse_time(reconciliation["finished_at"])
        account = reconciliation["account"]
        if reconciliation["read_only"] is not True:
            raise Refused("RECONCILIATION_NOT_READ_ONLY")
        if reconciliation["reconciliation"]["complete"] is not True:
            raise Refused("RECONCILIATION_NOT_COMPLETE")
        if reconciliation["reconciliation"]["positions_match_history"] is not True:
            raise Refused("RECONCILIATION_POSITIONS")
        if account["pagination_complete"] is not True or account["positions_explained"] is not True:
            raise Refused("RECONCILIATION_FACTS")
        if account["open_orders"] != []:
            raise Refused("OPEN_ORDERS_PRESENT")
        if account.get("positions", []) != []:
            raise Refused("OPEN_POSITIONS_PRESENT")
        cash = _atoms(account["collateral_atoms"])
        gas = _atoms(account["gas_atoms"])
        # A claimed payout is explained by the reconciler against the declared
        # pre-claim budget; the launcher only re-adds the verified evidence.
        claims = account.get("claimed_win", [])
        if not isinstance(claims, list):
            raise Refused("RECONCILIATION_FACTS")
        claimed = sum(_atoms(claim["claim_atoms"]) for claim in claims)
        explained = reconciliation["reconciliation"]["declared_orders"]
        if not isinstance(explained, list) or any(type(value) is not str for value in explained):
            raise Refused("RECONCILIATION_FACTS")
    except (OSError, ValueError, KeyError, TypeError):
        raise Refused("RECONCILIATION_FACTS") from None
    age = (now - observed).total_seconds()
    if not 0 <= age <= MAX_RECONCILIATION_AGE_SECONDS:
        raise Refused("STALE_RECONCILIATION")
    if cash != params["budget"] + claimed:
        raise Refused("CASH_BUDGET_MISMATCH")
    if gas < MIN_GAS_ATOMS:
        raise Refused("GAS_BELOW_MINIMUM")
    # One explicit history, in one order, for the reconciliation and the child.
    if list(declared_orders or ()) != explained:
        raise Refused("DECLARED_ORDERS_MISMATCH")
    return {"data": data, "params": params, "declared_orders": explained,
            "reconciliation_sha256": hashlib.sha256(
                Path(reconciliation_path).read_bytes()).hexdigest()}


def identity_windows(args: argparse.Namespace, started: dt.datetime) -> dict[str, Any]:
    """The run's public identity; the duration is the configuration's own."""
    deadline = started + dt.timedelta(seconds=args.params["duration"])
    return {"run_id": args.run_id, "authorization_ref": args.authorization_ref,
            "risk_window_id": args.risk_window_id,
            "operations_mode": args.operations_mode,
            "started_at": started.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "deadline_at": deadline.isoformat(timespec="milliseconds").replace("+00:00", "Z")}


def public_config_identity(data: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """The same public identity the controller writes, from the same values."""
    limits = data["strategies"]["limits"]
    return {"sha256": hashlib.sha256(_canonical(data)).hexdigest(),
            "initial_cash": data["controlled_run"]["budget_usdt"],
            "max_order_usd": limits["max_order_usd"],
            "max_market_usd": limits["max_market_usd"],
            "max_total_usd": limits["max_total_usd"],
            "max_loss_usd": limits["max_loss_usd"]}


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("utf-8")


def controller_arguments(args: argparse.Namespace, config_path: Path,
                         windows: dict[str, Any], account: dict[str, Any]) -> list[str]:
    """The one controller invocation; declarations travel in their own order."""
    command = ["--mode", "signed-future-live", "--config", str(config_path),
               "--run-id", windows["run_id"],
               "--predict-budget-confirmation", f"56:{account['trading_address']}:predict-account",
               "--predict-rpc-url", args.rpc_url,
               "--live-contract", "phase1_net_shares",
               "--operations-mode", windows["operations_mode"],
               "--risk-window-id", windows["risk_window_id"],
               "--authorization-ref", windows["authorization_ref"],
               "--deadline-at", windows["deadline_at"]]
    for value in args.declared_order or ():
        command += ["--predict-reconciled-order", value]
    return command


def _candidate_digest(identity: dict[str, Any], public: dict[str, Any]) -> str:
    """The digest the controller itself recomputes from the same two parts."""
    import btc5m_live_events as live
    return live.candidate_digest({"candidate_identity": identity,
                                  "public_config_identity": public})


def launch(args: argparse.Namespace, *, now: dt.datetime | None = None,
           fetch_account: Callable[[], dict[str, Any]],
           read_barrier: Callable[[str], dict[str, Any] | None],
           rotate_barrier: Callable[[dict[str, Any]], None],
           write_activation: Callable[[dict[str, Any]], None],
           write_evidence: Callable[[str, Any], None],
           controller: Callable[..., dict[str, Any]],
           read_secret: Callable[[], dict[str, str]],
           candidate_identity: Callable[[], dict[str, Any]],
           validate: Callable[[dict[str, Any], dict[str, Any], dt.datetime], Any]) -> dict[str, Any]:
    """Preflight, then the three durable actions, in order, at most once.

    Every precondition is checked before the activation is written, the old
    ARMED record is rotated, the trading secret is read or a child is
    dispatched. A failure anywhere leaves the following steps untouched.
    """
    now = dt.datetime.now(dt.timezone.utc) if now is None else now
    checked = preflight(Path(args.config), Path(args.reconciliation), now=now,
                        declared_orders=args.declared_order)
    params = checked["params"]
    args.params = params
    if args.operations_mode != params["operations_mode"]:
        raise Refused("LIVE_CONTEXT")
    account = fetch_account()
    if account.get("account_mode") != "predict-account":
        raise Refused("ACCOUNT")
    started = now
    windows = identity_windows(args, started)
    data = checked["data"]
    identity = candidate_identity()
    activation = {"schema_version": "btc5m-live-activation/1",
                  "authorization_ref": windows["authorization_ref"],
                  "risk_window_id": windows["risk_window_id"],
                  "operations_mode": windows["operations_mode"],
                  "account_identity": account,
                  "not_before": windows["started_at"], "deadline_at": windows["deadline_at"],
                  "candidate_digest": _candidate_digest(identity,
                                                        public_config_identity(data, params)),
                  "storage_binding": {"deployment_id": args.deployment_id,
                                      "database": "tradingagents", "collection": args.collection,
                                      "credential_env": "YQUANT_BTC5M_GUARD_MONGO_URI",
                                      "endpoint_id": {"scheme": "mongodb",
                                                      "hosts": ["172.25.240.1:27017"],
                                                      "replica_set": None, "tls": False}},
                  "gas_policy": {"payer_address": account["signer_address"],
                                 "minimum_balance_atoms": str(MIN_GAS_ATOMS),
                                 "maximum_consumption_atoms": str(MIN_GAS_ATOMS)},
                  "takeover_ref": args.takeover_ref}
    context = {"schema_version": "btc5m-live-context/1", "run_id": windows["run_id"],
               "risk_window_id": windows["risk_window_id"],
               "authorization_ref": windows["authorization_ref"],
               "operations_mode": windows["operations_mode"], "account_identity": account,
               "started_at": windows["started_at"], "deadline_at": windows["deadline_at"],
               "candidate_digest": activation["candidate_digest"],
               "asset_units": {"collateral_symbol": "USDT", "collateral_decimals": 18,
                               "share_decimals": 18, "gas_symbol": "BNB", "gas_decimals": 18},
               "gas_policy": activation["gas_policy"], "recovery_policy": "COLD_START_REFUSE",
               "cash_recovery_policy": "MANUAL_PAUSE"}
    target = {"state": "ARMED", "run_id": args.old_run,
              "risk_window_id": args.old_risk_window,
              "authorization_ref": args.old_authorization_ref,
              "candidate_digest": args.old_digest}
    if not all(value is not None for value in (args.old_run, args.old_risk_window,
                                               args.old_authorization_ref, args.old_digest)):
        raise Refused("STALE_BARRIER_INPUT")
    existing = read_barrier(account["trading_address"])
    if existing is None or {key: existing.get(key) for key in target} != target:
        raise Refused("UNEXPECTED_BARRIER")
    validate(activation, context, now)
    write_evidence("attempt-reserved.json", {
        "reserved_utc": now.isoformat(), "run_id": args.run_id,
        "committed_budget_atoms": params["budget"], "declared_orders": list(args.declared_order or ()),
        "reconciliation_sha256": checked["reconciliation_sha256"],
        "maximum_controller_invocations": 1, "automatic_retry": False})
    write_activation(activation)
    rotate_barrier(existing)
    command = controller_arguments(args, Path(args.config), windows, account)
    write_evidence("launch-dispatch.json", {
        "dispatched_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "launcher_pid": os.getpid(), "controller_argv": command,
        "binary_sha256": identity["binary_sha256"],
        "run_id": windows["run_id"], "deadline_at": windows["deadline_at"],
        "operations_mode": windows["operations_mode"],
        "controller_invocations": 1, "automatic_retry": False})
    environment = read_secret()
    facts = controller(command, environment=environment)
    return {"status": "ORIGINAL_CONTROLLER_RETURNED_INSPECT_FINAL", "facts": facts,
            "windows": windows}


def _barrier_collection():
    """The one barrier collection, read through the project's own loader."""
    import io
    import os
    from urllib.parse import quote

    import btc5m_live_guard as guard
    import pymongo
    from dotenv import dotenv_values
    import sys
    sys.path.insert(0, str(ROOT / "skills/data/data-pipeline/scripts"))
    from loaders.mongodb_loader import PortfolioMongoLoader
    from pymongo.read_concern import ReadConcern
    from pymongo.write_concern import WriteConcern

    keys = ("MONGODB_HOST", "MONGODB_PORT", "MONGODB_USERNAME", "MONGODB_PASSWORD",
            "MONGODB_DATABASE")
    selected = []
    with (ROOT / "skills/.env").open() as stream:
        for line in stream:
            line = line.lstrip()
            if line.startswith("export "):
                line = line[7:]
            if line.split("=", 1)[0].strip() in keys:
                selected.append(line)
    values = dict(dotenv_values(stream=io.StringIO("".join(selected)), interpolate=False))
    values = {key: os.environ.get(key) or values.get(key) for key in keys}
    if not all(values.values()):
        raise Refused("STORAGE_ENV")
    loader = PortfolioMongoLoader(config={"host": values["MONGODB_HOST"],
                                         "port": int(values["MONGODB_PORT"]),
                                         "username": values["MONGODB_USERNAME"],
                                         "password": values["MONGODB_PASSWORD"],
                                         "database": values["MONGODB_DATABASE"]})
    if loader.database != "tradingagents":
        raise Refused("STORAGE_ENV")
    uri = ("mongodb://" + quote(loader.username, safe="") + ":"
           + quote(loader.password, safe="")
           + "@" + loader.host + ":" + str(loader.port)
           + "/tradingagents?authSource=admin&tls=false")
    # Rotation is a single, potentially ambiguous write, not the shared loader's
    # retryable data-ingestion client. Keep that loader's other consumers unchanged.
    client = pymongo.MongoClient(uri, retryWrites=False, connect=False,
                                serverSelectionTimeoutMS=5000, connectTimeoutMS=5000,
                                socketTimeoutMS=5000)
    database = client.get_database("tradingagents", read_concern=ReadConcern("majority"))
    return client, uri, database, guard, pymongo, WriteConcern


def account_identity() -> dict[str, Any]:
    """Return the one enrolled Predict Account, not an arbitrary CLI account."""
    import local_signer
    return {"venue": "PREDICT_FUN", "chain_id": 56, "account_mode": "predict-account",
            "trading_address": local_signer.TRADING_ADDRESS.lower(),
            "signer_address": local_signer.SIGNER.lower()}


def _old_activation(path: Path, existing: dict, account: dict) -> bytes:
    """Match the exact stopped window before touching activation or its barrier."""
    import btc5m_live_events as live
    raw = live._read_file(path, live.MAX_LINE_BYTES)
    value = live.strict_json(raw)
    if (existing.get("state") != "ARMED" or value.get("account_identity") != account
            or any(value.get(key) != existing.get(key) for key in
                   ("authorization_ref", "risk_window_id", "candidate_digest"))):
        raise Refused("UNEXPECTED_ACTIVATION")
    return raw


def _check_old_guard(existing: dict, account: dict, now: dt.datetime) -> None:
    """Accept only the existing guard schema and the exact enrolled account scope."""
    import btc5m_live_events as live
    import btc5m_live_guard as guard
    live.exact(existing, "_id schema_version scope state run_id risk_window_id "
                        "authorization_ref candidate_digest armed_at")
    scope = existing["scope"]
    live.exact(scope, live.ACCOUNT_ID_KEYS + " host")
    live._scalar(scope["host"], "public")
    live.require({key: value for key, value in scope.items() if key != "host"} == account)
    live.require(existing["_id"] == guard.scope_id(account)
                 and existing["schema_version"] == guard.GUARD_SCHEMA
                 and existing["state"] == "ARMED")
    for key in ("run_id", "risk_window_id", "authorization_ref"):
        live._scalar(existing[key], "public")
    live.require(type(existing["candidate_digest"]) is str
                 and live.DIGEST.fullmatch(existing["candidate_digest"]) is not None)
    live.require(live.timestamp(existing["armed_at"]) <= now)


def _stopped_window_kind(ended: dict, final: dict, accounting: dict,
                         exit_facts: dict) -> str:
    """Classify a stopped window before a new window may take it over.

    Healthy, pending-payout and unresolved-order paths retain their admission.
    The specific filled-inventory stop may only qualify for two new complete
    zero-payout proofs; no qualification rewrites the old incomplete final.
    """
    if (accounting["loss_latched"] or not final["audit_complete"]
            or exit_facts["launcher_exit"] != 0 or exit_facts["child_returncode"] != 0
            or exit_facts["child_signal"] is not None or exit_facts["forced_cleanup"]):
        return "REFUSE"
    settled = accounting["reserved"] == "0" and final["unresolved"] == []
    if (settled and ended["incomplete_reason"] is None
            and not final["takeover_required"] and final["execution_complete"]
            and final["cash_complete"] and accounting["status"] == "COMPLETE"
            and accounting["reason"] == "NONE"):
        return "HEALTHY"
    # A fresh reconciliation may close an audited pending payout, not disguise
    # another exit fault, unresolved submission, loss latch or missing final.
    if (settled and final["stop_reason"] == "CASH_PENDING" and final["stop_detail"] is None
            and final["takeover_required"] and not final["cash_complete"]
            and ended["incomplete_reason"] == "FACT_INCOMPLETE"
            and ended["incomplete_detail"] is None
            and accounting["status"] == "INCOMPLETE"
            and accounting["reason"] == "FACT_INCOMPLETE"
            and _atoms(accounting["pending_claim_value"]) > 0):
        return "CASH_PENDING"
    if (settled and final["stop_reason"] == "RECONCILIATION_REQUIRED"
            and final["stop_detail"] == "INVENTORY_MISMATCH" and final["takeover_required"]
            and not final["execution_complete"] and not final["cash_complete"]
            and ended["incomplete_reason"] == "FACT_INCOMPLETE"
            and ended["incomplete_detail"] == "INVENTORY_MISMATCH"
            and accounting["status"] == "INCOMPLETE" and accounting["reason"] == "FACT_INCOMPLETE"
            and accounting["pending_claim_value"] is None and len(final["positions"]) == 1):
        return "FILLED_INVENTORY"
    # Terminal facts are incomplete for one known accepted order. Only a fresh,
    # complete read may prove it final; this audit does not prove zero fills.
    if (accounting["reserved"] != "0" and final["unresolved"] != []
            and final["stop_reason"] == "FACT_INCOMPLETE" and final["stop_detail"] is None
            and final["takeover_required"] and not final["execution_complete"]
            and not final["cash_complete"]
            and ended["incomplete_reason"] == "FACT_INCOMPLETE"
            and ended["incomplete_detail"] is None
            and accounting["status"] == "INCOMPLETE"
            and accounting["reason"] == "FACT_INCOMPLETE"):
        unresolved = final["unresolved"]
        # Exactly one unresolved hash; no inference of zero fills from inventory.
        if (len(unresolved) != 1 or unresolved[0].get("closure") != "UNPROVEN"
                or unresolved[0].get("reason") != "FACT_INCOMPLETE"
                or type(unresolved[0].get("signed_hash")) is not str
                or ORDER_HASH.fullmatch(unresolved[0]["signed_hash"]) is None):
            return "REFUSE"
        return "UNFILLED_ORDER"
    return "REFUSE"


UNSENT_EDGE_DETAIL = "EDGE_BELOW_MIN"
UNSENT_EDGE_STAGE = "FACTS"


def _unsent_edge_kind(ended: dict, final: dict, accounting: dict, exit_facts: dict,
                       order_events: list[dict], event_kinds: set[str]) -> str | None:
    """Classify a window whose only attempt is proven never to have been sent.

    The stop must be the economic edge refusal itself, not a generic
    incomplete-fact stop. Every order event in the window must have closed at
    the facts stage with the edge detail, and no order may ever have left the
    pre-send states: a later ``CLOSED`` never hides an earlier send.
    """
    if (accounting["loss_latched"] or not final["audit_complete"]
            or exit_facts["launcher_exit"] != 0 or exit_facts["child_returncode"] != 0
            or exit_facts["child_signal"] is not None or exit_facts["forced_cleanup"]):
        return None
    if (final["unresolved"] != [] or final["positions"] != []
            or final["stop_reason"] != "FACT_INCOMPLETE" or final["stop_detail"] is not None
            or not final["takeover_required"] or not final["execution_complete"]
            or final["cash_complete"] or final["run_status"] != "STOPPED"):
        return None
    if (ended["incomplete_reason"] != "FACT_INCOMPLETE" or ended["incomplete_detail"] is not None
            or accounting["status"] != "INCOMPLETE" or accounting["reason"] != "FACT_INCOMPLETE"
            or accounting["reserved"] != "0" or accounting["cost"] != "0"
            or accounting["pending_claim_value"] is not None):
        return None
    if not order_events or "execution" in event_kinds or "payout" in event_kinds:
        return None
    # No attempt may ever have been handed to the broker: a later CLOSED never
    # hides an earlier send.
    if any(event.get("attempt_phase") not in {"PREPARED", "CLOSED"}
           or event.get("submission") != "NOT_SUBMITTED"
           or event.get("venue_order_id") is not None
           for event in order_events):
        return None
    # Every attempt must close as never sent, at the edge refusal itself.
    closing = {}
    for event in order_events:
        closing[event.get("attempt_id")] = event
    for event in closing.values():
        diagnostic = event.get("diagnostic")
        if (event.get("attempt_phase") != "CLOSED" or event.get("submission") != "NOT_SUBMITTED"
                or type(diagnostic) is not dict
                or diagnostic.get("stage") != UNSENT_EDGE_STAGE
                or diagnostic.get("detail") != UNSENT_EDGE_DETAIL):
            return None
    return "UNSENT_EDGE"


def _unsent_edge_basis(relations: Any, final: dict, accounting: dict, started: dict,
                        previous: Path, seed: dict, seed_directory: Path,
                        initial: dict | None, order_events: list[dict]) -> dict[str, Any]:
    """Bind one proven-unsent edge stop to its old cash anchor, not a balance.

    The new window keeps the old C0: this stop created no inventory and no
    cost, so the committed cash is unchanged and is read from the old public
    configuration, the launch seed and the old native reconciliation, never
    fitted to a fresh balance.
    """
    import btc5m_live_events as live
    attempts = list(relations.attempts.values())
    if not attempts:
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    # No candidate may have reached the final-send phase: that phase exists
    # only for an order the broker was asked to send.
    if any(candidate.get("phase") == "FINAL_SEND"
           for candidate in relations.candidates.values()):
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    hashes = set()
    for attempt in attempts:
        if (not relations._closed(attempt) or attempt["remaining"] != 0
                or attempt["submission"] != "NOT_SUBMITTED"
                or attempt["venue"] is not None or attempt["execution"] is not None
                or type(attempt["hash"]) is not str
                or ORDER_HASH.fullmatch(attempt["hash"]) is None):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        hashes.add(attempt["hash"])
    if len({event.get("attempt_id") for event in order_events}) != len(attempts):
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    # The window's own signed hash was proven never to have been sent, so it
    # is not venue history. If it collides with the old history it is refused.
    if hashes & set(seed["declared_orders"]):
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    public_path = previous / "public-config.json"
    native_path = seed_directory / "reconciliation.json"
    raw = live._read_file(native_path, 1024 * 1024)
    if hashlib.sha256(raw).hexdigest() != seed.get("reconciliation_sha256"):
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    old_native = live.strict_json(raw)
    checked = preflight(public_path, native_path, now=_parse_time(started["started_at"]),
                        declared_orders=seed["declared_orders"])
    if checked["reconciliation_sha256"] != hashlib.sha256(raw).hexdigest():
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    old_account = old_native["account"]
    if old_account.get("positions") != []:
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    old_params = checked["params"]
    if (not _reconciled_identity_matches(old_native.get("account_identity"))
            or old_params["budget"] <= 0
            or initial is None or not initial["payload"]["complete"]
            or initial["payload"]["foreign_risk"]
            or initial["payload"]["collateral_atoms"] != old_account["collateral_atoms"]
            or initial["payload"]["open_hashes"] != [] or initial["payload"]["positions"] != []
            or not all(type(old_account.get(key)) is list for key in ("claimed_win", "settled_zero"))):
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    return {"old_run_id": started["run_id"], "prior_budget_atoms": str(old_params["budget"]),
            "next_budget_atoms": str(old_params["budget"]), "net_loss_atoms": "0",
            "unsent_hashes": sorted(hashes),
            "old_claimed_win": old_account["claimed_win"], "old_settled_zero": old_account["settled_zero"],
            "old_reconciliation_sha256": hashlib.sha256(raw).hexdigest()}


def _history_preserved(basis: dict, projection: dict) -> bool:
    """Match the complete historical multisets; no send justifies no extra record."""
    account = projection["account"]
    if account.get("positions") != []:
        return False
    if not all(type(account.get(key)) is list for key in ("claimed_win", "settled_zero")):
        return False
    expected = {key: basis["old_" + key] for key in ("claimed_win", "settled_zero")}
    actual = {key: account[key] for key in expected}
    return _account_facts(actual) == _account_facts(expected)


def _filled_inventory_basis(relations: Any, final: dict, accounting: dict, started: dict,
                            previous: Path, seed: dict, seed_directory: Path,
                            initial: dict | None) -> dict[str, Any]:
    """Bind one buy-only live-v1 Final to its old cash anchor, not a fresh balance."""
    import btc5m_live_events as live
    attempts = list(relations.attempts.values())
    owners = [item for item in attempts if item["submission"] == "ACCEPTED"]
    if (len(owners) != 1 or any(not relations._closed(item) or item["remaining"] != 0
            or item["submission"] not in {"ACCEPTED", "NOT_SUBMITTED", "REJECTED_FINAL"}
            for item in attempts)):
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    owner = owners[0]
    execution = owner["execution"]
    position = final["positions"][0]
    applied = owner["applied"]
    if (not owner["hash"] or owner["venue"] is None or execution is None
            or not relations._execution_closed(execution)
            or execution["signed_hash"] != owner["hash"] or execution["venue_order_id"] != owner["venue"]
            or (position["market_id"], position["token"]) != owner["identity"]
            or _atoms(position["cost_atoms"]) != applied["net_cash_atoms"]
            or _atoms(position["net_shares_atoms"]) != applied["net_inventory_atoms"]
            or _atoms(accounting["cost"]) != applied["net_cash_atoms"]
            or applied["net_cash_atoms"] <= 0 or applied["net_inventory_atoms"] <= 0
            or any(_atoms(execution["facts"][name]["atoms"]) != applied[name + "_atoms"]
                   for name in ("gross_cash", "collateral_fee", "share_fee", "net_cash", "net_inventory"))):
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    public_path = previous / "public-config.json"
    native_path = seed_directory / "reconciliation.json"
    raw = live._read_file(native_path, 1024 * 1024)
    if hashlib.sha256(raw).hexdigest() != seed.get("reconciliation_sha256"):
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    old_native = live.strict_json(raw)
    checked = preflight(public_path, native_path, now=_parse_time(started["started_at"]),
                        declared_orders=seed["declared_orders"])
    if checked["reconciliation_sha256"] != hashlib.sha256(raw).hexdigest():
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    old_account = old_native["account"]
    if old_account.get("positions") != []:
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    old_params = checked["params"]
    cost = applied["net_cash_atoms"]
    if (not _reconciled_identity_matches(old_native.get("account_identity"))
            or owner["hash"] in seed["declared_orders"]
            or cost >= old_params["loss"] or old_params["budget"] <= cost
            or initial is None or not initial["payload"]["complete"]
            or initial["payload"]["foreign_risk"]
            or initial["payload"]["collateral_atoms"] != old_account["collateral_atoms"]
            or initial["payload"]["open_hashes"] != [] or initial["payload"]["positions"] != []
            or not all(type(old_account.get(key)) is list for key in ("claimed_win", "settled_zero"))):
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    zero = {"market_id": int(position["market_id"]), "token": position["token"],
            "oracle_status": "RESOLVED", "trading_status": "CLOSED",
            "outcome_status": "LOST", "payout_millionths": 0}
    if any(item.get("token") == zero["token"] for key in ("claimed_win", "settled_zero")
           for item in old_account[key]):
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    return {"old_run_id": started["run_id"], "prior_budget_atoms": str(old_params["budget"]),
            "next_budget_atoms": str(old_params["budget"] - cost), "net_loss_atoms": str(cost),
            "order_hash": owner["hash"], "venue_order_id": owner["venue"],
            "generation": execution["generation"], "net_inventory_atoms": str(applied["net_inventory_atoms"]),
            "old_claimed_win": old_account["claimed_win"], "old_settled_zero": old_account["settled_zero"],
            "expected_zero": zero, "old_reconciliation_sha256": hashlib.sha256(raw).hexdigest()}


def _fresh_zero_closes_inventory(basis: dict, projection: dict) -> bool:
    """Allow only the one proven zero payout, preserving every historical record."""
    account = projection["account"]
    if account.get("positions") != []:
        return False
    if not all(type(account.get(key)) is list for key in ("claimed_win", "settled_zero")):
        return False
    expected = {"claimed_win": basis["old_claimed_win"],
                "settled_zero": basis["old_settled_zero"] + [basis["expected_zero"]]}
    actual = {key: account[key] for key in expected}
    return _account_facts(actual) == _account_facts(expected)


def _check_incomplete_order(relations: Any, entry: dict, accounting: dict) -> None:
    """Qualify exactly one known accepted attempt using the existing audit index."""
    open_attempts = {key: value for key, value in relations.attempts.items()
                     if not relations._closed(value)}
    attempt = open_attempts.get(entry.get("attempt_id"))
    if (len(open_attempts) != 1 or attempt is None
            or attempt["submission"] != "ACCEPTED" or attempt["venue"] is None
            or attempt["hash"] != entry.get("signed_hash")
            or entry.get("reserved_atoms") != accounting["reserved"]
            or _atoms(entry["reserved_atoms"]) != attempt["remaining"]):
        raise Refused("HISTORY_REQUIRES_TAKEOVER")


def _fresh_read_closes_window(window: dict[str, Any], fresh: dict[str, Any]) -> bool:
    """Say whether a fresh complete read closes the stopped window's one order.

    The read must prove the order final with no inventory, no open order and no
    position, and it must return the stopped window's committed cash to the
    atom. Anything else stays a takeover refusal.
    """
    # Public helper inputs use the same positive atoms contract as audit seeds:
    # an integer or ASCII integer text, never decimal USDT or a boolean.
    try:
        budget = _budget_atoms(window.get("budget_atoms"))
    except Refused:
        return False
    if budget <= 0:
        return False
    account = fresh.get("account") or {}
    reconciliation = fresh.get("reconciliation") or {}
    if (fresh.get("read_only") is not True
            or reconciliation.get("complete") is not True
            or reconciliation.get("positions_match_history") is not True
            or account.get("pagination_complete") is not True
            or account.get("positions_explained") is not True
            or account.get("open_orders") != []
            or account.get("positions", []) != []):
        return False
    proven = set(reconciliation.get("declared_orders") or ())
    unresolved = window["unresolved"]
    if not unresolved or any(entry.get("signed_hash") not in proven for entry in unresolved):
        return False
    cash = _atoms(account["collateral_atoms"])
    claims = sum(_atoms(claim["claim_atoms"]) for claim in account.get("claimed_win", []))
    return cash - claims == budget


def history_for_window(existing: dict, account: dict) -> list[str]:
    """Carry only the stopped run's declared history and system-accepted orders.

    Remote unknown orders are never added automatically. An unresolved submission
    or incomplete local audit requires takeover, even if no process remains.
    """
    return _stopped_window(existing, account)["orders"]


def _pre_send_window(previous: Path, reports: Path, existing: dict, old_bytes: bytes,
                     now: dt.datetime) -> dict[str, Any]:
    """Read only the exact frozen sources once, under the caller's original lock."""
    import stat
    import btc5m_pre_send_exit_recovery as recovery

    seeds = list(reports.glob(f"*/evidence/launch-attempts/{recovery.RUN}/attempt-reserved.json"))
    if ([previous] != [ROOT / recovery.R]
            or seeds != [ROOT / recovery.A / "attempt-reserved.json"]):
        raise Refused("HISTORY_MISSING_OR_AMBIGUOUS")
    files, artifacts = {}, {}
    try:
        for name in sorted(recovery.BYTE_PATHS | recovery.ARTIFACTS.keys()):
            path = ROOT / name
            if path.resolve(strict=True) != path.absolute():
                raise Refused("HISTORY_IDENTITY")
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise Refused("HISTORY_IDENTITY")
                if name in recovery.ARTIFACTS:
                    if info.st_size > 256 * 1024 * 1024:
                        raise Refused("HISTORY_IDENTITY")
                    artifacts[name] = hashlib.file_digest(stream, "sha256").hexdigest(), stat.S_IMODE(info.st_mode)
                else:
                    limit = 64 * 1024 if path.name == "live-context.json" else 1024 * 1024
                    raw = stream.read(limit + 1)
                    if len(raw) > limit:
                        raise Refused("HISTORY_IDENTITY")
                    files[name] = raw
        return recovery.qualify(files, artifacts, existing=existing, old_bytes=old_bytes,
                                now=now, max_age=MAX_RECONCILIATION_AGE_SECONDS)
    except OSError:
        raise Refused("HISTORY_IDENTITY") from None
    except recovery.Rejected as error:
        raise Refused(error.reason) from None


def _stopped_window(existing: dict, account: dict, *, old_bytes: bytes | None = None,
                    now: dt.datetime | None = None) -> dict[str, Any]:
    """Read the stopped window's own audit and say what kind of stop it was.

    The returned facts are the window's committed cash, its unresolved orders
    and the declarations the next window must carry. A known accepted order
    with incomplete terminal facts only qualifies for fresh read-only proof;
    the old audit neither proves zero fills nor becomes COMPLETE.
    """
    import btc5m_live_events as live
    import btc5m_payout_recovery as payout_recovery
    import btc5m_account_recovery as account_recovery
    run = existing["run_id"]
    if not isinstance(run, str) or re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", run) is None:
        raise Refused("HISTORY_IDENTITY")
    reports = ROOT / "data/reports"
    paths = list(reports.glob(f"*/evidence/{run}/started.json"))
    if len(paths) != 1:
        raise Refused("HISTORY_MISSING_OR_AMBIGUOUS")
    previous = paths[0].parent
    if run == "btc5m-live-4d0577d73dee4106" and old_bytes is not None and now is not None:
        return _pre_send_window(previous, reports, existing, old_bytes, now)
    metadata = {name: live._read_file(previous / name, limit) for name, limit in (
        ("started.json", 1024 * 1024), ("ended.json", 1024 * 1024),
        ("public-config.json", 1024 * 1024), ("live-context.json", live.MAX_LINE_BYTES))}
    started, ended, public, context = map(live.strict_json, metadata.values())
    live.check_metadata(started, public, context)
    if (started.get("run_id") != run or ended.get("run_id") != run
            or started.get("account_identity") != account
            or any(started.get(key) != existing.get(key) for key in
                   ("risk_window_id", "authorization_ref"))
            or live.candidate_digest(started) != existing["candidate_digest"]
            or (ended.get("audit_complete") is not True
                and not payout_recovery.known_ended(ended))):
        raise Refused("HISTORY_IDENTITY")

    seeds = list(reports.glob(f"*/evidence/launch-attempts/{run}/attempt-reserved.json"))
    budget = None
    seed_budget = None
    seed = seed_raw = None
    if len(seeds) == 1:
        seed_raw = live._read_file(seeds[0], 1024 * 1024)
        seed = live.strict_json(seed_raw)
        if seed.get("run_id") != run:
            raise Refused("HISTORY_IDENTITY")
        orders = seed["declared_orders"]
        seed_budget = seed.get("committed_budget_atoms")
    elif not seeds:
        # Read-only compatibility with the previous approved scratch launch's
        # public dispatch evidence. Never execute or modify that scratch script.
        declarations = []
        for path in reports.glob("*/evidence/unattended-prefly/launch-*/launch-dispatch.json"):
            value = live.strict_json(live._read_file(path, 1024 * 1024))
            argv = value.get("controller_argv", [])
            if "--run-id" in argv and argv[argv.index("--run-id") + 1] == run:
                declarations.append([argv[index + 1] for index, name in enumerate(argv)
                                     if name == "--predict-reconciled-order"])
        if len(declarations) != 1:
            raise Refused("HISTORY_MISSING_OR_AMBIGUOUS")
        orders = declarations[0]
    else:
        raise Refused("HISTORY_MISSING_OR_AMBIGUOUS")
    if (not isinstance(orders, list) or any(type(value) is not str
            or ORDER_HASH.fullmatch(value) is None for value in orders)):
        raise Refused("HISTORY_HASH")
    orders = list(dict.fromkeys(orders))
    # The stopped window's own public configuration is covered by its candidate
    # digest, so its declared C0 is an anchor. The launch attempt's seed is a
    # writable record: it may corroborate the anchor, never replace it.
    anchor = _anchor_budget_atoms(public)
    if seed_budget is not None:
        try:
            seed_budget = _budget_atoms(seed_budget)
        except Refused:
            raise Refused("HISTORY_REQUIRES_TAKEOVER") from None
    if anchor is not None and seed_budget is not None and anchor != seed_budget:
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    budget = anchor if anchor is not None else seed_budget
    latest = {}
    first_reconciliation = None
    order_events = []
    event_kinds = set()
    relations = live.LiveRelations(run, existing["risk_window_id"], started["public_config_identity"])
    event_path = previous / "events.jsonl"
    if event_path.resolve(strict=True) != event_path.absolute():
        raise Refused("HISTORY_IDENTITY")
    prefix_hash = hashlib.sha256()
    prefix_canonical = True
    checkpoint = checkpoint_event = None
    tail = []
    read_unavailable = account_recovery.known_read_unavailable(ended)
    recovery_prefix = []
    event = None
    with event_path.open("rb") as stream:
        while line := stream.readline(64 * 1024 + 1):
            prefix_hash.update(line)
            if len(line) > 64 * 1024:
                raise Refused("HISTORY_AUDIT")
            event = live.validate_live_event(
                live.EVENT_PREFIX + line, started["public_config_identity"])
            prefix_canonical &= line == live.canonical(event) + b"\n"
            relations = relations.stage(event)
            if read_unavailable:
                recovery_prefix.append(event)
            if (event["event_kind"] == "reconciliation" and event["payload"]["scope"] == "RUNTIME"
                    and event["payload"]["complete"]):
                checkpoint, checkpoint_event, tail = relations, event, []
            elif checkpoint is not None:
                tail.append(event)
            event_kinds.add(event.get("event_kind"))
            if event.get("event_kind") == "reconciliation" and first_reconciliation is None:
                first_reconciliation = event
            if event.get("event_kind") == "order":
                value = event["payload"]
                order_events.append(value)
                order = value.get("signed_hash")
                if order is not None:
                    if type(order) is not str or ORDER_HASH.fullmatch(order) is None:
                        raise Refused("HISTORY_HASH")
                    state = value.get("submission")
                    if state not in {"NOT_SUBMITTED", "REJECTED_FINAL", "ACCEPTED", "UNKNOWN"}:
                        raise Refused("HISTORY_AUDIT")
                    latest[order] = state
    if payout_recovery.known_ended(ended):
        if seed is None or anchor is None or budget != seed_budget or not prefix_canonical:
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        native_path = seeds[0].parent / "reconciliation.json"
        raw = live._read_file(native_path, 1024 * 1024)
        if hashlib.sha256(raw).hexdigest() != seed.get("reconciliation_sha256"):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        old_native = live.strict_json(raw)
        checked = preflight(previous / "public-config.json", native_path,
                            now=_parse_time(started["started_at"]),
                            declared_orders=seed["declared_orders"])
        if (checked["reconciliation_sha256"] != hashlib.sha256(raw).hexdigest()
                or not _reconciled_identity_matches(old_native.get("account_identity"))):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        stop_raw = live._read_file(previous / "stop-output.json", 1024 * 1024)
        source_sha256 = {name: hashlib.sha256(value).hexdigest() for name, value in {
            **metadata, "stop-output.json": stop_raw, "seed": seed_raw, "native": raw}.items()}
        source_sha256["events.jsonl"] = prefix_hash.hexdigest()
        execution = run == payout_recovery.EXECUTION_RUN
        if execution and live.canonical(checked["data"]) != live.canonical(public):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        qualifier = payout_recovery.qualify_execution if execution else payout_recovery.qualify
        try:
            recovery = qualifier(started=started, ended=ended, relations=relations, last=event,
                record=live.strict_json(stop_raw), old_native=old_native,
                old_params=checked["params"], seed=seed, initial=first_reconciliation,
                **({"order_events": order_events, "source_sha256": source_sha256, "root": ROOT}
                   if execution else {}))
        except (live.LiveInvalid, OSError):
            raise Refused("HISTORY_REQUIRES_TAKEOVER") from None
        recovery["source_sha256"] = source_sha256
        for owner in recovery["economic_orders"]:
            if owner["signed_hash"] not in orders:
                orders.append(owner["signed_hash"])
        return {"kind": "EXECUTION_AUDIT" if execution else "PAYOUT_AUDIT", "orders": orders,
                "budget_atoms": int(recovery["next_budget_atoms"]),
                "unresolved": [], "recovery": recovery}
    live.check_ended(ended, started, relations)
    if relations.final is None:
        raise Refused("HISTORY_AUDIT")
    final = relations.final["payload"]
    accounting = ended["final_accounting"]
    exit_facts = ended["exit_facts"]
    kind = _stopped_window_kind(ended, final, accounting, exit_facts)
    if kind == "REFUSE":
        # A window whose stop was the economic edge refusal itself may still
        # qualify, but only on its own complete evidence.
        kind = _unsent_edge_kind(ended, final, accounting, exit_facts,
                                  order_events, event_kinds) or "REFUSE"
    r20_stop = account_recovery.known_r20_fact_incomplete(ended)
    settled_data = run == account_recovery.SETTLED_DATA_RUN
    if settled_data or (kind == "REFUSE" and (account_recovery.known_ended(ended) or read_unavailable or r20_stop)):
        if seed is None or seed_raw is None or anchor is None or budget != seed_budget or not prefix_canonical:
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        native_path = seeds[0].parent / "reconciliation.json"
        raw = live._read_file(native_path, 1024 * 1024)
        if hashlib.sha256(raw).hexdigest() != seed.get("reconciliation_sha256"):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        old_native = live.strict_json(raw)
        checked = preflight(previous / "public-config.json", native_path,
                            now=_parse_time(started["started_at"]), declared_orders=seed["declared_orders"])
        if (checked["reconciliation_sha256"] != hashlib.sha256(raw).hexdigest()
                or not _reconciled_identity_matches(old_native.get("account_identity"))
                or ((r20_stop or settled_data) and live.canonical(checked["data"]) != live.canonical(public))):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        try:
            common: dict[str, Any] = dict(started=started, ended=ended, relations=relations,
                checkpoint_event=checkpoint_event, old_native=old_native, old_params=checked["params"],
                seed=seed, initial=first_reconciliation)
            if settled_data:
                sources = {name: hashlib.sha256(payload).hexdigest() for name, payload in metadata.items()}
                sources.update({"events.jsonl": prefix_hash.hexdigest(),
                    "attempt-reserved.json": hashlib.sha256(seed_raw).hexdigest(),
                    "reconciliation.json": hashlib.sha256(raw).hexdigest()})
                gas_raw = live._read_file(ROOT / account_recovery.SETTLED_DATA_GAS, 64 * 1024)
                recovery = account_recovery.qualify_settled_data(**common, gas_raw=gas_raw, sources=sources)
            else:
                qualifier = (account_recovery.qualify_read_unavailable if read_unavailable else
                             account_recovery.qualify_r20 if r20_stop else account_recovery.qualify)
                if read_unavailable:
                    common["prefix"] = recovery_prefix
                recovery = qualifier(checkpoint=checkpoint, tail=tail, **common)
        except (live.LiveInvalid, OSError):
            raise Refused("HISTORY_REQUIRES_TAKEOVER") from None
        recovery["source_sha256"] = {name: hashlib.sha256(value).hexdigest()
                                     for name, value in metadata.items()}
        recovery["source_sha256"].update(events=prefix_hash.hexdigest(), native=hashlib.sha256(raw).hexdigest(),
            seed=hashlib.sha256(seed_raw).hexdigest())
        window_orders = recovery.get("window_order_hashes",
                                     [owner["signed_hash"] for owner in recovery["economic_orders"]])
        for order in window_orders:
            if order not in orders:
                orders.append(order)
        return {"kind": "ACCOUNT_READ", "orders": orders,
                "budget_atoms": int(recovery["next_budget_atoms"]), "unresolved": [], "recovery": recovery}
    if kind == "REFUSE":
        raise Refused("HISTORY_REQUIRES_TAKEOVER")
    unresolved = final["unresolved"]
    recovery = None
    if kind in {"UNFILLED_ORDER", "FILLED_INVENTORY", "UNSENT_EDGE"}:
        # This restricted class requires both the verified public C0 and its
        # launch seed; legacy healthy/payout windows retain its old path.
        if anchor is None or seed_budget is None or budget is None or budget <= 0:
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        if kind == "UNFILLED_ORDER":
            _check_incomplete_order(relations, unresolved[0], accounting)
        elif kind == "FILLED_INVENTORY":
            if seed is None:
                raise Refused("HISTORY_REQUIRES_TAKEOVER")
            recovery = _filled_inventory_basis(relations, final, accounting, started,
                                               previous, seed, seeds[0].parent, first_reconciliation)
            budget = int(recovery["next_budget_atoms"])
        else:
            if seed is None:
                raise Refused("HISTORY_REQUIRES_TAKEOVER")
            recovery = _unsent_edge_basis(relations, final, accounting, started,
                                          previous, seed, seeds[0].parent, first_reconciliation,
                                          order_events)
            budget = int(recovery["next_budget_atoms"])
    if any(state == "UNKNOWN" for state in latest.values()):
        raise Refused("HISTORY_SUBMISSION_UNKNOWN")
    for order, state in latest.items():
        if state == "ACCEPTED" and order not in orders:
            orders.append(order)
    for entry in unresolved:
        signed = entry.get("signed_hash")
        if signed is not None and signed not in orders:
            orders.append(signed)
    return {"kind": kind, "orders": orders, "budget_atoms": budget,
            "unresolved": unresolved, "recovery": recovery}


def _reconciled_identity_matches(value: Any) -> bool:
    """Use the canonical fixed-account rules without rewriting native address spelling."""
    import btc5m_live_events as live
    try:
        live.exact(value, "chain_id account_mode trading_address signer_address")
        live.check_account_identity({"venue": "PREDICT_FUN", **value})
    except live.LiveInvalid:
        return False
    return True


def _cash_snapshot(value: dict, orders: list[str], *, require_complete: bool) -> tuple[int, int]:
    """Validate native read facts before deriving the exact new accounting C0."""
    if (value.get("read_only") is not True
            or not _reconciled_identity_matches(value.get("account_identity"))
            or value["account"]["pagination_complete"] is not True
            or value["account"]["open_orders"] != []
            or value["account"].get("positions", []) != []
            or value["reconciliation"]["declared_orders"] != orders):
        raise Refused("RECONCILIATION_FACTS")
    if require_complete and (value["reconciliation"]["complete"] is not True
                            or value["reconciliation"]["positions_match_history"] is not True
                            or value["account"]["positions_explained"] is not True):
        raise Refused("RECONCILIATION_NOT_COMPLETE")
    observed = _parse_time(value["observation"]["finished_at"])
    if not 0 <= (dt.datetime.now(dt.timezone.utc) - observed).total_seconds() <= MAX_RECONCILIATION_AGE_SECONDS:
        raise Refused("STALE_RECONCILIATION")
    account = value["account"]
    cash = _atoms(account["collateral_atoms"])
    claims = sum(_atoms(claim["claim_atoms"]) for claim in account.get("claimed_win", []))
    if cash <= claims or _atoms(account["gas_atoms"]) < MIN_GAS_ATOMS:
        raise Refused("CASH_OR_GAS_UNAVAILABLE")
    return cash, claims


def _account_facts(account: dict) -> dict[str, Any]:
    """Compare complete settled records without ordering them or losing duplicates."""
    facts = {key: value for key, value in account.items() if key != "positions_explained"}
    for key in ("claimed_win", "settled_zero"):
        if key in facts:
            records = facts[key]
            if type(records) is not list or any(type(record) is not dict for record in records):
                raise Refused("RECONCILIATION_FACTS")
            facts[key] = sorted(_canonical(record) for record in records)
    return facts


def _pre_send_fresh(result: dict, output: Path, orders: list[str], stopped: dict,
                    earliest: dt.datetime) -> tuple[str, dict]:
    """Bind the account facts to this particular complete native sample."""
    import btc5m_live_events as live
    import btc5m_pre_send_exit_recovery as recovery

    if type(result['exit_code']) is not int or result['exit_code'] != 0:
        raise Refused('RECONCILIATION_NOT_COMPLETE')
    try:
        raw = live._read_file(output, 1024 * 1024)
        value = live.strict_json(raw)
        if live.canonical(value) != live.canonical(result['projection']):
            raise Refused('RECONCILIATION_FACTS')
        recovery.sample(value, earliest=earliest, now=dt.datetime.now(dt.timezone.utc),
                        max_age=MAX_RECONCILIATION_AGE_SECONDS)
        matches = recovery.fresh_history_matches(stopped['recovery'], value)
        cash, claims = _cash_snapshot(value, orders, require_complete=True)
        if not matches:
            raise Refused('HISTORY_REQUIRES_TAKEOVER')
        if cash - claims != stopped['budget_atoms']:
            raise Refused('CASH_BUDGET_MISMATCH')
        return hashlib.sha256(raw).hexdigest(), value
    except recovery.Rejected as error:
        raise Refused(error.reason) from None
    except (live.LiveInvalid, KeyError, TypeError, ValueError):
        raise Refused('RECONCILIATION_FACTS') from None


def prepare_account(args: argparse.Namespace, directory: Path, environment: dict,
                   stopped: dict[str, Any] | None = None) -> None:
    """Two fixed native reads, exact C0 and one declaration list; no order calls."""
    import btc5m_control as control
    import btc5m_live_events as live
    import btc5m_live_guard as guard
    import btc5m_payout_recovery as payout_recovery
    data = control.read_public_config(Path(args.config))
    data["bot"].update(enable_trading=True, mock_trading=False)
    data.pop("controlled_paper", None)
    settings = data.pop("controlled_run", {})
    if settings and settings.get("operations_mode") != args.operations_mode:
        raise Refused("LIVE_CONTEXT")
    control.check_fixed_values(data, control.MODE_SIGNED_FUTURE_LIVE)
    descriptor = guard._directory(directory.parent)
    os.close(descriptor)
    directory.mkdir(mode=0o700, exist_ok=False)
    # A qualified stopped window anchors both reads before touching account authority.
    # Unfilled orders retain old C0; a filled zero-payout candidate deducts only
    # its audited cost. Healthy/pending-payout preparation retains its old path.
    # A fresh balance never supplies the anchor for either restricted recovery.
    presend = stopped if stopped is not None and stopped["kind"] == "PRE_SEND_EXIT" else None
    anchor = None
    if stopped is not None and stopped["kind"] in {"UNFILLED_ORDER", "FILLED_INVENTORY",
            "UNSENT_EDGE", "PAYOUT_AUDIT", "ACCOUNT_READ", "EXECUTION_AUDIT", "PRE_SEND_EXIT"}:
        anchor = stopped["budget_atoms"]
    if anchor is not None:
        data["controlled_run"] = {"budget_usdt": _atoms_text(anchor),
                                  "duration_seconds": settings.get("duration_seconds", 86400),
                                  "operations_mode": args.operations_mode}
        control.check_fixed_values(data, control.MODE_SIGNED_FUTURE_LIVE)
        if (stopped["kind"] in {"FILLED_INVENTORY", "PAYOUT_AUDIT", "ACCOUNT_READ", "EXECUTION_AUDIT"}
                and int(stopped["recovery"]["net_loss_atoms"]) >= control.parameters(data)["loss"]):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
    first_config = directory / "account-read-config.json"
    live._create_file(first_config, live.canonical(data) + b"\n")
    first_path = directory / "reconciliation-pass-1.json"
    first_started = dt.datetime.now(dt.timezone.utc) if presend else None
    fresh_digests: list[str] = []
    first = control.local_signer.reconcile_account(
        first_config, args.rpc_url, args.declared_order, first_path, environment=environment)
    first_value = first["projection"]
    if presend is not None and first_started is not None:
        first_digest, first_value = _pre_send_fresh(first, first_path, args.declared_order,
                                                    presend, first_started)
        fresh_digests.append(first_digest)
        budget = _atoms_text(presend["budget_atoms"])
    elif anchor is not None:
        # An anchored first read is a complete reconciliation like the second:
        # a partial read may not open a new window.
        if first["exit_code"] != 0:
            raise Refused("RECONCILIATION_NOT_COMPLETE")
        cash, claims = _cash_snapshot(first_value, args.declared_order, require_complete=True)
        if cash - claims != anchor:
            raise Refused("CASH_BUDGET_MISMATCH")
        budget = _atoms_text(anchor)
        if (stopped["kind"] == "FILLED_INVENTORY"
                and not _fresh_zero_closes_inventory(stopped["recovery"], first_value)):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        if (stopped["kind"] == "UNSENT_EDGE"
                and not _history_preserved(stopped["recovery"], first_value)):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        if (stopped["kind"] in {"PAYOUT_AUDIT", "ACCOUNT_READ", "EXECUTION_AUDIT"}
                and not payout_recovery.fresh_history_matches(stopped["recovery"], first_value)):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
    else:
        if first["exit_code"] not in (0, 4):
            raise Refused("RECONCILIATION_NOT_COMPLETE")
        cash, claims = _cash_snapshot(first_value, args.declared_order, require_complete=False)
        budget = _atoms_text(cash - claims)
    data["controlled_run"] = {"budget_usdt": budget,
        "duration_seconds": settings.get("duration_seconds", 86400),
        "operations_mode": args.operations_mode}
    config = directory / "public-config.json"
    live._create_file(config, live.canonical(data) + b"\n")
    second_path = directory / "reconciliation-pass-2.json"
    second_started = dt.datetime.now(dt.timezone.utc) if presend else None
    second = control.local_signer.reconcile_account(
        config, args.rpc_url, args.declared_order, second_path, environment=environment)
    value = second["projection"]
    if presend is not None and second_started is not None:
        second_digest, value = _pre_send_fresh(second, second_path, args.declared_order, presend,
            max(second_started, _parse_time(first_value["observation"]["finished_at"])))
        if second_digest in fresh_digests:
            raise Refused("STALE_RECONCILIATION")
        fresh_digests.append(second_digest)
    else:
        if second["exit_code"] != 0:
            raise Refused("RECONCILIATION_NOT_COMPLETE")
        _cash_snapshot(value, args.declared_order, require_complete=True)
    # Explanation can change from false to true when C0 is supplied, but the
    # account facts themselves must not change between the two reads.
    if _account_facts(first_value["account"]) != _account_facts(value["account"]):
        raise Refused("RECONCILIATION_DRIFT")
    if presend is not None:
        if len(fresh_digests) != 2:
            raise Refused("RECONCILIATION_NOT_COMPLETE")
        proof = {key: item for key, item in presend["recovery"].items() if key != "history"}
        proof["fresh_read_sha256"] = fresh_digests
        live._create_file(directory / "pre-send-exit-recovery.json", live.canonical(proof) + b"\n")
    if stopped is not None and stopped["kind"] == "UNFILLED_ORDER":
        # The stopped window's order may only be closed by this fresh read.
        if not _fresh_read_closes_window(stopped, value):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
    if stopped is not None and stopped["kind"] == "FILLED_INVENTORY":
        if not _fresh_zero_closes_inventory(stopped["recovery"], value):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        proof = {key: entry for key, entry in stopped["recovery"].items()
                 if key not in {"old_claimed_win", "old_settled_zero"}}
        proof["fresh_read_sha256"] = [hashlib.sha256(
            (directory / f"reconciliation-pass-{index}.json").read_bytes()).hexdigest() for index in (1, 2)]
        live._create_file(directory / "settled-loss-recovery.json", live.canonical(proof) + b"\n")
    if stopped is not None and stopped["kind"] == "UNSENT_EDGE":
        # The proven-unsent window keeps its old C0 and its full history: the
        # fresh read must carry both old claim and settled-zero records, and
        # the window's own signed hash must not appear in venue history.
        if not _history_preserved(stopped["recovery"], value):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        proof = {key: entry for key, entry in stopped["recovery"].items()
                 if key not in {"old_claimed_win", "old_settled_zero"}}
        proof["fresh_read_sha256"] = [hashlib.sha256(
            (directory / f"reconciliation-pass-{index}.json").read_bytes()).hexdigest() for index in (1, 2)]
        live._create_file(directory / "unsent-edge-recovery.json", live.canonical(proof) + b"\n")
    if stopped is not None and stopped["kind"] in {"PAYOUT_AUDIT", "ACCOUNT_READ", "EXECUTION_AUDIT"}:
        if not payout_recovery.fresh_history_matches(stopped["recovery"], value):
            raise Refused("HISTORY_REQUIRES_TAKEOVER")
        proof = dict(stopped["recovery"])
        proof["fresh_read_sha256"] = [hashlib.sha256(live._read_file(
            directory / f"reconciliation-pass-{index}.json", 1024 * 1024)).hexdigest() for index in (1, 2)]
        proof_name = {"PAYOUT_AUDIT": "payout-audit-recovery.json",
                      "ACCOUNT_READ": "account-read-recovery.json",
                      "EXECUTION_AUDIT": "execution-audit-recovery.json"}[stopped["kind"]]
        live._create_file(directory / proof_name, live.canonical(proof) + b"\n")
    value = {**value, "finished_at": value["observation"]["finished_at"]}
    record = directory / "reconciliation.json"
    live._create_file(record, live.canonical(value) + b"\n")
    args.config, args.reconciliation = str(config), str(record)
    preflight(config, record, now=dt.datetime.now(dt.timezone.utc), declared_orders=args.declared_order)


def _main(args: argparse.Namespace) -> dict[str, Any]:
    """Compose the real services once; release no lock until handoff is ready."""
    import btc5m_control as control
    import btc5m_live_events as live
    import btc5m_live_guard as guard

    if args.auto_prepare:
        import uuid
        args.run_id = args.run_id or "btc5m-live-" + uuid.uuid4().hex[:16]
        args.risk_window_id = args.risk_window_id or args.run_id
        args.takeover_ref = args.takeover_ref or args.authorization_ref
        if args.reconciliation is not None:
            raise Refused("CONFLICTING_PREPARATION")
    for value in (args.run_id, args.authorization_ref, args.risk_window_id, args.takeover_ref):
        if not isinstance(value, str) or control.RUN_ID_PATTERN.fullmatch(value) is None:
            raise Refused("BAD_ARGUMENTS")
    if args.collection != "07_trading_btc5m_live_guard":
        raise Refused("STORAGE_BINDING")
    for value in args.declared_order or ():
        if ORDER_HASH.fullmatch(value) is None:
            raise Refused("BAD_ARGUMENTS")
    if not args.auto_prepare:
        if args.reconciliation is None:
            raise Refused("RECONCILIATION_REQUIRED")
        checked = preflight(Path(args.config), Path(args.reconciliation),
                            now=dt.datetime.now(dt.timezone.utc), declared_orders=args.declared_order)
        args.params = checked["params"]
    else:
        template = control.read_public_config(Path(args.config))
        template["bot"].update(enable_trading=True, mock_trading=False)
        template.pop("controlled_paper", None)
        control.check_fixed_values(template, control.MODE_SIGNED_FUTURE_LIVE)
    control.local_signer._check_environment(os.environ)
    control.public_identity({}, live=True)  # Fail before account credentials on a bad deployment.
    account = account_identity()
    if not args.auto_prepare:
        projection = live.strict_json(live._read_file(Path(args.reconciliation), 1024 * 1024))
        if not _reconciled_identity_matches(projection.get("account_identity")):
            raise Refused("RECONCILIATION_ACCOUNT")
    now = dt.datetime.now(dt.timezone.utc)
    date = now.astimezone(dt.timezone(dt.timedelta(hours=8))).date()
    root = Path(args.evidence_root) if args.evidence_root else (
        ROOT / f"data/reports/{date}-btc5m-controlled-paper/evidence")
    directory = root / "launch-attempts" / args.run_id
    if (directory.exists() or directory.is_symlink() or (root / args.run_id).exists()
            or (root / args.run_id).is_symlink()
            or any((ROOT / "data/reports").glob(f"*/evidence/{args.run_id}"))
            or any((ROOT / "data/reports").glob(f"*/evidence/launch-attempts/{args.run_id}"))):
        raise Refused("DUPLICATE_RUN")
    client = None
    lock = guard.AccountLock(account, control.MODE_SIGNED_FUTURE_LIVE).acquire()
    previous_uri = os.environ.get(guard.CREDENTIAL_ENV)
    try:
        client, uri, database, _, pymongo, WriteConcern = _barrier_collection()
        collection = database[args.collection].with_options(
            write_concern=WriteConcern(w="majority", j=True))
        scope = guard.scope_id(account)
        with pymongo.timeout(20):
            existing = collection.find_one({"_id": scope})
        if existing is None:
            raise Refused("UNEXPECTED_BARRIER")
        _check_old_guard(existing, account, now)
        if (args.run_id == existing.get("run_id")
                or args.authorization_ref == existing.get("authorization_ref")
                or args.risk_window_id == existing.get("risk_window_id")):
            raise Refused("DUPLICATE_WINDOW")
        if args.auto_prepare:
            for field, key in (("old_run", "run_id"), ("old_risk_window", "risk_window_id"),
                               ("old_authorization_ref", "authorization_ref"),
                               ("old_digest", "candidate_digest")):
                supplied = getattr(args, field)
                if supplied is not None and supplied != existing.get(key):
                    raise Refused("UNEXPECTED_BARRIER")
                setattr(args, field, existing[key])
        expected = {"run_id": args.old_run, "risk_window_id": args.old_risk_window,
                    "authorization_ref": args.old_authorization_ref,
                    "candidate_digest": args.old_digest}
        if any(existing.get(key) != value for key, value in expected.items()):
            raise Refused("UNEXPECTED_BARRIER")
        old_bytes = _old_activation(guard.ACTIVATION_PATH, existing, account)
        binding = live.strict_json(old_bytes)["storage_binding"]
        guard._uri_matches(uri, binding)
        if args.deployment_id is not None and args.deployment_id != binding["deployment_id"]:
            raise Refused("STORAGE_BINDING")
        stopped = None
        if args.auto_prepare:
            if existing["run_id"] == "btc5m-live-4d0577d73dee4106":
                stopped = _stopped_window(existing, account, old_bytes=old_bytes, now=now)
            else:
                stopped = _stopped_window(existing, account)
            orders = stopped["orders"]
            if args.declared_order is not None and args.declared_order != orders:
                raise Refused("DECLARED_ORDERS_MISMATCH")
            args.declared_order = orders
            binding = live.strict_json(old_bytes)["storage_binding"]
            args.deployment_id = args.deployment_id or binding["deployment_id"]
        environment = {key: os.environ[key] for key in (*control.local_signer.INHERIT_ENV,
                       "DBUS_SESSION_BUS_ADDRESS", "XDG_RUNTIME_DIR") if key in os.environ}
        api_source = ({control.API_SOURCE_ENV: os.environ[control.API_SOURCE_ENV]}
                      if control.API_SOURCE_ENV in os.environ else {})
        environment.update(PATH="/usr/bin:/bin",
                           YQUANT_PREDICT_FUN_TOKEN=control.check_environment(api_source))
        os.environ[guard.CREDENTIAL_ENV] = uri

        def evidence(name: str, value: Any) -> None:
            if not directory.exists():
                descriptor = guard._directory(directory.parent)
                os.close(descriptor)
                directory.mkdir(mode=0o700, exist_ok=False)
            live._create_file(directory / name, live.canonical(value) + b"\n")

        def activate(value: dict) -> None:
            guard._uri_matches(uri, value["storage_binding"])
            if live._read_file(guard.ACTIVATION_PATH, live.MAX_LINE_BYTES) != old_bytes:
                raise Refused("ACTIVATION_DRIFT")
            live._create_file(directory / "superseded-activation.json", old_bytes)
            pending = directory / "next-activation.json"
            raw = live.canonical(value) + b"\n"
            live._create_file(pending, raw)
            if live._read_file(pending, live.MAX_LINE_BYTES) != raw:
                raise Refused("ACTIVATION_WRITE")
            # The old bytes are durable before atomic publication; no unlink gap.
            os.replace(pending, guard.ACTIVATION_PATH)
            live._sync_directory(guard.ACTIVATION_PATH.parent)
            live._sync_directory(directory)
            if live._read_file(guard.ACTIVATION_PATH, live.MAX_LINE_BYTES) != raw:
                raise Refused("ACTIVATION_WRITE")

        def rotate(record: dict) -> None:
            evidence("barrier-before-rotation.json", record)
            with pymongo.timeout(20):
                deleted = collection.delete_one(record)
                if deleted.deleted_count != 1 or collection.find_one({"_id": scope}) is not None:
                    raise Refused("BARRIER_ROTATION")
            evidence("barrier-rotation.json", {"previous_run_id": record["run_id"],
                     "new_run_id": args.run_id, "deleted_count": 1, "readback_absent": True})

        def dispatch(command: list[str], *, environment: dict) -> dict:
            # The controller owns the same cooperative account lock thereafter.
            # During this handoff, no other launcher can rotate an absent barrier.
            lock.close()
            return control.run(command, environment=environment, evidence_root=root)

        def read_barrier(_address: str) -> dict:
            with pymongo.timeout(20):
                observed = collection.find_one({"_id": scope})
            if observed != existing:
                raise Refused("BARRIER_DRIFT")
            return observed

        if args.auto_prepare:
            prepare_account(args, directory, environment, stopped)
        return launch(args, fetch_account=lambda: account,
                      read_barrier=read_barrier,
                      rotate_barrier=rotate, write_activation=activate, write_evidence=evidence,
                      controller=dispatch, read_secret=lambda: environment,
                      candidate_identity=lambda: control.public_identity({}, live=True),
                      validate=guard.validate_activation)
    finally:
        lock.close()
        if previous_uri is None:
            os.environ.pop(guard.CREDENTIAL_ENV, None)
        else:
            os.environ[guard.CREDENTIAL_ENV] = previous_uri
        if client is not None:
            client.close()


def main(argv: list[str] | None = None) -> int:
    """Public error boundary; never print raw driver, child or credential errors."""
    import btc5m_control as control
    import btc5m_live_events as live
    import btc5m_live_guard as guard
    try:
        result = _main(build_parser().parse_args(argv))
    except (Refused, control.Refused, guard.GuardRefused) as error:
        result = {"status": "REFUSED_NO_RETRY", "reason": error.reason}
    except control.local_signer.Failure as error:
        result = {"status": "REFUSED_NO_RETRY", "reason": error.result["reason"]}
    except Exception:
        # Driver exceptions can contain URIs or HTTP bodies. They must never
        # escape the public CLI boundary, and failure never triggers a retry.
        result = {"status": "STOPPED_NO_RETRY", "reason": "LAUNCH_PREPARATION_FAILED"}
    print(json.dumps(result, ensure_ascii=False)[:2000])
    return 0 if result.get("facts", {}).get("launcher_exit") == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
