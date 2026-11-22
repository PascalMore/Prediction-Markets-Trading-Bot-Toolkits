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
from pathlib import Path
from typing import Any, Callable, NoReturn

ROOT = Path(__file__).resolve().parents[4]
BOT = Path(__file__).resolve().parents[2]
TOOLS = Path(__file__).resolve().parent
MAX_RECONCILIATION_AGE_SECONDS = 900
MIN_GAS_ATOMS = 2_000_000_000_000_000


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
    parser.add_argument("--config", required=True)
    parser.add_argument("--reconciliation", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--authorization-ref", required=True)
    parser.add_argument("--risk-window-id", required=True)
    parser.add_argument("--operations-mode", choices=("supervised", "unattended"),
                        required=True)
    parser.add_argument("--declared-order", action="append", default=None, metavar="HASH")
    parser.add_argument("--rpc-url", default="https://bsc-dataseed.bnbchain.org")
    parser.add_argument("--evidence-root")
    parser.add_argument("--old-run")
    parser.add_argument("--old-risk-window")
    parser.add_argument("--old-authorization-ref")
    parser.add_argument("--old-digest")
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--collection", default="07_trading_btc5m_live_guard")
    parser.add_argument("--takeover-ref", required=True)
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
    if type(value) is not str or not value.isdigit():
        raise Refused("RECONCILIATION_FACTS")
    return int(value)


def preflight(config_path: Path, reconciliation_path: Path, *,
              now: dt.datetime) -> dict[str, Any]:
    """Read-only: the configuration and one fresh reconciliation must agree.

    The controlled window's C0 is exactly the cash it accounts for: the observed
    collateral must equal the configured budget atom-for-atom. A different
    balance is never an automatic budget change; it is refused before any order
    so the window cannot trade into an identity it can never close.
    """
    import btc5m_control as control

    data = control.read_public_config(Path(config_path))
    control.check_fixed_values(data, control.MODE_SIGNED_FUTURE_LIVE)
    params = control.parameters(data)
    if params is None:
        raise Refused("PARAMETERIZED_CONFIG_REQUIRED")
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
        cash = _atoms(account["collateral_atoms"])
        gas = _atoms(account["gas_atoms"])
        # A claimed payout is explained by the reconciler against the declared
        # pre-claim budget; the launcher only re-adds the verified evidence.
        claims = account.get("claimed_win", [])
        if not isinstance(claims, list):
            raise Refused("RECONCILIATION_FACTS")
        claimed = sum(_atoms(claim["claim_atoms"]) for claim in claims)
    except (OSError, ValueError, KeyError, TypeError):
        raise Refused("RECONCILIATION_FACTS") from None
    age = (now - observed).total_seconds()
    if not 0 <= age <= MAX_RECONCILIATION_AGE_SECONDS:
        raise Refused("STALE_RECONCILIATION")
    if cash != params["budget"] + claimed:
        raise Refused("CASH_BUDGET_MISMATCH")
    if gas < MIN_GAS_ATOMS:
        raise Refused("GAS_BELOW_MINIMUM")
    return {"data": data, "params": params,
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
    checked = preflight(Path(args.config), Path(args.reconciliation), now=now)
    params = checked["params"]
    if args.operations_mode != params["operations_mode"]:
        raise Refused("LIVE_CONTEXT")
    account = fetch_account()
    if account.get("account_mode") != "predict-account":
        raise Refused("ACCOUNT")
    started = now
    windows = identity_windows(args, started)
    data = checked["data"]
    activation = {"schema_version": "btc5m-live-activation/1",
                  "authorization_ref": windows["authorization_ref"],
                  "risk_window_id": windows["risk_window_id"],
                  "operations_mode": windows["operations_mode"],
                  "account_identity": account,
                  "not_before": windows["started_at"], "deadline_at": windows["deadline_at"],
                  "candidate_digest": _candidate_digest(candidate_identity(),
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
        "committed_budget_atoms": params["budget"],
        "reconciliation_sha256": checked["reconciliation_sha256"],
        "maximum_controller_invocations": 1, "automatic_retry": False})
    write_activation(activation)
    rotate_barrier(existing)
    write_evidence("launch-dispatch.json", {
        "dispatched_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "run_id": windows["run_id"], "deadline_at": windows["deadline_at"],
        "operations_mode": windows["operations_mode"],
        "controller_invocations": 1, "automatic_retry": False})
    environment = read_secret()
    facts = controller(controller_arguments(args, Path(args.config), windows, account),
                       environment=environment)
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
    client = loader._get_client()
    database = client.get_database("tradingagents", read_concern=ReadConcern("majority"))
    uri = ("mongodb://" + quote(values["MONGODB_USERNAME"], safe="") + ":"
           + quote(values["MONGODB_PASSWORD"], safe="")
           + "@" + values["MONGODB_HOST"] + ":" + values["MONGODB_PORT"]
           + "/tradingagents?authSource=admin&tls=false")
    return client, uri, database, guard, pymongo, WriteConcern


def main(argv: list[str] | None = None) -> int:
    """The only side-effecting entry point: real storage, controller, no retry."""
    args = build_parser().parse_args(argv)
    import btc5m_control as control
    import btc5m_live_events as live
    import btc5m_live_guard as guard

    params = control.parameters(control.read_public_config(Path(args.config)))
    if params is None:
        raise Refused("PARAMETERIZED_CONFIG_REQUIRED")
    args.params = params
    started = dt.datetime.now(dt.timezone.utc)
    windows = identity_windows(args, started)
    root = Path(args.evidence_root) if args.evidence_root else (
        ROOT / f"data/reports/{started.date().isoformat()}-btc5m-controlled-paper/evidence")
    directory = root / args.run_id
    if directory.exists() or directory.is_symlink():
        raise Refused("DUPLICATE_RUN")
    client, uri, database, guard, pymongo, WriteConcern = _barrier_collection()
    collection = database[args.collection].with_options(write_concern=WriteConcern(w="majority", j=True))
    scope = (f"predict_fun:56:{control.local_signer.TRADING_ADDRESS.lower()}:predict-account")

    def rotate(record: dict[str, Any]) -> None:
        # Exactly one ARMED record, matched on every frozen field.
        deleted = collection.delete_one({"_id": scope, "state": "ARMED",
                                         "run_id": record["run_id"],
                                         "risk_window_id": record["risk_window_id"],
                                         "authorization_ref": record["authorization_ref"],
                                         "candidate_digest": record["candidate_digest"]})
        if deleted.deleted_count != 1 or collection.find_one({"_id": scope}, {"_id": 1}) is not None:
            raise Refused("BARRIER_ROTATION")

    def write_activation(activation: dict[str, Any]) -> None:
        guard._uri_matches(uri, activation["storage_binding"])
        directory.mkdir(parents=True, exist_ok=False)
        live._create_file(guard.ACTIVATION_PATH, live.canonical(activation) + b"\n")

    result: dict[str, Any] = {"status": "STOPPED_NO_RETRY", "errors": []}
    try:
        result = launch(
            args, now=started,
            fetch_account=lambda: {"venue": "PREDICT_FUN", "chain_id": 56,
                                   "account_mode": "predict-account",
                                   "trading_address": control.local_signer.TRADING_ADDRESS.lower(),
                                   "signer_address": control.local_signer.SIGNER.lower()},
            read_barrier=lambda _address: collection.find_one({"_id": scope}),
            rotate_barrier=rotate,
            write_activation=write_activation,
            write_evidence=lambda name, value: live._create_file(
                directory / name, live.canonical(value) + b"\n"),
            controller=lambda command, environment: control.run(command, environment=environment),
            read_secret=lambda: {"PATH": "/usr/bin:/bin"},
            candidate_identity=lambda: control.public_identity({}, live=True),
            validate=lambda activation, context, at: guard.validate_activation(
                activation, context, at))
    except (Refused, control.Refused) as error:
        result = {"status": "REFUSED_NO_RETRY", "reason": getattr(error, "reason", None)}
    finally:
        client.close()
    print(json.dumps(result, ensure_ascii=False)[:2000])
    return 0 if result.get("facts", {}).get("launcher_exit") == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
