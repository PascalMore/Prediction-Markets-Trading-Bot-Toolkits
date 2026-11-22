"""Single-host account lock and lazy Mongo COLD_START_REFUSE barrier.

No signer, child launcher, dotenv, balance recovery, reset or delete operation.
Production storage requires an explicitly approved, pre-existing binding.
"""
from __future__ import annotations

import copy
import datetime as dt
import fcntl
import os
from pathlib import Path
import re
import stat
from urllib.parse import parse_qsl, urlsplit

import btc5m_live_events as events

ROOT = Path(__file__).resolve().parents[7]
LOCK_DIRECTORY = ROOT / "data/runtime/btc5m-live/locks"
ACTIVATION_PATH = Path(__file__).parent / "btc5m-live-activation.json"
GUARD_SCHEMA = "btc5m-live-guard/1"
CREDENTIAL_ENV = "YQUANT_BTC5M_GUARD_MONGO_URI"


class GuardRefused(ValueError):
    """Stable public refusal, never raw driver/credential/endpoint text."""

    def __init__(self, reason: str = "RECOVERY_GUARD_UNAVAILABLE") -> None:
        super().__init__(reason)
        self.reason = reason


def _require(condition: bool, reason: str = "RECOVERY_GUARD_UNAVAILABLE") -> None:
    if not condition:
        raise GuardRefused(reason)


def scope_id(account_identity: dict) -> str:
    """The unique account scope excludes run/window/host/mode/signer."""
    try:
        events.check_account_identity(account_identity)
    except events.LiveInvalid:
        raise GuardRefused() from None
    return f"predict_fun:56:{account_identity['trading_address'].lower()}:predict-account"


def _directory(path: Path) -> int:
    """Walk with directory FDs, rejecting symlinks without chmod of old resources."""
    path = path.absolute()
    descriptor = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for component in path.parts[1:]:
            try:
                os.mkdir(component, 0o700, dir_fd=descriptor)
            except FileExistsError:
                pass
            next_fd = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                              dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_fd
            metadata = os.fstat(descriptor)
            _require(stat.S_ISDIR(metadata.st_mode))
            # Ancestors may be root-owned; the lock directory itself must be ours.
        _require(os.fstat(descriptor).st_uid == os.getuid())
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


class AccountLock:
    """A cooperative account lock held until the original child is confirmed gone."""

    def __init__(self, account_identity: dict, execution_mode: str) -> None:
        self.scope = scope_id(account_identity)
        _require(execution_mode in ("signed-future-live", "unsigned-paper"))
        self.unsigned = execution_mode == "unsigned-paper"
        self.descriptor: int | None = None

    def acquire(self) -> AccountLock:
        """Acquire nonblocking before any Mongo or trading secret is read."""
        if self.unsigned:
            return self
        _require(self.descriptor is None)
        directory_fd = None
        descriptor = None
        try:
            directory_fd = _directory(LOCK_DIRECTORY)
            name = f"predict_fun-56-{events.TRADING_ADDRESS}-predict-account.lock"
            descriptor = os.open(name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
                                 0o600, dir_fd=directory_fd)
            metadata = os.fstat(descriptor)
            _require(stat.S_ISREG(metadata.st_mode) and metadata.st_uid == os.getuid()
                     and metadata.st_nlink == 1 and stat.S_IMODE(metadata.st_mode) == 0o600)
            linked = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            _require((linked.st_dev, linked.st_ino) == (metadata.st_dev, metadata.st_ino))
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise GuardRefused("ALREADY_RUNNING") from None
            self.descriptor = descriptor
            descriptor = None
            return self
        except OSError:
            raise GuardRefused() from None
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if directory_fd is not None:
                os.close(directory_fd)

    def close(self) -> None:
        """Release the FD, never unlink the inode or revoke the persistent barrier."""
        if self.descriptor is not None:
            os.close(self.descriptor)
            self.descriptor = None

    def __enter__(self) -> AccountLock:
        return self.acquire()

    def __exit__(self, *_exc) -> None:
        self.close()


def check_storage_binding(binding: dict) -> None:
    """Reject missing/ambiguous deployment binding before reading its sole secret."""
    try:
        events.exact(binding, "deployment_id database collection credential_env endpoint_id")
        events._scalar(binding["deployment_id"], "public")
        _require(binding["database"] == "tradingagents"
                 and binding["collection"] == "07_trading_btc5m_live_guard"
                 and binding["credential_env"] == CREDENTIAL_ENV)
        endpoint = binding["endpoint_id"]
        events.exact(endpoint, "scheme hosts replica_set tls")
        _require(endpoint["scheme"] == "mongodb" and type(endpoint["tls"]) is bool)
        _require(type(endpoint["hosts"]) is list and 0 < len(endpoint["hosts"]) <= 1000)
        _require(len(set(endpoint["hosts"])) == len(endpoint["hosts"]))
        for host in endpoint["hosts"]:
            _require(type(host) is str and re.fullmatch(r"[a-z0-9][a-z0-9.-]*:[1-9][0-9]{0,4}", host) is not None)
            _require(int(host.rsplit(":", 1)[1]) <= 65535)
        replica = endpoint["replica_set"]
        _require(replica is None or (type(replica) is str and
                                    re.fullmatch(r"[A-Za-z0-9_-]{1,128}", replica) is not None))
    except (events.LiveInvalid, TypeError, ValueError):
        raise GuardRefused() from None


def validate_activation(activation: dict, expected: dict, now: dt.datetime) -> dict:
    """Validate public activation against the original launcher's frozen context."""
    try:
        events.exact(activation, "schema_version authorization_ref risk_window_id operations_mode "
                     "account_identity not_before deadline_at candidate_digest storage_binding gas_policy takeover_ref")
        events.exact(expected, events.CONTEXT_KEYS)
        _require(activation["schema_version"] == "btc5m-live-activation/1"
                 and expected["schema_version"] == "btc5m-live-context/1")
        events.check_account_identity(activation["account_identity"])
        for key in ("authorization_ref", "risk_window_id", "takeover_ref"):
            events._scalar(activation[key], "public")
        events._scalar(expected["run_id"], "public")
        _require(activation["operations_mode"] in ("supervised", "unattended"))
        _require(type(activation["candidate_digest"]) is str and
                 events.DIGEST.fullmatch(activation["candidate_digest"]) is not None)
        for key in ("authorization_ref", "risk_window_id", "operations_mode", "account_identity",
                    "deadline_at", "candidate_digest", "gas_policy"):
            _require(activation[key] == expected[key])
        _require(expected["recovery_policy"] == "COLD_START_REFUSE"
                 and expected["cash_recovery_policy"] == "MANUAL_PAUSE")
        start, deadline = events.timestamp(expected["started_at"]), events.timestamp(activation["deadline_at"])
        _require(events.timestamp(activation["not_before"]) <= start <= now < deadline)
        _require(events.activation_window_allowed(expected["operations_mode"], deadline - start))
        units = expected["asset_units"]
        events.fields(units, "collateral_symbol:USDT collateral_decimals:integer share_decimals:integer gas_symbol:BNB gas_decimals:integer")
        _require(all(units[key] == 18 for key in ("collateral_decimals", "share_decimals", "gas_decimals")))
        gas = activation["gas_policy"]
        events.fields(gas, "payer_address:text minimum_balance_atoms:atoms maximum_consumption_atoms:atoms")
        _require(gas["payer_address"] == activation["account_identity"]["signer_address"]
                 and int(gas["minimum_balance_atoms"]) > 0 and int(gas["maximum_consumption_atoms"]) > 0)
        check_storage_binding(activation["storage_binding"])
        return copy.deepcopy(activation)
    except (events.LiveInvalid, KeyError, TypeError, ValueError):
        raise GuardRefused() from None


def _uri_matches(uri: str, binding: dict) -> None:
    """Inspect URI only in memory; reject options that could redirect/downgrade it."""
    try:
        parts = urlsplit(uri)
        _require(parts.scheme == "mongodb" and not parts.fragment)
        hosts = parts.netloc.rsplit("@", 1)[-1].split(",")
        _require(hosts == binding["endpoint_id"]["hosts"])
        _require(parts.path == "/" + binding["database"])
        pairs = parse_qsl(parts.query, keep_blank_values=True, strict_parsing=True)
        options = dict(pairs)
        _require(len(pairs) == len(options)
                 and set(options) <= {"tls", "replicaSet", "authSource", "authMechanism"})
        endpoint = binding["endpoint_id"]
        _require(options.get("tls", "false") == ("true" if endpoint["tls"] else "false"))
        _require(options.get("replicaSet") == endpoint["replica_set"])
        if "authSource" in options:
            _require(options["authSource"] in ("admin", binding["database"]))
        if "authMechanism" in options:
            _require(options["authMechanism"] in ("SCRAM-SHA-256", "SCRAM-SHA-1"))
    except (ValueError, TypeError):
        raise GuardRefused() from None


def _arm_collection(collection, document: dict) -> dict:
    """One unique insert, acknowledged majority+j, then exact same-target readback.

    Private storage seam for offline collection tests; production always obtains
    this collection from the validated lazy driver path below.
    """
    try:
        result = collection.insert_one(copy.deepcopy(document))
        _require(result.acknowledged)
        readback = collection.find_one({"_id": document["_id"]})
        _require(readback == document, "RECONCILIATION_REQUIRED")
        return copy.deepcopy(document)
    except GuardRefused:
        raise
    except Exception as error:
        # Driver exceptions are deliberately not rendered or chained.
        reason = "RECONCILIATION_REQUIRED" if getattr(error, "code", None) == 11000 else "RECOVERY_GUARD_UNAVAILABLE"
        raise GuardRefused(reason) from None


def arm_live_guard(lock: AccountLock, expected: dict) -> dict:
    """Read the fixed activation and arm a persistent refusal before signer/child.

    No test backend parameter exists in this production entry point. Tests patch
    the existing driver's constructor and fixed public-file reader instead.
    """
    _require(not lock.unsigned and lock.descriptor is not None)
    client = None
    try:
        now = dt.datetime.now(dt.timezone.utc)
        activation = events.strict_json(events._read_file(ACTIVATION_PATH, events.MAX_LINE_BYTES))
        activation = validate_activation(activation, expected, now)
        account = activation["account_identity"]
        _require(scope_id(account) == lock.scope)
        metadata = os.fstat(lock.descriptor)
        _require(stat.S_ISREG(metadata.st_mode) and metadata.st_uid == os.getuid() and metadata.st_nlink == 1)
        binding = activation["storage_binding"]
        uri = os.environ.get(binding["credential_env"])
        _require(type(uri) is str and bool(uri))
        _uri_matches(uri, binding)
        # Lazy, same-process import. No MongoWriter or dotenv side effects.
        from pymongo import MongoClient, ReadPreference
        from pymongo.read_concern import ReadConcern
        from pymongo.write_concern import WriteConcern
        milliseconds = max(1, min(15000, int((events.timestamp(activation["deadline_at"]) - now).total_seconds() * 1000)))
        client = MongoClient(uri, retryWrites=False, connect=False, serverSelectionTimeoutMS=milliseconds,
                             connectTimeoutMS=milliseconds, socketTimeoutMS=milliseconds,
                             read_preference=ReadPreference.PRIMARY)
        # CSOT bounds the entire startup attempt, not a new timeout per operation.
        import pymongo
        with pymongo.timeout(milliseconds / 1000):
            database = client.get_database(binding["database"])
            _require(binding["collection"] in database.list_collection_names())
            collection = database.get_collection(binding["collection"],
                write_concern=WriteConcern(w="majority", j=True),
                read_concern=ReadConcern("majority"), read_preference=ReadPreference.PRIMARY)
            host = os.uname().nodename.lower()
            events._scalar(host, "public")
            document = {"_id": lock.scope, "schema_version": GUARD_SCHEMA,
                        "scope": {**account, "host": host}, "risk_window_id": activation["risk_window_id"],
                        "run_id": expected["run_id"], "state": "ARMED",
                        "armed_at": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                        "authorization_ref": activation["authorization_ref"],
                        "candidate_digest": activation["candidate_digest"]}
            return _arm_collection(collection, document)
    except GuardRefused:
        raise
    except Exception:
        raise GuardRefused() from None
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                raise GuardRefused() from None
