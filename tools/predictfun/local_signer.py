"""Fixed Predict.fun local keyring entry. No signing, login or transaction implementation."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import sys
import tempfile
import termios
import threading
import time
from collections.abc import Iterator, Mapping

from eth_account import Account
from jeepney import DBusAddress, HeaderFields, MessageFlag, MessageType, new_method_call
from jeepney.bus import get_bus
from jeepney.io.blocking import DBusConnection, prep_socket

BOT = Path(__file__).resolve().parents[2]
REPORT_ROOT = BOT.parents[4] / "data/reports"
RECONCILE_BINARY = (REPORT_ROOT / "2026-11-27-btc5m-fact-incomplete-stop/evidence"
                    / "closed-reread-bound-candidate/btc5m_reconcile")
RECONCILE_SHA256 = "178b251f0e23fdbc6a282471418b75aeecccc20a155ecbce316bd7c23b259f08"
SIGNER = "0x5129330159f4E6a99eF873f86901E3cafa7f9A7D"
TRADING_ADDRESS = "0xfc9eb66e6F22f96608058C0A711Ed048eED8c2E2"
SERVICE = "org.freedesktop.Secret.Service"
ITEM = "org.freedesktop.Secret.Item"
COLLECTION = "org.freedesktop.Secret.Collection"
PROPERTIES = "org.freedesktop.DBus.Properties"
ROOT = "/org/freedesktop/secrets"
LOGIN = ROOT + "/collection/login"
CONTENT_TYPE = "text/plain; charset=utf-8"
ATTRIBUTES = {
    "purpose": "yquant.predictfun.local-signer.v1",
    "chain": "56",
    "account_mode": "predict-account",
    "trading_address": TRADING_ADDRESS.lower(),
    "signer_address": SIGNER.lower(),
}
INHERIT_ENV = (
    "HOME", "USER", "LANG", "LC_ALL", "LC_CTYPE", "TZ",
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
    "http_proxy", "https_proxy", "all_proxy", "no_proxy", "SSL_CERT_FILE", "SSL_CERT_DIR",
)

# The native reader's public failure vocabulary, frozen from the pinned sources
# (`src/bin/btc5m_reconcile.rs`, `src/service/predict_fun/account.rs`, the venue
# error type it classifies and `src/config.rs`). Only these exact tokens may
# cross the boundary: an uppercase shape is never a reason to trust a string.
NATIVE_CODES = frozenset({
    "ACCOUNT_MISMATCH", "BUDGET_UNCONFIRMED", "DATA_UNAVAILABLE", "EXECUTION_NOT_ENABLED",
    "FACT_INCOMPLETE", "INVALID_INPUT", "OUTPUT_UNAVAILABLE", "READ_UNAVAILABLE",
    "RECONCILIATION_REQUIRED", "SDK_INCOMPATIBLE", "SUBMISSION_UNKNOWN", "TX_UNCONFIRMED",
})
NATIVE_STEPS = frozenset({
    "account_identity", "account_snapshot", "arguments", "budget",
    "chain", "controlled_gate", "controlled_place", "controlled_reservation",
    "controlled_route", "controlled_send", "credentials", "execution_snapshot",
    "input", "output", "positions", "public_config",
    "reconciled_order", "reconciliation", "reservation", "rpc",
    "rpc_input", "run_context",
})
NATIVE_REASONS = frozenset({
    "ABI_RESULT", "ACCOUNT_NOT_INITIALIZED", "ADDRESS_MISMATCH", "ANOTHER_ATTEMPT_UNCONFIRMED",
    "ASSET_ROUTE_MISMATCH", "ATTEMPT_NOT_PREPARED", "AUTHORIZE_FAULT", "BAD_ACCOUNT_MODE",
    "BAD_ADDRESS", "BAD_CHAIN_ID", "BAD_ORDER_HASH", "BALANCE_FAULT",
    "BLOCK_HASH", "BUDGET_UNCONFIRMED", "CHAIN_MISMATCH", "COLLATERAL_DECIMALS",
    "CONDITION_MISSING", "CONDITION_TOKEN_MISMATCH", "CONFIG_MISSING_VALUE", "CONFIG_REQUIRED",
    "CONTEXT_MISSING", "CONTROLLED_MODE", "CTF_ARRAY_LENGTH", "CTF_TRANSFER_ABI",
    "DECLARATION_MISMATCH", "DUPLICATE_OPTION", "EVENT_ASSET_MISMATCH", "EXECUTION_NOT_ENABLED",
    "FACT_INCOMPLETE:AMBIGUOUS_MATCH_DUPLICATE", "FACT_INCOMPLETE:AMOUNT", "FACT_INCOMPLETE:DUPLICATE_ORDER", "FACT_INCOMPLETE:DUPLICATE_POSITION",
    "FACT_INCOMPLETE:EXPIRATION", "FACT_INCOMPLETE:MISSING_CONTRACT_ORDER", "FACT_INCOMPLETE:MISSING_TOKEN", "FACT_INCOMPLETE:PAGINATION",
    "FACT_INCOMPLETE:SIDE", "HASH_MISMATCH", "HASH_MISSING", "HISTORICAL_INVENTORY_CONFLICT",
    "HISTORICAL_INVENTORY_OVERFLOW", "IDENTITY_MISMATCH", "INCOMPLETE_PAGES", "INSUFFICIENT_ALLOWANCE",
    "INSUFFICIENT_INVENTORY", "INVALID_HEX", "INVENTORY_MISMATCH", "LOG_ADDRESS",
    "LOG_DATA", "LOG_TOPICS", "MISSING_READ_CONTEXT", "OPERATOR_NOT_APPROVED",
    "ORDER_FILLED_ABI", "ORDER_HASH_MISSING_VALUE", "OUTPUT_CREATE", "OUTPUT_ENCODE",
    "OUTPUT_EXISTS", "OUTPUT_MISSING_VALUE", "OUTPUT_REQUIRED", "OUTPUT_WRITE",
    "OWNER_MISMATCH", "PREDICT_RPC_URL_INVALID", "PUBLIC_CONFIG", "READ_FAILED",
    "READ_UNAVAILABLE", "READ_UNAVAILABLE:PAGE_CAP", "READ_UNAVAILABLE:PAGINATION_LOOP", "RECEIPT_BLOCK_MISMATCH",
    "RECEIPT_LOGS", "RECEIPT_LOG_IDENTITY", "RECONCILIATION_REQUIRED:CLAIM_GENERATION", "RECONCILIATION_REQUIRED:CLAIM_IDENTITY",
    "RECONCILIATION_REQUIRED:HISTORY_NOT_CLOSED", "RECONCILIATION_REQUIRED:UNBOUND_ORDER", "RECONCILIATION_REQUIRED:UNEXPLAINED_HISTORY", "RECONCILIATION_REQUIRED:UNEXPLAINED_POSITIONS",
    "RPC_CLIENT", "RPC_HEAD_REGRESSED", "RPC_HTTP", "RPC_JSON",
    "RPC_METHOD_NOT_ALLOWED", "RPC_RESPONSE", "RPC_RESULT_MISSING", "RPC_TRANSPORT",
    "RPC_URL_INVALID", "RPC_URL_MISSING_VALUE", "RPC_URL_REQUIRED", "RUN_STOPPED",
    "SETTLEMENT_AMOUNT", "SETTLEMENT_OVERFLOW", "SETTLEMENT_TX_INVALID", "SETTLEMENT_TX_MISSING",
    "SIGNATURE_MISMATCH", "SIGNED_BOUND_UNPROVEN", "SIGNER_MISMATCH", "STARTUP_AUDIT",
    "STARTUP_HISTORY_CONFLICT", "STARTUP_INPUT", "STARTUP_SCOPE", "TOKEN_MISMATCH",
    "TOO_MANY_FIELDS", "TRANSFER_ABI", "UNCLASSIFIED", "UNKNOWN_OPTION",
    "UNSUPPORTED_VENUE", "WRAPPED_COLLATERAL_UNPROVEN",
})
# The reader prints its envelope on stderr; a bounded read keeps a noisy child
# from becoming an unbounded buffer. The read takes one byte more than the
# limit so an oversized stream is detected instead of silently truncated.
NATIVE_STDERR_LIMIT = 65536
# The reader's own failure exit codes. Anything else (0, 1, 6, 127, 255, a
# signal) is not a native failure classification this wrapper may publish.
NATIVE_FAILURE_EXITS = frozenset({2, 3, 4, 5, 64})


class Failure(Exception):
    """Only fixed, public error tokens may cross the CLI boundary."""

    def __init__(self, code: str, step: str, reason: str, exit_code: int = 1):
        super().__init__(code)
        self.result = {"code": code, "step": step, "reason": reason}
        self.exit_code = exit_code


def _backend_error(reason: str = "UNAVAILABLE") -> Failure:
    return Failure("BACKEND_UNAVAILABLE", "backend", reason)


def _storage_error(reason: str) -> Failure:
    return Failure("STORAGE_FAILURE", "store", reason)


def _invalid(reason: str, step: str = "args") -> Failure:
    return Failure("INVALID_INPUT", step, reason, 2)


def _alarm(_signum, _frame):
    raise TimeoutError


def _cancel(_signum, _frame):
    raise Failure("TTY_REQUIRED", "tty", "CANCELLED", 2)


@contextmanager
def _cancellation() -> Iterator[None]:
    previous = {}
    try:
        for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            previous[number] = signal.signal(number, _cancel)
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def _tty_fd() -> int:
    try:
        incoming, outgoing = sys.stdin.fileno(), sys.stderr.fileno()
        if not (os.isatty(incoming) and os.isatty(outgoing)):
            raise ValueError
        if os.fstat(incoming).st_rdev != os.fstat(outgoing).st_rdev:
            raise ValueError
        return incoming
    except (OSError, ValueError, AttributeError):
        raise Failure("TTY_REQUIRED", "tty", "NOT_TTY", 2) from None


def read_key_tty() -> str:
    """Read one bounded line with verified echo suppression; never use getpass fallback."""
    descriptor = _tty_fd()
    original = None
    try:
        with _cancellation():
            original = termios.tcgetattr(descriptor)
            masked = original.copy()
            masked[3] &= ~(termios.ECHO | termios.ECHONL)
            termios.tcsetattr(descriptor, termios.TCSAFLUSH, masked)
            if termios.tcgetattr(descriptor)[3] & (termios.ECHO | termios.ECHONL):
                raise Failure("TTY_REQUIRED", "tty", "ECHO_UNAVAILABLE", 2)
            sys.stderr.write("Private key (masked, local TTY only): ")
            sys.stderr.flush()
            raw = os.read(descriptor, 68)
            if not raw:
                raise Failure("TTY_REQUIRED", "tty", "CANCELLED", 2)
            # Canonical TTY reads stop at a line. Reject a queued second line as well.
            if (not raw.endswith(b"\n") or len(raw) > 67
                    or select.select([descriptor], [], [], 0)[0]):
                raise _invalid("KEY_FORMAT", "identity")
            return raw[:-1].decode("ascii")
    except (OSError, termios.error):
        raise Failure("TTY_REQUIRED", "tty", "ECHO_UNAVAILABLE", 2) from None
    except UnicodeError:
        raise _invalid("KEY_FORMAT", "identity") from None
    finally:
        if original is not None:
            try:
                termios.tcflush(descriptor, termios.TCIFLUSH)
            finally:
                termios.tcsetattr(descriptor, termios.TCSANOW, original)


def validate_key(raw: str) -> bytes:
    """Validate a secp256k1 scalar and match the fixed signer without signing anything."""
    if not isinstance(raw, str) or not re.fullmatch(r"(?:0x)?[0-9a-fA-F]{64}", raw):
        raise _invalid("KEY_FORMAT", "identity")
    canonical = raw.removeprefix("0x").lower()
    try:
        address = Account.from_key(bytes.fromhex(canonical)).address
    except Exception:
        raise _invalid("KEY_FORMAT", "identity") from None
    if bytes.fromhex(address[2:]) != bytes.fromhex(SIGNER[2:]):
        raise Failure("ACCOUNT_MISMATCH", "identity", "SIGNER", 2)
    return canonical.encode("ascii")


class _Bus:
    """One fixed Secret Service connection; no discovery, activation or unlock API."""

    def __init__(self):
        self.conn = self.sock = None
        self.owner = self.session = None
        self.deadline = time.monotonic() + 9.0
        self.create_attempted = self.created = False

    def call(self, path, interface, method, signature=None, body=(), reply="", dest=None):
        remaining = min(2.0, self.deadline - time.monotonic())
        if remaining <= 0:
            raise TimeoutError
        self.sock.settimeout(remaining)
        message = new_method_call(
            DBusAddress(path, bus_name=dest or self.owner, interface=interface),
            method, signature, body,
        )
        message.header.flags = MessageFlag.no_auto_start
        response = self.conn.send_and_get_reply(message, timeout=remaining)
        if response.header.message_type == MessageType.error:
            error = response.header.fields.get(HeaderFields.error_name)
            raise _backend_error("LOCKED" if error == "org.freedesktop.Secret.Error.IsLocked"
                                 else "UNAVAILABLE")
        if (response.header.message_type != MessageType.method_return
                or response.header.fields.get(HeaderFields.signature, "") != reply
                or not isinstance(response.body, tuple) or (reply == "" and response.body != ())):
            raise _backend_error()
        return response.body

    def property(self, path, interface, name, kind):
        (variant,) = self.call(path, PROPERTIES, "Get", "ss", (interface, name), "v")
        if not isinstance(variant, tuple) or len(variant) != 2 or variant[0] != kind:
            raise _backend_error()
        return variant[1]

    def owned_item(self):
        (alias,) = self.call(ROOT, SERVICE, "ReadAlias", "s", ("default",), "o")
        if alias != LOGIN:
            raise _backend_error()
        if self.property(LOGIN, COLLECTION, "Locked", "b") is not False:
            raise _backend_error("LOCKED")
        unlocked, locked = self.call(ROOT, SERVICE, "SearchItems", "a{ss}",
                                     (ATTRIBUTES.copy(),), "aoao")
        if not isinstance(unlocked, list) or not isinstance(locked, list):
            raise _backend_error()
        paths = unlocked + locked
        if len(paths) > 1:
            raise _storage_error("DUPLICATE")
        if not paths:
            return None
        path = paths[0]
        if not isinstance(path, str) or not path.startswith(LOGIN + "/"):
            raise _storage_error("OWNERSHIP")
        if locked or self.property(path, ITEM, "Locked", "b") is not False:
            raise _backend_error("LOCKED")
        attributes = self.property(path, ITEM, "Attributes", "a{ss}")
        # GNOME may add this one schema marker; business attributes remain exact.
        with_schema = {**ATTRIBUTES, "xdg:schema": "org.freedesktop.Secret.Generic"}
        if not isinstance(attributes, dict) or attributes not in (ATTRIBUTES, with_schema):
            raise _storage_error("OWNERSHIP")
        return path

    def open_session(self):
        output, session = self.call(ROOT, SERVICE, "OpenSession", "sv",
                                    ("plain", ("s", "")), "vo")
        if (output != ("s", "") or not isinstance(session, str)
                or not session.startswith("/") or session == "/"):
            raise _backend_error()
        self.session = session


@contextmanager
def _backend() -> Iterator[_Bus]:
    # A reply timeout alone cannot bound connect, Hello, sendall or unrelated messages.
    if (threading.current_thread() is not threading.main_thread()
            or not hasattr(signal, "setitimer") or signal.getitimer(signal.ITIMER_REAL) != (0, 0)):
        raise _backend_error()
    bus = _Bus()
    previous = signal.signal(signal.SIGALRM, _alarm)
    failure = None
    try:
        signal.setitimer(signal.ITIMER_REAL, 9.0)
        address = os.environ.get("DBUS_SESSION_BUS_ADDRESS", "")
        if not address.startswith("unix:") or ";" in address:
            raise _backend_error()
        bus.sock = prep_socket(get_bus("SESSION"), enable_fds=False, timeout=1.0)
        # Retain the partially constructed connection too, so failed Hello closes its selector.
        bus.conn = DBusConnection.__new__(DBusConnection)
        DBusConnection.__init__(bus.conn, bus.sock, enable_fds=False)
        (bus.owner,) = bus.call("/org/freedesktop/DBus", "org.freedesktop.DBus",
                               "GetNameOwner", "s", ("org.freedesktop.secrets",), "s",
                               dest="org.freedesktop.DBus")
        if not isinstance(bus.owner, str) or not re.fullmatch(r":[\w-]+(?:\.[\w-]+)+", bus.owner):
            raise _backend_error()
        yield bus
    except BaseException as error:
        if isinstance(error, Failure):
            failure = error
        elif isinstance(error, (TimeoutError, KeyboardInterrupt)):
            failure = (_backend_error("TIMEOUT") if isinstance(error, TimeoutError)
                       else Failure("TTY_REQUIRED", "tty", "CANCELLED", 2))
        else:
            failure = _backend_error()
        if bus.create_attempted and failure.result["reason"] != "PROMPT_REQUIRED":
            failure = (Failure("STORAGE_FAILURE", "readback", "READBACK") if bus.created
                       else _storage_error("CREATE_UNKNOWN"))
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        try:
            if bus.session:
                bus.deadline = time.monotonic() + 1.0
                signal.setitimer(signal.ITIMER_REAL, 1.0)
                bus.call(bus.session, "org.freedesktop.Secret.Session", "Close")
        except BaseException:
            if failure is None:
                failure = (Failure("STORAGE_FAILURE", "readback", "READBACK") if bus.created
                           else _backend_error())
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            try:
                try:
                    if bus.conn is not None and hasattr(bus.conn, "selector"):
                        bus.conn.close()
                finally:
                    if bus.sock is not None:
                        bus.sock.close()
            except Exception:
                if failure is None:
                    failure = (Failure("STORAGE_FAILURE", "readback", "READBACK") if bus.created
                               else _backend_error())
            finally:
                signal.signal(signal.SIGALRM, previous)
    if failure is not None:
        raise failure from None


def _require_absent(bus):
    if bus.owned_item() is not None:
        raise _storage_error("EXISTS")


def _create_owned(key: bytes) -> str:
    with _backend() as bus:
        _require_absent(bus)
        bus.open_session()
        properties = {
            ITEM + ".Label": ("s", "YQuant Predict.fun local signer v1"),
            ITEM + ".Attributes": ("a{ss}", ATTRIBUTES.copy()),
        }
        bus.create_attempted = True
        item, prompt = bus.call(LOGIN, COLLECTION, "CreateItem", "a{sv}(oayays)b",
                                (properties, (bus.session, b"", key, CONTENT_TYPE), False), "oo")
        if prompt != "/":
            raise _backend_error("PROMPT_REQUIRED")
        if not isinstance(item, str) or not item.startswith(LOGIN + "/"):
            raise _storage_error("CREATE_UNKNOWN")
        bus.created = True
    return item


def _read_owned(expected_item=None) -> bytes:
    with _backend() as bus:
        item = bus.owned_item()
        if item is None:
            raise Failure("CREDENTIAL_MISSING", "backend", "ABSENT")
        if expected_item is not None and item != expected_item:
            raise _storage_error("OWNERSHIP")
        bus.open_session()
        (secret,) = bus.call(item, ITEM, "GetSecret", "o", (bus.session,), "(oayays)")
        if (not isinstance(secret, tuple) or len(secret) != 4 or secret[0] != bus.session
                or secret[1] != b"" or not isinstance(secret[3], str)
                or secret[3] not in (CONTENT_TYPE, "text/plain")
                or not isinstance(secret[2], bytes)
                or re.fullmatch(b"[0-9a-f]{64}", secret[2]) is None):
            raise _storage_error("CORRUPT")
        try:
            key = validate_key(secret[2].decode("ascii"))
        except Failure:
            raise _storage_error("CORRUPT") from None
    return key


def _check_environment(environment: Mapping[str, str]) -> None:
    if "PM_PRIVATE_KEY" in environment:
        raise Failure("ENV_CONFLICT", "env", "PRIVATE_KEY_PRESENT", 2)
    if ("PM_PREDICT_ACCOUNT" in environment
            and environment["PM_PREDICT_ACCOUNT"] != TRADING_ADDRESS):
        raise Failure("ENV_CONFLICT", "env", "ACCOUNT", 2)


def _api_source(environment: Mapping[str, str]) -> str:
    source = environment.get("YQUANT_PREDICT_FUN_TOKEN", "")
    if not source.strip():
        raise Failure("ENV_CONFLICT", "env", "API_SOURCE", 2)
    if "PM_PREDICT_API_KEY" in environment and environment["PM_PREDICT_API_KEY"] != source:
        raise Failure("ENV_CONFLICT", "env", "API_CONFLICT", 2)
    return source


def build_child_env(key: bytes, environment: Mapping[str, str]) -> dict[str, str]:
    """Construct a child-only allowlisted environment; never mutate the parent."""
    _check_environment(environment)
    child = {name: environment[name] for name in INHERIT_ENV if name in environment}
    child.update(PATH="/usr/bin:/bin", RUST_LOG="warn", PM_PRIVATE_KEY=key.decode("ascii"),
                 PM_PREDICT_API_KEY=_api_source(environment), PM_PREDICT_ACCOUNT=TRADING_ADDRESS)
    return child


def _native_envelope(stderr: bytes | None) -> dict | None:
    """Extract the native reader's own failure envelope from its stderr.

    The reader prints exactly one JSON object per failure
    (``btc5m_reconcile.rs:670-676``). The whole stream must therefore be that
    one line: a single optional trailing newline is allowed, and anything
    else — a tracing line before or after it, a second object, a duplicate
    key, an extra field, malformed JSON or a multi-line mix — makes the
    stream ambiguous and is refused outright. Nothing that is not an approved
    token is ever returned: an unknown reason becomes ``UNCLASSIFIED``.
    """
    if not isinstance(stderr, bytes) or not stderr or len(stderr) > NATIVE_STDERR_LIMIT:
        return None
    try:
        text = stderr.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if text.endswith("\n"):
        text = text[:-1]
    if not text or "\n" in text or "\r" in text:
        return None
    try:
        value = json.loads(text, object_pairs_hook=_unique_object,
                           parse_constant=_reject_constant)
    except (ValueError, TypeError):
        return None
    if not isinstance(value, dict) or set(value) != {"code", "step", "reason"}:
        return None
    code, step, reason = value["code"], value["step"], value["reason"]
    if not all(isinstance(item, str) for item in (code, step, reason)):
        return None
    if code not in NATIVE_CODES or step not in NATIVE_STEPS:
        return None
    return {"code": code, "step": step,
            "reason": reason if reason in NATIVE_REASONS else "UNCLASSIFIED"}


def reconcile_account(config: Path, rpc_url: str, orders: list[str], output: Path,
                      *, environment: Mapping[str, str]) -> dict:
    """Run only the pinned read binary; authentication is not order authorization.

    No arbitrary executable, extra argv or output outside the attempt's report
    directory is accepted. The native reader can authenticate and query, never
    order, approve, redeem or transfer. Child secrets and raw output are discarded.
    """
    from urllib.parse import urlsplit
    import btc5m_control as control

    _check_environment(environment)
    _api_source(environment)
    public, target = Path(config).absolute(), Path(output).absolute()
    try:
        rpc = urlsplit(rpc_url)
        if (rpc.scheme not in {"http", "https"} or not rpc.hostname or rpc.username
                or rpc.password or rpc.query or rpc.fragment
                or public.resolve(strict=True) != public
                or not public.is_relative_to(REPORT_ROOT.resolve())
                or public.suffix != ".json" or target.parent != public.parent
                or target.name not in {"reconciliation-pass-1.json", "reconciliation-pass-2.json"}
                or os.path.lexists(target)
                or any(type(value) is not str or re.fullmatch(r"0x[0-9a-fA-F]{64}", value) is None
                       for value in orders)):
            raise ValueError
        data = control.read_public_config(public)
        control.check_fixed_values(data, control.MODE_SIGNED_FUTURE_LIVE)
        if (RECONCILE_BINARY.is_symlink() or not RECONCILE_BINARY.is_file()
                or not os.access(RECONCILE_BINARY, os.X_OK)
                or hashlib.sha256(RECONCILE_BINARY.read_bytes()).hexdigest() != RECONCILE_SHA256):
            raise ValueError
    except (OSError, ValueError, TypeError, control.Refused):
        raise _invalid("RECONCILIATION_INPUT") from None
    command = [str(RECONCILE_BINARY), "--config", str(public), "--rpc-url", rpc_url,
               "--output", str(target)]
    for value in orders:
        command += ["--predict-reconciled-order", value]
    key = _read_owned()
    child = build_child_env(key, environment)
    result = None
    stderr_file = None
    try:
        try:
            stderr_file = tempfile.TemporaryFile()
        except OSError:
            raise _read_failure(None, b"") from None
        try:
            result = subprocess.run(command, cwd=BOT, env=child, shell=False, close_fds=True,
                                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                    stderr=stderr_file, timeout=300, check=False)
            if result.returncode not in (0, 4) or target.is_symlink():
                raise ValueError
            value = json.loads(target.read_text(encoding="utf-8"),
                               object_pairs_hook=_unique_object, parse_constant=_reject_constant)
            if value.get("read_only") is not True:
                raise ValueError
            return {"exit_code": result.returncode, "projection": value}
        except (OSError, ValueError, TypeError, subprocess.SubprocessError):
            raise _read_failure(result, _read_stderr(stderr_file)) from None
    finally:
        try:
            if stderr_file is not None:
                try:
                    stderr_file.close()
                except OSError:
                    pass
        finally:
            child.clear()
            del key


def _read_stderr(stderr_file) -> bytes:
    """Read at most the bounded prefix of the child's stderr, then discard it.

    The reader's own failure envelope is a single short line, so only a bounded
    prefix is ever held. One byte more than the limit is read so an oversized
    stream is detected instead of silently truncated. The buffer is transient
    and never becomes a log.
    """
    try:
        stderr_file.seek(0)
        return stderr_file.read(NATIVE_STDERR_LIMIT + 1)
    except (OSError, ValueError):
        return b""


def _read_failure(result, stderr: bytes) -> Failure:
    """Turn a failed native read into the generic failure, plus what is safe.

    The outer code, step and reason stay exactly what they were, so no existing
    consumer changes. The native classification is added only when the reader
    published an envelope from its own frozen vocabulary and exited with one of
    its own known failure codes; everything else is dropped rather than carried.
    """
    failure = Failure("LAUNCH_FAILED", "reconcile", "READ_FAILED")
    if result is None:
        return failure
    envelope = _native_envelope(stderr)
    if envelope is None:
        return failure
    exit_code = getattr(result, "returncode", None)
    if type(exit_code) is not int or exit_code not in NATIVE_FAILURE_EXITS:
        return failure
    failure.result["native_error"] = {**envelope, "native_exit": exit_code}
    return failure


def enroll() -> None:
    """Preflight, masked entry, create once, then verify through a fresh connection."""
    _check_environment(os.environ)
    _tty_fd()
    with _backend() as bus:
        _require_absent(bus)
    key = validate_key(read_key_tty())
    item = _create_owned(key)
    try:
        if _read_owned(item) != key:
            raise _storage_error("READBACK")
    except Failure:
        raise Failure("STORAGE_FAILURE", "readback", "READBACK") from None


def _unique_object(pairs):
    result = {}
    for name, value in pairs:
        if name in result or name == "credentials":
            raise ValueError
        result[name] = value
    return result


def _reject_constant(_value):
    raise ValueError


def _check_paths(config: str) -> tuple[Path, Path, Path]:
    try:
        public = Path(config)
        if public.suffix != ".json" or not public.is_file():
            raise ValueError
        public = public.resolve(strict=True)
        if public.suffix != ".json":
            raise ValueError
        data = json.loads(public.read_text(encoding="utf-8"), object_pairs_hook=_unique_object,
                          parse_constant=_reject_constant)
        if (not isinstance(data, dict) or data.get("venue") != "predict_fun"
                or not isinstance(data.get("predict_fun"), dict)
                or data["predict_fun"].get("network") != "mainnet"
                or not isinstance(data.get("bot"), dict)
                or data["bot"].get("enable_trading") is not False
                or data["bot"].get("mock_trading") is not True):
            raise ValueError
    except (OSError, ValueError, TypeError, RecursionError):
        raise _invalid("PUBLIC_CONFIG") from None
    binary = BOT / "target/debug/polymarket-toolkits"
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise _invalid("CHILD_PATH")
    credentials = BOT / "tools/predictfun/.keyring-no-credentials"
    if os.path.lexists(credentials):
        raise _invalid("CREDENTIAL_PATH_PRESENT")
    return public, binary, credentials


def _verify_sign_in_output(stdout: bytes) -> None:
    """Require unique, ordered public markers and the fixed authenticated account."""
    reason = "EVIDENCE_INVALID"
    try:
        signing_count = login_count = 0
        account = None
        valid = isinstance(stdout, bytes)
        for line in stdout.split(b"\n"):
            if line.startswith(b"signing"):
                signing_count += 1
                if login_count or re.fullmatch(
                    rb"signing        ok: 1 share would cost [0-9]+(?:\.[0-9]+)? USDT \(not submitted\)",
                    line,
                ) is None:
                    valid = False
            elif line.startswith(b"sign-in"):
                login_count += 1
                match = re.fullmatch(
                    rb"sign-in        ok: API identifies you as (0x[0-9a-fA-F]{40})", line,
                )
                if signing_count != 1 or match is None:
                    valid = False
                elif account is None:
                    account = bytes.fromhex(match[1][2:].decode("ascii"))
        if valid and signing_count == login_count == 1:
            if account == bytes.fromhex(TRADING_ADDRESS[2:]):
                return
            reason = "AUTH_ACCOUNT_MISMATCH"
    except Exception:
        reason = "EVIDENCE_INVALID"
    raise Failure("LAUNCH_FAILED", "launch", reason) from None


def launch_check(config: str, sign_in: bool = False) -> None:
    """Launch fixed check-venue, with opt-in sign-in verified from public evidence."""
    public, binary, credentials = _check_paths(config)
    _check_environment(os.environ)
    _api_source(os.environ)
    key = _read_owned()
    child = build_child_env(key, os.environ)
    argv = [str(binary), "--config", str(public), "--credentials", str(credentials),
            "--venue", "predict-fun", "check-venue"]
    if sign_in:
        argv.append("--sign-in")
    result = None
    try:
        try:
            result = subprocess.run(argv, cwd=BOT, env=child, shell=False, close_fds=True,
                                    stdin=subprocess.DEVNULL,
                                    stdout=subprocess.PIPE if sign_in else subprocess.DEVNULL,
                                    stderr=subprocess.PIPE if sign_in else subprocess.DEVNULL,
                                    timeout=60, check=False)
        except subprocess.TimeoutExpired:
            raise Failure("LAUNCH_FAILED", "launch", "TIMEOUT") from None
        except Exception:
            raise Failure("LAUNCH_FAILED", "launch", "START") from None
        if result.returncode != 0:
            raise Failure("LAUNCH_FAILED", "launch", "SIGNAL" if result.returncode < 0 else "EXIT")
        if sign_in:
            _verify_sign_in_output(result.stdout)
    finally:
        child.clear()
        del key, result


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        self.print_usage(file=sys.stdout)
        raise _invalid("ARGUMENTS")


def main(argv: list[str] | None = None) -> int:
    """CLI boundary: fixed public errors only, no underlying exception or child output."""
    parser = _Parser(prog="local_signer.py", allow_abbrev=False)
    commands = parser.add_subparsers(dest="command", required=True, parser_class=_Parser)
    commands.add_parser("enroll", allow_abbrev=False)
    check = commands.add_parser("check", allow_abbrev=False)
    check.add_argument("--config", required=True)
    check.add_argument("--sign-in", action="store_true")
    try:
        args = parser.parse_args(argv)
        with _cancellation():
            if args.command == "enroll":
                enroll()
            else:
                launch_check(args.config, sign_in=args.sign_in)
        print("Saved and independently verified." if args.command == "enroll"
              else f"Sign-in verified for Predict Account {TRADING_ADDRESS}; no trading authorization implied."
              if args.sign_in else "Check child completed; no trading authorization implied.")
        return 0
    except Failure as error:
        print(json.dumps(error.result), file=sys.stderr)
        return error.exit_code
    except (Exception, KeyboardInterrupt):
        print(json.dumps(_backend_error().result), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
