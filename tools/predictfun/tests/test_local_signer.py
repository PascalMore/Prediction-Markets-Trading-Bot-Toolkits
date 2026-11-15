"""07-003 offline consumers: real Jeepney wire/Account, synthetic keys, PTY and fake child.

No shared bus connection, real keyring item, .env, provider or wallet signature is used.
"""

from __future__ import annotations

from contextlib import ExitStack
import copy
import importlib.metadata
import io
import json
import multiprocessing
import os
from pathlib import Path
import pty
import select
import signal
import socket
import subprocess
import sys
import tempfile
import termios
import threading
import time
import unittest
from unittest.mock import patch

TOOL_ROOT = Path(__file__).resolve().parents[1]
TEST_SCRATCH = tempfile.gettempdir()  # Resolve TMPDIR before isolated child environments.
if str(TOOL_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOL_ROOT))

import local_signer as signer  # noqa: E402
from eth_account import Account  # noqa: E402
from jeepney import HeaderFields as F, Message, MessageType, Parser  # noqa: E402
from jeepney.wrappers import new_error, new_method_return  # noqa: E402

# Public secp256k1 test vector 1; never fund or use as a real credential.
KEY = "0" * 63 + "1"
ADDRESS = Account.from_key(bytes.fromhex(KEY)).address
ITEM_PATH = signer.LOGIN + "/synthetic_item"
PUBLIC_CONFIG = {"venue": "predict_fun", "predict_fun": {"network": "mainnet"},
                 "bot": {"enable_trading": False, "mock_trading": True}}
FAKE_ENV = {"DBUS_SESSION_BUS_ADDRESS": "unix:path=/offline-only",
            "YQUANT_PREDICT_FUN_TOKEN": "synthetic-api-only"}


class WireBackend:
    """A socketpair Secret Service peer, not a replacement for the candidate adapter.

    The real DBusConnection performs Hello, serialises each call and parses each reply.
    Only the server/transport endpoint is fake. Its single owned item stays in memory.
    """

    def __init__(self):
        self.key = None
        self.paths = None
        self.attributes = signer.ATTRIBUTES.copy()
        self.alias = signer.LOGIN
        self.collection_locked = self.item_locked = False
        self.prompt = "/"
        self.calls = []
        self.clients = []
        self.threads = []
        self.sessions = []
        self.closed_sessions = []
        self.fault = None
        self.connections = 0
        self.errors = []
        self.stop = threading.Event()

    def connect(self, address, enable_fds=False, timeout=None):
        if address != "/offline-only" or enable_fds or timeout != 1.0:
            raise AssertionError("wrong fixed connect arguments")
        client, server = socket.socketpair()
        self.connections += 1
        number = self.connections
        self.clients.append(client)
        thread = threading.Thread(target=self.serve, args=(server, number), daemon=True)
        self.threads.append(thread)
        thread.start()
        return client

    def serve(self, server, number):
        parser = Parser()
        try:
            with server:
                server.settimeout(0.1)
                while not self.stop.is_set():
                    try:
                        raw = server.recv(4096)
                    except socket.timeout:
                        continue
                    if not raw:
                        break
                    for message in parser.feed(raw):
                        self.calls.append((number, message))
                        response = self.reply(message, number)
                        if response is not None:
                            server.sendall(response.serialise(serial=len(self.calls)))
        except (BrokenPipeError, ConnectionResetError):
            pass
        except BaseException as error:
            self.errors.append(type(error).__name__)

    def reply(self, message, number):
        method = message.header.fields[F.member]
        if self.fault:
            override = self.fault(message, number)
            if override is not NotImplemented:
                return override
        path = message.header.fields[F.path]
        if method == "Hello":
            signature, body = "s", (f":1.{number}",)
        elif method == "GetNameOwner":
            signature, body = "s", (":1.400",)
        elif method == "ReadAlias":
            signature, body = "o", (self.alias,)
        elif method == "Get":
            interface, name = message.body
            if name == "Locked":
                value = self.collection_locked if interface == signer.COLLECTION else self.item_locked
                signature, body = "v", (("b", value),)
            else:
                signature, body = "v", (("a{ss}", self.attributes.copy()),)
        elif method == "SearchItems":
            paths = self.paths if self.paths is not None else ([ITEM_PATH] if self.key else [])
            signature, body = "aoao", (paths, [])
        elif method == "OpenSession":
            session = signer.ROOT + f"/session/s{number}"
            self.sessions.append(session)
            signature, body = "vo", (("s", ""), session)
        elif method == "CreateItem":
            self.key = message.body[1][2]
            signature, body = "oo", (ITEM_PATH, self.prompt)
        elif method == "GetSecret":
            signature, body = "(oayays)", ((message.body[0], b"", self.key, signer.CONTENT_TYPE),)
        elif method == "Close":
            self.closed_sessions.append(path)
            signature, body = None, ()
        else:
            raise AssertionError("forbidden method")
        return new_method_return(message, signature, body)

    def __enter__(self):
        self.stack = ExitStack()
        self.stack.enter_context(patch.dict(os.environ, FAKE_ENV, clear=True))
        self.stack.enter_context(patch.object(signer, "prep_socket", self.connect))
        self.stack.enter_context(patch.object(signer, "SIGNER", ADDRESS))
        self.stack.enter_context(patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")))
        return self

    def __exit__(self, *args):
        self.stack.close()
        self.stop.set()
        for thread in self.threads:
            thread.join(1)
        if self.errors:
            raise AssertionError(self.errors)
        if any(client.fileno() != -1 for client in self.clients):
            raise AssertionError("candidate leaked a bus socket")

    def methods(self):
        return [message.header.fields[F.member] for _, message in self.calls]


def run_main(arguments):
    output, errors = io.StringIO(), io.StringIO()
    with patch.object(sys, "stdout", output), patch.object(sys, "stderr", errors):
        code = signer.main(arguments)
    return code, output.getvalue(), errors.getvalue()


class IdentityAndParserTests(unittest.TestCase):
    def test_installed_distribution_identity(self):
        self.assertEqual(importlib.metadata.version("jeepney"), "0.9.0")
        import jeepney
        self.assertTrue(Path(jeepney.__file__).resolve().is_relative_to(TOOL_ROOT / ".venv"))
        self.assertEqual(importlib.metadata.version("eth-account"), "0.14.0")

    def test_true_derivation_normalization_and_fixed_identity(self):
        with patch.object(signer, "SIGNER", ADDRESS):
            for raw in (KEY, "0x" + KEY, KEY.upper()):
                self.assertEqual(signer.validate_key(raw), KEY.encode())
        with self.assertRaises(signer.Failure) as failure:
            signer.validate_key(KEY)
        self.assertEqual(failure.exception.result["code"], "ACCOUNT_MISMATCH")
        self.assertEqual(signer.ATTRIBUTES["signer_address"], signer.SIGNER.lower())
        self.assertNotEqual(signer.SIGNER.lower(), "0xb54b2b4667b33661f83f0d6aeebb1bfa295d03be")
        self.assertNotEqual(signer.SIGNER.lower(), "0x2fcdbb1a61f4a0439c9952710edce3cee071828c")

    def test_reject_format_and_invalid_scalars(self):
        invalid = ["", " "+KEY, KEY+" ", KEY+"\n", "0X"+KEY, "0x0x"+KEY,
                   "g"*64, "０"*64, "0"*64, "f"*64, KEY[:-1], KEY+"0"]
        with patch.object(signer, "SIGNER", ADDRESS):
            for raw in invalid:
                with self.subTest(length=len(raw)):
                    with self.assertRaises(signer.Failure) as failure:
                        signer.validate_key(raw)
                    self.assertEqual(failure.exception.result["reason"], "KEY_FORMAT")

    def test_parser_rejects_without_echo_or_backend(self):
        secret_sentinel = "synthetic-unknown-argument-never-echo"
        cases = [[], ["run"], ["enroll", secret_sentinel],
                 ["check", "--conf", secret_sentinel], ["enroll", "--sign-in"],
                 *[["check", "--config", "public.json", *tail] for tail in
                   (["--sign-in=true"], ["--sign"], ["--sign-in", secret_sentinel],
                    ["--binary", secret_sentinel], ["--account", secret_sentinel])],
                 ["check", "--config", "public.json", "--chain", "97"],
                 ["enroll", "--private-key", secret_sentinel], ["check", "--", "run"]]
        with patch.object(signer, "_backend", side_effect=AssertionError("NO_BACKEND")):
            for args in cases:
                code, output, errors = run_main(args)
                self.assertEqual(code, 2)
                self.assertIn("usage: local_signer.py", output)
                self.assertEqual(json.loads(errors)["reason"], "ARGUMENTS")
                self.assertNotIn(secret_sentinel, errors)
            with self.assertRaises(SystemExit) as exit_info:
                run_main(["--help"])
            self.assertEqual(exit_info.exception.code, 0)

    def test_pipe_and_echo_failure_do_not_read(self):
        with patch.object(sys, "stdin", io.StringIO(KEY)), patch.object(os, "read") as read:
            code, _, errors = run_main(["enroll"])
            self.assertEqual(code, 2)
            self.assertIn("NOT_TTY", errors)
            read.assert_not_called()
        master, slave = pty.openpty()
        original = termios.tcgetattr(slave)
        try:
            with patch.object(signer, "_tty_fd", return_value=slave), patch.object(
                termios, "tcsetattr", side_effect=[OSError("synthetic"), None]
            ), patch.object(os, "read") as read:
                with self.assertRaises(signer.Failure) as failure:
                    signer.read_key_tty()
                self.assertEqual(failure.exception.result["reason"], "ECHO_UNAVAILABLE")
                read.assert_not_called()
            self.assertEqual(termios.tcgetattr(slave), original)
        finally:
            os.close(master)
            os.close(slave)


class AdapterTests(unittest.TestCase):
    def test_content_type_exact_literals_allow_owned_read(self):
        for content_type in ("text/plain; charset=utf-8", "text/plain"):
            with self.subTest(content_type=content_type), WireBackend() as backend:
                backend.key = KEY.encode()

                def fault(message, number):
                    if message.header.fields[F.member] == "GetSecret":
                        return new_method_return(message, "(oayays)",
                                                 ((message.body[0], b"", backend.key, content_type),))
                    return NotImplemented

                backend.fault = fault
                self.assertTrue(signer._read_owned() == KEY.encode())
                self.assertEqual(backend.methods().count("GetSecret"), 1)
                self.assertEqual(backend.sessions, backend.closed_sessions)

    def test_plain_readback_enroll_keeps_charset_write(self):
        with WireBackend() as backend:
            def fault(message, number):
                if message.header.fields[F.member] == "GetSecret":
                    return new_method_return(message, "(oayays)",
                                             ((message.body[0], b"", backend.key, "text/plain"),))
                return NotImplemented

            backend.fault = fault
            with patch.object(signer, "_tty_fd", return_value=0), patch.object(
                signer, "read_key_tty", return_value=KEY
            ):
                code, output, errors = run_main(["enroll"])
            self.assertEqual((code, errors), (0, ""))
            self.assertNotIn(KEY, output + errors)
            self.assertEqual(backend.connections, 3)
            self.assertEqual(backend.sessions, backend.closed_sessions)
            creates = [(number, message) for number, message in backend.calls
                       if message.header.fields[F.member] == "CreateItem"]
            reads = [(number, message) for number, message in backend.calls
                     if message.header.fields[F.member] == "GetSecret"]
            self.assertEqual((len(creates), len(reads)), (1, 1))
            self.assertNotEqual(creates[0][0], reads[0][0])
            self.assertEqual(creates[0][1].body[1][3], "text/plain; charset=utf-8")
            self.assertIs(creates[0][1].body[2], False)

    def test_content_type_nonexact_strings_rejected(self):
        cases = ("", "unknown", "application/octet-stream", "text/html",
                 "text/plain; charset=ascii", "text/plain; charset=UTF-8",
                 "text/plain;charset=utf-8", "text/plain; charset=utf-8; extra=1",
                 " text/plain", "text/plain ", "text/plain\n", "TEXT/PLAIN",
                 "Text/Plain", " text/plain; charset=utf-8", "text/plain; charset=utf-8 ")
        for content_type in cases:
            with self.subTest(content_type=content_type), WireBackend() as backend:
                backend.key = KEY.encode()

                def fault(message, number):
                    if message.header.fields[F.member] == "GetSecret":
                        return new_method_return(message, "(oayays)",
                                                 ((message.body[0], b"", backend.key, content_type),))
                    return NotImplemented

                backend.fault = fault
                with self.assertRaises(signer.Failure) as failure:
                    signer._read_owned()
                self.assertEqual(failure.exception.result,
                                 {"code": "STORAGE_FAILURE", "step": "store", "reason": "CORRUPT"})
                self.assertEqual(backend.sessions, backend.closed_sessions)

    def test_content_type_missing_or_nonstring_rejected_at_read_seam(self):
        # D-Bus 's' cannot encode these values: inject after the real wire call,
        # retaining the actual _read_owned validation and backend cleanup path.
        for case in ("missing", None, 1, False, b"text/plain", ["text/plain"], {"text/plain": True}):
            with self.subTest(kind=type(case).__name__, missing=case == "missing"), WireBackend() as backend:
                backend.key = KEY.encode()
                original_call = signer._Bus.call

                def call(bus, path, interface, method, *args, **kwargs):
                    reply = original_call(bus, path, interface, method, *args, **kwargs)
                    if method == "GetSecret":
                        secret = reply[0]
                        return (secret[:3] if case == "missing" else (*secret[:3], case),)
                    return reply

                with patch.object(signer._Bus, "call", call):
                    with self.assertRaises(signer.Failure) as failure:
                        signer._read_owned()
                self.assertEqual(failure.exception.result,
                                 {"code": "STORAGE_FAILURE", "step": "store", "reason": "CORRUPT"})
                self.assertEqual(backend.methods().count("GetSecret"), 1)
                self.assertEqual(backend.sessions, backend.closed_sessions)

    def test_schema_absent_or_exact_allows_owned_read(self):
        for schema in (None, "org.freedesktop.Secret.Generic"):
            with self.subTest(schema=schema), WireBackend() as backend:
                backend.key = KEY.encode()
                if schema is not None:
                    backend.attributes["xdg:schema"] = schema
                self.assertTrue(signer._read_owned() == KEY.encode())
                self.assertEqual(backend.methods().count("GetSecret"), 1)
                self.assertEqual(backend.sessions, backend.closed_sessions)
                for _, message in backend.calls:
                    if message.header.fields[F.member] == "SearchItems":
                        self.assertEqual(message.body, (signer.ATTRIBUTES,))

    def test_exact_schema_enroll_preserves_five_attribute_create_and_search(self):
        with WireBackend() as backend:
            backend.attributes["xdg:schema"] = "org.freedesktop.Secret.Generic"
            with patch.object(signer, "_tty_fd", return_value=0), patch.object(
                signer, "read_key_tty", return_value=KEY
            ):
                code, output, errors = run_main(["enroll"])
            self.assertEqual((code, errors), (0, ""))
            self.assertIn("independently verified", output)
            self.assertEqual(backend.connections, 3)
            self.assertEqual(backend.methods().count("CreateItem"), 1)
            self.assertEqual(backend.methods().count("GetSecret"), 1)
            self.assertEqual(backend.sessions, backend.closed_sessions)
            for _, message in backend.calls:
                method = message.header.fields[F.member]
                if method == "SearchItems":
                    self.assertEqual(message.body, (signer.ATTRIBUTES,))
                elif method == "CreateItem":
                    self.assertEqual(message.body[0][signer.ITEM + ".Attributes"],
                                     ("a{ss}", signer.ATTRIBUTES))
                    self.assertIs(message.body[2], False)
            self.assertFalse(set(backend.methods()) &
                             {"Delete", "Unlock", "Prompt", "Dismiss", "StartServiceByName"})
            self.assertNotIn(KEY, output + errors)

    def test_schema_wrong_unknown_extra_and_each_business_attribute_rejected(self):
        cases = [("wrong-schema", {**signer.ATTRIBUTES, "xdg:schema": "wrong"}),
                 ("empty-schema", {**signer.ATTRIBUTES, "xdg:schema": ""})]
        for schema in (None, "org.freedesktop.Secret.Generic"):
            attributes = signer.ATTRIBUTES.copy()
            if schema is not None:
                attributes["xdg:schema"] = schema
            cases.append(("unknown-extra", {**attributes, "unknown": "reject"}))
            for name in signer.ATTRIBUTES:
                missing = attributes.copy()
                del missing[name]
                cases.append(("missing-" + name, missing))
                cases.append(("changed-" + name, {**attributes, name: "wrong"}))
        for case, attributes in cases:
            with self.subTest(case=case, schema=attributes.get("xdg:schema")), WireBackend() as backend:
                backend.key = KEY.encode()
                backend.attributes = attributes
                with self.assertRaises(signer.Failure) as failure:
                    signer._read_owned()
                self.assertEqual(failure.exception.result["reason"], "OWNERSHIP")
                self.assertFalse(set(backend.methods()) &
                                 {"OpenSession", "GetSecret", "CreateItem", "Delete", "Unlock",
                                  "Prompt", "Dismiss", "StartServiceByName"})

    def test_malformed_attribute_variants_rejected_before_secret(self):
        variants = [("s", "not-a-dictionary"), ("as", list(signer.ATTRIBUTES)),
                    ("a{sv}", {**{name: ("s", value) for name, value in signer.ATTRIBUTES.items()},
                               "xdg:schema": ("u", 1)}),
                    ("a{sv}", {**{name: ("s", value) for name, value in signer.ATTRIBUTES.items()},
                               "chain": ("u", 56)})]
        for variant in variants:
            with self.subTest(kind=variant[0]), WireBackend() as backend:
                backend.key = KEY.encode()

                def fault(message, number):
                    if (message.header.fields[F.member] == "Get"
                            and message.body == (signer.ITEM, "Attributes")):
                        return new_method_return(message, "v", (variant,))
                    return NotImplemented

                backend.fault = fault
                with self.assertRaises(signer.Failure) as failure:
                    signer._read_owned()
                self.assertEqual(failure.exception.result["reason"], "UNAVAILABLE")
                self.assertNotIn("OpenSession", backend.methods())
                self.assertNotIn("GetSecret", backend.methods())

    def test_full_enroll_fresh_readback_then_actual_check_seam(self):
        with WireBackend() as backend, TemporaryBot() as bot:
            with patch.object(signer, "_tty_fd", return_value=0), patch.object(
                signer, "read_key_tty", return_value="0x"+KEY
            ):
                code, output, errors = run_main(["enroll"])
            self.assertEqual((code, errors), (0, ""))
            self.assertIn("independently verified", output)
            self.assertEqual(backend.connections, 3)
            self.assertEqual(backend.key, KEY.encode())
            self.assertEqual(backend.sessions, backend.closed_sessions)
            created_connection = next(number for number, message in backend.calls
                                      if message.header.fields[F.member] == "CreateItem")
            read_connection = next(number for number, message in backend.calls
                                   if message.header.fields[F.member] == "GetSecret")
            self.assertNotEqual(created_connection, read_connection)
            capture = []
            real_run = subprocess.run

            def fake_child(*args, **kwargs):
                capture.append((copy.deepcopy(args), copy.deepcopy(kwargs)))
                return real_run(*args, **kwargs)

            before = dict(os.environ)
            with patch.object(signer.subprocess, "run", side_effect=fake_child):
                code, output, errors = run_main(["check", "--config", str(bot.config)])
            self.assertEqual((code, errors), (0, ""))
            self.assertEqual(output, "Check child completed; no trading authorization implied.\n")
            self.assertEqual(dict(os.environ), before)
            self.assertEqual(len(capture), 1)
            args, kwargs = capture[0]
            self.assertEqual(args[0], [str(bot.binary), "--config", str(bot.config),
                                     "--credentials", str(bot.credentials), "--venue",
                                     "predict-fun", "check-venue"])
            self.assertEqual(kwargs["cwd"], bot.root)
            self.assertEqual(kwargs["env"]["PM_PRIVATE_KEY"], KEY)
            self.assertEqual(kwargs["env"]["PM_PREDICT_API_KEY"], FAKE_ENV["YQUANT_PREDICT_FUN_TOKEN"])
            self.assertEqual(kwargs["env"]["PM_PREDICT_ACCOUNT"], signer.TRADING_ADDRESS)
            self.assertEqual(kwargs["timeout"], 60)
            for stream in ("stdin", "stdout", "stderr"):
                self.assertEqual(kwargs[stream], subprocess.DEVNULL)
            self.assertIs(kwargs["shell"], False)
            self.assertIs(kwargs["close_fds"], True)
            self.assertFalse(bot.credentials.exists())
            self.assertNotIn(KEY, output+errors)
            self.assertNotIn("synthetic-api-only", output+errors)
            self.assertEqual(backend.connections, 4)
            self.assertEqual(backend.sessions, backend.closed_sessions)
            for number, message in backend.calls:
                method = message.header.fields[F.member]
                if method == "Hello":
                    continue
                self.assertEqual(message.header.flags, 2)
                signatures = {"GetNameOwner": "s", "ReadAlias": "s", "Get": "ss",
                              "SearchItems": "a{ss}", "OpenSession": "sv",
                              "CreateItem": "a{sv}(oayays)b", "GetSecret": "o", "Close": ""}
                self.assertEqual(message.header.fields.get(F.signature, ""), signatures[method])
                if method == "GetNameOwner":
                    self.assertEqual(message.body, ("org.freedesktop.secrets",))
                elif method == "ReadAlias":
                    self.assertEqual(message.body, ("default",))
                elif method == "Get":
                    self.assertIn(message.body, ((signer.COLLECTION, "Locked"),
                                                (signer.ITEM, "Locked"), (signer.ITEM, "Attributes")))
                elif method == "OpenSession":
                    self.assertEqual(message.body, ("plain", ("s", "")))
                elif method == "GetSecret":
                    self.assertIn(message.body[0], backend.sessions)
                    self.assertEqual(message.header.fields[F.path], ITEM_PATH)
                elif method == "Close":
                    self.assertEqual(message.body, ())
                    self.assertIn(message.header.fields[F.path], backend.sessions)
                self.assertEqual(message.header.fields[F.destination],
                                 "org.freedesktop.DBus" if method == "GetNameOwner" else ":1.400")
                if method == "SearchItems":
                    self.assertEqual(message.body, (signer.ATTRIBUTES,))
                elif method == "CreateItem":
                    self.assertEqual(message.header.fields[F.signature], "a{sv}(oayays)b")
                    properties, secret, replace = message.body
                    self.assertIs(replace, False)
                    self.assertEqual(set(properties), {signer.ITEM+".Label", signer.ITEM+".Attributes"})
                    self.assertEqual(properties[signer.ITEM+".Attributes"], ("a{ss}", signer.ATTRIBUTES))
                    self.assertEqual(secret[1:], (b"", KEY.encode(), signer.CONTENT_TYPE))
            self.assertFalse(set(backend.methods()) & {"Unlock", "Prompt", "Dismiss", "StartServiceByName"})

    def test_missing_existing_locked_duplicate_and_ownership(self):
        cases = ["absent", "exists", "collection-locked", "item-locked", "duplicate",
                 "both-lists", "other-collection", "prefix-boundary", "attributes"]
        expected = ["ABSENT", "EXISTS", "LOCKED", "LOCKED", "DUPLICATE", "DUPLICATE",
                    "OWNERSHIP", "OWNERSHIP", "OWNERSHIP"]
        for case, reason in zip(cases, expected):
            with self.subTest(case=case), WireBackend() as backend:
                backend.key = KEY.encode() if case != "absent" else None
                if case == "collection-locked":
                    backend.collection_locked = True
                elif case == "item-locked":
                    backend.item_locked = True
                elif case == "duplicate":
                    backend.paths = [ITEM_PATH, ITEM_PATH]
                elif case == "both-lists":
                    backend.fault = lambda msg, n: (new_method_return(msg, "aoao", ([ITEM_PATH], [ITEM_PATH]))
                                                    if msg.header.fields[F.member] == "SearchItems"
                                                    else NotImplemented)
                elif case in ("other-collection", "prefix-boundary"):
                    backend.paths = [signer.ROOT+"/collection/other/item" if case == "other-collection"
                                     else signer.LOGIN+"evil/item"]
                elif case == "attributes":
                    backend.attributes["extra"] = "reject"
                with self.assertRaises(signer.Failure) as failure:
                    if case == "exists":
                        signer._create_owned(KEY.encode())
                    else:
                        signer._read_owned()
                self.assertEqual(failure.exception.result["reason"], reason)
                self.assertNotIn("GetSecret", backend.methods())
                self.assertNotIn("CreateItem", backend.methods())

    def test_alias_owner_bus_and_protocol_errors(self):
        for stage in ("alias", "no-owner", "bad-owner", "signature", "variant", "error", "locked-error"):
            with self.subTest(stage=stage), WireBackend() as backend:
                backend.key = KEY.encode()

                def fault(message, number):
                    method = message.header.fields[F.member]
                    if stage == "no-owner" and method == "GetNameOwner":
                        return new_error(message, "org.freedesktop.DBus.Error.NameHasNoOwner", "s", (KEY,))
                    if stage == "bad-owner" and method == "GetNameOwner":
                        return new_method_return(message, "s", ("not-unique",))
                    if stage == "signature" and method == "ReadAlias":
                        return new_method_return(message, "s", (signer.LOGIN,))
                    if stage == "variant" and method == "Get":
                        return new_method_return(message, "v", (("s", "False"),))
                    if stage in ("error", "locked-error") and method == "ReadAlias":
                        name = ("org.freedesktop.Secret.Error.IsLocked" if stage == "locked-error"
                                else "org.freedesktop.DBus.Error.AccessDenied")
                        return new_error(message, name, "s", (KEY,))
                    return NotImplemented

                backend.fault = fault
                if stage == "alias":
                    backend.alias = signer.ROOT + "/collection/other"
                with self.assertRaises(signer.Failure) as failure:
                    signer._read_owned()
                self.assertEqual(failure.exception.result["reason"],
                                 "LOCKED" if stage == "locked-error" else "UNAVAILABLE")
                self.assertNotIn("GetSecret", backend.methods())
                self.assertNotIn(KEY, str(failure.exception))
        for address in ("", "autolaunch:", "tcp:host=localhost", "unix:path=/a;unix:path=/b"):
            with patch.dict(os.environ, {"DBUS_SESSION_BUS_ADDRESS": address}, clear=True), patch.object(
                signer, "prep_socket", side_effect=AssertionError("MUST_NOT_CONNECT")
            ):
                with self.assertRaises(signer.Failure):
                    signer._read_owned()

    def test_create_prompt_unknown_result_readback_and_cleanup_failures(self):
        for stage in ("prompt", "create-error", "create-timeout", "readback", "changed-path",
                      "close-create", "close-read"):
            with self.subTest(stage=stage), WireBackend() as backend:
                backend.prompt = signer.ROOT+"/prompt/p1" if stage == "prompt" else "/"

                def fault(message, number):
                    method = message.header.fields[F.member]
                    if stage in ("create-error", "create-timeout") and method == "CreateItem":
                        # A write may have happened even though the reply is an error/timeout.
                        backend.key = KEY.encode()
                        if stage == "create-timeout":
                            return None
                        return new_error(message, "org.freedesktop.DBus.Error.Failed", "s", (KEY,))
                    if stage == "readback" and method == "GetSecret":
                        return new_error(message, "org.freedesktop.DBus.Error.Failed", "s", (KEY,))
                    if stage == "changed-path" and method == "SearchItems" and number == 3:
                        return new_method_return(message, "aoao", ([ITEM_PATH+"2"], []))
                    if method == "Close" and ((stage == "close-create" and number == 2)
                                              or (stage == "close-read" and number == 3)):
                        return new_error(message, "org.freedesktop.DBus.Error.Failed", "s", (KEY,))
                    return NotImplemented

                backend.fault = fault
                with patch.object(signer, "_tty_fd", return_value=0), patch.object(
                    signer, "read_key_tty", return_value=KEY
                ), patch.object(signer.subprocess, "run", side_effect=AssertionError("NO_CHILD")):
                    code, output, errors = run_main(["enroll"])
                self.assertEqual((code, output), (1, ""))
                reason = json.loads(errors)["reason"]
                self.assertEqual(reason, "PROMPT_REQUIRED" if stage == "prompt" else
                                 "CREATE_UNKNOWN" if stage.startswith("create-") else "READBACK")
                self.assertNotIn(KEY, errors)
                self.assertEqual(backend.methods().count("CreateItem"), 1)
                self.assertFalse(set(backend.methods()) & {"Delete", "Unlock", "Prompt", "Dismiss"})

    def test_corrupt_secret_session_parameters_content_and_format(self):
        for stage in ("session", "parameters", "content", "uppercase", "prefix", "zero", "mismatch"):
            with self.subTest(stage=stage), WireBackend() as backend:
                backend.key = KEY.encode()

                def fault(message, number):
                    if message.header.fields[F.member] != "GetSecret":
                        return NotImplemented
                    secret = [message.body[0], b"", KEY.encode(), signer.CONTENT_TYPE]
                    if stage == "session":
                        secret[0] += "wrong"
                    elif stage == "parameters":
                        secret[1] = b"bad"
                    elif stage == "content":
                        secret[3] = "application/octet-stream"
                    elif stage == "uppercase":
                        secret[2] = ("a"*64).upper().encode()
                    elif stage == "prefix":
                        secret[2] = ("0x"+KEY).encode()
                    elif stage == "zero":
                        secret[2] = b"0"*64
                    else:
                        secret[2] = ("0"*63+"2").encode()
                    return new_method_return(message, "(oayays)", (tuple(secret),))

                backend.fault = fault
                with self.assertRaises(signer.Failure) as failure:
                    signer._read_owned()
                self.assertEqual(failure.exception.result["reason"], "CORRUPT")
                self.assertEqual(backend.sessions, backend.closed_sessions)

    def test_sessions_shapes_locked_list_and_cleanup_preserve_first_failure(self):
        for stage in ("locked-list", "session-output", "session-root", "secret-shape", "message-type",
                      "corrupt-and-close", "create-bad-path", "race-existing"):
            with self.subTest(stage=stage), WireBackend() as backend:
                backend.key = KEY.encode() if stage not in ("create-bad-path", "race-existing") else None
                def fault(message, number):
                    method = message.header.fields[F.member]
                    if stage == "locked-list" and method == "SearchItems":
                        return new_method_return(message, "aoao", ([], [ITEM_PATH]))
                    if stage == "session-output" and method == "OpenSession":
                        return new_method_return(message, "vo", (("s", "unsupported"), "/session/s1"))
                    if stage == "session-root" and method == "OpenSession":
                        return new_method_return(message, "vo", (("s", ""), "/"))
                    if stage == "message-type" and method == "ReadAlias":
                        response = new_method_return(message, "o", (signer.LOGIN,))
                        response.header.message_type = MessageType.signal
                        return response
                    if stage == "secret-shape" and method == "GetSecret":
                        return new_method_return(message, "s", ("not-a-secret-structure",))
                    if stage == "corrupt-and-close" and method == "GetSecret":
                        return new_method_return(message, "(oayays)",
                                                 ((message.body[0], b"", b"0"*64, signer.CONTENT_TYPE),))
                    if stage == "corrupt-and-close" and method == "Close":
                        return new_error(message, "org.freedesktop.DBus.Error.Failed", "s", (KEY,))
                    if stage == "create-bad-path" and method == "CreateItem":
                        return new_method_return(message, "oo", ("/", "/"))
                    if stage == "race-existing" and method == "SearchItems" and number == 2:
                        return new_method_return(message, "aoao", ([ITEM_PATH], []))
                    return NotImplemented
                backend.fault = fault
                with self.assertRaises(signer.Failure) as failure:
                    if stage == "create-bad-path":
                        signer._create_owned(KEY.encode())
                    elif stage == "race-existing":
                        with patch.object(signer, "_tty_fd", return_value=0), patch.object(
                            signer, "read_key_tty", return_value=KEY
                        ):
                            signer.enroll()
                    else:
                        signer._read_owned()
                expected = {"locked-list": "LOCKED", "corrupt-and-close": "CORRUPT",
                            "create-bad-path": "CREATE_UNKNOWN", "race-existing": "EXISTS"}
                self.assertEqual(failure.exception.result["reason"], expected.get(stage, "UNAVAILABLE"))
                if stage == "race-existing":
                    self.assertNotIn("CreateItem", backend.methods())

    def test_disconnect_failure_after_create_is_unconfirmed_readback(self):
        for fail_before_socket_close in (False, True):
            with self.subTest(early=fail_before_socket_close), WireBackend() as backend:
                real_close = signer.DBusConnection.close

                def close_then_fail(connection):
                    if fail_before_socket_close:
                        connection.selector.close()
                    else:
                        real_close(connection)
                    raise OSError("synthetic-disconnect-error")

                with patch.object(signer.DBusConnection, "close", close_then_fail):
                    with self.assertRaises(signer.Failure) as failure:
                        signer._create_owned(KEY.encode())
                self.assertEqual(failure.exception.result,
                                 {"code": "STORAGE_FAILURE", "step": "readback", "reason": "READBACK"})
                self.assertEqual(backend.methods().count("CreateItem"), 1)
                self.assertEqual(backend.sessions, backend.closed_sessions)

    def test_invalid_enroll_identity_stops_before_create(self):
        for key in ("0"*64, "f"*64, "bad", "0"*63+"2"):
            with self.subTest(kind=key[-1]), WireBackend() as backend, patch.object(
                signer, "_tty_fd", return_value=0
            ), patch.object(signer, "read_key_tty", return_value=key):
                code, _, _ = run_main(["enroll"])
                self.assertEqual(code, 2)
                self.assertEqual(backend.connections, 1)
                self.assertNotIn("CreateItem", backend.methods())


class TemporaryBot:
    """Scratch-only public config and a no-network executable child; no secret files."""

    def __init__(self, stdout=b"SYNTHETIC_CHILD_OUTPUT_MUST_BE_SUPPRESSED\n", stderr=b"",
                 returncode=0, sign_in=False, stall=False):
        self.stdout, self.stderr, self.returncode = stdout, stderr, returncode
        self.sign_in, self.stall = sign_in, stall

    def __enter__(self):
        self.directory = tempfile.TemporaryDirectory(dir=TEST_SCRATCH)
        self.root = Path(self.directory.name)
        self.binary = self.root / "target/debug/polymarket-toolkits"
        self.binary.parent.mkdir(parents=True)
        self.binary.write_text(
            "#!/usr/bin/python3\nimport os, sys\n"
            f"assert sys.argv[1:] == ['--config', {str(self.root / 'public.json')!r}, "
            f"'--credentials', {str(self.root / 'tools/predictfun/.keyring-no-credentials')!r}, "
            f"'--venue', 'predict-fun', 'check-venue'] + {['--sign-in'] if self.sign_in else []!r}\n"
            f"assert os.getcwd() == {str(self.root)!r}\n"
            f"assert os.environ['PM_PREDICT_ACCOUNT'] == {signer.TRADING_ADDRESS!r}\n"
            "assert os.environ['PM_PRIVATE_KEY'] == '0'*63+'1'\n"
            "assert os.environ['PM_PREDICT_API_KEY'] == 'synthetic-api-only'\n"
            "assert 'YQUANT_PREDICT_FUN_TOKEN' not in os.environ\n"
            "assert 'UNRELATED_SECRET' not in os.environ\n"
            f"sys.stdout.buffer.write({self.stdout!r})\n"
            f"sys.stderr.buffer.write({self.stderr!r})\n"
            "for name in ('PM_PRIVATE_KEY', 'PM_PREDICT_API_KEY', 'HTTPS_PROXY'):\n"
            "    print(os.environ.get(name, ''), flush=True)\n"
            "    print(os.environ.get(name, ''), file=sys.stderr, flush=True)\n"
            + ("import time\ntime.sleep(100)\n" if self.stall else "")
            + (f"os.kill(os.getpid(), {-self.returncode})\n" if self.returncode < 0
               else f"sys.exit({self.returncode})\n"),
            encoding="utf-8",
        )
        self.binary.chmod(0o700)
        self.config = self.root / "public.json"
        self.config.write_text(json.dumps(PUBLIC_CONFIG), encoding="utf-8")
        self.credentials = self.root / "tools/predictfun/.keyring-no-credentials"
        self.bot_patch = patch.object(signer, "BOT", self.root)
        self.bot_patch.start()
        return self

    def __exit__(self, *args):
        self.bot_patch.stop()
        self.directory.cleanup()


class LaunchBoundaryTests(unittest.TestCase):
    def test_reject_public_paths_and_config_before_lookup(self):
        variants = [None, [], {}, {**PUBLIC_CONFIG, "venue": "polymarket"},
                    {**PUBLIC_CONFIG, "predict_fun": {"network": "testnet"}},
                    {**PUBLIC_CONFIG, "bot": {"enable_trading": True, "mock_trading": True}},
                    {**PUBLIC_CONFIG, "bot": {"enable_trading": False, "mock_trading": False}},
                    {**PUBLIC_CONFIG, "bot": {"enable_trading": 0, "mock_trading": 1}},
                    {**PUBLIC_CONFIG, "credentials": {}},
                    {**PUBLIC_CONFIG, "nested": {"credentials": {}}}]
        with TemporaryBot() as bot, patch.object(signer, "_read_owned") as lookup:
            for data in variants:
                bot.config.write_text(json.dumps(data))
                code, _, errors = run_main(["check", "--config", str(bot.config)])
                self.assertEqual(code, 2)
                self.assertIn("PUBLIC_CONFIG", errors)
            for raw in ('{"venue":"predict_fun","venue":"predict_fun"}', "not-json"):
                bot.config.write_text(raw)
                self.assertEqual(run_main(["check", "--config", str(bot.config)])[0], 2)
            for path in (str(bot.root), str(bot.root/"missing.json"), str(bot.root/".env"),
                         str(bot.root/"credentials.yaml")):
                self.assertEqual(run_main(["check", "--config", path])[0], 2)
            lookup.assert_not_called()

    def test_reject_missing_child_and_existing_reserved_credentials(self):
        with TemporaryBot() as bot, patch.object(signer, "_read_owned") as lookup:
            bot.binary.chmod(0o600)
            self.assertIn("CHILD_PATH", run_main(["check", "--config", str(bot.config)])[2])
            bot.binary.chmod(0o700)
            bot.credentials.parent.mkdir(parents=True)
            bot.credentials.symlink_to(bot.root / "missing-credential-target")
            self.assertIn("CREDENTIAL_PATH_PRESENT",
                          run_main(["check", "--config", str(bot.config)])[2])
            bot.credentials.unlink()
            bot.binary.unlink()
            self.assertIn("CHILD_PATH", run_main(["check", "--config", str(bot.config)])[2])
            # Also cover an injected lexists result; never touch the production reserved path.
            bot.binary.write_text("#!/usr/bin/python3\n")
            bot.binary.chmod(0o700)
            with patch.object(os.path, "lexists", return_value=True):
                self.assertIn("CREDENTIAL_PATH_PRESENT",
                              run_main(["check", "--config", str(bot.config)])[2])
            lookup.assert_not_called()

    def test_api_conflicts_private_key_presence_and_account_before_lookup(self):
        variants = [({"PM_PRIVATE_KEY": ""}, "PRIVATE_KEY_PRESENT"),
                    ({"PM_PREDICT_ACCOUNT": ADDRESS}, "ACCOUNT"),
                    ({"YQUANT_PREDICT_FUN_TOKEN": ""}, "API_SOURCE"),
                    ({"YQUANT_PREDICT_FUN_TOKEN": " \n "}, "API_SOURCE"),
                    ({"PM_PREDICT_API_KEY": "different"}, "API_CONFLICT")]
        with TemporaryBot() as bot, patch.object(signer, "_read_owned") as lookup:
            for delta, reason in variants:
                with patch.dict(os.environ, {**FAKE_ENV, **delta}, clear=True):
                    code, _, errors = run_main(["check", "--config", str(bot.config)])
                    self.assertEqual(code, 2)
                    self.assertEqual(json.loads(errors)["reason"], reason)
            lookup.assert_not_called()
        class PresenceOnly(dict):
            def __getitem__(self, name):
                if name == "PM_PRIVATE_KEY":
                    raise AssertionError("must not read competing key")
                return super().__getitem__(name)
        with self.assertRaises(signer.Failure):
            signer._check_environment(PresenceOnly(PM_PRIVATE_KEY="not-read"))

    def test_child_env_allowlist_parent_unchanged_and_no_output_leak(self):
        with WireBackend() as backend, TemporaryBot() as bot:
            backend.key = KEY.encode()
            sentinels = {"UNRELATED_SECRET": "SYNTHETIC_UNRELATED", "PM_API_SECRET": "OTHER_PM",
                         "LD_PRELOAD": "BAD_LOAD", "PYTHONPATH": "BAD_PATH", "PYTHONHOME": "BAD_HOME",
                         "PM_PREDICT_API_KEY": "synthetic-api-only", "HTTPS_PROXY": "SYNTHETIC_PROXY",
                         "SSL_CERT_FILE": "SYNTHETIC_CA", "HOME": str(Path.home()), "TZ": "UTC"}
            with patch.dict(os.environ, sentinels):
                before = dict(os.environ)
                captured = []
                def child(*args, **kwargs):
                    captured.append(copy.deepcopy(kwargs["env"]))
                    return subprocess.CompletedProcess(args[0], 0)
                with patch.object(signer.subprocess, "run", side_effect=child):
                    code, output, errors = run_main(["check", "--config", str(bot.config)])
                self.assertEqual((code, errors), (0, ""))
                self.assertEqual(dict(os.environ), before)
                env = captured[0]
                self.assertEqual(set(env), {"HOME", "TZ", "HTTPS_PROXY", "SSL_CERT_FILE", "PATH",
                                           "RUST_LOG", "PM_PRIVATE_KEY", "PM_PREDICT_API_KEY", "PM_PREDICT_ACCOUNT"})
                self.assertEqual(env["HTTPS_PROXY"], "SYNTHETIC_PROXY")
                self.assertEqual(env["SSL_CERT_FILE"], "SYNTHETIC_CA")
                self.assertEqual((env["PATH"], env["RUST_LOG"]), ("/usr/bin:/bin", "warn"))
                for value in [KEY, *sentinels.values(), "synthetic-api-only"]:
                    self.assertNotIn(value, output+errors)

    def test_child_start_exit_signal_timeout_are_sanitized_and_not_retried(self):
        for mode, reason in [("start", "START"), ("exit", "EXIT"), ("signal", "SIGNAL"), ("timeout", "TIMEOUT")]:
            with self.subTest(mode=mode), WireBackend() as backend, TemporaryBot() as bot:
                backend.key = KEY.encode()
                exception = (OSError(KEY) if mode == "start" else
                             subprocess.TimeoutExpired([KEY], 60, output=KEY, stderr=KEY)
                             if mode == "timeout" else None)
                with patch.object(signer.subprocess, "run", side_effect=exception,
                                  return_value=subprocess.CompletedProcess([], -15 if mode == "signal" else 1)) as child:
                    code, output, errors = run_main(["check", "--config", str(bot.config)])
                self.assertEqual((code, output), (1, ""))
                self.assertEqual(json.loads(errors)["reason"], reason)
                self.assertNotIn(KEY, errors)
                self.assertEqual(child.call_count, 1)
                # Candidate clears its transient child environment even on failure.
                self.assertEqual(child.call_args.kwargs["env"], {})

    def test_backend_failure_never_starts_child(self):
        with WireBackend(), TemporaryBot() as bot, patch.object(signer.subprocess, "run") as child:
            code, output, errors = run_main(["check", "--config", str(bot.config)])
            self.assertEqual((code, output), (1, ""))
            self.assertIn("ABSENT", errors)
            child.assert_not_called()


class SignInTests(unittest.TestCase):
    signing = b"signing        ok: 1 share would cost 0.25 USDT (not submitted)\n"
    login = b"sign-in        ok: API identifies you as " + signer.TRADING_ADDRESS.encode() + b"\n"
    success = ("Sign-in verified for Predict Account " + signer.TRADING_ADDRESS
               + "; no trading authorization implied.\n")

    def consume(self, stdout, stderr=b"", reason=None, returncode=0, stall=False,
                effect=None, parse_error=False):
        with WireBackend() as backend, TemporaryBot(stdout, stderr, returncode, True, stall) as bot:
            backend.key = KEY.encode()
            real_run, real_open, real_match = subprocess.run, io.open, signer.re.fullmatch
            captured, processes, environments = [], [], []
            def match(pattern, value):
                if parse_error and isinstance(pattern, bytes) and pattern.startswith(b"sign"):
                    raise RuntimeError(KEY + "synthetic-api-only-jwt-signature-SYNTHETIC_PROXY")
                return real_match(pattern, value)
            def child(*args, **kwargs):
                environments.append(kwargs["env"])
                captured.append((copy.deepcopy(args), copy.deepcopy(kwargs)))
                if effect is not None:
                    raise effect
                if stall:
                    kwargs["timeout"] = 0.15
                return real_run(*args, **kwargs)
            def opened(file, mode="r", *args, **kwargs):
                self.assertFalse(any(flag in mode for flag in "wax+"), "unexpected output file")
                return real_open(file, mode, *args, **kwargs)
            real_popen = subprocess.Popen
            def popen(*args, **kwargs):
                process = real_popen(*args, **kwargs)
                processes.append(process)
                return process
            with patch.dict(os.environ, {"HTTPS_PROXY": "SYNTHETIC_PROXY"}):
                before = dict(os.environ)
                with patch.object(signer.subprocess, "run", side_effect=child), patch.object(
                    signer.subprocess, "Popen", side_effect=popen
                ), patch("io.open", side_effect=opened), patch("builtins.open", side_effect=opened), patch(
                    "logging.Logger._log"
                ) as log, patch.object(signer.re, "fullmatch", side_effect=match):
                    code, output, errors = run_main(["check", "--config", str(bot.config), "--sign-in"])
                self.assertEqual(dict(os.environ), before)
            self.assertEqual((code, output), (1, "") if reason else (0, self.success))
            self.assertEqual(json.loads(errors) if reason else errors,
                             {"code": "LAUNCH_FAILED", "step": "launch", "reason": reason} if reason else "")
            self.assertEqual(len(captured), 1)
            args, kwargs = captured[0]
            self.assertEqual(args[0][-2:], ["check-venue", "--sign-in"])
            self.assertEqual((kwargs["cwd"], kwargs["timeout"]), (bot.root, 60))
            self.assertEqual((kwargs["stdin"], kwargs["stdout"], kwargs["stderr"]),
                             (subprocess.DEVNULL, subprocess.PIPE, subprocess.PIPE))
            self.assertEqual(backend.methods().count("GetSecret"), 1)
            self.assertEqual(environments, [{}])
            self.assertEqual(set(kwargs["env"]), {"HTTPS_PROXY", "PATH", "RUST_LOG",
                                                 "PM_PRIVATE_KEY", "PM_PREDICT_API_KEY", "PM_PREDICT_ACCOUNT"})
            log.assert_not_called()
            self.assertFalse(set(backend.methods()) & {"CreateItem", "Unlock", "Prompt"})
            self.assertTrue(all(process.poll() is not None for process in processes))
            if stall:
                self.assertEqual(len(processes), 1)
                self.assertEqual(processes[0].returncode, -signal.SIGKILL)

    def test_sign_in_actual_child_success_and_address_case(self):
        for address in (signer.TRADING_ADDRESS, "0x" + signer.TRADING_ADDRESS[2:].upper()):
            for amount in (b"0", b"2", b"0.125"):
                self.consume(self.signing.replace(b"0.25", amount) + self.login.replace(
                    signer.TRADING_ADDRESS.encode(), address.encode()) + b"synthetic-jwt-signature\n",
                    b"synthetic-stderr-jwt-signature\n")

    def test_sign_in_invalid_evidence_table(self):
        good = self.signing + self.login
        wrong = self.login.replace(signer.TRADING_ADDRESS.encode(), ADDRESS.encode())
        cases = [b"", b"signer         none (synthetic)\n", self.signing, self.login,
                 self.login + self.signing, good + self.login, good + self.signing,
                 self.signing + wrong + self.login, good + b"signing invalid\n",
                 good + b"sign-in invalid\n"]
        for marker in (self.signing, self.login):
            for bad in (b"prefix " + marker, b"\x1b[32m" + marker, marker.upper(),
                        marker.rstrip(b"\n") + b" suffix\n", marker.replace(b"\n", b"\r\n")):
                cases.append(good.replace(marker, bad))
        for amount in (b"-1", b"+1", b"1e3", b"NaN", b"Infinity", b".1", b"1.", "１".encode()):
            cases.append(good.replace(b"0.25", amount))
        for address in (b"0x123", b"0x" + b"a"*41, b"0x" + b"g"*40,
                        b" " + signer.TRADING_ADDRESS.encode(), signer.TRADING_ADDRESS.encode() + b" "):
            cases.append(self.signing + self.login.replace(signer.TRADING_ADDRESS.encode(), address))
        for index, stdout in enumerate(cases):
            with self.subTest(case=index):
                self.consume(stdout, good, "EVIDENCE_INVALID")
        self.consume(self.signing + wrong, reason="AUTH_ACCOUNT_MISMATCH")

    def test_sign_in_child_failure_precedence_and_real_timeout_reaping(self):
        good = self.signing + self.login
        self.consume(good, reason="EXIT", returncode=1)
        self.consume(good, reason="SIGNAL", returncode=-signal.SIGTERM)
        self.consume(good, reason="TIMEOUT", stall=True)
        self.consume(good, reason="START", effect=OSError(
            KEY + "synthetic-api-only-jwt-signature-SYNTHETIC_PROXY"))
        self.consume(good, reason="EVIDENCE_INVALID", parse_error=True)
        self.consume(good, reason="TIMEOUT", effect=subprocess.TimeoutExpired(
            ["synthetic-argv"], 60, output=KEY.encode(), stderr=b"synthetic-jwt-signature"))

    def test_sign_in_backend_rejections_never_start_child(self):
        cases = [("missing", "ABSENT"), ("locked", "LOCKED"), ("duplicate", "DUPLICATE"),
                 ("ownership", "OWNERSHIP"), ("schema", "OWNERSHIP"),
                 ("content", "CORRUPT"), ("key", "CORRUPT")]
        for case, reason in cases:
            with self.subTest(case=case), WireBackend() as backend, TemporaryBot() as bot:
                backend.key = None if case == "missing" else b"0"*64 if case == "key" else KEY.encode()
                backend.collection_locked = case == "locked"
                if case in ("duplicate", "ownership"):
                    backend.paths = [ITEM_PATH, ITEM_PATH] if case == "duplicate" else ["/other/item"]
                if case == "schema":
                    backend.attributes["xdg:schema"] = "wrong"
                if case == "content":
                    backend.fault = lambda msg, n: (new_method_return(msg, "(oayays)",
                        ((msg.body[0], b"", KEY.encode(), "text/html"),))
                        if msg.header.fields[F.member] == "GetSecret" else NotImplemented)
                with patch.object(signer.subprocess, "run") as child:
                    code, output, errors = run_main(["check", "--config", str(bot.config), "--sign-in"])
                self.assertEqual((code, output, json.loads(errors)["reason"]), (1, "", reason))
                child.assert_not_called()
                self.assertFalse(set(backend.methods()) & {"CreateItem", "Unlock", "Prompt"})

    def test_sign_in_reuses_prelaunch_rejections(self):
        # Re-run the existing boundary matrix through the explicit-login consumer.
        original = run_main
        def signed(arguments):
            return original(arguments + ["--sign-in"] if arguments[0] == "check" else arguments)
        with patch.dict(os.environ, FAKE_ENV, clear=True), patch(__name__ + ".run_main", signed), patch.object(
            signer.subprocess, "run"
        ) as child:
            boundary = LaunchBoundaryTests()
            boundary.test_reject_public_paths_and_config_before_lookup()
            boundary.test_reject_missing_child_and_existing_reserved_credentials()
            boundary.test_api_conflicts_private_key_presence_and_account_before_lookup()
            boundary.test_backend_failure_never_starts_child()
            child.assert_not_called()


def _pty_consumer(slave, master, report, mode):
    os.close(master)
    sys.stdin = os.fdopen(os.dup(slave), "r")
    sys.stderr = os.fdopen(os.dup(slave), "w", buffering=1)
    sys.stdout = os.fdopen(os.dup(slave), "w", buffering=1)
    with WireBackend() as backend:
        result = signer.main(["enroll"])
        report.send((result, backend.methods(), termios.tcgetattr(slave)))
    report.close()


class TtyConsumerTests(unittest.TestCase):
    def test_actual_main_enroll_pty_mask_and_restore_all_paths(self):
        for mode in ("valid", "multiline", "overlong", "eof", "SIGINT", "SIGTERM", "SIGHUP"):
            with self.subTest(mode=mode):
                master, slave = pty.openpty()
                original = termios.tcgetattr(slave)
                parent, child = multiprocessing.get_context("fork").Pipe(duplex=False)
                process = multiprocessing.get_context("fork").Process(
                    target=_pty_consumer, args=(slave, master, child, mode))
                process.start()
                child.close()
                output = b""
                try:
                    deadline = time.monotonic()+5
                    while b"local TTY only): " not in output and time.monotonic() < deadline:
                        if select.select([master], [], [], 0.1)[0]:
                            output += os.read(master, 4096)
                    self.assertIn(b"local TTY only): ", output)
                    self.assertFalse(termios.tcgetattr(slave)[3] & (termios.ECHO | termios.ECHONL))
                    if mode == "valid":
                        os.write(master, KEY.encode()+b"\n")
                    elif mode == "multiline":
                        os.write(master, KEY.encode()+b"\n"+KEY.encode()+b"\n")
                    elif mode == "overlong":
                        os.write(master, KEY.encode()+b"012345\n")
                    elif mode == "eof":
                        os.write(master, b"\x04")
                    else:
                        os.kill(process.pid, getattr(signal, mode))
                    self.assertTrue(parent.poll(5), "candidate TTY did not exit")
                    code, methods, restored = parent.recv()
                    process.join(2)
                    self.assertFalse(process.is_alive())
                    self.assertEqual(process.exitcode, 0)
                    while select.select([master], [], [], 0)[0]:
                        output += os.read(master, 4096)
                    self.assertNotIn(KEY.encode(), output)
                    self.assertEqual(restored, original)
                    self.assertEqual(termios.tcgetattr(slave), original)
                    self.assertEqual(code, 0 if mode == "valid" else 2)
                    self.assertEqual(methods.count("CreateItem"), 1 if mode == "valid" else 0)
                    if mode == "valid":
                        self.assertIn(b"independently verified", output)
                finally:
                    if process.is_alive():
                        process.kill()
                        process.join()
                    parent.close()
                    os.close(master)
                    os.close(slave)


def _stall_consumer(report, mode):
    started = time.monotonic()
    previous = signal.getsignal(signal.SIGALRM)
    with WireBackend() as backend:
        backend.key = KEY.encode()
        with ExitStack() as stack:
            if mode in ("connect", "auth"):
                # Exercise the actual prep_socket and its exception/socket cleanup.
                stack.enter_context(patch.object(signer, "prep_socket", __import__(
                    "jeepney.io.blocking", fromlist=["prep_socket"]).prep_socket))
                class StalledSocket:
                    closed = False
                    def settimeout(self, value):
                        pass
                    def connect(self, address):
                        if mode == "connect":
                            time.sleep(100)
                    def send(self, data):
                        return len(data)
                    def sendall(self, data):
                        pass
                    def recv(self, size):
                        time.sleep(100)
                    def close(self):
                        self.closed = True
                stalled = StalledSocket()
                stack.enter_context(patch("jeepney.io.blocking.socket.socket", return_value=stalled))
            elif mode == "sendall":
                class SendStallSocket:
                    def __init__(self, raw):
                        self.raw = raw
                    def __getattr__(self, name):
                        return getattr(self.raw, name)
                    def sendall(self, data):
                        message = Message.from_buffer(data)
                        if message.header.fields[F.member] == "ReadAlias":
                            time.sleep(100)
                        return self.raw.sendall(data)
                def connect(*args, **kwargs):
                    return SendStallSocket(backend.connect(*args, **kwargs))
                stack.enter_context(patch.object(signer, "prep_socket", connect))
            elif mode == "unrelated":
                real_receive = signer.DBusConnection.receive
                real_send = signer.DBusConnection.send_message
                def send(connection, message, serial=None):
                    if message.header.fields[F.member] == "ReadAlias":
                        connection.stall_request = message
                    return real_send(connection, message, serial)
                def receive(connection, *, timeout=None):
                    if hasattr(connection, "stall_request"):
                        response = new_method_return(connection.stall_request)
                        response.header.fields[F.reply_serial] = 999999
                        return response
                    return real_receive(connection, timeout=timeout)
                stack.enter_context(patch.object(signer.DBusConnection, "send_message", send))
                stack.enter_context(patch.object(signer.DBusConnection, "receive", receive))
            else:
                def fault(message, number):
                    method = message.header.fields[F.member]
                    if (mode == "hello" and method == "Hello") or (mode == "reply" and method == "ReadAlias"):
                        return None
                    if mode == "close" and method == "Close":
                        return None
                    if mode == "open-session" and method == "OpenSession":
                        return None
                    return NotImplemented
                backend.fault = fault
            try:
                signer._read_owned()
                reason = "UNEXPECTED_SUCCESS"
            except signer.Failure as error:
                reason = error.result["reason"]
            restored = signal.getsignal(signal.SIGALRM) == previous and signal.getitimer(signal.ITIMER_REAL) == (0, 0)
            socket_closed = stalled.closed if mode in ("connect", "auth") else all(
                client.fileno() == -1 for client in backend.clients)
            report.send((reason, time.monotonic()-started, restored, socket_closed,
                         backend.methods().count("Close")))
    report.close()


class TotalBudgetTests(unittest.TestCase):
    def test_real_wall_clock_bounds_connect_auth_hello_send_and_unrelated_messages(self):
        for mode in ("connect", "auth", "hello", "sendall", "unrelated", "reply", "open-session", "close"):
            with self.subTest(mode=mode):
                parent, child = multiprocessing.get_context("fork").Pipe(duplex=False)
                process = multiprocessing.get_context("fork").Process(target=_stall_consumer, args=(child, mode))
                process.start()
                child.close()
                try:
                    self.assertTrue(parent.poll(12), "test watchdog, NOT candidate timeout")
                    reason, elapsed, restored, closed, close_count = parent.recv()
                    process.join(2)
                    self.assertFalse(process.is_alive())
                    self.assertEqual(process.exitcode, 0)
                    self.assertTrue(restored)
                    self.assertTrue(closed)
                    self.assertLessEqual(elapsed, 11)
                    self.assertEqual(reason, "UNAVAILABLE" if mode == "close" else "TIMEOUT")
                    if mode in ("connect", "auth", "hello", "sendall", "unrelated"):
                        self.assertGreaterEqual(elapsed, 8.8, "must measure candidate total deadline")
                    if mode == "close":
                        self.assertGreaterEqual(elapsed, 0.9)
                        self.assertLess(elapsed, 2)
                        self.assertEqual(close_count, 1)
                    if mode == "open-session":
                        self.assertEqual(close_count, 0)
                finally:
                    if process.is_alive():
                        process.kill()
                        process.join()
                    parent.close()

    def test_non_main_thread_and_existing_timer_are_rejected_without_connection(self):
        failures = []
        with patch.object(signer, "prep_socket") as connect:
            def run():
                try:
                    signer._read_owned()
                except signer.Failure as error:
                    failures.append(error.result["reason"])
            thread = threading.Thread(target=run)
            thread.start()
            thread.join(2)
            self.assertEqual(failures, ["UNAVAILABLE"])
            previous = signal.getsignal(signal.SIGALRM)
            signal.setitimer(signal.ITIMER_REAL, 30)
            try:
                with self.assertRaises(signer.Failure):
                    signer._read_owned()
                self.assertGreater(signal.getitimer(signal.ITIMER_REAL)[0], 20)
                self.assertEqual(signal.getsignal(signal.SIGALRM), previous)
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0)
            connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
