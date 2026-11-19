"""Offline flock/guard tests. Fakes patch driver transport, not a parallel guard."""
from __future__ import annotations

import copy
import datetime as dt
import multiprocessing
import os
from pathlib import Path
import socket
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import btc5m_live_events as events
import btc5m_live_guard as guard

ACCOUNT = {"venue": "PREDICT_FUN", "chain_id": 56, "account_mode": "predict-account",
           "trading_address": events.TRADING_ADDRESS, "signer_address": events.SIGNER_ADDRESS}


def public_inputs():
    now = dt.datetime.now(dt.timezone.utc)
    stamp = lambda value: value.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    gas = {"payer_address": ACCOUNT["trading_address"], "minimum_balance_atoms": "1", "maximum_consumption_atoms": "2"}
    context = {"schema_version": "btc5m-live-context/1", "run_id": "offline-run", "risk_window_id": "offline-window",
               "authorization_ref": "offline-auth", "operations_mode": "supervised", "account_identity": copy.deepcopy(ACCOUNT),
               "started_at": stamp(now - dt.timedelta(seconds=1)), "deadline_at": stamp(now + dt.timedelta(minutes=5)),
               "candidate_digest": "a" * 64, "asset_units": {"collateral_symbol": "USDT", "collateral_decimals": 18,
                    "share_decimals": 18, "gas_symbol": "BNB", "gas_decimals": 18},
               "gas_policy": gas, "recovery_policy": "COLD_START_REFUSE", "cash_recovery_policy": "MANUAL_PAUSE"}
    activation = {key: copy.deepcopy(context[key]) for key in ("authorization_ref", "risk_window_id", "operations_mode",
                  "account_identity", "deadline_at", "candidate_digest", "gas_policy")}
    activation.update(schema_version="btc5m-live-activation/1", not_before=stamp(now - dt.timedelta(minutes=1)),
                      takeover_ref="offline-human", storage_binding={"deployment_id": "offline-deployment",
                          "database": "tradingagents", "collection": "07_trading_btc5m_live_guard",
                          "credential_env": guard.CREDENTIAL_ENV,
                          "endpoint_id": {"scheme": "mongodb", "hosts": ["offline.invalid:27017"], "replica_set": None, "tls": False}})
    return activation, context


class Duplicate(Exception):
    code = 11000


class FakeCollection:
    """Storage only. All guard decisions remain in the production guard module."""

    def __init__(self):
        self.documents = {}
        self.calls = []
        self.fault = None

    def insert_one(self, document):
        self.calls.append(("insert", copy.deepcopy(document)))
        if self.fault == "timeout":
            self.documents[document["_id"]] = copy.deepcopy(document)
            raise TimeoutError()
        if document["_id"] in self.documents:
            raise Duplicate()
        self.documents[document["_id"]] = copy.deepcopy(document)
        return Mock(acknowledged=self.fault != "unacknowledged")

    def find_one(self, query):
        self.calls.append(("read", copy.deepcopy(query)))
        if self.fault == "read_timeout":
            raise TimeoutError()
        result = copy.deepcopy(self.documents.get(query["_id"]))
        if self.fault == "missing":
            return None
        if self.fault == "partial":
            del result["scope"]
        if self.fault == "version":
            result["schema_version"] = "unknown"
        if self.fault == "identity":
            result["risk_window_id"] = "other"
        return result


def lock_worker(directory, label, started, release, result):
    """Two real isolated test processes compete for the original flock path."""
    guard.LOCK_DIRECTORY = Path(directory)
    # These labels deliberately differ, but must never influence account scope.
    run_id, risk_window_id, operations_mode = label
    started.wait(5)
    lock = guard.AccountLock(ACCOUNT, "signed-future-live")
    try:
        lock.acquire()
        result.send(("held", run_id, risk_window_id, operations_mode))
        release.wait(5)
    except guard.GuardRefused as error:
        result.send((error.reason, run_id, risk_window_id, operations_mode))
    finally:
        lock.close()
        result.close()


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="btc5m-l2-guard-", dir=os.environ["TMPDIR"])
        self.root = Path(self.temporary.name)
        self.lock_directory = self.root / "locks"
        self.patchers = [patch.object(guard, "LOCK_DIRECTORY", self.lock_directory),
                        patch.object(socket.socket, "connect", side_effect=AssertionError("real network")),
                        patch.object(subprocess, "Popen", side_effect=AssertionError("business child"))]
        self.lock_path_patch, self.network, self.child = [item.start() for item in self.patchers]
        self.activation, self.context = public_inputs()
        self.collection = FakeCollection()
        self.database = Mock()
        self.database.list_collection_names.return_value = ["07_trading_btc5m_live_guard"]
        self.database.get_collection.return_value = self.collection
        self.client = Mock()
        self.client.get_database.return_value = self.database
        self.locks = []

    def tearDown(self):
        for lock in self.locks:
            lock.close()
        self.assertEqual(self.network.call_count, 0)
        self.assertEqual(self.child.call_count, 0)
        for item in reversed(self.patchers):
            item.stop()
        self.temporary.cleanup()

    def lock(self, mode="signed-future-live"):
        lock = guard.AccountLock(ACCOUNT, mode)
        self.locks.append(lock)
        return lock.acquire()

    def arm(self, lock, context=None, uri="mongodb://offline.invalid:27017/tradingagents"):
        with patch.object(guard.events, "_read_file", return_value=events.canonical(self.activation)) as public_read, \
             patch.dict(os.environ, {guard.CREDENTIAL_ENV: uri}, clear=True), \
             patch.object(guard.os.environ, "get", wraps=os.environ.get) as secret, \
             patch("pymongo.MongoClient", return_value=self.client) as driver:
            result = guard.arm_live_guard(lock, context or self.context)
            self.assertEqual(public_read.call_args.args[0], guard.ACTIVATION_PATH)
            self.assertEqual(secret.call_args_list[0].args, (guard.CREDENTIAL_ENV,))
            self.assertTrue(all(call.args[0] == guard.CREDENTIAL_ENV for call in secret.call_args_list))
            return result, driver.call_args

    def test_single_account_scope_excludes_run_window_mode_and_signer(self):
        first = self.lock()
        for operations_mode, run_id, window in (("supervised", "run-two", "new-window"), ("unattended", "run-three", "third-window")):
            with self.subTest(mode=operations_mode, run=run_id, window=window):
                with self.assertRaises(guard.GuardRefused) as failure, \
                     patch.object(guard.os.environ, "get", side_effect=AssertionError("secret before lock")):
                    self.lock()
                self.assertEqual(failure.exception.reason, "ALREADY_RUNNING")
        self.assertEqual(first.scope, f"predict_fun:56:{events.TRADING_ADDRESS}:predict-account")

    def test_two_processes_cross_run_window_and_operations_have_one_holder(self):
        context = multiprocessing.get_context("fork")
        started, release = context.Event(), context.Event()
        readers, workers = [], []
        for label in (("run-one", "window-one", "supervised"), ("run-two", "window-two", "unattended")):
            reader, writer = context.Pipe(duplex=False)
            process = context.Process(target=lock_worker, args=(str(self.lock_directory), label, started, release, writer))
            process.start()
            writer.close()
            readers.append(reader)
            workers.append(process)
        try:
            started.set()
            results = []
            for reader in readers:
                self.assertTrue(reader.poll(5))
                results.append(reader.recv()[0])
            self.assertCountEqual(results, ["held", "ALREADY_RUNNING"])
        finally:
            release.set()
            for process in workers:
                process.join(5)
                self.assertFalse(process.is_alive())
                self.assertEqual(process.exitcode, 0)
            for reader in readers:
                reader.close()

    def test_unsigned_does_not_create_or_take_live_lock(self):
        unsigned = self.lock("unsigned-paper")
        self.assertIsNone(unsigned.descriptor)
        self.assertFalse(self.lock_directory.exists())
        signed = self.lock()
        self.assertIsNotNone(signed.descriptor)
        self.lock("unsigned-paper")

    def test_lock_modes_inode_and_release_without_unlink(self):
        lock = self.lock()
        self.assertEqual(stat.S_IMODE(self.lock_directory.stat().st_mode), 0o700)
        path = next(self.lock_directory.iterdir())
        before = path.stat()
        self.assertEqual(stat.S_IMODE(before.st_mode), 0o600)
        lock.close()
        second = self.lock()
        self.assertEqual(path.stat().st_ino, before.st_ino)
        self.assertIsNotNone(second.descriptor)

    def test_symlink_directory_or_file_nonregular_hardlinks_uid_and_mode_refused(self):
        for fault in ("directory_symlink", "file_symlink", "fifo", "hardlink", "uid", "mode"):
            with self.subTest(fault=fault):
                directory = self.root / fault
                actual = self.root / (fault + "-target")
                actual.mkdir()
                if fault == "directory_symlink":
                    directory.symlink_to(actual, target_is_directory=True)
                else:
                    directory.mkdir()
                    path = directory / f"predict_fun-56-{events.TRADING_ADDRESS}-predict-account.lock"
                    if fault == "file_symlink":
                        path.symlink_to(actual / "missing")
                    elif fault == "fifo":
                        os.mkfifo(path, 0o600)
                    else:
                        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644 if fault == "mode" else 0o600)
                        os.close(descriptor)
                        if fault == "hardlink":
                            os.link(path, actual / "other-link")
                uid = os.getuid() + 1 if fault == "uid" else os.getuid()
                with patch.object(guard, "LOCK_DIRECTORY", directory), patch.object(guard.os, "getuid", return_value=uid):
                    with self.assertRaises(guard.GuardRefused):
                        self.lock()

    def test_guard_single_insert_majority_j_primary_readback_no_create(self):
        document, call = self.arm(self.lock())
        self.assertEqual(document["state"], "ARMED")
        self.assertEqual(document["schema_version"], guard.GUARD_SCHEMA)
        self.assertEqual(document["_id"], guard.scope_id(ACCOUNT))
        self.assertEqual([call[0] for call in self.collection.calls], ["insert", "read"])
        self.assertEqual(call.kwargs["retryWrites"], False)
        self.assertEqual(call.kwargs["connect"], False)
        self.assertLessEqual(call.kwargs["socketTimeoutMS"], 15000)
        options = self.database.get_collection.call_args.kwargs
        self.assertEqual(options["write_concern"].document, {"w": "majority", "j": True})
        self.assertEqual(options["read_concern"].document, {"level": "majority"})
        from pymongo import ReadPreference
        self.assertEqual(options["read_preference"], ReadPreference.PRIMARY)
        self.database.create_collection.assert_not_called()
        self.database.create_index.assert_not_called()
        self.client.close.assert_called_once()

    def test_duplicate_and_armed_survive_exit_lock_release_and_new_run(self):
        lock = self.lock()
        document, _ = self.arm(lock)
        lock.close()
        self.context["run_id"] = "new-run"
        self.context["risk_window_id"] = self.activation["risk_window_id"] = "new-window"
        self.context["operations_mode"] = self.activation["operations_mode"] = "unattended"
        with self.assertRaises(guard.GuardRefused) as failure:
            self.arm(self.lock())
        self.assertEqual(failure.exception.reason, "RECONCILIATION_REQUIRED")
        self.assertEqual(self.collection.documents[document["_id"]], document)
        self.assertEqual([call[0] for call in self.collection.calls], ["insert", "read", "insert"])

    def test_unacknowledged_timeout_readback_corruption_refuse_without_retry(self):
        for fault in ("unacknowledged", "timeout", "read_timeout", "missing", "partial", "version", "identity"):
            with self.subTest(fault=fault):
                self.collection.documents.clear()
                self.collection.calls.clear()
                self.collection.fault = fault
                lock = self.lock()
                with self.assertRaises(guard.GuardRefused) as failure:
                    self.arm(lock)
                self.assertIn(failure.exception.reason, ("RECOVERY_GUARD_UNAVAILABLE", "RECONCILIATION_REQUIRED"))
                self.assertEqual(sum(call[0] == "insert" for call in self.collection.calls), 1)
                if fault in ("unacknowledged", "timeout"):
                    self.assertEqual([call[0] for call in self.collection.calls], ["insert"])
                self.assertTrue(self.collection.documents)
                lock.close()

    def test_activation_bad_binding_unknown_version_before_credential_or_driver(self):
        mutations = [("schema_version", "unknown"), ("candidate_digest", "b" * 64),
                     ("takeover_ref", "bad/ref"), ("operations_mode", "auto"), ("extra", True)]
        for key, value in mutations:
            activation = copy.deepcopy(self.activation)
            activation[key] = value
            with self.subTest(key=key), patch.object(events, "_read_file", return_value=events.canonical(activation)), \
                 patch.object(guard.os.environ, "get", side_effect=AssertionError("credential")) as secret, \
                 patch("pymongo.MongoClient", side_effect=AssertionError("driver")) as driver:
                lock = self.lock()
                with self.assertRaises(guard.GuardRefused):
                    guard.arm_live_guard(lock, self.context)
                secret.assert_not_called()
                driver.assert_not_called()
                lock.close()
        for key, value in (("database", "other"), ("collection", "other"), ("credential_env", "OTHER_URI"), ("deployment_id", "bad/path")):
            binding = copy.deepcopy(self.activation["storage_binding"])
            binding[key] = value
            with self.assertRaises(guard.GuardRefused):
                guard.check_storage_binding(binding)

    def test_missing_collection_driver_credential_and_public_file_refuse(self):
        for fault in ("collection", "driver", "credential", "public"):
            with self.subTest(fault=fault):
                lock = self.lock()
                self.database.list_collection_names.return_value = [] if fault == "collection" else ["07_trading_btc5m_live_guard"]
                with patch.object(events, "_read_file", side_effect=OSError() if fault == "public" else None,
                                  return_value=events.canonical(self.activation)), \
                     patch.dict(os.environ, {} if fault == "credential" else {guard.CREDENTIAL_ENV: "mongodb://offline.invalid:27017/tradingagents"}, clear=True), \
                     patch("pymongo.MongoClient", side_effect=ImportError() if fault == "driver" else None,
                           return_value=self.client):
                    with self.assertRaises(guard.GuardRefused):
                        guard.arm_live_guard(lock, self.context)
                self.assertEqual(self.collection.calls, [])
                lock.close()

    def test_uri_binding_change_and_options_cannot_redirect_or_retry(self):
        binding = self.activation["storage_binding"]
        guard._uri_matches("mongodb://offline.invalid:27017/tradingagents", binding)
        for uri in ("mongodb://other.invalid:27017/tradingagents", "mongodb://offline.invalid:27017/other",
                    "mongodb://offline.invalid:27017/tradingagents?retryWrites=true",
                    "mongodb://offline.invalid:27017/tradingagents?tls=true",
                    "mongodb://offline.invalid:27017/tradingagents?replicaSet=other",
                    "mongodb://offline.invalid:27017/tradingagents?tls=false&tls=false",
                    "mongodb+srv://offline.invalid/tradingagents",
                    "mongodb://offline.invalid:27017,trusted.invalid:27017/tradingagents"):
            with self.subTest(uri=uri), self.assertRaises(guard.GuardRefused):
                guard._uri_matches(uri, binding)
        endpoint = copy.deepcopy(binding)
        endpoint["endpoint_id"]["tls"] = True
        endpoint["endpoint_id"]["replica_set"] = "offline-rs"
        guard._uri_matches("mongodb://offline.invalid:27017/tradingagents?tls=true&replicaSet=offline-rs", endpoint)

    def test_unsigned_or_unlocked_cannot_arm_or_read_storage_secret(self):
        for lock in (guard.AccountLock(ACCOUNT, "signed-future-live"), self.lock("unsigned-paper")):
            with patch.object(events, "_read_file", side_effect=AssertionError("public file")) as read, \
                 patch.object(guard.os.environ, "get", side_effect=AssertionError("secret")) as secret:
                with self.assertRaises(guard.GuardRefused):
                    guard.arm_live_guard(lock, self.context)
                read.assert_not_called()
                secret.assert_not_called()


if __name__ == "__main__":
    unittest.main()
