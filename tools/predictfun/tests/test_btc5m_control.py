"""Offline fixed-launcher consumers using real OS children and anonymous pipes."""
from __future__ import annotations

import copy
import datetime
import hashlib
import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import btc5m_control as control
import local_signer

# The fixed file path as the production code derives it, before any test patch.
ORIGINAL_API_SOURCE_FILE = control.API_SOURCE_FILE


def public_config(**overrides):
    data = json.loads((Path(__file__).resolve().parents[1] /
                       "btc5m-controlled-paper.json").read_text())
    for path, value in overrides.items():
        node = data
        parts = path.split(".")
        for name in parts[:-1]:
            node = node[name]
        node[parts[-1]] = value
    return data


def final_event():
    return {"run_id": "run-1", "event_kind": "final", "market_id": None, "token": None,
            "schema_version": control.SCHEMA_VERSION_V2, "event_seq": 1,
            "event_time": "2026-11-16T00:00:00Z",
            "accounting": {"cash": 100, "available": 100, "reserved": 0, "cost": 0,
                           "realized": 0, "unrealized": 0, "nav": 100, "net_pnl": 0,
                           "fees_in_cash": 0, "loss_latched": False},
            "source": {"markets": [], "finalized": True, "stop_reason": None, "cleanup_timed_out": False}}


def signed_final_event():
    event = final_event()
    event.pop("schema_version")
    event.pop("event_seq")
    event["accounting"] = {name: None for name in event["accounting"]}
    event["source"].update(markets=None, finalized=False, accounting_reason="LIVE_ACCOUNTING_UNAVAILABLE")
    return event


# Fixed no-network child fixture: never executes the real BOT or credentials.
CHILD = '''import os, sys, select, signal, time, subprocess
mode = sys.argv[1]
line = sys.argv[2]
if mode in ("rust-parent", "rust-parent-market"):
    child_env = {"PATH": "/usr/bin:/bin", "BTC5M_TEST_PIPE": "1"}
    if mode == "rust-parent-market": child_env["BTC5M_TEST_PIPE_MARKET"] = "1"
    child = subprocess.Popen([line, "bot::runtime::paper::tests::control_pipe_drive_probe",
                              "--exact", "--nocapture"], stdin=subprocess.PIPE,
                             env=child_env)
    while True: time.sleep(.01)
if mode == "refuse":
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    while True: time.sleep(.01)
if mode == "nonzero": sys.exit(124)
if mode == "stream":
    print(line, flush=True)
    sys.exit(0)
if mode == "signal-parent":
    os.kill(os.getppid(), signal.SIGINT)
    sys.stdin.buffer.read()
    sys.exit(0)
if mode == "deadline":
    ready, _, _ = select.select([sys.stdin.buffer], [], [], .03)
    if ready: sys.exit(41)
    if sys.stdin.buffer.read(1) != b"D": sys.exit(42)
if mode == "secret":
    print('BTC5M_EVENT {"key":"SYNTHETIC_SECRET"}', flush=True)
    print('SYNTHETIC_SECRET', file=sys.stderr, flush=True)
else:
    print('discarded ordinary log', file=sys.stderr, flush=True)
    if mode == "long":
        sys.stdout.write('BTC5M_EVENT ' + 'x' * 70000 + '\\n')
    elif mode != "missing":
        print('BTC5M_EVENT ' + line, flush=True)
'''


class ControlledLiveSupervisorTests(unittest.TestCase):
    """Original supervisor, real pipes and intact native financial producer frames."""

    def setUp(self):
        import btc5m_live_events as live
        from test_btc5m_live_events import metadata, native_stream
        self.live, self.metadata, self.native_stream = live, metadata, native_stream
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.script = self.root / "child.py"
        self.script.write_text('''import json, sys
frames = open(sys.argv[1], "rb").readlines()
for frame in frames:
    sys.stdout.buffer.write(frame)
    sys.stdout.buffer.flush()
    event = json.loads(frame[len("BTC5M_EVENT "):])
    payload = event["payload"]
    if (event["event_kind"] == "order" and payload["attempt_phase"] == "PREPARED"
            and payload["signed_hash"] is not None and payload["reason"] == "NONE"):
        expected = f"A {event['event_seq']} {payload['signed_hash']}\\n".encode()
        if sys.stdin.buffer.readline() != expected:
            sys.exit(42)
sys.exit(int(sys.argv[2]))
''')
        self.network = patch("socket.socket.connect", side_effect=AssertionError("NETWORK_FORBIDDEN"))
        self.network_spy = self.network.start()
        self.addCleanup(self.network.stop)
        self.addCleanup(lambda: self.assertEqual(self.network_spy.call_count, 0))

    def consume(self, frames, fault=None, native_exit=None):
        live = self.live
        started, public, context = self.metadata(frames[0])
        directory = self.root / "run"
        store = live.LiveEventStore(directory, started, public, context)
        self.addCleanup(store.close)
        wire = self.root / "native.txt"
        wire.write_bytes(b"".join(frames))
        process = subprocess.Popen([sys.executable, "-B", str(self.script), str(wire), str(native_exit or 0)],
                                   env={"PATH": "/usr/bin:/bin"}, stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        now = datetime.datetime.now(datetime.timezone.utc)
        with patch.object(control, "CLEANUP_SECONDS", .5):
            if fault:
                with fault(store, process):
                    facts, final = control.supervise(process, store, now, control.boottime())
            else:
                facts, final = control.supervise(process, store, now, control.boottime())
        try:
            # The launcher's own ending step, including its failure handling.
            control.finish_live_evidence(store, facts, final)
        except (OSError, ValueError):
            facts["launcher_exit"] = 1
        return facts, final, live.read_live_run(directory), store

    def test_r17_native_wait_drive_stream_and_diagnostics_reach_existing_consumers(self):
        name = ("bot::runtime::live_control::order_wait_tests::"
                "original_drive_observes_zero_then_resumes_a_second_post_on_the_same_adapter")
        result = subprocess.run(
            ["cargo", "test", "--locked", "--offline", "--target-dir",
             os.environ["CARGO_TARGET_DIR"], "--lib", name,
             "--", "--exact", "--nocapture"],
            cwd=control.BOT, capture_output=True, timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode() + result.stdout.decode())
        self.assertIn(b"1 passed", result.stdout)
        frames = [line + b"\n" for line in result.stdout.splitlines()
                  if line.startswith(self.live.EVENT_PREFIX)]
        started, public, context = self.metadata(frames[0])
        probe = self.live.LiveEventStore(self.root / "probe", started, public, context)
        self.addCleanup(probe.close)
        for index, frame in enumerate(frames):
            try:
                probe.append(frame, ack=lambda _ack: None)
            except ValueError as error:
                self.fail(f"native frame {index + 1}: {error}: {frame!r}")
        facts, final, read, store = self.consume(frames)
        self.assertEqual(facts["launcher_exit"], 0, (facts, read.get("reasons")))
        self.assertIsNotNone(final)
        self.assertEqual(len(store.relations.attempts), 2)
        first = [event for event in read["events"]
                 if event["event_kind"] == "execution" and event["payload"]["attempt_id"] == "1"]
        self.assertTrue(any(event["payload"]["closure"] == "OPEN" for event in first))
        self.assertTrue(any(event["payload"]["closure"] == "FINAL" for event in first))
        releases = [event for event in read["events"] if event["event_kind"] == "reservation"
                    and event["payload"]["attempt_id"] == "1"
                    and event["payload"]["action"] == "RELEASE"]
        self.assertEqual(len(releases), 1)
        logs = [line for line in result.stderr.splitlines() if b"order closure observation" in line]
        self.assertTrue(logs)
        budget = [0]
        for line in logs:
            control.append_run_log(store.directory, "stderr", line, budget)
        text = (store.directory / control.RUN_LOG_NAME).read_text()
        for field in ("venue_status", "expiration", "block_time", "closure_branch"):
            self.assertIn(field, text)
        self.assertIn("SIGNED_EXPIRY", text)
        self.assertLessEqual(budget[0], control.RUN_LOG_LIMIT)

    def test_native_nonzero_fee_stream_reaches_disk_and_final(self):
        frames = self.native_stream(813, 827)
        facts, final, read, _store = self.consume(frames)
        self.assertEqual(facts["events"], len(frames))
        self.assertEqual(facts["launcher_exit"], 0)
        self.assertEqual(final, self.live.validate_live_event(frames[-1]))
        self.assertEqual(read["events"], [self.live.validate_live_event(frame) for frame in frames])
        self.assertEqual(read["ended"]["final_event_seq"], len(frames))
        self.assertTrue(any(int(event["payload"]["facts"]["share_fee"]["atoms"] or "0") > 0
                            for event in read["events"] if event["event_kind"] == "execution"))
        self.assertEqual(read["status"], "INCOMPLETE")
        self.assertFalse(read["ended"]["cash_complete"])

    def test_two_round_native_prefix_never_invents_a_final(self):
        frames = self.native_stream(1664, 1690)
        facts, final, read, store = self.consume(frames)
        self.assertEqual(facts["events"], 27)
        self.assertEqual(facts["launcher_exit"], 1)
        self.assertIsNone(final)
        self.assertEqual(len(store.relations.attempts), 2)
        self.assertEqual(read["status"], "INCOMPLETE")
        self.assertIsNone(read["ended"]["final_accounting"])

    def test_native_failure_is_not_upgraded_by_a_final(self):
        facts, final, read, _store = self.consume(self.native_stream(813, 827), native_exit=9)
        self.assertIsNotNone(final)
        self.assertEqual(read["ended"]["exit_facts"]["child_returncode"], 9)
        self.assertIn("PROCESS_FAILED", read["reasons"])

    def test_the_stopped_run_records_the_failure_code_it_stopped_with(self):
        # run005 stopped with only the generic reason: the code the check held
        # is now on the stop, the final and ended.json, and nothing else moved.
        frames = self.native_stream(96, 98)
        _facts, final, read, _store = self.consume(frames)
        stop = next(event for event in read["events"] if event["event_kind"] == "stop")
        self.assertEqual(stop["payload"]["stop_detail"], "ACCOUNT_LEDGER_MISMATCH")
        self.assertEqual(final["payload"]["stop_detail"], "ACCOUNT_LEDGER_MISMATCH")
        self.assertEqual(read["ended"]["incomplete_detail"], "ACCOUNT_LEDGER_MISMATCH")
        self.assertEqual(read["ended"]["incomplete_reason"], "FACT_INCOMPLETE")
        self.assertEqual(final["payload"]["stop_reason"], "RECONCILIATION_REQUIRED")

    def launch_fixture(self, *, real_identity=False):
        """Strict fake public file and lowest-level Mongo transport only."""
        from contextlib import ExitStack
        import btc5m_live_guard as guard
        from test_btc5m_live_guard import FakeCollection, public_inputs
        stack = ExitStack()
        self.addCleanup(stack.close)
        frames = self.native_stream(813, 827)
        native_started, _public, _context = self.metadata(frames[0])
        data = public_config(**{"bot.enable_trading": True, "bot.mock_trading": False})
        data.pop("controlled_paper")
        config = self.root / "config.json"
        config.write_bytes(self.live.canonical(data))
        activation, _ = public_inputs()
        activation["risk_window_id"] = native_started["risk_window_id"]
        identity = control.public_identity({}, live=True) if real_identity else native_started["candidate_identity"]
        public_identity = {"sha256": hashlib.sha256(self.live.canonical(data)).hexdigest(),
                           "initial_cash": 98, "max_order_usd": 1, "max_market_usd": 1,
                           "max_total_usd": 10, "max_loss_usd": 20}
        activation["candidate_digest"] = self.live.candidate_digest(
            {"candidate_identity": identity, "public_config_identity": public_identity})
        activation_path = self.root / "activation.json"
        activation_path.write_bytes(self.live.canonical(activation))
        stack.enter_context(patch.object(guard, "ACTIVATION_PATH", activation_path))
        stack.enter_context(patch.object(guard, "LOCK_DIRECTORY", self.root / "locks"))
        if not real_identity:
            stack.enter_context(patch.object(control, "public_identity", return_value=identity))
        stack.enter_context(patch.dict(os.environ, {guard.CREDENTIAL_ENV:
                             "mongodb://offline.invalid:27017/tradingagents"}, clear=True))
        collection = FakeCollection()
        database, client = MagicMock(), MagicMock()
        database.list_collection_names.return_value = ["07_trading_btc5m_live_guard"]
        database.get_collection.return_value = collection
        client.get_database.return_value = database
        driver = stack.enter_context(patch("pymongo.MongoClient", return_value=client))
        order = []
        evidence = self.root / "evidence"
        def secret():
            self.assertEqual([call[0] for call in collection.calls], ["insert", "read"])
            directory = evidence / native_started["run_id"]
            for name in ("started.json", "public-config.json", "live-context.json", "events.jsonl"):
                self.assertTrue((directory / name).is_file())
            order.append("secret")
            return b"offline-not-a-key"
        loader = stack.enter_context(patch.object(local_signer, "_read_owned", side_effect=secret))
        builder = stack.enter_context(patch.object(local_signer, "build_child_env",
                                                   return_value={"PATH": "/usr/bin:/bin"}))
        wire = self.root / "wire.txt"
        wire.write_bytes(b"".join(frames))
        def popen(command, **kwargs):
            self.assertEqual(order, ["secret"])
            self.assertEqual(command[0], str(control.LIVE_BINARY))
            self.assertIn("--controlled-live-manifest", command)
            self.assertEqual(kwargs["env"], {"PATH": "/usr/bin:/bin"})
            order.append("child")
            return subprocess.Popen([sys.executable, "-B", str(self.script), str(wire), "0"], **kwargs)
        child = MagicMock(side_effect=popen)
        args = ["--mode", "signed-future-live", "--config", str(config), "--run-id", native_started["run_id"],
                "--predict-budget-confirmation", f"56:{local_signer.TRADING_ADDRESS.lower()}:predict-account",
                "--predict-rpc-url", "http://offline.invalid", "--live-contract", "phase1_net_shares",
                "--operations-mode", activation["operations_mode"], "--risk-window-id", activation["risk_window_id"],
                "--authorization-ref", activation["authorization_ref"], "--deadline-at", activation["deadline_at"]]
        return args, evidence, child, loader, collection, activation_path, driver, database, builder

    def test_deadline_window_cap_is_mode_bound_at_original_dispatch(self):
        """Accepted windows reach fake dispatch; refused windows never read secrets/storage."""
        instant = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        class FixedTime(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                return instant.astimezone(tz) if tz else instant.replace(tzinfo=None)
        original_root = self.root
        with patch.object(control.datetime, "datetime", FixedTime), \
                patch.object(control, "boottime", return_value=100):
            for mode, cap in (("supervised", 21600), ("unattended", 86400)):
                for seconds in (-1, 0, 1, 21599, 21600, 21601, 86399, 86400, 86401):
                    with self.subTest(mode=mode, seconds=seconds):
                        self.root = original_root / f"{mode}-{seconds}"
                        self.root.mkdir()
                        fixture = self.launch_fixture()
                        args, root, child, loader, _, path, driver, _, _ = fixture
                        activation = json.loads(path.read_bytes())
                        activation["operations_mode"] = args[args.index("--operations-mode") + 1] = mode
                        activation["deadline_at"] = args[args.index("--deadline-at") + 1] = (
                            instant + datetime.timedelta(seconds=seconds)).isoformat(
                                timespec="milliseconds").replace("+00:00", "Z")
                        path.write_bytes(self.live.canonical(activation))
                        if 0 < seconds <= cap:
                            facts = control.run(args, environment={}, evidence_root=root, popen=child)
                            self.assertEqual(facts["launcher_exit"], 0)
                            child.assert_called_once()
                            loader.assert_called_once()
                            started = json.loads((root / args[args.index("--run-id") + 1]
                                                  / "started.json").read_bytes())
                            self.assertEqual(started["deadline_at"], activation["deadline_at"])
                            self.assertEqual(started["operations_mode"], mode)
                        else:
                            with self.assertRaises(control.Refused) as refused:
                                control.run(args, environment={}, evidence_root=root, popen=child)
                            self.assertEqual(refused.exception.reason, "DEADLINE")
                            driver.assert_not_called()
                            loader.assert_not_called()
                            child.assert_not_called()
                            self.assertFalse(root.exists())
        self.root = original_root

    def test_original_run_orders_guard_evidence_secret_child_and_refuses_cold_restart(self):
        import btc5m_live_guard as guard
        args, root, child, loader, collection, _path, driver, database, builder = self.launch_fixture()
        facts = control.run(args, environment={}, evidence_root=root, popen=child)
        self.assertEqual(facts["launcher_exit"], 0)
        self.assertEqual(child.call_count, 1)
        self.assertEqual(loader.call_count, 1)
        self.assertEqual(builder.call_count, 1)
        self.assertFalse(driver.call_args.kwargs["retryWrites"])
        self.assertEqual(database.get_collection.call_args.kwargs["write_concern"].document,
                         {"w": "majority", "j": True})
        read = self.live.read_live_run(next(root.iterdir()))
        self.assertIsNotNone(read["ended"])
        self.assertEqual(read["ended"]["exit_facts"]["child_returncode"], 0)
        args[args.index("--run-id") + 1] = "new-run"
        with self.assertRaises(control.Refused) as refused:
            control.run(args, environment={}, evidence_root=root, popen=child)
        self.assertEqual(refused.exception.reason, "RECONCILIATION_REQUIRED")
        self.assertEqual(loader.call_count, 1)
        self.assertEqual(child.call_count, 1)
        self.assertEqual(len(collection.documents), 1)
        self.assertEqual(next(iter(collection.documents.values()))["state"], "ARMED")

    def test_deployed_identity_reaches_original_signed_dispatch_and_fixed_manifest(self):
        """Actual identity and controller; only storage/secret/child transports are fake."""
        instant = datetime.datetime.now(datetime.timezone.utc)
        class FixedTime(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                return instant.astimezone(tz) if tz else instant.replace(tzinfo=None)
        # Freeze only test time; never patch public_identity or controller dispatch.
        with patch.object(control.datetime, "datetime", FixedTime), \
                patch.object(control, "boottime", return_value=100):
            args, root, child, loader, collection, _path, _driver, _database, builder = self.launch_fixture(
                real_identity=True)
            facts = control.run(args, environment={}, evidence_root=root, popen=child)
        self.assertEqual(facts["launcher_exit"], 0)
        self.assertEqual(facts["child_returncode"], 0)
        child.assert_called_once()
        loader.assert_called_once()
        builder.assert_called_once()
        command = child.call_args.args[0]
        expected = control.BOT.parents[4] / (
            "data/reports/2026-11-26-btc5m-settlement-root-fix-deploy/evidence/"
            "candidate-root-fix/polymarket-toolkits")
        self.assertEqual(command[0], str(expected))
        directory = root / args[args.index("--run-id") + 1]
        self.assertEqual(command[command.index("--controlled-live-manifest") + 1],
                         str(directory / "live-context.json"))
        self.assertEqual(command.count("--controlled-live-manifest"), 1)
        self.assertEqual(command[command.index("--controlled-mode") + 1], "signed-future-live")
        self.assertEqual(command[-2:], ["run", "btc-arb"])
        self.assertEqual(child.call_args.kwargs["cwd"], control.BOT)
        self.assertIs(child.call_args.kwargs["shell"], False)
        started = json.loads((directory / "started.json").read_bytes())
        self.assertEqual(started["candidate_identity"], control.public_identity({}, live=True))
        context = json.loads((directory / "live-context.json").read_bytes())
        self.assertEqual(context["candidate_digest"], self.live.candidate_digest(started))
        self.assertEqual(len(started["candidate_identity"]["files"]), 89)
        print("R17_CANDIDATE_IDENTITY " + json.dumps(started["candidate_identity"], sort_keys=True))
        self.assertEqual([call[0] for call in collection.calls], ["insert", "read"])
        self.assertEqual(len(collection.documents), 1)
        with self.assertRaises(control.Refused) as refused:
            control.run(args, environment={}, evidence_root=root, popen=child)
        self.assertEqual(refused.exception.reason, "DUPLICATE_RUN")
        self.assertEqual(child.call_count, 1)
        self.assertEqual(loader.call_count, 1)
        print("DEPLOYED_SIGNED_DISPATCH " + json.dumps({"argv": command, "facts": facts,
              "controller_dispatches": child.call_count,
              "manifest": str(directory / "live-context.json"),
              "candidate_digest": context["candidate_digest"]}, sort_keys=True))

    def test_invalid_activation_has_zero_storage_trading_secret_and_child(self):
        args, root, child, loader, _collection, path, driver, _database, _builder = self.launch_fixture()
        activation = json.loads(path.read_bytes())
        activation["candidate_digest"] = "f" * 64
        path.write_bytes(self.live.canonical(activation))
        with self.assertRaises(control.Refused):
            control.run(args, environment={}, evidence_root=root, popen=child)
        driver.assert_not_called()
        loader.assert_not_called()
        child.assert_not_called()
        self.assertFalse(root.exists())

    def test_gas_payer_rejects_before_storage_secret_or_child(self):
        args, root, child, loader, _collection, path, driver, _database, builder = self.launch_fixture()
        activation = json.loads(path.read_bytes())
        self.assertNotEqual(activation["account_identity"]["trading_address"],
                            activation["account_identity"]["signer_address"])
        for payer in (activation["account_identity"]["trading_address"], "0x" + "3" * 40, "bad", "", None):
            with self.subTest(payer=payer):
                broken = copy.deepcopy(activation)
                if payer is None:
                    del broken["gas_policy"]["payer_address"]
                else:
                    broken["gas_policy"]["payer_address"] = payer
                path.write_bytes(self.live.canonical(broken))
                with self.assertRaises(control.Refused):
                    control.run(args, environment={}, evidence_root=root, popen=child)
                driver.assert_not_called()
                loader.assert_not_called()
                builder.assert_not_called()
                child.assert_not_called()
                self.assertFalse(root.exists())

    def test_partial_opt_in_and_old_mode_new_flags_refuse_before_any_read(self):
        args, root, child, loader, _collection, _path, driver, _database, _builder = self.launch_fixture()
        variants = [args + ["--duration-hours", "6"], args + ["--post-entry-observer"],
                    args[:args.index("--deadline-at")],
                    ["--mode", "unsigned-paper", "--config", "absent", "--run-id", "r", "--live-contract", "phase1_net_shares"]]
        with patch.object(control, "read_public_config", side_effect=AssertionError("read before args")):
            for variant in variants:
                with self.subTest(variant=variant), self.assertRaises(control.Refused):
                    control.run(variant, environment={}, evidence_root=root, popen=child)
        driver.assert_not_called()
        loader.assert_not_called()
        child.assert_not_called()

    def test_keyring_failure_after_armed_keeps_barrier_and_starts_no_child(self):
        args, root, child, loader, collection, _path, _driver, _database, _builder = self.launch_fixture()
        loader.side_effect = local_signer.Failure("KEYRING_LOCKED", "backend", "LOCKED")
        facts = control.run(args, environment={}, evidence_root=root, popen=child)
        self.assertEqual(facts["stop_reason"], "KEYRING_UNAVAILABLE")
        self.assertEqual(facts["launcher_exit"], 1)
        loader.assert_called_once()
        child.assert_not_called()
        self.assertEqual(len(collection.documents), 1)
        read = self.live.read_live_run(next(root.iterdir()))
        self.assertIsNone(read["ended"]["final_accounting"])
        self.assertEqual(read["ended"]["exit_facts"]["stop_reason"], "KEYRING_UNAVAILABLE")

    def test_two_real_launch_processes_have_at_most_one_secret_and_child(self):
        import multiprocessing
        args, root, child, loader, _collection, _path, _driver, _database, _builder = self.launch_fixture()
        context = multiprocessing.get_context("fork")
        ready, release = context.Event(), context.Event()
        original_secret = loader.side_effect
        def held_secret():
            value = original_secret()
            ready.set()
            if not release.wait(5):
                raise AssertionError("fixture release timeout")
            return value
        loader.side_effect = held_secret
        def launch(argv, connection):
            try:
                facts = control.run(argv, environment={}, evidence_root=root, popen=child)
                connection.send(("ended", loader.call_count, child.call_count, facts["child_returncode"]))
            except control.Refused as error:
                connection.send((error.reason, loader.call_count, child.call_count, None))
            finally:
                connection.close()
        first_read, first_write = context.Pipe(duplex=False)
        second_read, second_write = context.Pipe(duplex=False)
        first = context.Process(target=launch, args=(args, first_write))
        changed = list(args)
        changed[changed.index("--run-id") + 1] = "second-run"
        second = context.Process(target=launch, args=(changed, second_write))
        first.start()
        try:
            self.assertTrue(ready.wait(5))
            second.start()
            self.assertTrue(second_read.poll(5))
            self.assertEqual(second_read.recv(), ("ALREADY_RUNNING", 0, 0, None))
            release.set()
            self.assertTrue(first_read.poll(5))
            self.assertEqual(first_read.recv(), ("ended", 1, 1, 0))
        finally:
            release.set()
            first.join(5)
            if second.pid is not None:
                second.join(5)
            for worker in (first, second):
                self.assertFalse(worker.is_alive())
                if worker.pid is not None:
                    self.assertEqual(worker.exitcode, 0)
            for connection in (first_read, first_write, second_read, second_write):
                connection.close()

    def test_actual_live_public_identity_covers_upstream_and_loaded_files(self):
        """Read-only candidate identity, also proving digest cross-consumer agreement."""
        live = self.live
        dynamic_paths = {"tools/predictfun/btc5m_control.py", "tools/predictfun/btc5m_live_events.py",
                         "tools/predictfun/tests/test_btc5m_control.py",
                         "tools/predictfun/tests/test_btc5m_live_events.py",
                         "src/bot/arbitrage/phase1/tests.rs"}
        gas_paths = {"src/config/controlled_live.rs", "src/bot/runtime/taker.rs",
                     "src/bot/runtime/live_control/tests.rs", "tools/predictfun/btc5m_live_guard.py",
                     "tools/predictfun/btc5m_live_events.py", "tools/predictfun/tests/test_btc5m_live_guard.py",
                     "tools/predictfun/tests/test_btc5m_live_events.py"}
        identity = control.public_identity({}, live=True)
        repair_root = control.BOT.parents[4] / "data/reports/2026-11-21-btc5m-live003-fact-incomplete/evidence"
        validation = repair_root / "validation.json"
        self.assertEqual(hashlib.sha256(validation.read_bytes()).hexdigest(),
                         "bb1e8dbab96c20ce671bc991b4009ae6e3e1ab1fc8518ab700fa3b6e6a2af4b7")
        repaired_rust = json.loads(validation.read_bytes())["candidate"]["source_sha256"]
        self.assertEqual(set(repaired_rust), {
            "src/bot/runtime/taker.rs", "src/bot/runtime/live_control.rs",
            "src/bot/runtime/live_accounting.rs", "src/bot/runtime/live_control/tests.rs",
            "src/bot/runtime/live_control/tests/live003_regression.rs",
            "src/service/predict_fun/controlled.rs", "src/service/predict_fun/fills.rs"})
        repair_changed = {"src/bot/runtime/taker.rs", "src/bot/runtime/live_control/tests.rs",
                          "src/service/predict_fun/controlled.rs"}
        # live004 rewrote the book check, the refusal type, its fixture and the submit branch.
        live004_changed = {"src/bot/runtime/live_control.rs", "src/bot/runtime/live_control/tests.rs",
                           "src/bot/runtime/taker.rs", "src/service/predict_fun.rs",
                           "src/service/predict_fun/controlled.rs", "src/service/venue.rs"}
        # Reproduce the current binding; historical live003/live004 records stay pinned.
        stop_diag_root = control.BOT.parents[4] / (
            "data/reports/2026-11-21-btc5m-stop-diagnostics/evidence")
        # The stopped run's own failure code on the wire: live.rs records it, the
        # envelope's types and emitter carry it, the two Python consumers forward it.
        stop_diag_changed = {"src/bot/runtime/live.rs", "src/bot/runtime/live_events.rs",
                             "src/bot/runtime/live_events/types.rs", "src/bot/runtime/live_events/tests.rs",
                             "tools/predictfun/btc5m_live_events.py",
                             "tools/predictfun/tests/test_btc5m_live_events.py",
                             # Current account/request diagnostic boundary correction.
                             "src/service/predict_fun/request.rs", "src/service/predict_fun/account.rs"}
        added = {"src/bot/runtime/live_control/tests/live003_regression.rs", "src/bot/runtime/live_control/tests/live004_regression.rs",
                 "src/bot/runtime/live/order_wait.rs", "src/bot/runtime/live_control/tests/order_wait.rs",
                 "src/service/predict_fun/fills/order_wait_tests.rs"}
        send_latency_changed = {"src/bot/runtime.rs", "src/bot/runtime/taker.rs", "src/service/predict_fun/account.rs", "src/service/predict_fun/fills.rs", "src/service/predict_fun/request.rs"}
        deployment_changed = {"tools/predictfun/btc5m_control.py",
                              "tools/predictfun/tests/test_btc5m_control.py"}
        # This round's 1-USDT contract change: the two public-config contracts,
        # the live constants and dust floor, the frozen identities, the deployment
        # JSON and the fixtures that follow them.
        config_a1_changed = {"src/config.rs", "src/bot/runtime/live_accounting.rs",
                             "src/bot/runtime/taker.rs", "src/bot/runtime/live_control.rs",
                             "src/bot/runtime/live_control/tests.rs",
                             "src/bot/runtime/live_control/tests/live003_regression.rs",
                             # The native valuation fixture is sized at the same
                             # 1-USDT contract the window now runs.
                             "src/bot/arbitrage/phase1/tests.rs",
                             "tools/predictfun/btc5m_control.py", "tools/predictfun/btc5m_live_events.py",
                             "tools/predictfun/tests/test_btc5m_control.py",
                             "tools/predictfun/tests/test_btc5m_live_events.py",
                             "tools/predictfun/btc5m-controlled-paper.json"}
        # Settled-to-zero reconciliation round: the account consumer's synthetic
        # harness changed with the settlement check; no live execution file here.
        settled_zero_changed = {"src/service/predict_fun/fills.rs"}
        r2_changed = {"src/config/controlled_live.rs", "src/bot/runtime/live_control.rs",
                      "src/bot/runtime/live_control/tests.rs", "src/bot/runtime/paper.rs",
                      "src/service/predict_fun.rs", "src/service/predict_fun/account.rs",
                      "tools/predictfun/btc5m_control.py", "tools/predictfun/btc5m_live_events.py",
                      "tools/predictfun/btc5m_live_guard.py", "tools/predictfun/tests/test_btc5m_control.py",
                      "tools/predictfun/tests/test_btc5m_live_events.py", "tools/predictfun/tests/test_btc5m_live_guard.py"}
        # R5: bounded re-read of accepted orders' invisible facts, plus the binding.
        r5_changed = {"src/bot/runtime/live.rs", "src/bot/runtime/taker.rs",
                      "src/service/predict_fun/controlled.rs",
                      "src/bot/runtime/live_control/tests.rs",
                      "tools/predictfun/btc5m_control.py",
                      "tools/predictfun/tests/test_btc5m_control.py"}
        # R6: the startup read's own code reaches the existing stop detail.
        r6_changed = {"src/bot/runtime/live.rs", "src/bot/runtime/live_control/tests.rs",
                      "tools/predictfun/btc5m_control.py", "tools/predictfun/tests/test_btc5m_control.py",
                      "tools/predictfun/tests/test_btc5m_live_events.py"}
        # Parameterization: one public config froze this window's money and readers.
        parameterization_changed = {"src/config.rs", "src/config/run_parameters.rs", "src/config/run_parameters/tests.rs", "src/config/controlled_live.rs", "src/main.rs", "src/bot/runtime/live_accounting.rs", "src/bot/runtime/live.rs", "src/bot/runtime/live_control.rs", "src/bot/runtime/live_control/tests.rs", "src/bot/runtime/taker.rs", "tools/predictfun/btc5m_control.py", "tools/predictfun/btc5m_live_events.py", "tools/predictfun/btc5m_launch.py", "tools/predictfun/tests/test_btc5m_control.py", "tools/predictfun/tests/test_btc5m_launch.py", "tools/predictfun/tests/test_btc5m_parameterization.py", "tools/predictfun/local_signer.py", "docs/LIVE-TRADING.md"}
        r13_changed = {"src/service/predict_fun/fills.rs", "src/bot/runtime/live_events.rs", "src/bot/runtime/live_events/tests.rs"}  # R13 claimed-win evidence
        # R15 includes the test watchdog pin in channel.rs; R16 includes F1's tail cap.
        r15_changed = {"src/bot/runtime/live_control.rs", "src/bot/runtime/live_control/channel.rs", "src/bot/runtime/live_control/tests.rs", "src/bot/runtime/taker.rs", "src/service/predict_fun.rs", "src/service/predict_fun/account.rs", "src/service/predict_fun/controlled.rs", "src/service/predict_fun/fills.rs", "src/service/venue.rs"}
        r16_changed = {"src/bot/runtime.rs", "src/bot/runtime/live.rs", "src/bot/runtime/live_accounting.rs", "src/bot/runtime/live_control.rs", "src/bot/runtime/live_control/tests.rs", "src/bot/runtime/live_events.rs", "src/bot/runtime/live_events/types.rs", "src/bot/runtime/paper.rs", "src/bot/runtime/timing.rs", "src/service/predict_fun/account.rs", "src/service/venue.rs", "tools/predictfun/btc5m_live_events.py", "tools/predictfun/tests/test_btc5m_live_events.py"}
        # R13c: judge the claim before booking; carry the paid claim and cleared wait.
        r13c_changed = {"src/bot/runtime/live.rs", "src/bot/runtime/live_accounting.rs", "src/bot/runtime/live_control.rs",
                        "src/bot/runtime/live_events.rs", "src/bot/runtime/live_events/types.rs",
                        "src/bot/runtime/live_events/tests.rs", "tools/predictfun/btc5m_live_events.py", "tools/predictfun/tests/test_btc5m_live_events.py"}
        for path, expected in repaired_rust.items():
            if path not in live004_changed | config_a1_changed | settled_zero_changed | send_latency_changed | r13c_changed | r15_changed | r16_changed:
                self.assertEqual(identity["files"][path], expected, path)
        self.assertEqual({path for path, expected in repaired_rust.items()
                          if identity["files"][path] != expected},
                         (live004_changed | config_a1_changed | settled_zero_changed | send_latency_changed | r13c_changed | r15_changed | r16_changed)
                         & set(repaired_rust))
        manifest = control.BOT.parents[4] / "data/reports/2026-11-19-btc5m-live-parity-and-operations/evidence/run3906-final-candidate.tsv"
        self.assertEqual(hashlib.sha256(manifest.read_bytes()).hexdigest(),
                         "14efba3e960fb5011fefac26d9a539298db545f1e73d15e388ff65e6936d71fd")
        evidence = manifest.parent
        rust_paths = {"src/bot/runtime/paper.rs", "src/bot/runtime/paper/tests.rs",
                      "src/bot/runtime/live_accounting.rs", "src/bot/runtime/live_control/tests.rs"}
        reference = evidence / "run3909-m01-rust-reference.json"
        self.assertEqual(hashlib.sha256(reference.read_bytes()).hexdigest(),
                         "5f7a7bd87419db645efabfc0b830f1f895a33b3c13f9635ad3e6e2d01546367a")
        expected_rust = json.loads(reference.read_bytes())
        self.assertEqual(set(expected_rust), rust_paths)
        upstream = {}
        for row in manifest.read_text().splitlines()[1:]:
            path, _lines, digest = row.split("\t")
            upstream[path] = digest
            if path not in gas_paths | dynamic_paths | repair_changed | live004_changed | stop_diag_changed | config_a1_changed | settled_zero_changed | r2_changed | parameterization_changed | send_latency_changed | r13_changed | r13c_changed | r15_changed | r16_changed:
                self.assertEqual(identity["files"][path], expected_rust.get(path, digest), path)
        self.assertEqual(len(upstream), 29)
        self.assertTrue(rust_paths <= upstream.keys())
        baseline = evidence / "run3907-final3-candidate.json"
        self.assertEqual(hashlib.sha256(baseline.read_bytes()).hexdigest(),
                         "17f77c2fff85facf0dc3e018ebe0a58f9c296b7fa0f614595d674394f9d3490d")
        old_files = json.loads(baseline.read_bytes())["candidate_identity"]["files"]
        allowed = rust_paths | {"tools/predictfun/btc5m_control.py",
            "tools/predictfun/tests/test_btc5m_control.py", "tools/predictfun/README.md", "docs/LIVE-TRADING.md",
            "tools/predictfun/tests/test_btc5m_paper72.py"}
        self.assertEqual(set(identity["files"]) - set(old_files), added | set(control.LIVE_PATHS) - set(old_files))
        self.assertEqual(set(old_files) - set(identity["files"]), set())
        self.assertEqual(len(old_files), 57)
        self.assertEqual(len(identity["files"]), 89)
        protected = set(old_files) - allowed
        self.assertEqual(len(protected), 48)
        for path in protected - gas_paths - dynamic_paths - repair_changed - live004_changed - stop_diag_changed - config_a1_changed - settled_zero_changed - r2_changed - parameterization_changed - send_latency_changed - r13_changed - r13c_changed - r15_changed - r16_changed:
            self.assertEqual(identity["files"][path], old_files[path], path)
        changed = {path for path in old_files if identity["files"][path] != old_files[path]} | added
        self.assertEqual(changed, (allowed | gas_paths | dynamic_paths | repair_changed
                                   | live004_changed | stop_diag_changed | config_a1_changed
                                   | settled_zero_changed | r2_changed | added | send_latency_changed | r13_changed | r13c_changed | ((r15_changed | r16_changed) & set(old_files))
                                   | (parameterization_changed & set(old_files))) - {"src/bot/runtime/paper/tests.rs"})
        self.assertEqual(control.PAPER72_BINARY, evidence / "candidate-v2/polymarket-toolkits")
        self.assertEqual(hashlib.sha256(control.PAPER72_BINARY.read_bytes()).hexdigest(),
                         "8d4bfa158011e9f15c52ab0a87eb284e86686347956aac3dc3ecc5366857ccaa")
        self.assertEqual(control.LIVE_BINARY, control.BOT.parents[4] / "data/reports/2026-11-26-btc5m-settlement-root-fix-deploy/evidence/candidate-root-fix/polymarket-toolkits")
        old_live = control.BOT.parents[4] / "data/reports/2026-11-21-btc5m-gas-payer-fix/evidence/candidate/polymarket-toolkits"
        self.assertEqual(hashlib.sha256(old_live.read_bytes()).hexdigest(),
                         "f8171a45bc6ca09d5f1bbbe4d3f35bba6fdd31927f86974930e5960dfa624043")
        accepted = evidence / "run3909-m01-final-candidate.json"
        self.assertEqual(hashlib.sha256(accepted.read_bytes()).hexdigest(),
                         "db0efa3c2979c44094992e324a4fb8151646e1511a9a5da00911eeab056ff9e9")
        accepted_files = json.loads(accepted.read_bytes())["candidate_identity"]["files"]
        stage_allowed = {"tools/predictfun/btc5m_control.py",
                         "tools/predictfun/tests/test_btc5m_control.py",
                         "tools/predictfun/tests/test_btc5m_paper72.py"}
        self.assertEqual(set(identity["files"]) - set(accepted_files), added | set(control.LIVE_PATHS) - set(accepted_files))
        self.assertEqual(set(accepted_files) - set(identity["files"]), set())
        self.assertEqual(len(set(accepted_files) - stage_allowed), 54)
        self.assertEqual({path for path in accepted_files
                          if identity["files"][path] != accepted_files[path]} | added,
                         stage_allowed | gas_paths | dynamic_paths | repair_changed
                         | live004_changed | stop_diag_changed | config_a1_changed
                         | settled_zero_changed | r2_changed | added | send_latency_changed | r13_changed | r13c_changed | ((r15_changed | r16_changed | parameterization_changed) & set(accepted_files)))
        historical = control.BOT.parents[4] / ("data/reports/2026-11-21-btc5m-live002-output-invalid/"
                                               "evidence/fix/candidate-identity.json")
        self.assertEqual(hashlib.sha256(historical.read_bytes()).hexdigest(),
                         "635d35b8d68ca9200961ab8d0185d7b51caac346f672fed5aa20c79c981376e3")
        historical_files = json.loads(historical.read_bytes())["candidate_identity"]["files"]
        self.assertEqual(set(identity["files"]) - set(historical_files), added | set(control.LIVE_PATHS) - set(historical_files))
        self.assertEqual(set(historical_files) - set(identity["files"]), set())
        guard_changed = {"tools/predictfun/btc5m_live_guard.py",
                         "tools/predictfun/tests/test_btc5m_live_guard.py"}
        self.assertEqual({path for path in historical_files
                          if identity["files"][path] != historical_files[path]},
                         repair_changed | deployment_changed | live004_changed | stop_diag_changed
                         | guard_changed
                         | (config_a1_changed & set(historical_files))
                         | settled_zero_changed | r2_changed | send_latency_changed | r13_changed | r13c_changed | ((r15_changed | r16_changed | parameterization_changed) & set(historical_files)))
        print("M01_PROTECTED_PARTITIONS " + json.dumps({"upstream_total": len(upstream),
              "upstream_protected": len(set(upstream) - rust_paths), "protected": sorted(protected),
              "changed": sorted(changed), "rust_reference": expected_rust}, sort_keys=True))
        for path in control.LIVE_PATHS:
            self.assertIn(path, identity["files"])
        self.assertEqual(identity["binary_sha256"], control.LIVE_BINARY_SHA256)
        data = public_config(**{"bot.enable_trading": True, "bot.mock_trading": False})
        data.pop("controlled_paper")
        public = {"sha256": hashlib.sha256(live.canonical(data)).hexdigest(), "initial_cash": 98,
                  "max_order_usd": 1, "max_market_usd": 1, "max_total_usd": 10, "max_loss_usd": 20}
        # The 1-USDT contract is the current window; the frozen records below keep
        # their own historical tuple (100, 2, 2, 10, 20) byte for byte.
        historical_public = {"initial_cash": 100, "max_order_usd": 2, "max_market_usd": 2,
                             "max_total_usd": 10, "max_loss_usd": 20}
        digest_input = {"candidate_identity": identity, "public_config_identity": public}
        digest = live.candidate_digest(digest_input)
        self.assertEqual(digest, hashlib.sha256(live.canonical(digest_input)).hexdigest())
        line_counts = {path: len((control.BOT / path).read_bytes().splitlines()) for path in identity["files"]}
        for path in ("tools/predictfun/btc5m_control.py", "tools/predictfun/tests/test_btc5m_control.py"):
            self.assertLessEqual(line_counts[path], 3599 if path.endswith("test_btc5m_control.py") else 2400)
        # Historical live003 binding: the record on disk is pinned, not recomputed.
        live003 = control.BOT.parents[4] / "data/reports/2026-11-21-btc5m-controlled-paper/evidence/btc5m-supervised-live-003"
        live_public = json.loads((live003 / "public-config.json").read_bytes())
        live_started = json.loads((live003 / "started.json").read_bytes())
        live_public_identity = live_started["public_config_identity"]
        self.assertEqual(hashlib.sha256(live.canonical(live_public)).hexdigest(),
                         live_public_identity["sha256"])
        self.assertEqual(live_public["bot"]["enable_trading"], True)
        self.assertEqual({key: live_public_identity[key] for key in historical_public},
                         historical_public)
        saved = json.loads((repair_root / "deployment/candidate-identity.json").read_bytes())
        self.assertEqual(saved["candidate_digest"],
                         "f9b993feb311614482b3c712300f64d4bd94754a88477306a9bfde816501ecb0")
        self.assertEqual(saved["public_config_identity"], live_public_identity)
        self.assertEqual(saved["scope"],
                         "deployed and bound; pending independent Validate; not activated or started")
        # Historical live005 binding remains a frozen record, never recomputed.
        live005 = control.BOT.parents[4] / "data/reports/2026-11-21-btc5m-controlled-paper/evidence/btc5m-supervised-live-005"
        live005_public = json.loads((live005 / "public-config.json").read_bytes())
        live005_identity = json.loads((live005 / "started.json").read_bytes())["public_config_identity"]
        self.assertEqual(hashlib.sha256(live.canonical(live005_public)).hexdigest(),
                         live005_identity["sha256"])
        self.assertEqual(live005_public["bot"]["enable_trading"], True)
        self.assertEqual({key: live005_identity[key] for key in historical_public},
                         historical_public)
        # Pin historical R6 records to each other: C1 deadline cap; C2 settlement fixtures.
        c1_changed = {"tools/predictfun/btc5m_control.py", "tools/predictfun/btc5m_live_events.py",
                      "tools/predictfun/btc5m_live_guard.py", "tools/predictfun/tests/test_btc5m_control.py",
                      "tools/predictfun/tests/test_btc5m_live_events.py",
                      "tools/predictfun/tests/test_btc5m_live_guard.py"}
        c2_changed = {"src/service/predict_fun/account.rs", "src/service/predict_fun/fills.rs",
                      "src/service/venue.rs", "src/bot/runtime/live.rs",
                      "src/bot/runtime/live_control/tests.rs", "tools/predictfun/tests/test_btc5m_control.py"}
        final_binary = control.LIVE_BINARY
        final_sha = hashlib.sha256(final_binary.read_bytes()).hexdigest()
        self.assertEqual(final_sha, control.LIVE_BINARY_SHA256)
        candidate_identity = control.public_identity({}, live=True)
        live_digest_input = {"candidate_identity": candidate_identity,
                             "public_config_identity": public}
        live_digest = live.candidate_digest(live_digest_input)
        binding_root = control.BOT.parents[4] / (
            "data/reports/2026-11-22-btc5m-minimal-remediation/evidence")
        current_path = binding_root / "parameterization/post-binding-identity-r14b.json"  # R14b: test-only guard rebind
        current = json.loads(current_path.read_bytes())
        c1_bound = json.loads((binding_root / "c1-c2-final/post-binding-identity.json").read_bytes())
        self.assertEqual(c1_bound["candidate_digest"], "617f9f264454bf2fd5c72d153e22c633458b4b88e99201fd2ac867b9f56562d4")
        self.assertEqual({path for path, sha in c1_bound["candidate_identity"]["files"].items()
                          if candidate_identity["files"][path] != sha}, r2_changed | r5_changed | r6_changed | {"src/service/venue.rs"} | send_latency_changed | r13_changed | r13c_changed | ((r15_changed | r16_changed | parameterization_changed) & set(c1_bound["candidate_identity"]["files"])) | {"src/bot/runtime/live_control/tests/live003_regression.rs", "src/bot/runtime/live_control/tests/live004_regression.rs"})
        pre_binding = json.loads((binding_root / "rework-r6/final-candidate-identity.json").read_bytes())
        self.assertEqual(pre_binding["candidate_digest"],
                         "e20da168d8cd2b08203e9128ce23145cc4d72e55d7d04cf7f31c600d69b34d5c")
        self.assertEqual(pre_binding["binary_sha256"],
                         "2c6eb7b7397923a52016624a8fb15019dd87ebb07f28333e5d25ca1e6aea260b")
        r6_bound = json.loads((binding_root / "rework-r6-binding/final-candidate-identity.json").read_bytes())
        self.assertEqual(r6_bound["candidate_digest"],
                         "17eb926f23a3c11bc94e89888438e62f40687776989bb6b3bbe3fb1bec35f10d")
        for record in (pre_binding, r6_bound):
            digest_input = {key: record[key] for key in ("candidate_identity", "public_config_identity")}
            self.assertEqual(record["candidate_digest"], live.candidate_digest(digest_input))
            self.assertEqual(set(record["candidate_identity"]["files"]) ^ set(candidate_identity["files"]), set(control.LIVE_PATHS) - set(record["candidate_identity"]["files"]))
            self.assertEqual(hashlib.sha256((control.BOT.parents[4] / record["binary_path"]).read_bytes()).hexdigest(),
                             record["binary_sha256"])
        self.assertEqual({path for path, digest in pre_binding["candidate_identity"]["files"].items()
                          if r6_bound["candidate_identity"]["files"][path] != digest}, deployment_changed)
        self.assertEqual({path for path, digest in r6_bound["candidate_identity"]["files"].items()
                          if c1_bound["candidate_identity"]["files"][path] != digest}, c1_changed | c2_changed)
        final_pre = json.loads((binding_root / "c1-c2-final/pre-binding-identity.json").read_bytes())
        self.assertEqual({path for path, digest in final_pre["candidate_identity"]["files"].items()
                          if c1_bound["candidate_identity"]["files"][path] != digest}, deployment_changed)
        previous = json.loads((stop_diag_root / "deployment/candidate-identity.json").read_bytes())
        remediation_changed = {"src/bot/runtime/live.rs", "src/bot/runtime/live_control/tests.rs",
                               "src/service/predict_fun/request.rs", "src/service/predict_fun/account.rs",
                               "tools/predictfun/btc5m_control.py", "tools/predictfun/btc5m_live_events.py",
                               "tools/predictfun/tests/test_btc5m_control.py",
                               "tools/predictfun/tests/test_btc5m_live_events.py"}
        self.assertEqual(set(identity["files"]) ^ set(previous["candidate_identity"]["files"]), set(control.LIVE_PATHS) - set(previous["candidate_identity"]["files"]))
        self.assertEqual({path for path, digest in previous["candidate_identity"]["files"].items()
                          if identity["files"][path] != digest},
                         remediation_changed | config_a1_changed | c1_changed | c2_changed | r2_changed | r5_changed | send_latency_changed | r13_changed | r13c_changed | ((r15_changed | r16_changed | parameterization_changed) & set(previous["candidate_identity"]["files"])) | {"src/bot/runtime/live_control/tests/live004_regression.rs"})
        self.assertNotEqual(current["candidate_digest"], previous["candidate_digest"])
        self.assertNotEqual(live_digest, saved["candidate_digest"])
        self.assertNotEqual(live_digest, "6908dd0072d916494e5e2f5059c34ef143ac2acbebd0f9a7f7a3c8e7f6ebb617")
        self.assertNotEqual(live_digest, candidate_identity["binary_sha256"])
        self.assertEqual(current["candidate_digest"], live.candidate_digest({key: current[key] for key in ("candidate_identity", "public_config_identity")}))
        self.assertEqual({path for path, digest in current["candidate_identity"]["files"].items() if candidate_identity["files"][path] != digest}, deployment_changed | r15_changed | r16_changed | {"tools/predictfun/local_signer.py", "tools/predictfun/btc5m_launch.py", "tools/predictfun/tests/test_btc5m_launch.py", "docs/LIVE-TRADING.md", "src/bot/runtime/live_control/tests/live004_regression.rs"})
        self.assertEqual(set(current["candidate_identity"]["files"]) ^ set(candidate_identity["files"]), set(control.LIVE_PATHS) - set(current["candidate_identity"]["files"]))
        self.assertEqual(current["public_config_identity"], public)
        historical_binary = binding_root / "parameterization/candidate-r14/polymarket-toolkits"
        self.assertEqual(current["expected_binary_sha256"], "96a7ed059eb13253f36c8dd6bd8abf42a803574b7b8171bfa698b1aad7b1e1b7")
        self.assertEqual(current["binary_sha256"], current["expected_binary_sha256"])
        self.assertEqual(current["binary_sha256"], hashlib.sha256(historical_binary.read_bytes()).hexdigest())
        self.assertEqual(current["file_count"], 65)
        self.assertEqual(current["file_count"], len(current["candidate_identity"]["files"]))
        self.assertEqual(final_sha, "bee8cbc4ab916133dcf2bd86306c65d0d0923d1009f2300880601c506e652dd1")
        self.assertEqual(current["scope"],
                         "deployed and bound; pending independent Validate; not activated or started")
        self.assertEqual({key: current[key] for key in public if key != "sha256"},
                         {key: public[key] for key in public if key != "sha256"})
        print("CURRENT_LIVE_CANDIDATE_IDENTITY " + json.dumps({**live_digest_input,
              "candidate_digest": live_digest, "file_count": len(identity["files"])}, sort_keys=True))
        print("FINAL_LIVE_CANDIDATE " + json.dumps({**digest_input, "candidate_digest": digest,
              "file_count": len(identity["files"]),
              "public_config_kind": "offline signed projection of unchanged paper public input; not deployment",
              "lines": line_counts}, sort_keys=True, indent=2))

    def test_missing_activation_has_zero_storage_secret_child(self):
        args, root, child, loader, _collection, _path, driver, _database, _builder = self.launch_fixture()
        import btc5m_live_guard as guard
        with patch.object(guard, "ACTIVATION_PATH", self.root / "absent.json"):
            with self.assertRaises(control.Refused):
                control.run(args, environment={}, evidence_root=root, popen=child)
        driver.assert_not_called()
        loader.assert_not_called()
        child.assert_not_called()

    def test_guard_readback_failure_and_lock_conflict_precede_trading_secrets(self):
        import btc5m_live_guard as guard
        args, root, child, loader, collection, _path, driver, _database, _builder = self.launch_fixture()
        activation = json.loads(_path.read_bytes())
        lock = guard.AccountLock(activation["account_identity"], "signed-future-live").acquire()
        try:
            with self.assertRaises(control.Refused) as refused:
                control.run(args, environment={}, evidence_root=root, popen=child)
            self.assertEqual(refused.exception.reason, "ALREADY_RUNNING")
            driver.assert_not_called()
        finally:
            lock.close()
        collection.fault = "read_timeout"
        with self.assertRaises(control.Refused):
            control.run(args, environment={}, evidence_root=root, popen=child)
        self.assertEqual(len(collection.documents), 1)
        loader.assert_not_called()
        child.assert_not_called()
        self.assertFalse(root.exists())

    def test_fsync_failure_prevents_first_commit(self):
        facts, final, read, store = self.consume(self.native_stream(813, 827),
            lambda store, process: patch.object(self.live.os, "fsync", side_effect=OSError()))
        self.assertEqual(facts["events"], 0)
        self.assertEqual(facts["stop_reason"], "AUDIT_UNAVAILABLE")
        self.assertEqual(store.relations.seq, 0)
        self.assertTrue(store.failed)
        self.assertIsNone(final)

    def test_ack_broken_pipe_keeps_durable_order_but_no_final(self):
        facts, final, read, store = self.consume(self.native_stream(813, 827),
            lambda store, process: patch.object(process.stdin, "write", side_effect=BrokenPipeError()))
        self.assertEqual(facts["stop_reason"], "AUDIT_UNAVAILABLE")
        self.assertEqual(store.relations.seq, 5)
        self.assertTrue(store.failed)
        self.assertIsNone(final)
        self.assertEqual(read["events"][-1]["event_kind"], "order")
        self.assertIsNone(read["ended"])

    def test_wrong_wire_identity_never_reaches_first_write(self):
        frames = self.native_stream(813, 827)
        first = self.live.validate_live_event(frames[0])
        # Keep metadata's identity, change only the child's actual first frame.
        original = self.metadata
        self.metadata = lambda frame: original(frames[0])
        first["run_id"] = "unrelated-run"
        broken = self.live.EVENT_PREFIX + self.live.canonical(first) + b"\n"
        facts, final, read, store = self.consume([broken] + frames[1:])
        self.assertEqual(facts["events"], 0)
        self.assertEqual((store.directory / "events.jsonl").read_bytes(), b"")
        self.assertIsNone(final)

    def test_first_write_failure_closes_control_and_withholds_ack(self):
        facts, final, read, store = self.consume(self.native_stream(813, 827),
            lambda store, process: patch.object(store.stream, "write", side_effect=OSError("offline IO")))
        self.assertEqual(facts["events"], 0)
        self.assertEqual(facts["launcher_exit"], 1)
        self.assertTrue(store.failed)
        self.assertFalse((store.directory / "ended.json").exists())
        self.assertEqual((store.directory / "events.jsonl").read_bytes(), b"")
        self.assertEqual(read["status"], "INCOMPLETE")


class LauncherFixture(unittest.TestCase):
    """The shared launcher fixture: no network, no keyring, no real child."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.config_path = self.root / "controlled.json"
        self.config_path.write_text(json.dumps(public_config()))
        self.script = self.root / "fake_child.py"
        self.script.write_text(CHILD)
        self.environment = {"YQUANT_PREDICT_FUN_TOKEN": "synthetic-api"}
        self.children = []
        self.identity = {"main_head": "1" * 40, "bot_head": "2" * 40,
                         "binary_sha256": "3" * 64, "files": {}}
        patches = [patch.object(control, "public_identity", return_value=self.identity),
                   patch.object(control, "check_paths", side_effect=lambda path: (path, self.script)),
                   # No test in this file may ever read the production file.
                   patch.object(control, "API_SOURCE_FILE", self.root / "absent.env"),
                   patch("socket.socket.connect", side_effect=AssertionError("NETWORK_FORBIDDEN")),
                   patch.object(local_signer, "_read_owned", side_effect=AssertionError("KEYRING_FORBIDDEN")),
                   patch.object(control, "RUN_SECONDS", .1),
                   patch.object(control, "CLEANUP_SECONDS", .2),
                   patch.object(control, "TAIL_SECONDS", .1)]
        self.mocks = [item.start() for item in patches]
        for item in patches:
            self.addCleanup(item.stop)

    def make_popen(self, mode="normal", signed=False, event=None):
        """Build the fixed no-network fake child launcher and its capture."""
        captured = {}

        def popen(argv, **kwargs):
            captured.update(argv=argv, kwargs=kwargs)
            snapshot = Path(argv[argv.index("--config") + 1])
            self.assertNotEqual(snapshot, self.config_path)
            self.assertEqual(json.loads(snapshot.read_text()), json.loads(self.config_path.read_text()))
            self.config_path.write_text('{}')  # The child must use the frozen snapshot.
            child = subprocess.Popen([sys.executable, "-B", str(self.script), mode,
                                      json.dumps(event or (signed_final_event() if signed else final_event()))], **kwargs)
            self.children.append(child)
            return child

        return popen, captured

    def capture_evidence_writes(self):
        """Return a patcher and a list holding every byte written into the evidence."""
        original_open = open
        records = []
        test = self

        class WriteSpy:
            def __init__(self, handle):
                self.handle = handle

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return self.handle.__exit__(*args)

            def write(self, text):
                for secret in ("FIX_SYNTHETIC_SECRET_SENTINEL", "SYNTHETIC_SECRET"):
                    test.assertNotIn(secret, text)
                    test.assertNotIn(hashlib.sha256(secret.encode()).hexdigest(), text)
                records.append(text)
                return self.handle.write(text)

            def flush(self):
                self.handle.flush()

            def __getattr__(self, name):
                return getattr(self.handle, name)

        def spy(path, *args, **kwargs):
            handle = original_open(path, *args, **kwargs)
            return WriteSpy(handle) if "evidence" in Path(path).parts else handle

        return patch("builtins.open", side_effect=spy), records

    def launch(self, extra=(), data=None, environment=None, mode="normal", signed=False, event=None,
               run_id="run-1", evidence_root=None):
        if data is not None:
            self.config_path.write_text(json.dumps(data))
        popen, captured = self.make_popen(mode=mode, signed=signed, event=event)
        facts = control.run(["--mode", "signed-future-live" if signed else "unsigned-paper",
                             "--config", str(self.config_path), "--run-id", run_id, *extra],
                            environment=self.environment if environment is None else environment,
                            evidence_root=evidence_root or (self.root / "evidence"), popen=popen)
        for child in self.children:
            self.assertIsNotNone(child.poll())
            with self.assertRaises(ChildProcessError):
                os.waitpid(child.pid, os.WNOHANG)
        return facts, captured

    def main_launch(self, extra=(), data=None, environment=None, mode="normal", signed=False,
                    run_id="run-1", evidence_root=None):
        """Drive the real public entry point so its own stdout/stderr are observable."""
        if data is not None:
            self.config_path.write_text(json.dumps(data))
        popen, captured = self.make_popen(mode=mode, signed=signed)
        original = control.run

        def wrapper(argv=None, **kwargs):
            kwargs.update(environment=self.environment if environment is None else environment,
                          evidence_root=evidence_root or (self.root / "evidence"), popen=popen)
            return original(argv, **kwargs)

        out, err = io.StringIO(), io.StringIO()
        with patch.object(control, "run", side_effect=wrapper), \
                redirect_stdout(out), redirect_stderr(err):
            code = control.main(["--mode", "signed-future-live" if signed else "unsigned-paper",
                                 "--config", str(self.config_path), "--run-id", run_id, *extra])
        return code, out.getvalue(), err.getvalue(), captured


class LauncherTest(LauncherFixture):

    def test_the_mode_is_required(self):
        with self.assertRaises(control.Refused):
            control.build_parser().parse_args(["--config", "x", "--run-id", "r"])

    def test_the_run_id_must_be_a_public_identifier(self):
        for value in ("../escape", "Run-1", "", "-lead", "a" * 65, "with space", "r\n"):
            with self.subTest(value=value), self.assertRaises(control.Refused):
                control.run(["--mode", "unsigned-paper", "--config", str(self.config_path),
                             "--run-id", value], environment=self.environment)

    def test_the_live_options_are_refused(self):
        for extra in (["--predict-budget-confirmation", "56:address:eoa"],
                      ["--predict-rpc-url", "https://offline.invalid"]):
            with self.assertRaises(control.Refused):
                self.launch(extra=extra)

    def test_signed_mode_requires_confirmation_then_uses_original_env_builder(self):
        with self.assertRaises(control.Refused):
            self.launch(signed=True)
        data = public_config(**{"bot.enable_trading": True, "bot.mock_trading": False})
        data.pop("controlled_paper")
        with patch.object(local_signer, "_read_owned", return_value=b"01" * 32), patch.object(
                local_signer, "build_child_env", wraps=local_signer.build_child_env) as build:
            facts, captured = self.launch(signed=True, data=data, extra=[
                "--predict-budget-confirmation", f"56:{local_signer.TRADING_ADDRESS}:predict-account",
                "--predict-rpc-url", "https://offline.invalid"])
        self.assertEqual(build.call_count, 1)
        self.assertIn("signed-future-live", captured["argv"])
        self.assertEqual(captured["kwargs"]["env"]["PM_PRIVATE_KEY"], "01" * 32)
        self.assertEqual(facts["child_returncode"], 0)
        self.assertEqual(facts["launcher_exit"], 0)
        started = json.loads((self.root / "evidence/run-1/started.json").read_text())
        ended = json.loads((self.root / "evidence/run-1/ended.json").read_text())
        self.assertIsNone(started["public_config_identity"]["initial_cash"])
        self.assertIsNone(ended["final_accounting"]["cash"])
        self.assertNotIn("gas NOT_INCURRED", ended["limitation"])
        self.assertIn("LIVE_ACCOUNTING_UNAVAILABLE", ended["limitation"])

    def test_a_changed_fixed_value_is_refused(self):
        for path, value in {"controlled_paper.initial_cash": 500, "controlled_paper.fee_model": "gross",
                            "strategies.limits.max_order_usd": True, "strategies.limits.max_total_usd": 100,
                            "strategies.btc_arb.symbols": ["ETHUSDT"], "strategies.btc_arb.round_interval_secs": 900,
                            "strategies.btc_arb.min_edge": .01, "bot.enable_trading": True,
                            "bot.mock_trading": False, "predict_fun.network": "testnet", "venue": "polymarket"}.items():
            with self.subTest(path=path), self.assertRaises(control.Refused):
                self.launch(data=public_config(**{path: value}))

    def test_complete_app_config_is_checked_before_secret_or_child(self):
        mutations = [(section, None) for section in
                     ("site", "strategy", "trading", "risk", "exchange")]
        mutations += [("site.clob_api_base", 1), ("bot.wallets_to_track", [1]),
                      ("trading.rate_limit", -1), ("risk.consecutive_trigger", True),
                      ("strategy.copy_strategy", "unknown"),
                      ("strategies.poll_secs", 6), ("strategies.rescan_secs", 301),
                      ("strategies.quote_ttl_secs", 301), ("strategies.poll_secs", 5.0),
                      ("filters.tags_allow", "not-a-list"),
                      ("tp_sl.enabled", None), ("exchange.chain_id", "137")]
        for path, value in mutations:
            with self.subTest(path=path), self.assertRaises(control.Refused):
                self.launch(data=public_config(**{path: value}))
        for section, field in (("site", "polygon_ws_url"), ("strategy", "copy_size"),
                               ("trading", "rate_limit"), ("risk", "min_depth_usd"),
                               ("exchange", "domain_name"), ("strategies", "poll_secs")):
            data = public_config()
            del data[section][field]
            with self.subTest(missing=field), self.assertRaises(control.Refused):
                self.launch(data=data)
        self.assertFalse(self.children)

    def test_a_missing_controlled_block_is_refused(self):
        data = public_config()
        data.pop("controlled_paper")
        with self.assertRaises(control.Refused):
            self.launch(data=data)

    def test_a_duplicate_key_is_refused(self):
        for raw in ('{"a":1,"a":2}', '{"a":{"b":1,"b":2}}', '{"a":[{"b":1,"b":2}]}',
                    '{"a":NaN}', '{"a":{"jwt":"SYNTHETIC_SECRET"}}'):
            self.config_path.write_text(raw)
            with self.assertRaises(control.Refused):
                control.read_public_config(self.config_path)

    def test_secret_json_keys_are_refused_before_first_evidence_write(self):
        sentinel = "FIX_SYNTHETIC_SECRET_SENTINEL"
        names = set(control.FORBIDDEN_ENV) | {"YQUANT_PREDICT_FUN_TOKEN", "SIGNATURE",
                "PM_PREDICT_API_KEY", "password", "api_key", "credentials",
                "predict_api_key", "seed", "mnemonic"}
        original_open = open
        writes = []

        def spy(path, *args, **kwargs):
            if "evidence" in Path(path).parts:
                writes.append(str(path))
            return original_open(path, *args, **kwargs)

        for mode in ("unsigned-paper", "signed-future-live"):
            for name in sorted(names):
                for spelling in {name, name.lower(), name.swapcase()}:
                    for nested in (False, True):
                        data = public_config()
                        if mode == "signed-future-live":
                            data["bot"].update(enable_trading=True, mock_trading=False)
                            data.pop("controlled_paper")
                        node = {spelling: sentinel}
                        if nested:
                            data["predict_fun"]["extra"] = [{"child": [node]}]
                        else:
                            data.update(node)
                        self.config_path.write_text(json.dumps(data))
                        args = ["--mode", mode, "--config", str(self.config_path), "--run-id", "run-1"]
                        if mode == "signed-future-live":
                            args += ["--predict-budget-confirmation",
                                     f"56:{local_signer.TRADING_ADDRESS}:predict-account",
                                     "--predict-rpc-url", "https://offline.invalid"]
                        with self.subTest(mode=mode, name=spelling, nested=nested), patch(
                                "builtins.open", side_effect=spy), patch.object(
                                local_signer, "_read_owned", side_effect=AssertionError("KEYRING_FORBIDDEN")) as secret, patch.object(
                                control, "subprocess") as child, patch.object(
                                control, "public_identity", side_effect=AssertionError("IDENTITY_FORBIDDEN")) as identity, self.assertRaises(control.Refused):
                            control.run(args, environment=self.environment,
                                        evidence_root=self.root / "evidence" / f"case-{len(writes)}",
                                        popen=child.Popen)
                        identity.assert_not_called()
                        secret.assert_not_called()
                        child.Popen.assert_not_called()
        self.assertFalse(writes)
        self.assertFalse((self.root / "evidence").exists())

    def test_a_wallet_in_the_environment_is_refused(self):
        for name in control.FORBIDDEN_ENV:
            with self.subTest(name=name), self.assertRaises(control.Refused):
                self.launch(environment={**self.environment, name: "SYNTHETIC_SECRET"})
        self.assertFalse(self.children)

    def test_a_missing_api_key_is_refused(self):
        # The fallback file is isolated to a fixture that does not exist:
        # the real production skills/.env is never read by a test.
        with patch.object(control, "API_SOURCE_FILE", self.root / "absent.env"):
            with self.assertRaises(control.Refused):
                self.launch(environment={})

    def test_the_child_environment_holds_only_the_market_key(self):
        facts, captured = self.launch(environment={**self.environment, "PM_OTHER": "not-forwarded"})
        env = captured["kwargs"]["env"]
        self.assertEqual(env["PM_PREDICT_API_KEY"], "synthetic-api")
        self.assertNotIn("PM_OTHER", env)
        self.assertTrue(all(name not in env for name in control.FORBIDDEN_ENV))
        self.assertEqual(facts["launcher_exit"], 0)

    def test_matching_api_environment_alias_remains_supported(self):
        environment = {**self.environment, "PM_PREDICT_API_KEY": "synthetic-api"}
        facts, captured = self.launch(environment=environment)
        self.assertEqual(facts["launcher_exit"], 0)
        self.assertEqual(captured["kwargs"]["env"]["PM_PREDICT_API_KEY"], "synthetic-api")
        self.assertEqual(environment["PM_PREDICT_API_KEY"], "synthetic-api")
        with self.assertRaises(control.Refused) as caught:
            control.check_environment({**environment, "PM_PREDICT_API_KEY": "synthetic-conflict"})
        self.assertEqual(caught.exception.code, "ENV_CONFLICT")

    def test_the_child_command_line_is_fixed(self):
        _, captured = self.launch()
        self.assertEqual(captured["argv"][-2:], ["run", "btc-arb"])
        self.assertFalse(captured["kwargs"]["shell"])
        self.assertTrue(captured["kwargs"]["close_fds"])

    def test_the_evidence_is_exclusive(self):
        self.launch()
        self.config_path.write_text(json.dumps(public_config()))
        with self.assertRaises(control.Refused) as caught:
            self.launch()
        self.assertEqual(caught.exception.code, "DUPLICATE_RUN")
        self.assertEqual(len(self.children), 1)

    def test_only_the_accounting_events_are_written(self):
        facts, _ = self.launch()
        self.assertEqual(facts["events"], 1)
        self.assertGreaterEqual(facts["discarded"], 1)
        self.assertEqual(json.loads((self.root / "evidence/run-1/events.jsonl").read_text())["event_kind"], "final")

    def test_the_child_output_is_not_written(self):
        facts, _ = self.launch(mode="secret")
        self.assertNotEqual(facts["launcher_exit"], 0)
        for file in (self.root / "evidence/run-1").iterdir():
            self.assertNotIn("SYNTHETIC_SECRET", file.read_text())

    def test_the_child_exit_is_reported_separately(self):
        facts, _ = self.launch(mode="nonzero")
        self.assertEqual(facts["child_returncode"], 124)
        self.assertEqual(facts["launcher_exit"], 1)

    def test_the_ended_record_states_the_limits(self):
        self.launch()
        ended = json.loads((self.root / "evidence/run-1/ended.json").read_text())
        self.assertIn("gas NOT_INCURRED", ended["limitation"])
        self.assertEqual(ended["final_accounting"]["cash"], 100)
        started = json.loads((self.root / "evidence/run-1/started.json").read_text())
        self.assertEqual(started["public_config_identity"]["sha256"], hashlib.sha256(
            (self.root / "evidence/run-1/public-config.json").read_bytes()).hexdigest())

    def test_real_pipe_is_open_until_deadline_and_D_precedes_eof(self):
        facts, _ = self.launch(mode="deadline")
        self.assertEqual(facts["child_returncode"], 0)
        self.assertEqual(facts["stop_reason"], "DEADLINE")
        self.assertEqual(facts["launcher_exit"], 0)

    def test_real_refusing_child_is_killed_and_reaped(self):
        facts, _ = self.launch(mode="refuse")
        self.assertEqual(facts["child_returncode"], -signal.SIGKILL)
        self.assertTrue(facts["forced_cleanup"])
        self.assertEqual(facts["launcher_exit"], 1)

    def test_kill_and_reap_share_one_absolute_cleanup_budget(self):
        import time
        evidence = control.Evidence(self.root, "run-1", "", "", "unsigned-paper")
        process = subprocess.Popen([sys.executable, "-B", str(self.script), "refuse", ""],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        original_wait = process.wait
        budgets = []

        def wait(*, timeout):
            budgets.append(timeout)
            if len(budgets) == 1:
                time.sleep(.01)  # Reaping itself consumes the existing budget.
                raise subprocess.TimeoutExpired("synthetic-child", timeout)
            return original_wait(timeout=timeout)

        with patch.object(process, "wait", side_effect=wait), patch.object(control, "TAIL_SECONDS", 0):
            facts, _ = control.supervise(process, evidence,
                datetime.datetime.now(datetime.timezone.utc), control.boottime())
        self.assertEqual(len(budgets), 2)
        self.assertGreaterEqual(budgets[1], 0)
        self.assertLess(budgets[1], budgets[0] - .005)
        self.assertLessEqual(budgets[0], control.CLEANUP_SECONDS)
        self.assertEqual(facts["child_returncode"], -signal.SIGKILL)
        with self.assertRaises(ChildProcessError):
            os.waitpid(process.pid, os.WNOHANG)

    def test_signal_is_propagated_and_native_exit_is_retained(self):
        facts, _ = self.launch(mode="signal-parent")
        self.assertEqual(facts["launcher_exit"], 130)
        self.assertIsNotNone(facts["child_returncode"])

    def test_missing_final_and_oversize_output_are_incomplete(self):
        for mode in ("missing", "long"):
            with self.subTest(mode=mode):
                self.root = self.root / mode
                self.root.mkdir()
                self.config_path.write_text(json.dumps(public_config()))
                facts, _ = self.launch(mode=mode)
                self.assertEqual(facts["launcher_exit"], 1)

    def test_nested_unknown_fields_nan_duplicate_keys_and_bad_types_are_rejected(self):
        for mutate in (lambda event: event["accounting"].update(secret="SYNTHETIC_SECRET"),
                       lambda event: event["accounting"].update(cash=float("nan")),
                       lambda event: event["accounting"].update(cash=True),
                       lambda event: event["source"].update(markets=[{"url": "SYNTHETIC_SECRET"}]),
                       lambda event: event.update(event_time="SYNTHETIC_SECRET")):
            event = final_event()
            mutate(event)
            with self.assertRaises((ValueError, TypeError)):
                control.validate_event(json.dumps(event).encode(), "run-1")
        with self.assertRaises(ValueError):
            control.validate_event(b'{"run_id":"run-1","run_id":"run-1"}', "run-1")

    def test_event_kind_requires_amounts_and_nonnullable_cash_before_write(self):
        for name in ("cash", "available", "reserved", "cost", "realized", "fees_in_cash", "loss_latched"):
            event = final_event()
            event["accounting"][name] = None
            with self.subTest(name=name), self.assertRaises(ValueError):
                control.validate_event(json.dumps(event).encode(), "run-1")
        event = final_event()
        event.update(event_kind="fill", market_id="1", token="77", source={})
        with self.assertRaises(ValueError):
            control.validate_event(json.dumps(event).encode(), "run-1")
        event = final_event()
        event["accounting"].update(cash=99, available=99)
        with self.assertRaises(ValueError):
            control.validate_event(json.dumps(event).encode(), "run-1")

    def test_inconsistent_adjacent_snapshots_are_never_written(self):
        first = final_event()
        first.update(event_kind="summary", source=None)
        second = final_event()
        second["event_seq"] = 2
        second["accounting"].update(cash=101, available=101, realized=1, nav=101, net_pnl=1)
        # Both satisfy per-event equations, but no fill or payout explains the extra cash.
        lines = "\n".join(control.EVENT_PREFIX + json.dumps(event) for event in (first, second))
        self.script.write_text(CHILD)
        evidence = control.Evidence(self.root, "run-1", "", "", "unsigned-paper")
        process = subprocess.Popen([sys.executable, "-B", str(self.script), "stream", lines],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        facts, final = control.supervise(process, evidence, datetime.datetime.now(datetime.timezone.utc),
                                         control.boottime())
        self.assertEqual(facts["launcher_exit"], 1)
        self.assertIsNone(final)
        self.assertEqual(len((self.root / "events.jsonl").read_text().splitlines()), 1)

    def test_preparation_time_counts_against_deadline(self):
        with patch.object(control, "boottime", side_effect=[0, 1]):
            facts, _ = self.launch()
        self.assertEqual(facts["stop_reason"], "DEADLINE")
        self.assertFalse(self.children)


class ApiSourceFallbackTest(LauncherFixture):
    """The single-key file fallback: environment first, then exactly one key."""

    def setUp(self):
        super().setUp()
        self.source_file = self.root / "skills.env"
        patcher = patch.object(control, "API_SOURCE_FILE", self.source_file)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.reads = []
        original = control.read_api_source_file

        def spy(path=None):
            self.reads.append(path)
            return original(path)

        patcher = patch.object(control, "read_api_source_file", side_effect=spy)
        patcher.start()
        self.addCleanup(patcher.stop)

    def write_source(self, text):
        self.source_file.write_text(text, encoding="utf-8")

    def test_the_fixed_file_is_derived_from_the_bot_location(self):
        root = control.BOT.parents[4]
        self.assertEqual(ORIGINAL_API_SOURCE_FILE.resolve(), (root / "skills/.env").resolve())
        # The fixed path must not come from the process working directory, so prove
        # it under a cwd that is never the repository root, wherever pytest is run.
        with tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"]) as elsewhere:
            previous = os.getcwd()
            os.chdir(elsewhere)
            try:
                self.assertNotEqual(ORIGINAL_API_SOURCE_FILE.resolve(),
                                    (Path.cwd() / "skills/.env").resolve())
            finally:
                os.chdir(previous)

    def test_a_missing_source_reads_only_the_one_key_from_the_fixed_file(self):
        self.write_source("# comment\nexport  YQUANT_PREDICT_FUN_TOKEN = 'file-key'  # trailing\n"
                          "OTHER_SECRET=must-not-be-imported\n")
        environment = {}
        before = dict(os.environ)
        facts, captured = self.launch(environment=environment)
        self.assertEqual(facts["launcher_exit"], 0)
        self.assertEqual(len(self.reads), 1)
        env = captured["kwargs"]["env"]
        self.assertEqual(env["PM_PREDICT_API_KEY"], "file-key")
        self.assertNotIn("OTHER_SECRET", env)
        self.assertTrue(all(name not in env for name in control.FORBIDDEN_ENV))
        self.assertEqual(environment, {})
        self.assertEqual(dict(os.environ), before)

    def test_the_fallback_accepts_the_fixed_single_line_syntax(self):
        for text, expected in (
                ("YQUANT_PREDICT_FUN_TOKEN=plain", "plain"),
                ("YQUANT_PREDICT_FUN_TOKEN=plain # note", "plain"),
                ("YQUANT_PREDICT_FUN_TOKEN=plain#literal", "plain#literal"),
                ("YQUANT_PREDICT_FUN_TOKEN=\"double quoted\"", "double quoted"),
                ("YQUANT_PREDICT_FUN_TOKEN='single quoted'", "single quoted"),
                ("  export\tYQUANT_PREDICT_FUN_TOKEN\t=\tvalue  ", "value"),
                ("YQUANT_PREDICT_FUN_TOKEN=\"quoted\" # note", "quoted"),
                ("YQUANT_PREDICT_FUN_TOKEN=$HOME", "$HOME"),
                ("YQUANT_PREDICT_FUN_TOKEN=`whoami`", "`whoami`"),
                ("YQUANT_PREDICT_FUN_TOKEN=a=b", "a=b")):
            with self.subTest(text=text):
                self.write_source(text)
                self.assertEqual(control.read_api_source_file(), expected)

    def test_a_trailing_comment_is_not_a_line_continuation(self):
        """A comment that ends in a backslash is still only a comment."""
        for text, expected in (
                ("YQUANT_PREDICT_FUN_TOKEN=plain # note \\", "plain"),
                ("YQUANT_PREDICT_FUN_TOKEN=plain # note\\", "plain"),
                ("YQUANT_PREDICT_FUN_TOKEN=\"quoted\" # note \\", "quoted"),
                ("YQUANT_PREDICT_FUN_TOKEN='quoted' # note \\", "quoted"),
                ("YQUANT_PREDICT_FUN_TOKEN=plain\\ # note", None),
                ("YQUANT_PREDICT_FUN_TOKEN=plain\\", None)):
            with self.subTest(text=text):
                self.write_source(text)
                if expected is None:
                    with self.assertRaises(control.Refused) as caught:
                        control.read_api_source_file()
                    self.assertEqual(caught.exception.code, "API_SOURCE")
                    self.assertEqual(caught.exception.step, "env")
                    self.assertEqual(caught.exception.reason, "API_SOURCE")
                    self.assertEqual(caught.exception.exit_code, 2)
                else:
                    self.assertEqual(control.read_api_source_file(), expected)

    def test_the_syntax_positives_reach_the_real_child(self):
        """Every accepted syntax drives a real run into the no-network fake child."""
        for index, (text, expected) in enumerate((
                ("YQUANT_PREDICT_FUN_TOKEN=plain", "plain"),
                ("YQUANT_PREDICT_FUN_TOKEN=plain # note", "plain"),
                ("YQUANT_PREDICT_FUN_TOKEN=\"double quoted\"", "double quoted"),
                ("YQUANT_PREDICT_FUN_TOKEN='single quoted'", "single quoted"),
                ("YQUANT_PREDICT_FUN_TOKEN=\"quoted#literal\" # note", "quoted#literal"),
                ("YQUANT_PREDICT_FUN_TOKEN=$(whoami)", "$(whoami)"),
                ("YQUANT_PREDICT_FUN_TOKEN=plain # note \\", "plain"),
                ("YQUANT_PREDICT_FUN_TOKEN=plain#literal", "plain#literal"),
                ("YQUANT_PREDICT_FUN_TOKEN=\"double quoted\" # note \\", "double quoted"),
                ("YQUANT_PREDICT_FUN_TOKEN='single quoted' # note \\", "single quoted"),
                ("  export\tYQUANT_PREDICT_FUN_TOKEN\t=\tvalue  # note \\", "value"),
                ("YQUANT_PREDICT_FUN_TOKEN=$HOME", "$HOME"),
                ("YQUANT_PREDICT_FUN_TOKEN=`whoami`", "`whoami`"),
                ("YQUANT_PREDICT_FUN_TOKEN=a=b", "a=b"))):
            with self.subTest(text=text):
                self.write_source(text + "\nOTHER_SECRET=must-not-be-imported\n")
                self.config_path.write_text(json.dumps(public_config()))
                environment = {}
                before = dict(os.environ)
                facts, captured = self.launch(environment=environment,
                                             run_id="run-1",
                                             evidence_root=self.root / "evidence" / f"case-{index}")
                self.assertEqual(facts["launcher_exit"], 0)
                env = captured["kwargs"]["env"]
                self.assertEqual(env, {"PATH": "/usr/bin:/bin", "RUST_LOG": "warn",
                                       "PM_PREDICT_API_KEY": expected})
                self.assertEqual(len(self.children), index + 1)
                self.assertEqual(len(self.reads), index + 1)
                self.assertNotIn("OTHER_SECRET", env)
                self.assertTrue(all(name not in env for name in control.FORBIDDEN_ENV))
                self.assertEqual(environment, {})
                self.assertEqual(dict(os.environ), before)

    def test_an_existing_source_never_reads_the_file(self):
        self.write_source("YQUANT_PREDICT_FUN_TOKEN=file-key\n")
        facts, captured = self.launch(environment={"YQUANT_PREDICT_FUN_TOKEN": "synthetic-api"})
        self.assertEqual(facts["launcher_exit"], 0)
        self.assertEqual(self.reads, [])
        self.assertEqual(captured["kwargs"]["env"]["PM_PREDICT_API_KEY"], "synthetic-api")

    def test_a_blank_source_is_refused_without_reading_the_file(self):
        self.write_source("YQUANT_PREDICT_FUN_TOKEN=file-key\n")
        for value in ("", "   "):
            with self.subTest(value=value):
                with self.assertRaises(control.Refused) as caught:
                    self.launch(environment={"YQUANT_PREDICT_FUN_TOKEN": value})
                self.assertEqual(caught.exception.code, "API_SOURCE")
                self.assertEqual(self.reads, [])

    def test_an_alias_conflict_is_refused_without_reading_the_file(self):
        self.write_source("YQUANT_PREDICT_FUN_TOKEN=file-key\n")
        with self.assertRaises(control.Refused) as caught:
            self.launch(environment={"YQUANT_PREDICT_FUN_TOKEN": "synthetic-api",
                                     "PM_PREDICT_API_KEY": "synthetic-conflict"})
        self.assertEqual(caught.exception.code, "ENV_CONFLICT")
        self.assertEqual(self.reads, [])

    def test_a_matching_alias_stays_compatible(self):
        self.write_source("YQUANT_PREDICT_FUN_TOKEN=file-key\n")
        facts, captured = self.launch(environment={"PM_PREDICT_API_KEY": "file-key"})
        self.assertEqual(facts["launcher_exit"], 0)
        self.assertEqual(captured["kwargs"]["env"]["PM_PREDICT_API_KEY"], "file-key")

    def test_the_fallback_still_checks_the_alias_conflict(self):
        self.write_source("YQUANT_PREDICT_FUN_TOKEN=file-key\n")
        with self.assertRaises(control.Refused) as caught:
            self.launch(environment={"PM_PREDICT_API_KEY": "synthetic-conflict"})
        self.assertEqual(caught.exception.code, "ENV_CONFLICT")
        self.assertEqual(self.reads, [self.source_file])

    def test_a_bad_file_is_refused_without_a_child_or_evidence(self):
        for text in ("", "OTHER_KEY=value\n", "yquant_predict_fun_token=value\n",
                     "YQUANT_PREDICT_FUN_TOKEN=\n", "YQUANT_PREDICT_FUN_TOKEN=   \n",
                     "YQUANT_PREDICT_FUN_TOKEN=value\nYQUANT_PREDICT_FUN_TOKEN=value\n",
                     "YQUANT_PREDICT_FUN_TOKEN=one\nYQUANT_PREDICT_FUN_TOKEN=two\n",
                     "YQUANT_PREDICT_FUN_TOKEN=\"unterminated\n",
                     "YQUANT_PREDICT_FUN_TOKEN='value' trailing\n",
                     "YQUANT_PREDICT_FUN_TOKEN=value \\\n",
                     "YQUANT_PREDICT_FUN_TOKEN=\"multi\nline\"\n"):
            with self.subTest(text=text):
                self.write_source(text)
                with self.assertRaises(control.Refused) as caught:
                    self.launch(environment={})
                self.assertEqual(caught.exception.code, "API_SOURCE")
                self.assertEqual(caught.exception.step, "env")
                self.assertEqual(caught.exception.reason, "API_SOURCE")
                self.assertEqual(caught.exception.exit_code, 2)
                self.assertFalse(self.children)
                self.assertFalse((self.root / "evidence").exists())
                writer, records = self.capture_evidence_writes()
                with writer:
                    code, out, err, captured = self.main_launch(environment={})
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertEqual(json.loads(err), {"error": {"code": "API_SOURCE",
                                 "step": "env", "reason": "API_SOURCE"}})
                self.assertEqual(records, [])
                self.assertEqual(captured, {})
                if text:
                    self.assertNotIn(text, out + err)
                for raw in (text, "FIX_SYNTHETIC_SECRET_SENTINEL"):
                    self.assertNotIn(hashlib.sha256(raw.encode()).hexdigest(), out + err)

    def test_an_unreadable_or_missing_file_is_refused(self):
        """Both I/O failure classes refuse with the fixed error and no side effect."""
        cases = {
            "missing": None,
            "permission": PermissionError(13, "FIX_SYNTHETIC_SECRET_SENTINEL"),
            "oserror": OSError(5, "FIX_SYNTHETIC_SECRET_SENTINEL"),
            "missing-spy": FileNotFoundError(2, "FIX_SYNTHETIC_SECRET_SENTINEL"),
        }
        for name, error in cases.items():
            with self.subTest(case=name):
                if error is None:
                    self.source_file.unlink(missing_ok=True)
                else:
                    self.write_source("YQUANT_PREDICT_FUN_TOKEN=file-key\n")

                original = Path.read_text
                reads = []

                def spy_read_text(path, *args, **kwargs):
                    if Path(path) == self.source_file:
                        reads.append(path)
                        if error is not None:
                            raise error
                    return original(path, *args, **kwargs)

                writer, records = self.capture_evidence_writes()
                with patch.object(Path, "read_text", spy_read_text), writer:
                    with self.assertRaises(control.Refused) as caught:
                        self.launch(environment={})
                    code, out, err, captured = self.main_launch(environment={})
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertEqual(json.loads(err), {"error": {"code": "API_SOURCE",
                                 "step": "env", "reason": "API_SOURCE"}})
                self.assertEqual(captured, {})
                self.assertEqual(caught.exception.code, "API_SOURCE")
                self.assertEqual(caught.exception.step, "env")
                self.assertEqual(caught.exception.reason, "API_SOURCE")
                self.assertEqual(caught.exception.exit_code, 2)
                self.assertEqual(len(reads), 2)
                self.assertEqual(records, [])
                self.assertFalse(self.children)
                self.assertFalse((self.root / "evidence").exists())

    def test_an_invalid_utf8_file_is_refused(self):
        self.source_file.write_bytes(b"YQUANT_PREDICT_FUN_TOKEN=\xff\xfe\n")
        writer, records = self.capture_evidence_writes()
        with writer:
            with self.assertRaises(control.Refused) as caught:
                self.launch(environment={})
        self.assertEqual(caught.exception.code, "API_SOURCE")
        self.assertEqual(caught.exception.step, "env")
        self.assertEqual(caught.exception.reason, "API_SOURCE")
        self.assertEqual(caught.exception.exit_code, 2)
        self.assertEqual(records, [])
        self.assertFalse(self.children)
        self.assertFalse((self.root / "evidence").exists())

    def test_a_public_config_refusal_happens_before_the_file_is_read(self):
        self.write_source("YQUANT_PREDICT_FUN_TOKEN=file-key\n")
        with self.assertRaises(control.Refused):
            self.launch(environment={}, data=public_config(**{"bot.mock_trading": False}))
        self.assertEqual(self.reads, [])
        self.assertFalse(self.children)

    def test_the_secret_json_matrix_refuses_before_any_credential_read(self):
        """B-004: both modes, missing parent source, usable file, zero side effects."""
        self.write_source("YQUANT_PREDICT_FUN_TOKEN=file-key\n")
        sentinel = "FIX_SYNTHETIC_SECRET_SENTINEL"
        names = sorted(set(control.FORBIDDEN_ENV) | {"YQUANT_PREDICT_FUN_TOKEN", "PM_PREDICT_API_KEY",
                                                   "SIGNATURE", "predict_api_key", "api_secret",
                                                   "api_passphrase", "predict_jwt",
                                                   "credentials", "password", "api_key", "seed", "mnemonic"})
        for mode in ("unsigned-paper", "signed-future-live"):
            for name in names:
                for spelling in {name, name.lower(), name.swapcase()}:
                    for nested in (False, True):
                        with self.subTest(mode=mode, spelling=spelling, nested=nested):
                            data = public_config()
                            if mode == "signed-future-live":
                                data["bot"].update(enable_trading=True, mock_trading=False)
                                data.pop("controlled_paper")
                            node = {spelling: sentinel}
                            if nested:
                                data["predict_fun"]["extra"] = [{"child": [node]}]
                            else:
                                data.update(node)
                            self.config_path.write_text(json.dumps(data))
                            extra = []
                            if mode == "signed-future-live":
                                extra = ["--predict-budget-confirmation",
                                         f"56:{local_signer.TRADING_ADDRESS}:predict-account",
                                         "--predict-rpc-url", "https://offline.invalid"]
                            writer, records = self.capture_evidence_writes()
                            with writer, \
                                    patch.object(control, "subprocess") as process, \
                                    patch.object(local_signer, "_read_owned",
                                                 side_effect=AssertionError("KEYRING_FORBIDDEN")) as secret, \
                                    patch.object(control, "read_api_source_file",
                                                 side_effect=AssertionError("READ_FORBIDDEN")) as reader, \
                                    self.assertRaises(control.Refused) as caught:
                                control.run(["--mode", mode, "--config", str(self.config_path),
                                             "--run-id", "run-1", *extra],
                                            environment={},
                                            evidence_root=self.root / "evidence" / "never",
                                            popen=process.Popen)
                            self.assertEqual(caught.exception.code, "PUBLIC_CONFIG")
                            self.assertEqual(caught.exception.step, "config")
                            self.assertEqual(caught.exception.reason, "PUBLIC_CONFIG")
                            self.assertEqual(caught.exception.exit_code, 2)
                            reader.assert_not_called()
                            secret.assert_not_called()
                            process.Popen.assert_not_called()
                            self.assertEqual(records, [])
                            self.assertFalse((self.root / "evidence").exists())

    def test_a_parent_wallet_key_is_refused_before_the_file_is_read(self):
        self.write_source("YQUANT_PREDICT_FUN_TOKEN=file-key\n")
        for name in control.FORBIDDEN_ENV:
            with self.subTest(name=name):
                with self.assertRaises(control.Refused) as caught:
                    self.launch(environment={name: "SYNTHETIC_SECRET"})
                self.assertEqual(caught.exception.code, "ENV_CONFLICT")
                self.assertEqual(caught.exception.step, "env")
                self.assertEqual(caught.exception.exit_code, 2)
        self.assertEqual(self.reads, [])
        self.assertFalse(self.children)

    def test_a_parent_wallet_key_is_refused_with_zero_side_effects(self):
        """B-004: the parent forbidden key refuses first, with no read, write or child."""
        self.write_source("YQUANT_PREDICT_FUN_TOKEN=file-key\n")
        for name in control.FORBIDDEN_ENV:
            with self.subTest(name=name):
                writer, records = self.capture_evidence_writes()
                reader = MagicMock(side_effect=AssertionError("READ_FORBIDDEN"))
                with writer, patch.object(control, "read_api_source_file", reader), \
                        patch.object(control, "subprocess") as process, \
                        patch.object(local_signer, "_read_owned",
                                     side_effect=AssertionError("KEYRING_FORBIDDEN")) as secret, \
                        self.assertRaises(control.Refused) as caught:
                    control.run(["--mode", "unsigned-paper", "--config", str(self.config_path),
                                 "--run-id", "run-1"],
                                environment={name: "SYNTHETIC_SECRET"},
                                evidence_root=self.root / "evidence" / "never",
                                popen=process.Popen)
                self.assertEqual(caught.exception.code, "ENV_CONFLICT")
                self.assertEqual(caught.exception.step, "env")
                self.assertEqual(caught.exception.reason, "SECRET_PRESENT")
                self.assertEqual(caught.exception.exit_code, 2)
                reader.assert_not_called()
                secret.assert_not_called()
                process.Popen.assert_not_called()
                self.assertEqual(records, [])
                self.assertFalse((self.root / "evidence").exists())

    def test_the_signed_mode_never_reads_the_file(self):
        self.write_source("YQUANT_PREDICT_FUN_TOKEN=file-key\n")
        data = public_config(**{"bot.enable_trading": True, "bot.mock_trading": False})
        data.pop("controlled_paper")
        with self.assertRaises(control.Refused):
            self.launch(signed=True, data=data, environment={}, extra=[
                "--predict-budget-confirmation", f"56:{local_signer.TRADING_ADDRESS}:predict-account",
                "--predict-rpc-url", "https://offline.invalid"])
        self.assertEqual(self.reads, [])
        self.assertFalse(self.children)

    def test_the_read_comes_after_the_config_and_before_the_first_write(self):
        self.write_source("YQUANT_PREDICT_FUN_TOKEN=file-key\n")
        order = []
        original_open = open
        original_check = control.check_fixed_values

        def spy_open(path, *args, **kwargs):
            if "evidence" in Path(path).parts:
                order.append("write")
            return original_open(path, *args, **kwargs)

        def spy_check(*args, **kwargs):
            order.append("config")
            return original_check(*args, **kwargs)

        def spy_read(path=None):
            order.append("read")
            return "file-key"

        with patch("builtins.open", side_effect=spy_open), patch.object(
                control, "check_fixed_values", side_effect=spy_check), patch.object(
                control, "read_api_source_file", side_effect=spy_read):
            facts, _ = self.launch(environment={})
        self.assertEqual(facts["launcher_exit"], 0)
        self.assertEqual(order[0], "config")
        self.assertEqual(order[1], "read")
        self.assertEqual(order[2], "write")
        self.assertEqual(order.count("read"), 1)

    def test_no_secret_reaches_the_output_or_the_evidence(self):
        sentinel = "FIX_SYNTHETIC_SECRET_SENTINEL"
        self.write_source(f"YQUANT_PREDICT_FUN_TOKEN={sentinel}\nOTHER_SECRET={sentinel}\n")
        facts, captured = self.launch(environment={})
        self.assertEqual(facts["launcher_exit"], 0)
        self.assertEqual(captured["kwargs"]["env"]["PM_PREDICT_API_KEY"], sentinel)
        self.assertNotIn("OTHER_SECRET", captured["kwargs"]["env"])
        for file in (self.root / "evidence/run-1").iterdir():
            self.assertNotIn(sentinel, file.read_text())

    def test_no_secret_reaches_stdout_stderr_argv_or_the_first_write(self):
        """Scan success and rejected child output before and after persistence."""
        sentinel = "FIX_SYNTHETIC_SECRET_SENTINEL"
        self.write_source(f"YQUANT_PREDICT_FUN_TOKEN={sentinel}\nOTHER_SECRET={sentinel}\n")
        for mode, expected_code in (("normal", 0), ("secret", 1)):
            self.config_path.write_text(json.dumps(public_config()))
            root = self.root / "evidence" / mode
            writer, records = self.capture_evidence_writes()
            with writer:
                code, out, err, captured = self.main_launch(environment={}, mode=mode,
                                                           evidence_root=root)
            self.assertEqual(code, expected_code)
            self.assertTrue(records)
            self.assertEqual(captured["kwargs"]["env"]["PM_PREDICT_API_KEY"], sentinel)
            surfaces = [out, err, " ".join(captured["argv"]), "".join(records)]
            surfaces += [file.read_text() for file in (root / "run-1").iterdir()]
            for surface in surfaces:
                for raw in (sentinel, "SYNTHETIC_SECRET",
                            f"YQUANT_PREDICT_FUN_TOKEN={sentinel}"):
                    self.assertNotIn(raw, surface)
                    self.assertNotIn(hashlib.sha256(raw.encode()).hexdigest(), surface)

    def test_a_refused_source_leaks_nothing_into_the_output(self):
        """The refusal path leaks neither the value nor any digest of it."""
        sentinel = "FIX_SYNTHETIC_SECRET_SENTINEL"
        digest = hashlib.sha256(sentinel.encode()).hexdigest()
        self.write_source(f"YQUANT_PREDICT_FUN_TOKEN={sentinel}\n"
                          f"YQUANT_PREDICT_FUN_TOKEN={sentinel}\n")
        writer, records = self.capture_evidence_writes()
        with writer:
            code, out, err, _ = self.main_launch(environment={})
        self.assertEqual(code, 2)
        self.assertEqual(records, [])
        for surface in (out, err):
            self.assertNotIn(sentinel, surface)
            self.assertNotIn(digest, surface)
        self.assertIn("API_SOURCE", err)
        self.assertFalse((self.root / "evidence").exists())


class RustProtocolTest(unittest.TestCase):
    def test_parent_sigkill_closes_real_rust_pipe_and_interrupts_blocked_drive(self):
        self._assert_parent_loss("rust-parent")

    def test_parent_sigkill_interrupts_actual_blocked_market_call(self):
        self._assert_parent_loss("rust-parent-market")

    def _assert_parent_loss(self, mode):
        import select
        import time
        result = subprocess.run(["cargo", "test", "--offline", "--locked", "--target-dir", os.environ["CARGO_TARGET_DIR"], "--lib", "--no-run",
                                 "--message-format=json"], cwd=control.BOT, capture_output=True, timeout=120)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        artifacts = [json.loads(line) for line in result.stdout.splitlines() if line.startswith(b"{")]
        executable = next(item["executable"] for item in artifacts
                          if item.get("reason") == "compiler-artifact" and item.get("executable"))
        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / "fixture.py"
            script.write_text(CHILD)
            parent = subprocess.Popen([sys.executable, "-B", str(script), mode, executable],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            assert parent.stdout is not None
            output = b""
            deadline = time.monotonic() + 5
            try:
                while b"PIPE_READY" not in output and time.monotonic() < deadline:
                    if select.select([parent.stdout], [], [], .05)[0]:
                        output += os.read(parent.stdout.fileno(), 8192)
                self.assertIn(b"PIPE_READY", output)
            finally:
                parent.kill()
                parent.wait(timeout=1)
            rest, errors = parent.communicate(timeout=6)
            output += rest
            self.assertEqual(parent.returncode, -signal.SIGKILL)
            self.assertNotIn(b"FAILED", output, errors.decode())
            events = [json.loads(line[len(control.EVENT_PREFIX):]) for line in output.decode().splitlines()
                      if line.startswith(control.EVENT_PREFIX)]
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["source"]["stop_reason"], "PARENT_LOST")
            self.assertFalse(events[0]["accounting"]["loss_latched"])

    def test_signed_rust_final_is_unknown_not_a_paper_balance(self):
        result = subprocess.run(["cargo", "test", "--offline", "--locked", "--target-dir", os.environ["CARGO_TARGET_DIR"], "--lib",
                                 "bot::runtime::paper::tests::signed_ctx_uses_original_factory_and_identity_bound_budget_gate",
                                 "--", "--exact", "--nocapture"], cwd=control.BOT, capture_output=True, timeout=120)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        lines = [line for line in result.stdout.splitlines() if line.startswith(control.EVENT_PREFIX.encode())]
        self.assertEqual(len(lines), 2)
        for line in lines:
            event = control.validate_event(line[len(control.EVENT_PREFIX):], "signed-test", "signed-future-live")
            self.assertTrue(all(value is None for value in event["accounting"].values()))
            self.assertEqual(event["source"]["accounting_reason"], "LIVE_ACCOUNTING_UNAVAILABLE")
            with self.assertRaises(ValueError):
                control.validate_event(line[len(control.EVENT_PREFIX):], "signed-test", "unsigned-paper")

    def test_extended_rust_streams_validate_before_first_write(self):
        result = subprocess.run(["cargo", "test", "--offline", "--locked", "--target-dir", os.environ["CARGO_TARGET_DIR"], "--lib",
                                 "bot::runtime::paper::tests::extended_protocol_consumer_fixture",
                                 "--", "--exact", "--nocapture"],
                                cwd=control.BOT, capture_output=True, timeout=120)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        scenarios = {}
        for line in result.stdout.decode().splitlines():
            if line.startswith(control.EVENT_PREFIX):
                event = json.loads(line[len(control.EVENT_PREFIX):])
                scenarios.setdefault(event["run_id"], []).append(event)
        self.assertEqual(set(scenarios), {"test-" + name for name in
                         ("release", "sell", "loss", "pending", "conflict", "paid-conflict", "draw")})
        original_open = open
        writes = []

        class WriteSpy:
            def __init__(self, handle): self.handle = handle
            def __enter__(self): return self
            def __exit__(self, *args): return self.handle.__exit__(*args)
            def write(self, text):
                event = json.loads(text)
                # The actual first persistence operation must see validated facts.
                self_event = control.validate_event(text.encode(), event["run_id"])
                writes.append(self_event)
                return self.handle.write(text)
            def flush(self): self.handle.flush()

        def spy(path, *args, **kwargs):
            handle = original_open(path, *args, **kwargs)
            return WriteSpy(handle) if str(path).endswith("events.jsonl") else handle

        def consume(root, run_id, events):
            root.mkdir()
            script = root / "fixture.py"
            script.write_text(CHILD)
            lines = "\n".join(control.EVENT_PREFIX + json.dumps(event) for event in events)
            child = subprocess.Popen([sys.executable, "-B", str(script), "stream", lines],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            now = datetime.datetime.now(datetime.timezone.utc)
            evidence = control.Evidence(root, run_id, now.isoformat(), now.isoformat(), "unsigned-paper")
            with patch("builtins.open", side_effect=spy):
                return control.supervise(child, evidence, now, control.boottime())

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for run_id, events in scenarios.items():
                with self.subTest(run_id=run_id):
                    writes.clear()
                    facts, final = consume(root / run_id, run_id, events)
                    self.assertEqual(facts["launcher_exit"], 0)
                    self.assertEqual(len(writes), len(events))
                    self.assertEqual(writes[-1], final)
                    if run_id == "test-release":
                        self.assertEqual([event["event_kind"] for event in writes],
                            ["summary", "summary", "summary", "reservation", "release", "blocked", "final"])
                        self.assertEqual([event["source"]["decision"]["phase"] for event in writes
                            if isinstance(event["source"], dict) and "decision" in event["source"]],
                            ["EVALUATE", "PRE_SUBMIT"])
                        self.assertEqual(final["accounting"]["cash"], 100)
                    if run_id == "test-sell":
                        sell = next(event for event in writes if (event["source"] or {}).get("side") == "sell")
                        self.assertEqual(sell["source"]["fee_direction"], "cash")
                        self.assertEqual(sell["source"]["cash_unit"], "USDT")
                        self.assertAlmostEqual(sell["source"]["net_cash"], 2.9008)
                        self.assertAlmostEqual(final["accounting"]["cash"], 100.9008)
                    if run_id == "test-loss":
                        self.assertEqual(sum(event["event_kind"] == "loss_latch" for event in writes), 1)
                        self.assertEqual(final["accounting"]["net_pnl"], -20)
                        self.assertTrue(final["accounting"]["loss_latched"])
                    if run_id == "test-draw":
                        paid = next(event for event in writes if event["event_kind"] == "settlement")
                        self.assertEqual(paid["source"]["payout"], .5)
                        self.assertAlmostEqual(paid["source"]["income"], 2.45)
                        self.assertAlmostEqual(final["accounting"]["realized"], .45)
                        self.assertAlmostEqual(final["accounting"]["cash"], 100.45)
                    if run_id in {"test-pending", "test-conflict", "test-paid-conflict"}:
                        expected = "PENDING" if run_id == "test-pending" else "CONFLICT"
                        self.assertEqual(final["source"]["markets"][0]["status"], expected)
                        self.assertFalse(final["source"]["finalized"])
                        self.assertAlmostEqual(final["accounting"]["cash"],
                                               102.9 if run_id == "test-paid-conflict" else 98)
            # Offline folds use actual emitter facts, never a second execution model.
            for run_id in ("test-draw", "test-loss", "test-pending"):
                metrics = fold_phase1_metrics(scenarios[run_id])
                if run_id == "test-draw":
                    self.assertEqual((metrics["direction_total"], metrics["draw"],
                                      metrics["entry_total"], metrics["cash_positive"]), (0, 1, 0, 1))
                elif run_id == "test-loss":
                    self.assertEqual((metrics["direction_total"], metrics["direction_hits"],
                                      metrics["entry_total"], metrics["entry_wins"]), (10, 0, 10, 0))
                else:
                    self.assertEqual((metrics["pending"], metrics["fills_pending"]), (1, 1))
                    self.assertIsNone(metrics["cash_pnl"])
                if metrics["cash_pnl"] is not None:
                    self.assertAlmostEqual(metrics["cash_pnl"],
                                           scenarios[run_id][-1]["accounting"]["realized"], delta=1e-9)
            # Single-field corruption of actual Rust facts must never be persisted.
            mutations = {
                "sell": [("pnl", 1.9008), ("price", .9), ("fee_rate_bps", 0),
                         ("fee_direction", "shares"), ("shares", -4.9),
                         ("price", 0), ("price", 1), ("fee_rate_bps", -1),
                         ("fee_rate_bps", 10001), ("fee_rate_bps", 200.5),
                         ("net_cash", -1), ("basis", -2), ("fee", -.0392)],
                "buy": [("price", .9), ("fee_rate_bps", 0),
                        ("fee_direction", "cash"), ("gross_shares", -5),
                        ("fee_rate_bps", 200.5), ("price", 1),
                        ("net_shares", -4.9), ("cost", -2), ("fee", -.04)],
            }
            for side, cases in mutations.items():
                for number, (field, value) in enumerate(cases):
                    events = copy.deepcopy(scenarios["test-sell"])
                    index = next(index for index, event in enumerate(events)
                                 if (event["source"] or {}).get("side") == side)
                    events[index]["source"][field] = value
                    writes.clear()
                    facts, final = consume(root / f"bad-{side}-{number}", "test-sell", events)
                    with self.subTest(side=side, field=field, value=value):
                        self.assertEqual(facts["launcher_exit"], 1)
                        self.assertIsNone(final)
                        self.assertEqual(len(writes), index)

    def test_real_rust_events_reconcile_before_first_write(self):
        result = subprocess.run(["cargo", "test", "--offline", "--locked", "--target-dir", os.environ["CARGO_TARGET_DIR"], "--lib",
                                 "bot::runtime::paper::tests::protocol_consumer_fixture", "--",
                                 "--exact", "--nocapture"], cwd=control.BOT, capture_output=True, timeout=120)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        lines = [line for line in result.stdout.decode().splitlines() if line.startswith(control.EVENT_PREFIX)]
        self.assertTrue(lines)
        for line in lines:
            control.validate_event(line[len(control.EVENT_PREFIX):].encode(), "test-run")
        writes = []
        original_open = open
        class WriteSpy:
            def __init__(self, handle): self.handle = handle
            def __enter__(self): return self
            def __exit__(self, *args): return self.handle.__exit__(*args)
            def write(self, text):
                writes.append(json.loads(text))
                return self.handle.write(text)
            def flush(self): self.handle.flush()
        def spy(path, *args, **kwargs):
            handle = original_open(path, *args, **kwargs)
            return WriteSpy(handle) if str(path).endswith("events.jsonl") else handle
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / "fixture.py"
            script.write_text(CHILD)
            now = datetime.datetime.now(datetime.timezone.utc)
            evidence = control.Evidence(root, "test-run", now.isoformat(), now.isoformat(), "unsigned-paper")
            child = subprocess.Popen([sys.executable, "-B", str(script), "stream", "\n".join(lines)],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            with patch("builtins.open", side_effect=spy):
                facts, final = control.supervise(child, evidence, now, control.boottime())
            self.assertEqual(facts["launcher_exit"], 0)
            self.assertEqual(len(writes), len(lines))
            self.assertEqual([item["event_kind"] for item in writes],
                ["summary", "summary", "summary", "reservation", "summary", "fill",
                 "summary", "summary", "blocked", "settlement", "final"])
            self.assertEqual([event["source"]["decision"]["phase"] for event in writes
                if isinstance(event["source"], dict) and "decision" in event["source"]],
                ["EVALUATE", "PRE_SUBMIT", "PRE_APPLY", "EVALUATE", "PRE_SUBMIT"])
            reservation = next(event for event in writes if event["event_kind"] == "reservation")
            self.assertEqual(reservation["accounting"]["reserved"], 2)
            fill = next(event for event in writes if event["event_kind"] == "fill")
            self.assertAlmostEqual(fill["source"]["fee_shares"], .1)
            self.assertAlmostEqual(fill["accounting"]["cash"], 98)
            self.assertAlmostEqual(final["accounting"]["cash"], 102.9)
            self.assertEqual(writes[-2]["source"]["provider"], "UNKNOWN")
            # A missing required amount, a null cash, or duplicate final market cannot reach write.
            for index, bad in enumerate((copy.deepcopy(fill), copy.deepcopy(fill), copy.deepcopy(final))):
                if index == 0:
                    del bad["source"]["fee_shares"]
                elif index == 1:
                    bad["accounting"]["cash"] = None
                else:
                    bad["source"]["markets"] *= 2
                with self.assertRaises(ValueError):
                    control.validate_event(json.dumps(bad).encode(), "test-run")


def fold_phase1_metrics(events: list[dict]) -> dict:
    """Offline test fold of actual v2 facts; never an execution or balance ledger."""
    if any(event.get("schema_version") != control.SCHEMA_VERSION_V2 for event in events):
        raise ValueError("DIAGNOSTICS_UNAVAILABLE")
    final = events[-1]
    markets = {str(item["market_id"]): item for item in final["source"]["markets"]}
    result: dict = dict(direction_hits=0, direction_total=0, abstain=0, draw=0, pending=0,
                  unvalued=0, entry_wins=0, entry_total=0, cash_positive=0, cash_total=0,
                  fills_pending=0, cash_pnl=0.0, net_by_token={}, strata={})
    fills = {event["source"]["candidate_id"]: event for event in events
             if event["event_kind"] == "fill" and event["source"]["side"] == "buy"}
    settlements = {event["source"]["candidate_id"]: event for event in events
                   if event["event_kind"] == "settlement"}
    for event in events:
        decision = (event["source"] or {}).get("decision")
        if decision is None or decision["phase"] != "EVALUATE":
            continue
        key = (decision["prediction_source"], decision["source_epoch"])
        result["strata"][key] = result["strata"].get(key, 0) + 1
        fair = decision["p_up"]
        market = markets.get(str(event["market_id"]), {})
        payout = market.get("payout_yes") if market.get("status") == "CONFIRMED" else None
        if fair is None: result["unvalued"] += 1
        elif fair == .5: result["abstain"] += 1
        elif payout is None: result["pending"] += 1
        elif payout == .5: result["draw"] += 1
        else:
            result["direction_total"] += 1
            result["direction_hits"] += (fair > .5) == (payout == 1)
    for candidate, event in fills.items():
        source = event["source"]
        result["net_by_token"][str(event["token"])] = {
            name: source[name] for name in ("gross_shares", "fee_shares", "net_shares")}
        settlement = settlements.get(candidate)
        if settlement is None:
            result["fills_pending"] += 1
            continue
        settled = settlement["source"]
        pnl = settled["income"] - settled["basis"]
        result["cash_pnl"] += pnl
        result["cash_total"] += 1
        result["cash_positive"] += pnl > 0
        if settled["payout"] != .5:
            result["entry_total"] += 1
            result["entry_wins"] += settled["payout"] == 1
    if result["fills_pending"]:
        result["cash_pnl"] = None  # An open mark is not a final cash outcome.
    return result


class Phase1ProtocolTest(LauncherFixture):
    """Actual Rust v2 output traverses the original first-write consumer."""

    @classmethod
    def setUpClass(cls):
        result = subprocess.run(["cargo", "test", "--offline", "--locked", "--target-dir", os.environ["CARGO_TARGET_DIR"], "--lib",
            "bot::arbitrage::phase1::tests::phase1_protocol_consumer_fixture",
            "--", "--exact", "--nocapture"], cwd=control.BOT, capture_output=True, timeout=120)
        if result.returncode or b"1 passed" not in result.stdout:
            raise AssertionError(result.stderr.decode() + result.stdout.decode())
        cls.events = [json.loads(line[len(control.EVENT_PREFIX):])
                      for line in result.stdout.decode().splitlines()
                      if line.startswith(control.EVENT_PREFIX)]
        # Existing id schema canonicalizes numeric market ids, not diagnostic numbers.
        for event in cls.events:
            if event["market_id"] is not None:
                event["market_id"] = str(event["market_id"])
            if event["event_kind"] == "final":
                for market in event["source"]["markets"]:
                    market["market_id"] = str(market["market_id"])
        if len(cls.events) != 9:
            raise AssertionError("real v2 fixture must produce nine events")

    def consume(self, events, *, raw_lines=None):
        writes = []
        original_open = open

        class WriteSpy:
            def __init__(self, handle): self.handle = handle
            def __enter__(self): return self
            def __exit__(self, *args): return self.handle.__exit__(*args)
            def write(self, text):
                writes.append(json.loads(text))
                return self.handle.write(text)
            def flush(self): self.handle.flush()

        def spy(path, *args, **kwargs):
            handle = original_open(path, *args, **kwargs)
            return WriteSpy(handle) if str(path).endswith("events.jsonl") else handle

        with tempfile.TemporaryDirectory(dir=self.root) as directory:
            root = Path(directory)
            lines = raw_lines if raw_lines is not None else [json.dumps(event) for event in events]
            stream = "\n".join(control.EVENT_PREFIX + line for line in lines)
            process = subprocess.Popen([sys.executable, "-B", str(self.script), "stream", stream],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            now = datetime.datetime.now(datetime.timezone.utc)
            evidence = control.Evidence(root, self.events[0]["run_id"], "", "", "unsigned-paper")
            with patch("builtins.open", side_effect=spy):
                facts, final = control.supervise(process, evidence, now, control.boottime())
            self.assertEqual(writes, [json.loads(line) for line in
                                      (root / "events.jsonl").read_text().splitlines()])
        return facts, final, writes

    def test_shared_candidate_comparison_preserves_paper_sides_and_depth_exception(self):
        frozen = self.events[1]["source"]["decision"]
        for side in ("up", "down"):
            changed = copy.deepcopy(self.events[2])
            decision = changed["source"]["decision"]
            decision["p_up"] = 0.75
            decision[side]["fair"] = 0.75
            decision[side]["edge"] = 0.1
            relation = control._EventRelations()
            for event in self.events[:2]:
                relation.accept(event, None)
            relation.accept(changed, self.events[1])
            self.assertEqual(relation.candidates[decision["candidate_id"]]["last"], decision)
            for key in ("depth_levels", "depth_complete"):
                diagnostic = copy.deepcopy(decision)
                diagnostic.update(reason="DIAGNOSTIC_LIMIT")
                diagnostic[side][key] = None
                self.assertIsNone(control.candidate_input_error(
                    frozen, diagnostic, "1", "1", allow_diagnostic_limit=True))
                self.assertEqual(control.candidate_input_error(frozen, diagnostic, "1", "1"),
                                 "CANDIDATE_SIDE")
                diagnostic["reason"] = "ELIGIBLE"
                self.assertEqual(control.candidate_input_error(
                    frozen, diagnostic, "1", "1", allow_diagnostic_limit=True), "CANDIDATE_SIDE")
            missing = copy.deepcopy(decision)
            missing[side] = None
            self.assertEqual(control.candidate_input_error(
                frozen, missing, "1", "1", allow_diagnostic_limit=True), "CANDIDATE_SIDE")
            missing_frozen = copy.deepcopy(frozen)
            missing_frozen[side] = None
            self.assertIsNone(control.candidate_input_error(
                missing_frozen, missing, "1", "1", allow_diagnostic_limit=True))
        changed = copy.deepcopy(self.events[2])
        changed["source"]["decision"]["spot"] += 1
        relation = control._EventRelations()
        for event in self.events[:2]:
            relation.accept(event, None)
        with self.assertRaisesRegex(ValueError, "^CANDIDATE_INPUTS$"):
            relation.accept(changed, self.events[1])

    def test_paired_original_mode_consumers_compare_the_same_native_inputs(self):
        import btc5m_live_events as live
        self.assertIs(live.candidate_input_error, control.candidate_input_error)
        result = subprocess.run(
            ["cargo", "test", "--locked", "--offline", "--lib",
             "bot::runtime::live_control::candidate_tests::paired_original_paper_live_pre_submit_fixture",
             "--", "--exact", "--nocapture"], cwd=control.BOT, capture_output=True, timeout=120)
        self.assertEqual(result.returncode, 0, result.stderr.decode() + result.stdout.decode())
        pairs = {"paper": [], "live": []}
        for line in result.stdout.decode().splitlines():
            if not line.startswith(control.EVENT_PREFIX):
                continue
            event = json.loads(line[len(control.EVENT_PREFIX):])
            mode = "live" if "payload" in event else "paper"
            decision = event.get("payload", event.get("source", {})).get("decision")
            if decision is None or decision["phase"] not in ("EVALUATE", "PRE_SUBMIT"):
                continue
            if mode == "paper":
                event["market_id"] = str(event["market_id"])
            if decision["phase"] == "EVALUATE":
                pairs[mode].append([event])
            else:
                pairs[mode][-1].append(event)
        self.assertEqual(len(pairs["paper"]), 4)
        self.assertEqual(len(pairs["live"]), 4)
        for paper_pair, live_pair in zip(pairs["paper"], pairs["live"]):
            self.assertEqual(len(paper_pair), 2)
            self.assertEqual(len(live_pair), 2)
            for paper_event, live_event in zip(paper_pair, live_pair):
                self.assertEqual(paper_event["source"]["decision"], live_event["payload"]["decision"])
                # Accounting/execution remains mode-specific, not translated into
                # a shared fictitious ledger just to compare candidate rules.
                self.assertNotEqual(paper_event["accounting"], live_event["accounting"])
            for fault in (None, "spot", "fee_rate_bps", "strike", "gross_shares"):
                with self.subTest(reason=paper_pair[-1]["source"]["decision"]["reason"], fault=fault):
                    paper = control._EventRelations()
                    paper.accept(paper_pair[0], None)
                    live_rel = live.LiveRelations(live_pair[0]["run_id"], live_pair[0]["risk_window_id"])
                    live_rel = live_rel.stage(live_pair[0])
                    decisions = [copy.deepcopy(paper_pair[1]), copy.deepcopy(live_pair[1])]
                    for event in decisions:
                        decision = event.get("payload", event.get("source", {}))["decision"]
                        if fault == "gross_shares":
                            decision["up"][fault] += 1
                        elif fault is not None:
                            decision[fault] += 1
                    accepted = []
                    for consume, event in ((lambda event: paper.accept(event, paper_pair[0]), decisions[0]),
                                           (live_rel.stage, decisions[1])):
                        try:
                            consume(event)
                        except (ValueError, live.LiveInvalid):
                            accepted.append(False)
                        else:
                            accepted.append(True)
                    self.assertEqual(accepted, [fault is None, fault is None])

    def test_candidate_lifecycle_is_checked_before_write(self):
        cases = [(1, "seq"), (1, "repeat-evaluate"), (2, "phase"),
                 (3, "candidate"), (4, "epoch"), (5, "token"), (7, "settlement")]
        for index, fault in cases:
            events = copy.deepcopy(self.events)
            event = events[index]
            if fault == "seq": event["event_seq"] = events[index - 1]["event_seq"]
            elif fault == "repeat-evaluate":
                event["source"]["decision"].update(decision_id="1", candidate_id="1")
            elif fault == "phase": event["source"]["decision"]["phase"] = "PRE_APPLY"
            elif fault == "candidate": event["source"].update(candidate_id="999", decision_id="999")
            elif fault == "epoch": event["source"]["decision"]["source_epoch"] += 1
            elif fault == "token": event["token"] = "999"
            else: event["source"]["candidate_id"] = "999"
            with self.subTest(fault=fault):
                facts, final, writes = self.consume(events)
                self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
                self.assertIsNone(final)
                self.assertEqual(writes, self.events[:index])
        events = copy.deepcopy(self.events[:2] + [self.events[-1]])
        events[-1] = final_event()
        events[-1].update(run_id=self.events[0]["run_id"], schema_version=control.SCHEMA_VERSION_V2,
                          event_seq=3)
        facts, final, writes = self.consume(events)
        self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
        self.assertIsNone(final)
        self.assertEqual(writes, self.events[:2])

    def test_attempt_ids_and_reservation_reasons_are_not_free_labels(self):
        for index, fault in ((0, "attempt-id"), (3, "reservation-reason")):
            events = copy.deepcopy(self.events)
            if fault == "attempt-id":
                events[index]["source"]["decision"]["decision_id"] = "999"
            else:
                events[index]["source"]["diagnostic_reason"] = "BOOK_STALE"
            with self.subTest(fault=fault):
                facts, final, writes = self.consume(events)
                self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
                self.assertIsNone(final)
                self.assertEqual(writes, self.events[:index])

    def test_fill_must_match_the_frozen_book_walk_not_just_the_limit(self):
        events = copy.deepcopy(self.events)
        event = events[5]
        fill = event["source"]
        price = fill["price"] - .01
        gross = fill["gross_shares"]
        fee = gross * fill["fee_rate_bps"] / 10000 * min(price, 1 - price)
        net = gross - fee / price
        cost = gross * price
        fill.update(price=price, cost=cost, fee=fee, fee_shares=fee / price, net_shares=net)
        mark = events[1]["source"]["decision"]["best_yes_bid"]
        unrealized = net * mark - cost
        event["accounting"].update(cash=100 - cost, available=100 - cost, cost=cost,
                                  fees_in_cash=fee, unrealized=unrealized,
                                  net_pnl=unrealized, nav=100 + unrealized)
        # All single-event fee/cash equations are consistent; only the frozen walk disagrees.
        control.validate_event(json.dumps(event).encode(), event["run_id"])
        facts, final, writes = self.consume(events)
        self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
        self.assertIsNone(final)
        self.assertEqual(writes, self.events[:5])

    def test_required_v2_diagnostic_fields_are_not_nullable(self):
        for name in ("phase", "reason", "symbol", "source_state", "reset_reason",
                     "settlement_provider", "decision_at", "checked_at", "samples",
                     "decision_elapsed_ms", "checked_elapsed_ms"):
            events = copy.deepcopy(self.events)
            events[0]["source"]["decision"][name] = None
            with self.subTest(field=name):
                facts, final, writes = self.consume(events)
                self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
                self.assertIsNone(final)
                self.assertEqual(writes, [])

    def test_missing_input_is_a_legal_non_candidate_not_a_protocol_failure(self):
        missing = copy.deepcopy(self.events[0])
        decision = missing["source"]["decision"]
        decision.update(reason="MISSING_INPUT", candidate_id=None, selected_token=None,
                        prediction_source=None, actual_pair=None, source_state="UNAVAILABLE",
                        source_epoch=None, spot=None, spot_clock=None, book_clock=None,
                        sigma=None, p_up=None, up=None, down=None)
        end = final_event()
        end.update(run_id=missing["run_id"], event_seq=2)
        facts, final, writes = self.consume([missing, end])
        self.assertEqual(facts["launcher_exit"], 0)
        self.assertEqual(writes, [missing, end])
        self.assertEqual(final, end)

    def test_v2_shape_and_version_corruption_never_reaches_first_write(self):
        baseline = self.events[0]
        mutations = []
        for version in (None, control.SCHEMA_VERSION, "unknown"):
            event = copy.deepcopy(baseline)
            event["schema_version"] = version
            mutations.append(json.dumps(event))
        for field, value in (("samples", True), ("reason", "UNKNOWN"),
                             ("phase", "UNKNOWN"), ("source_epoch", True)):
            event = copy.deepcopy(baseline)
            event["source"]["decision"][field] = value
            mutations.append(json.dumps(event))
        event = copy.deepcopy(baseline)
        event["source"]["decision"]["spot_clock"].pop("received_at")
        mutations.append(json.dumps(event))
        event = copy.deepcopy(baseline)
        event["source"]["decision"]["up"]["UnKnOwN"] = "must-not-persist"
        mutations.append(json.dumps(event))
        event = copy.deepcopy(baseline)
        event["source"]["decision"]["decision_at"] = "2025-11-08T21:21:41Z"
        mutations.append(json.dumps(event))
        raw = json.dumps(baseline)
        mutations.extend([raw.replace('"event_seq": 1', '"event_seq": 1, "event_seq": 1'),
                          raw.replace('"samples": 0', '"samples": NaN')])
        for index, raw in enumerate(mutations):
            with self.subTest(mutation=index):
                facts, final, writes = self.consume([], raw_lines=[raw])
                self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
                self.assertIsNone(final)
                self.assertEqual(writes, [])

    def test_full_side_arithmetic_and_depth_are_checked_before_write(self):
        changes = {"roi": .123, "breakeven_fair": .123, "walk_vwap": .123,
                   "execution_net_shares": .123, "execution_cost": .123,
                   "execution_ev": .123, "execution_edge": .123,
                   "depth_complete": False, "depth_levels": [{"price": .4, "size": .1}]}
        for index in (0, 1):  # Actual WARMUP and ELIGIBLE snapshots, not invented values.
            for name, value in changes.items():
                events = copy.deepcopy(self.events)
                events[index]["source"]["decision"]["up"][name] = value
                with self.subTest(index=index, field=name):
                    facts, final, writes = self.consume(events)
                    self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
                    self.assertIsNone(final)
                    self.assertEqual(writes, self.events[:index])

    def test_eligible_requires_computable_inputs_and_valid_acquisition(self):
        for field, value in (("p_up", None), ("spot", None), ("sigma", None),
                             ("samples", 0), ("source_epoch", None), ("selected_token", None),
                             ("selected_token", "999"),
                             ("actual_pair", "BTC-USD"), ("source_state", "UNAVAILABLE"),
                             ("spot_clock", None), ("secs_left", 29.999)):
            events = copy.deepcopy(self.events)
            events[1]["source"]["decision"][field] = value
            with self.subTest(field=field):
                facts, final, writes = self.consume(events)
                self.assertEqual(facts["stop_reason"], "OUTPUT_INVALID")
                self.assertIsNone(final)
                self.assertEqual(writes, self.events[:1])

    def test_actual_event_metrics_keep_denominators_and_net_cash_separate(self):
        metrics = fold_phase1_metrics(self.events)
        self.assertEqual((metrics["direction_hits"], metrics["direction_total"],
                          metrics["entry_wins"], metrics["entry_total"]), (1, 1, 1, 1))
        self.assertEqual(metrics["unvalued"], 2)
        self.assertAlmostEqual(metrics["cash_pnl"], self.events[-1]["accounting"]["realized"], delta=1e-9)
        fill = next(event for event in self.events if event["event_kind"] == "fill")
        net = metrics["net_by_token"][fill["token"]]
        self.assertAlmostEqual(net["gross_shares"] - net["fee_shares"], net["net_shares"], delta=1e-9)
        self.assertAlmostEqual(net["net_shares"], self.events[7]["source"]["shares"], delta=1e-9)
        self.assertEqual(sum(metrics["strata"].values()), 3)
        # Pure classifier boundary mutation, NOT a claimed Rust model output or executable stream.
        boundary = copy.deepcopy(self.events)
        boundary[1]["source"]["decision"]["p_up"] = .5
        classified = fold_phase1_metrics(boundary)
        self.assertEqual((classified["direction_total"], classified["abstain"]), (0, 1))
        self.assertEqual(classified["entry_total"], 1)  # Entry and attempt denominators differ.

    def test_legacy_read_only_does_not_upgrade_or_enter_an_unsigned_run(self):
        legacy = final_event()
        legacy.pop("schema_version")
        legacy.pop("event_seq")
        raw = json.dumps(legacy).encode()
        parsed = control.read_legacy_event(raw, "run-1")
        self.assertEqual(parsed, {"event": legacy, "diagnostics_status": "DIAGNOSTICS_UNAVAILABLE"})
        self.assertEqual(json.loads(raw), legacy)
        with self.assertRaises(ValueError):
            control.validate_event(raw, "run-1")
        with self.assertRaises(ValueError):
            control.read_legacy_event(json.dumps(final_event()).encode(), "run-1")

    def test_global_summary_preserves_null_shape(self):
        event = final_event()
        event.update(event_kind="summary", source=None)
        rebuilt = control.validate_event(json.dumps(event).encode(), "run-1")
        self.assertIsNone(rebuilt["source"])

    def test_actual_v2_stream_reaches_every_first_write(self):
        # Validate directly as well, so a producer/consumer shape error is visible.
        for event in self.events:
            self.assertEqual(control.validate_event(json.dumps(event).encode(), event["run_id"]), event)
        facts, final, writes = self.consume(self.events)
        self.assertEqual(facts["launcher_exit"], 0)
        self.assertEqual(writes, self.events)
        self.assertEqual(final, self.events[-1])


class IdentityTest(unittest.TestCase):
    def test_real_git_and_actual_binary_bytes_and_failure_paths(self):
        # Isolated public fixture: real git, no production binary or credentials.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bot = root / "skills/trading/auto-trading/prediction-markets/bot"
            bot.mkdir(parents=True)
            for repo in (root, bot):
                subprocess.run(["/usr/bin/git", "init", "-q", str(repo)], check=True,
                               capture_output=True)
                subprocess.run(["/usr/bin/git", "-C", str(repo), "-c", "user.name=Test",
                                "-c", "user.email=test@example.invalid", "commit", "--allow-empty",
                                "-qm", "fixture"], check=True, capture_output=True)
            # Every fixed public path is supplied; the candidate identity hashes bytes, not labels.
            names = ("src/config.rs", "src/main.rs", "src/bot/runtime.rs", "src/bot/runtime/paper.rs",
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
                     "src/bin/btc_arb_replay.rs",
                     "target/debug/polymarket-toolkits")
            for name in names:
                path = bot / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"public fixture bytes")
            with patch.object(control, "BOT", bot):
                identity = control.public_identity({})
                self.assertEqual(set(identity["files"]), set(names[:-1]))
                for name in names[:-1]:
                    path = bot / name
                    path.unlink()
                    with self.subTest(missing=name), self.assertRaises(control.Refused):
                        control.public_identity({})
                    path.write_bytes(b"public fixture bytes")
                self.assertEqual(identity["binary_sha256"], hashlib.sha256(b"public fixture bytes").hexdigest())
                for label, repo in (("main", root), ("bot", bot)):
                    head = subprocess.run(["/usr/bin/git", "-C", str(repo), "rev-parse", "HEAD"],
                                          check=True, capture_output=True).stdout.decode().strip()
                    self.assertEqual(identity[label + "_head"], head)
                binary = bot / names[-1]
                binary.unlink()
                with self.assertRaises(control.Refused):
                    control.public_identity({})
                binary.write_bytes(b"public fixture bytes")
                with patch.object(control, "subprocess") as process:
                    process.SubprocessError = subprocess.SubprocessError
                    process.run.side_effect = subprocess.CalledProcessError(128, "git")
                    with self.assertRaises(control.Refused):
                        control.public_identity({})


if __name__ == "__main__":
    unittest.main()
