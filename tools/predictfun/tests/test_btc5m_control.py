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
        self.assertEqual(ORIGINAL_API_SOURCE_FILE, control.BOT.parents[4] / "skills/.env")
        self.assertNotEqual(ORIGINAL_API_SOURCE_FILE, Path.cwd() / "skills/.env")

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
        result = subprocess.run(["cargo", "test", "--offline", "--locked", "--lib", "--no-run",
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
        result = subprocess.run(["cargo", "test", "--offline", "--locked", "--lib",
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
        result = subprocess.run(["cargo", "test", "--offline", "--locked", "--lib",
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
        result = subprocess.run(["cargo", "test", "--offline", "--locked", "--lib",
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
        result = subprocess.run(["cargo", "test", "--offline", "--locked", "--lib",
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
