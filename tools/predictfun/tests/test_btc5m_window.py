"""R9: a frozen window starts where its configuration's duration says.

The launcher can only compute ``deadline = read_time + duration``; the
controller's own process clock therefore always starts later. These tests run
the real controller against the existing offline fixture (fake storage, secret
and child transports only) to prove the window is the configuration's, that a
stale deadline is still refused, and that the duration cap still holds.
"""
from __future__ import annotations

import datetime
import hashlib
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import btc5m_control as control
import test_btc5m_control as controller_suite


def stamp(value: datetime.datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class FrozenClock(datetime.datetime):
    """Freeze the controller's own clock at one instant."""

    instant = datetime.datetime.now(datetime.timezone.utc)

    @classmethod
    def now(cls, tz=None):
        return cls.instant.astimezone(tz) if tz else cls.instant.replace(tzinfo=None)


class FrozenWindowHarness(controller_suite.ControlledLiveSupervisorTests):
    """The original controller fixture, without its supervisor test methods."""

    __test__ = False

    def runTest(self):
        pass


class FrozenWindowTests(unittest.TestCase):
    """A frozen configuration owns its window's start and length."""

    def setUp(self):
        # Composition keeps the fixture's fake storage/secret/child transports
        # while leaving the fixture module's own test suite untouched.
        self.harness = FrozenWindowHarness("runTest")
        self.addCleanup(self.harness.doCleanups)
        self.harness.setUp()

    def frozen_fixture(self, run_parameters, deadline):
        """The original fixture, rewritten as a frozen configuration run."""
        harness = self.harness
        args, evidence, child, loader, _collection, path, driver, _database, _builder = (
            harness.launch_fixture())
        config = harness.root / "config.json"
        data = harness.live.strict_json(config.read_bytes())
        data["controlled_run"] = dict(run_parameters)
        config.write_bytes(harness.live.canonical(data))
        # The controller writes the configuration's own identity values, with
        # their exact JSON types; the activation digest must match it.
        identity = control.public_identity({}, live=True)
        limits = data["strategies"]["limits"]
        public = {"sha256": hashlib.sha256(harness.live.canonical(data)).hexdigest(),
                  "initial_cash": run_parameters["budget_usdt"],
                  "max_order_usd": limits["max_order_usd"],
                  "max_market_usd": limits["max_market_usd"],
                  "max_total_usd": limits["max_total_usd"],
                  "max_loss_usd": limits["max_loss_usd"]}
        activation = json.loads(path.read_bytes())
        activation["candidate_digest"] = harness.live.candidate_digest(
            {"candidate_identity": identity, "public_config_identity": public})
        activation["operations_mode"] = args[args.index("--operations-mode") + 1] = (
            run_parameters["operations_mode"])
        activation["deadline_at"] = args[args.index("--deadline-at") + 1] = stamp(deadline)
        activation["not_before"] = stamp(
            deadline - datetime.timedelta(seconds=run_parameters["duration_seconds"] + 60))
        path.write_bytes(harness.live.canonical(activation))
        return args, evidence, child, loader, driver

    def test_a_frozen_window_starts_at_the_declared_duration(self):
        """Launcher deadline T0+duration and controller clock T0+2s both reach it."""
        instant = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        FrozenClock.instant = instant + datetime.timedelta(seconds=2)
        original_root = self.harness.root
        try:
            for mode, seconds in (("unattended", 86400), ("supervised", 21600)):
                with self.subTest(mode=mode):
                    self.harness.root = original_root / f"frozen-{mode}"
                    self.harness.root.mkdir()
                    deadline = instant + datetime.timedelta(seconds=seconds)
                    # The native fixture books C0=98; a frozen run must carry the
                    # same budget or its own frames would be refused.
                    args, evidence, child, loader, driver = self.frozen_fixture(
                        dict(budget_usdt=98, duration_seconds=seconds, operations_mode=mode),
                        deadline)
                    with patch.object(control.datetime, "datetime", FrozenClock), \
                            patch.object(control, "boottime", return_value=100):
                        facts = control.run(args, environment={}, evidence_root=evidence,
                                            popen=child)
                    self.assertEqual(facts["launcher_exit"], 0)
                    child.assert_called_once()
                    loader.assert_called_once()
                    driver.assert_called_once()
                    run_id = args[args.index("--run-id") + 1]
                    started = json.loads((evidence / run_id / "started.json").read_bytes())
                    # The configuration's duration, not the clock's remainder.
                    self.assertEqual(started["started_at"],
                                     stamp(deadline - datetime.timedelta(seconds=seconds)))
                    self.assertEqual(started["deadline_at"], stamp(deadline))
                    command = child.call_args[0][0]
                    handed_start = self.harness.live.timestamp(
                        command[command.index("--controlled-started-at") + 1])
                    handed_deadline = self.harness.live.timestamp(
                        command[command.index("--controlled-deadline-at") + 1])
                    self.assertEqual(int((handed_deadline - handed_start).total_seconds()),
                                     seconds)
                    self.assertIsNotNone(
                        self.harness.live.read_live_run(evidence / run_id)["ended"])
        finally:
            self.harness.root = original_root

    def test_a_stale_frozen_deadline_is_still_refused(self):
        """The same deadline with the controller clock ten minutes late."""
        instant = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        FrozenClock.instant = instant + datetime.timedelta(seconds=600)
        original_root = self.harness.root
        self.harness.root = original_root / "frozen-stale"
        self.harness.root.mkdir()
        try:
            deadline = instant + datetime.timedelta(seconds=86400)
            args, evidence, child, loader, driver = self.frozen_fixture(
                dict(budget_usdt=97, duration_seconds=86400, operations_mode="unattended"),
                deadline)
            with patch.object(control.datetime, "datetime", FrozenClock), \
                    patch.object(control, "boottime", return_value=100):
                with self.assertRaises(control.Refused) as refused:
                    control.run(args, environment={}, evidence_root=evidence, popen=child)
            self.assertEqual((refused.exception.code, refused.exception.reason),
                             ("CONTROLLED_MODE", "DEADLINE"))
            child.assert_not_called()
            loader.assert_not_called()
            driver.assert_not_called()
            self.assertFalse(evidence.exists())
        finally:
            self.harness.root = original_root

    def test_a_frozen_window_beyond_its_mode_limit_is_still_refused(self):
        """The declared duration is still bounded by the mode's own window."""
        instant = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        FrozenClock.instant = instant + datetime.timedelta(seconds=2)
        original_root = self.harness.root
        try:
            for mode, seconds in (("unattended", 86401), ("supervised", 21601)):
                with self.subTest(mode=mode):
                    self.harness.root = original_root / f"frozen-limit-{mode}"
                    self.harness.root.mkdir()
                    deadline = instant + datetime.timedelta(seconds=seconds)
                    args, evidence, child, loader, driver = self.frozen_fixture(
                        dict(budget_usdt=97, duration_seconds=seconds, operations_mode=mode),
                        deadline)
                    with patch.object(control.datetime, "datetime", FrozenClock):
                        with self.assertRaises(control.Refused) as refused:
                            control.run(args, environment={}, evidence_root=evidence,
                                        popen=child)
                    self.assertEqual((refused.exception.code, refused.exception.reason),
                                     ("PUBLIC_CONFIG", "DURATION"))
                    child.assert_not_called()
                    loader.assert_not_called()
                    driver.assert_not_called()
                    self.assertFalse(evidence.exists())
        finally:
            self.harness.root = original_root
