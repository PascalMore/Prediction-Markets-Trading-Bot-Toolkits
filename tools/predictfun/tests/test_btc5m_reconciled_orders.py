"""The operator's declared history: live-only, verbatim, in order, or refused.

The live launcher must be able to carry a human's own explanation of a
historical order to the one child it starts, and must refuse everything else.
The live fixture (fake storage, fake secret, real argv) is the one in
``test_btc5m_control`` — bound here, never copied or weakened.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import btc5m_control as control
import test_btc5m_control as launcher_fixture

DECLARED = "0x" + "b9" * 32
SECOND = "0x" + "3F" * 32


class DeclaredHistoryTests(unittest.TestCase):
    """One declaration path, checked at its edges; no real dispatch anywhere."""

    # The live fixture is the one launcher test class already owns: bound here,
    # not subclassed, so its own tests are never collected twice.
    setUp = launcher_fixture.ControlledLiveSupervisorTests.setUp
    launch_fixture = launcher_fixture.ControlledLiveSupervisorTests.launch_fixture

    def dispatch(self, *declarations):
        """Run one fake-child live launch with these declarations; return argv."""
        self.root = self.root / f"declared-{len(declarations)}"
        self.root.mkdir()
        args, root, child, *_unused = self.launch_fixture()
        for value in declarations:
            args += ["--predict-reconciled-order", value]
        facts = control.run(args, environment={}, evidence_root=root, popen=child)
        self.assertEqual(facts["launcher_exit"], 0)
        child.assert_called_once()
        return args, child.call_args.args[0]

    def test_declared_history_hashes_reach_the_child_verbatim_and_in_order(self):
        _args, argv = self.dispatch(DECLARED, SECOND)
        passed = [argv[index + 1] for index, value in enumerate(argv)
                  if value == "--predict-reconciled-order"]
        self.assertEqual(passed, [DECLARED, SECOND])
        self.assertEqual(argv[-2:], ["run", "btc-arb"])

    def test_undeclared_history_leaves_the_child_argv_unchanged(self):
        args, argv = self.dispatch()
        given = {name: args[args.index(name) + 1] for name in
                 ("--predict-budget-confirmation", "--predict-rpc-url", "--operations-mode",
                  "--risk-window-id", "--authorization-ref", "--run-id", "--deadline-at")}
        directory = Path(argv[argv.index("--controlled-live-manifest") + 1]).parent
        started = json.loads((directory / "started.json").read_bytes())
        self.assertEqual(argv, [str(control.LIVE_BINARY),
                                "--predict-budget-confirmation", given["--predict-budget-confirmation"],
                                "--predict-rpc-url", given["--predict-rpc-url"],
                                "--controlled-live-contract", "phase1_net_shares",
                                "--controlled-operations-mode", given["--operations-mode"],
                                "--controlled-risk-window-id", given["--risk-window-id"],
                                "--controlled-authorization-ref", given["--authorization-ref"],
                                "--controlled-live-manifest", str(directory / "live-context.json"),
                                "--config", str(directory / "public-config.json"),
                                "--controlled-mode", "signed-future-live",
                                "--controlled-run-id", given["--run-id"],
                                "--controlled-started-at", started["started_at"],
                                "--controlled-deadline-at", given["--deadline-at"],
                                "run", "btc-arb"])

    def test_an_invalid_history_hash_is_refused_before_any_child(self):
        original_root = self.root
        invalid = ("0x" + "g" * 64, "0x" + "b" * 63, "0x" + "b" * 65, "b9" * 32, "0X" + "b9" * 32)
        for index, value in enumerate(invalid):
            with self.subTest(value=value):
                # The interface accepts the declaration; its value is what refuses.
                adapted = control.build_parser().parse_args(
                    ["--mode", "signed-future-live", "--config", "x", "--run-id", "r",
                     "--predict-reconciled-order", value])
                self.assertEqual(adapted.predict_reconciled_order, [value])
                self.root = original_root / f"invalid-{index}"
                self.root.mkdir()
                args, root, child, loader, _collection, _path, driver, *_unused = (
                    self.launch_fixture())
                args += ["--predict-reconciled-order", value]
                with self.assertRaises(control.Refused) as refused:
                    control.run(args, environment={}, evidence_root=root, popen=child)
                self.assertEqual(refused.exception.reason, "BAD_ARGUMENTS")
                child.assert_not_called()
                loader.assert_not_called()
                driver.assert_not_called()
                self.assertFalse(root.exists())
        self.root = original_root

    def test_a_declared_history_hash_is_a_live_only_input(self):
        """An explanation of a live order means nothing to a paper ledger."""
        for extra in ([], ["--duration-hours", "72"]):
            with self.subTest(extra=extra), self.assertRaises(control.Refused) as refused:
                control.run(["--mode", "unsigned-paper", "--config", "unread.json",
                             "--run-id", "paper-run", *extra,
                             "--predict-reconciled-order", DECLARED], environment={})
            self.assertEqual(refused.exception.reason, "LIVE_OPTION_REFUSED")

    def test_a_declared_history_hash_without_the_live_context_is_refused(self):
        with self.assertRaises(control.Refused) as refused:
            control.run(["--mode", "signed-future-live", "--config", "unread.json",
                         "--run-id", "no-context", "--predict-reconciled-order", DECLARED],
                        environment={})
        self.assertEqual(refused.exception.reason, "LIVE_CONTEXT")


if __name__ == "__main__":
    unittest.main()
