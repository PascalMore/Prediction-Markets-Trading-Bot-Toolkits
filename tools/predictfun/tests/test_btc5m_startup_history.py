"""R18 first-write/strict-reader regressions from the original Rust producer."""
from __future__ import annotations

import copy
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from test_btc5m_live_events import metadata, ended_from_native
import btc5m_live_events as live


class StartupHistoryConsumerTests(unittest.TestCase):
    def test_eleven_opening_targets_and_two_current_claims_use_original_strict_reader(self) -> None:
        for fixture, attempts in (("startup_history_keeps_current_claim_cash_checks", 2),
                                  ("native_startup_history_cash_first", 2),
                                  ("native_startup_history_sticky_stop", 1)):
            with self.subTest(fixture=fixture):
                name = "bot::runtime::live_control::startup_history::" + fixture
                result = subprocess.run(
                    ["cargo", "test", "--locked", "--offline", "--target-dir", os.environ["CARGO_TARGET_DIR"],
                     "--lib", name, "--", "--exact", "--nocapture"],
                    cwd=Path(__file__).resolve().parents[3], capture_output=True, timeout=120,
                )
                self.assertEqual(result.returncode, 0, result.stderr.decode() + result.stdout.decode())
                self.assertIn(b"1 passed", result.stdout)
                frames = [line + b"\n" for line in result.stdout.splitlines() if line.startswith(live.EVENT_PREFIX)]
                events = [live.validate_live_event(frame) for frame in frames]
                self.assertTrue(events)
                self.assertEqual([event["event_seq"] for event in events], list(range(1, len(events) + 1)))
                self.assertEqual(len({(event["run_id"], event["risk_window_id"]) for event in events}), 1)
                self.assertEqual(events[0]["payload"]["scope"], "STARTUP")
                self.assertEqual(events[0]["payload"]["collateral_atoms"], "147500000000000000000")
                self.assertEqual(events[0]["accounting"]["cash"], "98000000000000000000")
                self.assertEqual(events[0]["accounting"]["net_pnl"], "0")
                confirmed = [event for event in events if event["event_kind"] == "payout"
                             and event["payload"]["status"] == "CASH_CONFIRMED"]
                self.assertEqual(len(confirmed), attempts)
                self.assertEqual({event["token"] for event in confirmed}, {"77", "177"} if attempts == 2 else {"77"})
                with tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"], prefix="r18-reader-") as root:
                    store = live.LiveEventStore(Path(root) / "run", *metadata(frames[0]))
                    try:
                        for frame in frames:
                            store.append(frame)
                        final = store.relations.final
                        assert final is not None
                        self.assertTrue(final["payload"]["cash_complete"])
                        self.assertEqual(final["payload"]["positions"], [])
                        self.assertEqual(len(store.relations.attempts), attempts)
                        self.assertEqual(sum(claim["confirmed"] for claim in store.relations.claims.values()), attempts)
                        if attempts == 2:
                            self.assertEqual(final["payload"]["stop_reason"], "DEADLINE")
                        else:
                            hard = next(event for event in events if event["event_kind"] == "stop"
                                        and event["payload"]["reason"] != "CASH_PENDING")
                            self.assertEqual(final["payload"]["stop_reason"], hard["payload"]["reason"])
                            self.assertTrue(final["payload"]["takeover_required"])
                        ended = ended_from_native(final)
                        ended["incomplete_reason"] = None
                        store.finish(ended)
                        read = live.read_live_run(store.directory)
                        self.assertEqual(read["status"], "COMPLETE", read["reasons"])
                    finally:
                        store.close()
                # A consumed real stream, not a hand-built healthy projection:
                # a one-atom mutation of a current claim still fails relations.
                relation = live.LiveRelations(events[0]["run_id"], events[0]["risk_window_id"])
                for event in events:
                    if event in confirmed:
                        for delta in (-1, 1):
                            broken = copy.deepcopy(event)
                            amount = str(int(broken["payload"]["claim_value_atoms"]) + delta)
                            broken["payload"]["claim_value_atoms"] = amount
                            broken["payload"]["cash_received_atoms"] = amount
                            with self.assertRaises(live.LiveInvalid):
                                relation.stage(broken)
                        break
                    relation = relation.stage(event)


if __name__ == "__main__":
    unittest.main()
