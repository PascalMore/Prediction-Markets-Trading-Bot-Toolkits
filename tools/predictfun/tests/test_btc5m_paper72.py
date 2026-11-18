"""Offline duration/terminal capacity regressions; never a paper/backtest run.

The two run3857 RED inputs remain regression targets (their original XML is
preserved). Native sender fixtures seed output shapes; they are NOT simulated
865-round trading histories and must not manufacture COMPLETE observation coverage.
"""
from __future__ import annotations

import datetime
import hashlib
import json
import os
import socket
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import btc5m_control as control
import btc5m_observer as observer
from test_btc5m_control import CHILD, final_event
from test_btc5m_observer import producer, record

PAPER72_SECONDS = int(datetime.timedelta(hours=72).total_seconds())
PAPER72_ROUNDS = PAPER72_SECONDS // 300 + 1


def compact_bytes(value: dict) -> bytes:
    """Use compact JSON as the Rust emitter does."""
    return json.dumps(value, separators=(",", ":"), allow_nan=False).encode()


def capacity_final():
    event = final_event()
    event["source"]["markets"] = [
        {"market_id": str(index), "yes_token": str(100000 + 2 * index),
         "no_token": str(100001 + 2 * index), "status": "CONFIRMED",
         "holdings": [], "payout_yes": 1}
        for index in range(1, PAPER72_ROUNDS + 1)]
    return event


def capacity_end():
    series = [{"market_id": str(index), "token": str(100000 + index),
               "terminal_state": "SETTLED", "terminal_observation_seq": index}
              for index in range(1, PAPER72_ROUNDS + 1)]
    report = producer(attempted=PAPER72_ROUNDS, sent=PAPER72_ROUNDS,
                      last_seq=PAPER72_ROUNDS, series=series)
    assert observer.validate_producer(report) == report
    return {"wire_version": observer.WIRE_VERSION, "kind": "END",
            "run_id": "run-1", "producer": report}


def fixture_supervise(root, wire, seconds, age=0):
    root.mkdir()
    script = root / "fixture.py"
    script.write_text(CHILD.replace('if mode == "stream":\n    print(line, flush=True)',
        'if mode == "stream":\n    sys.stdout.buffer.write(open(line, "rb").read())\n    sys.stdout.buffer.flush()'))
    payload = root / "wire.txt"
    payload.write_bytes(wire)
    now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=age)
    evidence = control.Evidence(root, "run-1", now.isoformat(), now.isoformat(),
                                control.MODE_UNSIGNED_PAPER)
    child = subprocess.Popen([sys.executable, "-B", str(script), "stream",
                              str(payload)], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    facts, final = control.supervise(child, evidence, now, control.boottime() - age, run_seconds=seconds)
    assert child.poll() is not None
    return facts, final


def test_paper72_final_fits_original_launcher_real_child(tmp_path):
    event = capacity_final()
    raw = compact_bytes(event)
    assert control.validate_event(raw, "run-1") == event
    wire = control.EVENT_PREFIX.encode() + raw + b"\n"
    assert len(wire) == 96321  # The original run3857 RED, unchanged.
    facts, final = fixture_supervise(tmp_path / "extended", wire, PAPER72_SECONDS)
    print(json.dumps({"scenario": "original_RED_final", "wire_bytes": len(wire), "facts": facts}))
    assert facts["launcher_exit"] == 0 and final == event
    facts, final = fixture_supervise(tmp_path / "ordinary", wire, control.RUN_SECONDS)
    assert facts["stop_reason"] == "OUTPUT_INVALID" and final is None


def test_paper72_observer_terminal_frame_fits_existing_wire_bound(tmp_path):
    frame = capacity_end()
    wire = compact_bytes(frame) + b"\n"
    assert len(wire) == 82271  # The original run3857 RED, unchanged.
    assert PAPER72_ROUNDS <= observer.MAX_SERIES
    store = observer.ObservationStore("run-1", tmp_path)
    receiver = observer.ObserverReceiver(store, observer.TradeIndex(), observer.PAPER72_END_BYTES)
    receiver.receive(frame, len(wire))
    assert store.end_received and len(store.producer["series"]) == PAPER72_ROUNDS
    ordinary = observer.ObservationStore("run-1", tmp_path)
    observer.ObserverReceiver(ordinary, observer.TradeIndex()).receive(frame, len(wire))
    assert not ordinary.end_received and "TRUNCATED_OUTPUT" in ordinary.reasons


@pytest.mark.parametrize("seconds,age,stopped", [(259200, 21601, False),
                                                  (259200, 259201, True),
                                                  (21600, 21601, True)])
def test_parent_uses_same_selected_budget_after_six_hours(tmp_path, seconds, age, stopped):
    wire = control.EVENT_PREFIX.encode() + compact_bytes(final_event()) + b"\n"
    facts, final = fixture_supervise(tmp_path / "clock", wire, seconds, age)
    assert facts["launcher_exit"] == 0 and final is not None
    assert (facts["stop_reason"] == "DEADLINE") is stopped


def test_legal_terminal_shape_bound_includes_unsettled_holdings():
    # Conservative representation proof: actual market ids are u64 (20 digits),
    # tokens <=78 digits, time <=40 bytes, every float/u64/null <=64 bytes.
    # Even TWO holdings per market (more than the once-per-round entry permits)
    # fit 1 MiB; END fits 256 KiB even at the existing 1000-series bound.
    market_bound = len(compact_bytes({
        "market_id": "9" * 20, "yes_token": "9" * 78, "no_token": "8" * 78,
        "status": "CONFIRMED", "payout_yes": "9" * 64,
        "holdings": [{"token": "9" * 78, "shares": "9" * 64, "cost": "9" * 64,
                      "mark": "9" * 64, "mark_valid": False, "mark_time": "9" * 40}] * 2}))
    final_bound = 4096 + PAPER72_ROUNDS * (market_bound + 1)
    series_bound = len(compact_bytes({"market_id": "9" * 20, "token": "9" * 78,
        "terminal_state": "ROUND_ENDED", "terminal_observation_seq": "9" * 64}))
    end_bound = 4096 + observer.MAX_SERIES * (series_bound + 1)
    print(json.dumps({"scenario": "conservative_shape_bound", "final_bytes": final_bound,
                      "END_bytes": end_bound, "rounds": PAPER72_ROUNDS}))
    assert final_bound < control.PAPER72_FINAL_BYTES
    assert end_bound < observer.PAPER72_END_BYTES


def test_native_unix_datagram_capacity_and_receiver_boundaries(tmp_path):
    parent, child = observer.create_socketpair()
    try:
        child.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, observer.PAPER72_END_BYTES)
        parent.settimeout(1)
        frame = capacity_end()
        base = compact_bytes(frame) + b"\n"
        wire = b" " * (observer.PAPER72_END_BYTES - len(base)) + base
        assert child.send(wire) == observer.PAPER72_END_BYTES
        received = parent.recv(observer.PAPER72_END_BYTES + 1)
        assert received == wire
        print(json.dumps({"scenario": "native_AF_UNIX_boundary", "bytes": len(received),
            "SO_SNDBUF": child.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)}))
        for size, accepted in ((observer.PAPER72_END_BYTES, True),
                                (observer.PAPER72_END_BYTES + 1, False)):
            store = observer.ObservationStore("run-1", tmp_path)
            receiver = observer.ObserverReceiver(store, observer.TradeIndex(), observer.PAPER72_END_BYTES)
            receiver.receive(json.loads(received), size)
            assert store.end_received is accepted
        # Extended END capability never increases SAMPLE capacity.
        store = observer.ObservationStore("observer-test", tmp_path)
        receiver = observer.ObserverReceiver(store, observer.TradeIndex(), observer.PAPER72_END_BYTES)
        receiver.receive({"wire_version": observer.WIRE_VERSION, "kind": "SAMPLE",
            "run_id": "observer-test", "after_event_seq": 1, "record": record()}, 65537)
        assert store.rejected_samples == 1 and not receiver.buffer
    finally:
        parent.close()
        child.close()


@pytest.fixture(scope="module")
def native_executable():
    target = os.environ["CARGO_TARGET_DIR"]
    result = subprocess.run(["cargo", "test", "--offline", "--locked", "--target-dir", target,
        "--lib", "--no-run", "--message-format=json"], cwd=control.BOT,
        capture_output=True, timeout=180)
    assert result.returncode == 0, result.stderr.decode()
    artifacts = [json.loads(line) for line in result.stdout.splitlines() if line.startswith(b"{")]
    executables = [item["executable"] for item in artifacts if item.get("reason") == "compiler-artifact"
        and item.get("executable") and item["target"]["name"] == "polymarket_toolkits"
        and item["profile"]["test"]]
    assert len(executables) == 1
    return executables[0]


def native_launch(root, executable, fixture, run_id="run-1", observed=False):
    def popen(argv, **kwargs):
        assert Path(argv[0]) == control.PAPER72_BINARY
        start = argv[argv.index("--controlled-started-at") + 1]
        end = argv[argv.index("--controlled-deadline-at") + 1]
        assert (datetime.datetime.fromisoformat(end.replace("Z", "+00:00"))
                - datetime.datetime.fromisoformat(start.replace("Z", "+00:00"))).total_seconds() == PAPER72_SECONDS
        kwargs["env"].update(BTC5M_TEST_STARTED_AT=start, BTC5M_TEST_DEADLINE_AT=end)
        return subprocess.Popen([executable, fixture, "--exact", "--nocapture", "--test-threads=1"], **kwargs)
    args = ["--mode", "unsigned-paper", "--config", str(control.BOT / "tools/predictfun/btc5m-controlled-paper.json"),
            "--run-id", run_id, "--duration-hours", "72"]
    if observed:
        args.append("--post-entry-observer")
    # Fixed synthetic market key only; no keyring or actual business network.
    with patch("socket.socket.connect", side_effect=AssertionError("NETWORK_FORBIDDEN")):
        facts = control.run(args, environment={"YQUANT_PREDICT_FUN_TOKEN": "synthetic-market-only"},
                            evidence_root=root, popen=popen)
    assert facts["launcher_exit"] == 0 and facts["child_returncode"] == 0, facts
    return root / run_id


def test_actual_rust_final_through_original_launcher_and_strict_trading_reader(tmp_path, native_executable):
    directory = native_launch(tmp_path, native_executable,
        "bot::runtime::paper::tests::paper72_terminal_sender_fixture")
    event = json.loads((directory / "events.jsonl").read_bytes())
    assert len(event["source"]["markets"]) == PAPER72_ROUNDS
    assert all(len(item["yes_token"]) == 78 for item in event["source"]["markets"])
    pending = [item for item in event["source"]["markets"] if item["status"] == "PENDING"]
    assert len(pending) == 1 and len(pending[0]["holdings"]) == 1
    assert pending[0]["holdings"][0]["mark"] is None and not event["source"]["finalized"]
    identifier, _index = observer._read_trading(directory, "run-1")
    assert identifier == "run-1"
    result = observer.read_observations(directory)
    assert "TRADING_EVIDENCE_INVALID" not in result["reasons"]
    assert result["status"] == "OBSERVATIONS_UNAVAILABLE"  # Observer was not enabled.
    started = json.loads((directory / "started.json").read_text())
    assert started["candidate_identity"]["binary_sha256"] == hashlib.sha256(control.PAPER72_BINARY.read_bytes()).hexdigest()
    print(json.dumps({"scenario": "native_Rust_final", "persisted_bytes": (directory / "events.jsonl").stat().st_size,
        "wire_bytes": len(compact_bytes(event)) + len(control.EVENT_PREFIX) + 1,
        "markets": len(event["source"]["markets"]), "strict_trading_valid": True}))


def test_actual_rust_end_via_original_supervisor_and_strict_reader(tmp_path, native_executable):
    directory = native_launch(tmp_path, native_executable,
        "bot::arbitrage::observer::tests::paper72_end_sender_fixture", "observer-test", True)
    coverage = json.loads((directory / observer.COVERAGE_FILE).read_text())
    assert coverage["ready_received"] and coverage["end_received"]
    assert len(coverage["producer"]["series"]) == PAPER72_ROUNDS
    assert all(len(item["token"]) == 78 for item in coverage["producer"]["series"])
    assert coverage["producer"]["disabled_reason"] is None
    result = observer.read_observations(directory)
    assert "TRADING_EVIDENCE_INVALID" not in result["reasons"]
    assert result["status"] == "INCOMPLETE"  # Seeded terminals have no samples: never fake COMPLETE.
    print(json.dumps({"scenario": "native_Rust_END", "series": PAPER72_ROUNDS,
        "wire_bytes": len(compact_bytes({"wire_version": observer.WIRE_VERSION, "kind": "END",
            "run_id": "observer-test", "producer": coverage["producer"]})) + 1,
        "strict_status": result["status"], "END_accepted": coverage["end_received"]}))


def test_supervisor_terminal_boundary_and_malformed_nonfinal_rejection(tmp_path):
    event = capacity_final()
    raw = compact_bytes(event)
    width = len(control.EVENT_PREFIX) + len(raw) + 1
    wire = (control.EVENT_PREFIX.encode() + b" " * (control.PAPER72_FINAL_BYTES - width)
            + raw + b"\n")
    assert len(wire) == control.PAPER72_FINAL_BYTES
    facts, final = fixture_supervise(tmp_path / "exact", wire, PAPER72_SECONDS)
    assert facts["launcher_exit"] == 0 and final == event
    for name, invalid in (("over", wire[:-1] + b" \n"),
                          ("malformed", control.EVENT_PREFIX.encode() + b"{bad json}\n"),
                          ("nonfinal", wire.replace(b'"final"', b'"summary"', 1))):
        facts, final = fixture_supervise(tmp_path / name, invalid, PAPER72_SECONDS)
        assert facts["stop_reason"] == "OUTPUT_INVALID" and final is None


def test_duration_refusal_precedes_config_credentials_or_child():
    for value in ("0", "7", "71", "73", "72.0"):
        with pytest.raises(control.Refused):
            control.build_parser().parse_args(["--mode", "unsigned-paper", "--config", "absent",
                                               "--run-id", "run-1", "--duration-hours", value])
    with patch.object(control, "read_public_config", side_effect=AssertionError("CONFIG_FORBIDDEN")):
        with pytest.raises(control.Refused, match="CONTROLLED_MODE") as refused:
            control.run(["--mode", "signed-future-live", "--config", "absent",
                         "--run-id", "run-1", "--duration-hours", "72"], environment={})
    assert refused.value.reason == "DURATION_REFUSED"
    assert control.build_parser().parse_args(["--mode", "unsigned-paper", "--config", "absent",
                                              "--run-id", "run-1"]).duration_hours == 6
    assert control.check_paths(Path("absent"))[1] == control.BOT / "target/debug/polymarket-toolkits"
    assert control.check_paths(Path("absent"), True)[1] == control.PAPER72_BINARY


def test_built_cli_duration_admission_precedes_missing_config_or_credentials(tmp_path):
    start = datetime.datetime.now(datetime.timezone.utc)
    for mode, hours, expected in (("signed-future-live", 72, "fixed budget"),
                                   ("signed-future-live", 6, "LIVE_CONFIRMATION"),
                                   ("unsigned-paper", 72, "PUBLIC_CONFIG")):
        end = start + datetime.timedelta(hours=hours)
        command = control.child_arguments(control.PAPER72_BINARY, tmp_path / "absent.json",
                                          "run-1", start.isoformat(), end.isoformat())
        command[command.index("--controlled-mode") + 1] = mode
        result = subprocess.run(command, cwd=control.BOT, env={"PATH": "/usr/bin:/bin"},
                                capture_output=True, timeout=5)
        assert result.returncode != 0 and expected.encode() in result.stderr, (mode, result.stderr)


def test_actual_rust_END_exact_limit_plus_one_and_SAMPLE_limit(native_executable):
    parent, child = observer.create_socketpair()
    try:
        child.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, observer.PAPER72_END_BYTES)
        parent.settimeout(5)
        environment = {"PATH": "/usr/bin:/bin", "BTC5M_TEST_STARTED_AT": "fixture",
            observer.PAPER72_ENV: str(PAPER72_SECONDS), **observer.child_fd_environment(child.fileno())}
        process = subprocess.Popen([native_executable,
            "bot::arbitrage::observer::tests::paper72_native_sender_exact_boundary_fixture",
            "--exact", "--nocapture", "--test-threads=1"], env=environment,
            pass_fds=(child.fileno(),), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        child.close()
        assert json.loads(parent.recv(observer.PAPER72_END_BYTES + 1))["kind"] == "READY"
        wire = parent.recv(observer.PAPER72_END_BYTES + 1)
        assert len(wire) == observer.PAPER72_END_BYTES and wire.endswith(b"\n")
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == 0, (stdout, stderr)
        parent.settimeout(.05)
        with pytest.raises(socket.timeout):
            parent.recv(observer.PAPER72_END_BYTES + 1)
        print(json.dumps({"scenario": "native_Rust_END_boundary", "bytes": len(wire),
                          "over_limit_and_large_SAMPLE": "REJECTED", "native_exit": process.returncode}))
    finally:
        parent.close()
        child.close()
