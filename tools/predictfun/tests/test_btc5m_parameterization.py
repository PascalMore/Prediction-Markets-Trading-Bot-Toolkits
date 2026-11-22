"""Offline regression of public parameters at actual controller/reader boundaries."""
import copy
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import btc5m_control as control
import btc5m_live_events as live

NOW = dt.datetime(2026, 11, 22, 12, 0, tzinfo=dt.timezone.utc)
SETS = (
    dict(budget=97, target=1, order=1, market=1, total=10, loss=20, seconds=86400),
    dict(budget=80, target=2, order=3, market=5, total=12, loss=30, seconds=7200),
)


def configured(budget=97, target=1, order=1, market=1, total=10, loss=20, seconds=86400):
    data = json.loads((Path(__file__).resolve().parents[1] / 'btc5m-controlled-paper.json').read_text())
    data['bot'].update(enable_trading=True, mock_trading=False)
    data.pop('controlled_paper', None)
    data['controlled_run'] = dict(budget_usdt=budget, duration_seconds=seconds, operations_mode='unattended')
    data['strategies']['btc_arb']['order_usd'] = target
    data['strategies']['limits'].update(max_order_usd=order, max_market_usd=market,
                                      max_total_usd=total, max_loss_usd=loss)
    return data


def stamp(value):
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def records(data):
    """The frozen startup triple the run's own configuration declares."""
    limits = data["strategies"]["limits"]
    started = {
        "schema_version": live.SCHEMA_VERSION, "strategy_contract": "phase1_net_shares",
        "operations_mode": data["controlled_run"]["operations_mode"],
        "risk_window_id": "risk-012", "run_id": "btc5m-param-live-012",
        "authorization_ref": "offline-auth",
        "account_identity": {"venue": "PREDICT_FUN", "chain_id": 56,
                             "account_mode": "predict-account",
                             "trading_address": live.TRADING_ADDRESS,
                             "signer_address": live.SIGNER_ADDRESS},
        "recovery_policy": "COLD_START_REFUSE", "cash_recovery_policy": "MANUAL_PAUSE",
        "asset_units": {"collateral_symbol": "USDT", "collateral_decimals": 18,
                        "share_decimals": 18, "gas_symbol": "BNB", "gas_decimals": 18},
        "started_at": stamp(NOW),
        "deadline_at": stamp(NOW + dt.timedelta(seconds=data["controlled_run"]["duration_seconds"])),
        "candidate_identity": {"main_head": "a" * 40, "bot_head": "b" * 40,
                               "binary_sha256": "c" * 64, "files": {"src/main.rs": "d" * 64}},
        "public_config_identity": {
            "sha256": hashlib.sha256(live.canonical(data)).hexdigest(),
            "initial_cash": data["controlled_run"]["budget_usdt"],
            "max_order_usd": limits["max_order_usd"], "max_market_usd": limits["max_market_usd"],
            "max_total_usd": limits["max_total_usd"], "max_loss_usd": limits["max_loss_usd"]},
    }
    context = {key: copy.deepcopy(started[key]) for key in set(live.CONTEXT_KEYS.split()) & set(started)}
    context.update(schema_version="btc5m-live-context/1", candidate_digest=live.candidate_digest(started),
                   gas_policy={"payer_address": live.SIGNER_ADDRESS, "minimum_balance_atoms": "1",
                               "maximum_consumption_atoms": "2"})
    return started, data, context


def accounting(base_atoms, realized=0, unrealized=0):
    """A COMPLETE window projection whose C0 is the run's own budget."""
    net = realized + unrealized
    return dict(status="COMPLETE", cash=str(base_atoms + realized), available=str(base_atoms + realized),
                reserved="0", cost="0", realized=str(realized), unrealized=str(unrealized),
                nav=str(base_atoms + net), net_pnl=str(net), loss_latched=False, reason="NONE",
                mark_time=None, pending_claim_value=None, gas_status="KNOWN", gas_paid_atoms="0")


def frame(started, seq, kind, window):
    payload = ({"policy": "COLD_START_REFUSE", "state": "ARMED", "reason": "NONE"} if kind == "recovery"
               else {"run_status": "COMPLETED", "stop_reason": "NONE", "execution_complete": True,
                     "cash_complete": True, "audit_complete": True, "takeover_required": False,
                     "unresolved": [], "positions": [], "stop_detail": None})
    return {"schema_version": live.SCHEMA_VERSION, "run_id": started["run_id"],
            "risk_window_id": started["risk_window_id"], "event_seq": seq, "event_kind": kind,
            "event_time": stamp(NOW), "market_id": None, "token": None,
            "accounting": window, "payload": payload}


def ended_from(final):
    payload = final["payload"]
    return {"schema_version": live.SCHEMA_VERSION, "run_id": final["run_id"],
            "exit_facts": {"launcher_exit": 0, "child_returncode": 0, "child_signal": None,
                           "forced_cleanup": False, "stop_reason": payload["stop_reason"]},
            "final_event_seq": final["event_seq"], "final_accounting": final["accounting"],
            "incomplete_reason": "NONE", "incomplete_detail": payload["stop_detail"],
            **{key: payload[key] for key in
               ("execution_complete", "cash_complete", "audit_complete", "takeover_required")}}


def write_run(directory, started, public, context, events, ended):
    directory.mkdir()
    for name, value in (("started.json", started), ("public-config.json", public),
                        ("live-context.json", context), ("ended.json", ended)):
        (directory / name).write_bytes(live.canonical(value) + b"\n")
    (directory / "events.jsonl").write_bytes(b"".join(live.canonical(event) + b"\n" for event in events))


@pytest.mark.parametrize('data', [configured(), configured(80, 2, 3, 5, 12, 30, 7200)])
def test_controller_accepts_two_parameter_sets(data):
    control.check_fixed_values(data, control.MODE_SIGNED_FUTURE_LIVE)


def test_dynamic_projection_is_bound_to_frozen_budget_and_loss():
    # The exact ACCOUNTING schema, with the window's own C0 in cash/nav.
    projection = accounting(97 * live.SCALE)
    # Schema fields are taken from the real validator; no arbitrary-base relaxation.
    live.check_accounting(projection, budget_atoms=97 * live.SCALE, loss_atoms=20 * live.SCALE)
    with pytest.raises(live.LiveInvalid):
        live.check_accounting(projection, budget_atoms=98 * live.SCALE, loss_atoms=20 * live.SCALE)


@pytest.mark.parametrize('frozen', SETS, ids=lambda frozen: "budget%s" % frozen["budget"])
def test_event_and_ended_reader_use_the_runs_own_budget(frozen, tmp_path):
    """B1: a parameterized COMPLETE event and ending are read, not refused."""
    data = configured(**frozen)
    started, public, context = records(data)
    identity = started["public_config_identity"]
    window = accounting(frozen["budget"] * live.SCALE)
    events = [frame(started, 1, "recovery", window), frame(started, 2, "final", window)]
    for event in events:
        live.validate_live_event(event, identity)
    write_run(tmp_path / "run", started, public, context, events, ended_from(events[-1]))
    result = live.read_live_run(tmp_path / "run")
    assert result["status"] == "COMPLETE" and result["reasons"] == []
    # The other approved budget is not this run's: the same bytes are refused.
    other = 80 if frozen["budget"] == 97 else 97
    with pytest.raises(live.LiveInvalid):
        live.validate_live_event(frame(started, 1, "recovery", accounting(other * live.SCALE)), identity)


def test_accounting_reader_refuses_a_budget_the_started_record_did_not_declare(tmp_path):
    """B1 counterexample: started declares 97 while the evidence was booked at 80."""
    data = configured(budget=97, order=1)
    started, public, context = records(data)
    events = [frame(started, 1, "recovery", accounting(80 * live.SCALE)),
              frame(started, 2, "final", accounting(80 * live.SCALE))]
    with pytest.raises(live.LiveInvalid):
        live.validate_live_event(events[0], started["public_config_identity"])
    write_run(tmp_path / "run", started, public, context, events, ended_from(events[-1]))
    result = live.read_live_run(tmp_path / "run")
    assert result["status"] == "INCOMPLETE" and result["reasons"] == ["OUTPUT_INVALID"]


def test_legacy_evidence_without_a_public_identity_keeps_the_recorded_window():
    """Only evidence written before the identity carried the window stays at 98/100."""
    data = configured()
    started, _, _ = records(data)
    live.validate_live_event(frame(started, 1, "recovery", accounting(98 * live.SCALE)), None)
    live.validate_live_event(frame(started, 1, "recovery", accounting(100 * live.SCALE)), None)
    with pytest.raises(live.LiveInvalid):
        live.validate_live_event(frame(started, 1, "recovery", accounting(97 * live.SCALE)), None)


@pytest.mark.parametrize('frozen', SETS, ids=lambda frozen: "budget%s" % frozen["budget"])
def test_metadata_reader_accepts_a_target_at_or_below_the_order_cap(frozen, tmp_path):
    """B2: target<=cap is the published relation; the second set targets below it."""
    data = configured(**frozen)
    started, public, context = records(data)
    live.check_metadata(started, public, context)
    window = accounting(frozen["budget"] * live.SCALE)
    events = [frame(started, 1, "recovery", window), frame(started, 2, "final", window)]
    write_run(tmp_path / "run", started, public, context, events, ended_from(events[-1]))
    assert live.read_live_run(tmp_path / "run")["status"] == "COMPLETE"


def test_metadata_reader_refuses_a_target_above_the_order_cap():
    """B2 counterexample: the reader keeps target<=cap and public==limits."""
    data = configured(80, 4, 3, 5, 12, 30, 7200)
    started, public, context = records(data)
    with pytest.raises(live.LiveInvalid):
        live.check_metadata(started, public, context)
    ordered = configured(80, 2, 3, 5, 12, 30, 7200)
    started, public, context = records(ordered)
    public["strategies"]["limits"]["max_order_usd"] = 4
    with pytest.raises(live.LiveInvalid):
        live.check_metadata(started, public, context)


def test_loss_threshold_is_the_runs_own_frozen_value():
    """B1: the loss reader uses max_loss_usd, not a fixed 20 USDT."""
    data = configured(80, 2, 3, 5, 12, 30, 7200)
    started, _, _ = records(data)
    identity = started["public_config_identity"]
    window = accounting(80 * live.SCALE, realized=-25 * live.SCALE)
    live.validate_live_event(frame(started, 1, "recovery", window), identity)
    with pytest.raises(live.LiveInvalid):
        live.validate_live_event(frame(started, 1, "recovery", window),
                                 {**identity, "max_loss_usd": 20})
