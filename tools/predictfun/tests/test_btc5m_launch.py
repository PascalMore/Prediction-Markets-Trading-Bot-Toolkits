"""Offline proof that the launch wrapper is import-safe and ordered."""
import copy
import datetime as dt
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import btc5m_launch as launch


NOW = dt.datetime(2026, 11, 22, 12, 0, tzinfo=dt.timezone.utc)


def configured():
    data = json.loads((Path(__file__).resolve().parents[1] / 'btc5m-controlled-paper.json').read_text())
    data['bot'].update(enable_trading=True, mock_trading=False)
    data.pop('controlled_paper', None)
    data['controlled_run'] = dict(budget_usdt=97.422, duration_seconds=86400, operations_mode='unattended')
    data['strategies']['btc_arb']['order_usd'] = 1
    data['strategies']['limits'].update(max_order_usd=1, max_market_usd=1, max_total_usd=10,
                                        max_loss_usd=20)
    return data


def reconciliation(cash='97422000000000000000', gas='3000000000000000', **overrides):
    value = {'finished_at': (NOW - dt.timedelta(seconds=30)).isoformat().replace('+00:00', 'Z'),
             'read_only': True,
             'reconciliation': {'complete': True, 'positions_match_history': True},
             'account': {'pagination_complete': True, 'positions_explained': True,
                         'open_orders': [], 'collateral_atoms': cash, 'gas_atoms': gas}}
    value.update(overrides)
    return value


def write(tmp_path, data=None, facts=None):
    config = tmp_path / 'config.json'
    config.write_text(json.dumps(configured() if data is None else data))
    record = tmp_path / 'reconciliation.json'
    record.write_text(json.dumps(reconciliation() if facts is None else facts))
    return config, record


def arguments(config, record, **overrides):
    argv = ['--config', str(config), '--reconciliation', str(record),
            '--run-id', 'btc5m-unattended-live-012',
            '--authorization-ref', 'pascal-unattended-24h-20261122-012',
            '--risk-window-id', 'btc5m-unattended-24h-20261122-012',
            '--operations-mode', 'unattended', '--deployment-id', 'dep-1',
            '--takeover-ref', 'takeover-012',
            '--old-run', 'btc5m-unattended-live-011',
            '--old-risk-window', 'btc5m-unattended-24h-20261122-011',
            '--old-authorization-ref', 'pascal-unattended-24h-20261122-011',
            '--old-digest', 'a' * 64]
    args = launch.build_parser().parse_args(argv)
    # The parameters come from the configuration, never from the run's checks.
    import btc5m_control as control
    args.params = control.parameters(json.loads(Path(config).read_text()))
    for name, value in overrides.items():
        setattr(args, name, value)
    return args


def test_import_and_preflight_have_no_side_effects(tmp_path):
    config, record = write(tmp_path)
    before = sorted(path.name for path in tmp_path.iterdir())
    checked = launch.preflight(config, record, now=NOW)
    assert checked['params']['budget'] == 97422000000000000000
    assert sorted(path.name for path in tmp_path.iterdir()) == before


def test_preflight_refuses_stale_unfunded_or_unexplained_reconciliation(tmp_path):
    cases = {
        'stale': (reconciliation(finished_at=(NOW - dt.timedelta(hours=2)).isoformat()), 'STALE_RECONCILIATION'),
        # 1 USDT is not the 97.422 the window declares: a short or a high
        # balance is a mismatch, never an automatic resize.
        'mismatch': (reconciliation(cash='1000000000000000000'), 'CASH_BUDGET_MISMATCH'),
        'low_gas': (reconciliation(gas='1'), 'GAS_BELOW_MINIMUM'),
        'orders': (reconciliation(account={'pagination_complete': True, 'positions_explained': True,
                                          'open_orders': ['x'], 'collateral_atoms': '0',
                                          'gas_atoms': '1'}), 'OPEN_ORDERS_PRESENT'),
    }
    for name, (facts, reason) in cases.items():
        config, record = write(tmp_path / name if (tmp_path / name).mkdir() is None else tmp_path, facts=facts)
        with pytest.raises(launch.Refused) as refused:
            launch.preflight(config, record, now=NOW)
        assert refused.value.reason == reason, name


def test_preflight_refuses_a_cash_that_is_not_the_declared_budget(tmp_path):
    """A budget that is not the observed cash is refused atom-for-atom."""
    config, record = write(tmp_path)
    # The window declares 97.422; the venue holds one atom more.
    data = json.loads(Path(config).read_text())
    facts = reconciliation(cash='97422000000000000001')
    record.write_text(json.dumps(facts))
    with pytest.raises(launch.Refused) as refused:
        launch.preflight(config, record, now=NOW)
    assert refused.value.reason == 'CASH_BUDGET_MISMATCH'
    # A lower balance is the same mismatch, not a resize.
    facts = reconciliation(cash='97421999999999999999')
    record.write_text(json.dumps(facts))
    with pytest.raises(launch.Refused) as refused:
        launch.preflight(config, record, now=NOW)
    assert refused.value.reason == 'CASH_BUDGET_MISMATCH'
    assert data['controlled_run']['budget_usdt'] == 97.422


def test_budget_may_be_stated_at_atom_precision(tmp_path):
    """An eighteen-decimal budget string equals the observed cash to the atom."""
    import btc5m_control as control
    data = configured()
    data['controlled_run']['budget_usdt'] = '98.148096491228070176'
    config, record = write(tmp_path, data=data,
                           facts=reconciliation(cash='98148096491228070176'))
    checked = launch.preflight(config, record, now=NOW)
    assert checked['params']['budget'] == 98148096491228070176
    # One atom off is refused, never rounded or truncated.
    record.write_text(json.dumps(reconciliation(cash='98148096491228070175')))
    with pytest.raises(launch.Refused) as refused:
        launch.preflight(config, record, now=NOW)
    assert refused.value.reason == 'CASH_BUDGET_MISMATCH'
    # Sub-atom precision in the string is a public-config refusal.
    data['controlled_run']['budget_usdt'] = '98.1480964912280701761'
    with pytest.raises(control.Refused):
        control.parameters(data)


def test_preflight_accepts_a_budget_plus_the_verified_claimed_cash(tmp_path):
    """A claimed payout is the declared pre-claim C0 plus its exact atoms."""
    data = configured()
    data['controlled_run']['budget_usdt'] = '96.4245'
    claim = '1723596491228070176'
    facts = reconciliation(cash='98148096491228070176')
    facts['account']['claimed_win'] = [{
        'token': '77', 'market_id': 1, 'oracle_status': 'RESOLVED',
        'trading_status': 'CLOSED', 'outcome_status': 'WON',
        'payout_millionths': 1000000, 'inventory_atoms': claim, 'claim_atoms': claim}]
    config, record = write(tmp_path, data=data, facts=facts)
    checked = launch.preflight(config, record, now=NOW)
    assert checked['params']['budget'] == 96424500000000000000
    # One atom more cash, or one atom less claim, is a mismatch.
    facts['account']['collateral_atoms'] = '98148096491228070177'
    record.write_text(json.dumps(facts))
    with pytest.raises(launch.Refused) as refused:
        launch.preflight(config, record, now=NOW)
    assert refused.value.reason == 'CASH_BUDGET_MISMATCH'
    facts['account']['collateral_atoms'] = '98148096491228070176'
    facts['account']['claimed_win'][0]['claim_atoms'] = '1723596491228070175'
    record.write_text(json.dumps(facts))
    with pytest.raises(launch.Refused) as refused:
        launch.preflight(config, record, now=NOW)
    assert refused.value.reason == 'CASH_BUDGET_MISMATCH'
    # Malformed evidence is a facts refusal, never a silently empty claim list.
    facts['account']['claimed_win'] = 'claim'
    record.write_text(json.dumps(facts))
    with pytest.raises(launch.Refused) as refused:
        launch.preflight(config, record, now=NOW)
    assert refused.value.reason == 'RECONCILIATION_FACTS'


def test_launch_refuses_before_any_durable_action(tmp_path):
    config, record = write(tmp_path, facts=reconciliation(gas='1'))
    calls = []
    args = arguments(config, record)
    with pytest.raises(launch.Refused):
        launch.launch(args, now=NOW, fetch_account=lambda: calls.append('account') or {},
                      read_barrier=lambda address: calls.append('read'),
                      rotate_barrier=lambda record: calls.append('rotate'),
                      write_activation=lambda activation: calls.append('activation'),
                      write_evidence=lambda name, value: calls.append(name),
                      controller=lambda *a, **k: calls.append('child'),
                      read_secret=lambda: calls.append('secret') or {},
                      candidate_identity=lambda: calls.append('identity') or {},
                      validate=lambda *a: calls.append('validate'))
    assert calls == []


def test_launch_orders_qualification_rotation_secret_and_one_child(tmp_path):
    config, record = write(tmp_path)
    calls = []
    barrier = {'state': 'ARMED', 'run_id': 'btc5m-unattended-live-011',
               'risk_window_id': 'btc5m-unattended-24h-20261122-011',
               'authorization_ref': 'pascal-unattended-24h-20261122-011',
               'candidate_digest': 'a' * 64}
    args = arguments(config, record)
    result = launch.launch(
        args, now=NOW,
        fetch_account=lambda: calls.append('account') or {
            'venue': 'PREDICT_FUN', 'chain_id': 56, 'account_mode': 'predict-account',
            'trading_address': '0x' + '1' * 40, 'signer_address': '0x' + '2' * 40},
        read_barrier=lambda address: calls.append('read') or barrier,
        rotate_barrier=lambda record: calls.append('rotate'),
        write_activation=lambda activation: calls.append('activation'),
        write_evidence=lambda name, value: calls.append(name),
        controller=lambda command, environment: calls.append('child') or {'launcher_exit': 0},
        read_secret=lambda: calls.append('secret') or {'PATH': '/usr/bin:/bin'},
        candidate_identity=lambda: calls.append('identity') or {'binary_sha256': 'b' * 64},
        validate=lambda activation, context, at: calls.append('validate'))
    assert calls.index('validate') < calls.index('activation') < calls.index('rotate')
    assert calls.index('rotate') < calls.index('secret') < calls.index('child')
    assert calls.count('child') == 1
    assert result['windows']['deadline_at'] == '2026-11-23T12:00:00.000Z'
    assert launch.identity_windows(args, NOW)['operations_mode'] == 'unattended'


def test_launch_refuses_an_unexpected_barrier(tmp_path):
    config, record = write(tmp_path)
    args = arguments(config, record)
    with pytest.raises(launch.Refused) as refused:
        launch.launch(
            args, now=NOW,
            fetch_account=lambda: {'account_mode': 'predict-account', 'trading_address': '0x1',
                                   'signer_address': '0x2'},
            read_barrier=lambda address: {'state': 'ARMED', 'run_id': 'someone-else'},
            rotate_barrier=lambda record: pytest.fail('rotated'),
            write_activation=lambda activation: pytest.fail('wrote activation'),
            write_evidence=lambda name, value: pytest.fail('wrote evidence'),
            controller=lambda *a, **k: pytest.fail('dispatched'),
            read_secret=lambda: pytest.fail('read secret'),
            candidate_identity=lambda: {'binary_sha256': 'b' * 64},
            validate=lambda *a: pytest.fail('validated'))
    assert refused.value.reason == 'UNEXPECTED_BARRIER'
