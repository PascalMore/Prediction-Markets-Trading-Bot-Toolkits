"""Bounded offline takeover: frozen audit, real CLI, fake external boundaries."""
import copy
import datetime as dt
import json
from pathlib import Path

import pytest

from test_btc5m_launch import native_main, unfilled_stop, UNFILLED_ORDER
import btc5m_control as control
import btc5m_launch as launch
import btc5m_live_events as live
import btc5m_live_guard as guard


C0 = 95488500000000000000


@pytest.fixture
def takeover(native_main, unfilled_stop, monkeypatch):
    env = native_main
    saved = unfilled_stop
    env.previous = saved.previous
    env.fresh = saved.fresh
    env.old.update(saved.old)
    env.collection.record = copy.deepcopy(env.old)
    activation = json.loads(env.activation.read_text())
    for key in ('authorization_ref', 'risk_window_id', 'candidate_digest'):
        activation[key] = env.old[key]
    env.activation.write_text(json.dumps(activation))
    env.old_bytes = env.activation.read_bytes()
    env.seed_path = (env.previous.parent / 'launch-attempts' / env.old['run_id'] /
                     'attempt-reserved.json')
    env.orders = list(env.fresh['reconciliation']['declared_orders'])
    env.reads = []
    env.controllers = []
    env.audit_checked = []
    env.native_exit = [0, 0]
    env.projections = [copy.deepcopy(env.fresh), copy.deepcopy(env.fresh)]
    env.audit_bytes = {path: path.read_bytes() for path in env.previous.iterdir()}
    env.argv = ['--config', str(env.config), '--auto-prepare',
                '--run-id', 'btc5m-unattended-live-012',
                '--authorization-ref', 'offline-new-authorization',
                '--evidence-root', str(env.root)]
    original_check = live.check_ended

    def check(*args, **kwargs):
        result = original_check(*args, **kwargs)
        env.audit_checked.append(True)
        return result

    def reconcile(config, rpc_url, orders, output, *, environment):
        index = len(env.reads)
        assert index < 2
        data = json.loads(Path(config).read_text())
        assert control.parameters(data)['budget'] == C0
        assert orders == env.orders and len(orders) == 5
        assert orders[-1] == UNFILLED_ORDER
        env.reads.append((data, list(orders)))
        env.calls.append(f'read-{index + 1}')
        value = copy.deepcopy(env.projections[index])
        value['observation']['finished_at'] = dt.datetime.now(dt.timezone.utc).isoformat()
        Path(output).write_text(json.dumps(value))
        return {'exit_code': env.native_exit[index], 'projection': value}

    def controller(argv, *, environment, evidence_root):
        assert env.collection.record is None
        current = json.loads(env.activation.read_text())
        assert current['authorization_ref'] == 'offline-new-authorization'
        orders = [argv[index + 1] for index, name in enumerate(argv)
                  if name == '--predict-reconciled-order']
        assert orders == env.orders
        assert control.parameters(json.loads(Path(argv[argv.index('--config') + 1]).read_text()))[
            'budget'] == C0
        env.calls.append('controller')
        env.controllers.append(argv)
        return {'launcher_exit': 0}

    original_replace = launch.os.replace

    def replace(source, target):
        assert Path(target) == env.activation
        env.calls.append('activation')
        return original_replace(source, target)

    monkeypatch.setattr(live, 'check_ended', check)
    monkeypatch.setattr(control.local_signer, 'reconcile_account', reconcile)
    monkeypatch.setattr(control, 'run', controller)
    monkeypatch.setattr(launch.os, 'replace', replace)
    return env


def unchanged_audit(env):
    assert all(path.read_bytes() == raw for path, raw in env.audit_bytes.items())


def test_frozen_main_two_complete_anchored_reads_then_exact_rotation(takeover):
    env = takeover
    assert launch.main(env.argv) == 0
    assert env.audit_checked == [True]
    assert env.calls == ['read-1', 'read-2', 'activation', 'delete', 'controller']
    assert len(env.controllers) == 1
    attempt = env.root / 'launch-attempts/btc5m-unattended-live-012'
    reserved = json.loads((attempt / 'attempt-reserved.json').read_text())
    assert reserved['committed_budget_atoms'] == C0
    assert reserved['declared_orders'] == env.orders
    assert (attempt / 'superseded-activation.json').read_bytes() == env.old_bytes
    assert all(read[0]['controlled_run']['budget_usdt'] == '95.4885' for read in env.reads)
    unchanged_audit(env)


@pytest.mark.parametrize('damage', [
    'first_exit', 'first_incomplete', 'first_unexplained', 'second_exit',
    'cash_plus', 'cash_minus', 'second_cash_plus', 'claim_drift', 'gas_drift',
    'first_identity', 'second_identity', 'first_history', 'second_history',
    'first_pagination', 'second_pagination', 'first_open', 'second_position',
    'stale', 'seed_missing', 'seed_mismatch', 'seed_null', 'declaration',
    'old_identity', 'audit', 'guard_drift', 'lock',
])
def test_frozen_main_refuses_before_business_writes(takeover, monkeypatch, capsys, damage):
    env = takeover
    first, second = env.projections
    expected_reason = None
    audit_expected = damage not in ('old_identity', 'audit', 'lock', 'seed_mismatch')
    if damage == 'first_exit':
        env.native_exit[0] = 4
        expected_reason = 'RECONCILIATION_NOT_COMPLETE'
    elif damage == 'second_exit':
        env.native_exit[1] = 4
        expected_reason = 'RECONCILIATION_NOT_COMPLETE'
    elif damage == 'first_incomplete':
        first['reconciliation']['complete'] = False
        expected_reason = 'RECONCILIATION_NOT_COMPLETE'
    elif damage == 'first_unexplained':
        first['account']['positions_explained'] = False
        expected_reason = 'RECONCILIATION_NOT_COMPLETE'
    elif damage in ('cash_plus', 'cash_minus', 'second_cash_plus'):
        value = second if damage == 'second_cash_plus' else first
        value['account']['collateral_atoms'] = str(
            int(value['account']['collateral_atoms']) + (-1 if damage == 'cash_minus' else 1))
        expected_reason = ('RECONCILIATION_DRIFT' if damage == 'second_cash_plus'
                           else 'CASH_BUDGET_MISMATCH')
    elif damage == 'claim_drift':
        second['account']['claimed_win'][0]['claim_atoms'] = str(
            int(second['account']['claimed_win'][0]['claim_atoms']) + 1)
        expected_reason = 'RECONCILIATION_DRIFT'
    elif damage == 'gas_drift':
        second['account']['gas_atoms'] = str(int(second['account']['gas_atoms']) + 1)
        expected_reason = 'RECONCILIATION_DRIFT'
    elif damage.endswith('_identity') and damage != 'old_identity':
        value = first if damage.startswith('first') else second
        value['account_identity']['trading_address'] = '0x' + '1' * 40
        expected_reason = 'RECONCILIATION_FACTS'
    elif damage.endswith('_history'):
        value = first if damage.startswith('first') else second
        value['reconciliation']['declared_orders'] = env.orders[:-1]
        expected_reason = 'RECONCILIATION_FACTS'
    elif damage.endswith('_pagination'):
        value = first if damage.startswith('first') else second
        value['account']['pagination_complete'] = False
        expected_reason = 'RECONCILIATION_FACTS'
    elif damage == 'first_open':
        first['account']['open_orders'] = [UNFILLED_ORDER]
        expected_reason = 'RECONCILIATION_FACTS'
    elif damage == 'second_position':
        second['account']['positions'] = [{'token': '1', 'shares_atoms': '1'}]
        expected_reason = 'RECONCILIATION_FACTS'
    elif damage == 'stale':
        original = launch._cash_snapshot

        def stale(value, *args, **kwargs):
            value['observation']['finished_at'] = '2020-01-01T00:00:00Z'
            return original(value, *args, **kwargs)

        monkeypatch.setattr(launch, '_cash_snapshot', stale)
        expected_reason = 'STALE_RECONCILIATION'
    elif damage.startswith('seed_'):
        seed = json.loads(env.seed_path.read_text())
        if damage == 'seed_missing':
            seed.pop('committed_budget_atoms')
        else:
            seed['committed_budget_atoms'] = None if damage == 'seed_null' else C0 + 1
        env.seed_path.write_text(json.dumps(seed))
        expected_reason = 'HISTORY_REQUIRES_TAKEOVER'
    elif damage == 'declaration':
        env.argv += ['--declared-order', env.orders[0]]
        expected_reason = 'DECLARED_ORDERS_MISMATCH'
    elif damage == 'old_identity':
        env.argv += ['--old-run', 'wrong-old-run']
        expected_reason = 'UNEXPECTED_BARRIER'
    elif damage == 'audit':
        path = env.previous / 'ended.json'
        value = json.loads(path.read_text())
        value['audit_complete'] = False
        path.write_text(json.dumps(value))
        env.audit_bytes[path] = path.read_bytes()
        expected_reason = 'HISTORY_IDENTITY'
    elif damage == 'guard_drift':
        find = env.collection.find_one
        reads = []

        def drift(query, *args):
            reads.append(query)
            if len(reads) == 2:
                env.collection.record['scope']['host'] = 'concurrent-host'
            return find(query, *args)

        monkeypatch.setattr(env.collection, 'find_one', drift)
        expected_reason = 'BARRIER_DRIFT'
    else:
        lock = guard.AccountLock(env.account, control.MODE_SIGNED_FUTURE_LIVE).acquire()
    try:
        assert launch.main(env.argv) == 1
    finally:
        if damage == 'lock':
            lock.close()
    result = json.loads(capsys.readouterr().out)
    assert result['status'] == 'REFUSED_NO_RETRY'
    if expected_reason:
        assert result['reason'] == expected_reason
    assert bool(env.audit_checked) == audit_expected
    assert env.activation.read_bytes() == env.old_bytes
    assert 'activation' not in env.calls and 'delete' not in env.calls
    assert env.controllers == [] and env.children == []
    if damage != 'guard_drift':
        assert env.collection.record == env.old
    unchanged_audit(env)


@pytest.mark.parametrize('budget', [C0, str(C0)])
def test_seed_atoms_int_and_text_have_identical_anchor(unfilled_stop, budget):
    env = unfilled_stop
    seed_path = env.previous.parent / 'launch-attempts' / env.old['run_id'] / 'attempt-reserved.json'
    value = json.loads(seed_path.read_text())
    value['committed_budget_atoms'] = budget
    seed_path.write_text(json.dumps(value))
    assert launch._stopped_window(env.old, env.account)['budget_atoms'] == C0


@pytest.mark.parametrize('budget', [True, 1.5, {}, [], '1.0', '-1', '٣', '²', '0', 0])
def test_bad_seed_atoms_are_clean_refusals(unfilled_stop, budget):
    env = unfilled_stop
    path = env.previous.parent / 'launch-attempts' / env.old['run_id'] / 'attempt-reserved.json'
    seed = json.loads(path.read_text())
    seed['committed_budget_atoms'] = budget
    path.write_text(json.dumps(seed))
    with pytest.raises(launch.Refused, match='HISTORY_REQUIRES_TAKEOVER'):
        launch._stopped_window(env.old, env.account)


@pytest.mark.parametrize('budget', [C0, str(C0)])
def test_fresh_helper_accepts_atoms_text_or_int_without_mutation(unfilled_stop, budget):
    env = unfilled_stop
    window = launch._stopped_window(env.old, env.account)
    window['budget_atoms'] = budget
    before = copy.deepcopy(window)
    assert launch._fresh_read_closes_window(window, env.fresh)
    assert window == before


@pytest.mark.parametrize('budget', [None, True, {}, [], '1.0', '-1', '٣', '²', '0', 0])
def test_fresh_helper_bad_budget_is_false_not_exception(unfilled_stop, budget):
    env = unfilled_stop
    window = launch._stopped_window(env.old, env.account)
    window['budget_atoms'] = budget
    assert not launch._fresh_read_closes_window(window, env.fresh)


@pytest.mark.parametrize('budget', ['٣', '²', '1.٣', '+1', '1e2', '1.', '', '0'])
def test_ascii_public_budget_refuses_without_decimal_crash(budget):
    with pytest.raises(control.Refused):
        control._budget_atoms(budget)


@pytest.fixture
def audited_relations(unfilled_stop):
    """Replay the actual frozen audit before testing the qualification boundary."""
    env = unfilled_stop
    started = json.loads((env.previous / 'started.json').read_text())
    public = json.loads((env.previous / 'public-config.json').read_text())
    context = json.loads((env.previous / 'live-context.json').read_text())
    live.check_metadata(started, public, context)
    relations = live.LiveRelations(started['run_id'], started['risk_window_id'],
                                   started['public_config_identity'])
    for line in (env.previous / 'events.jsonl').read_bytes().splitlines(keepends=True):
        relations = relations.stage(live.validate_live_event(live.EVENT_PREFIX + line,
                                                          started['public_config_identity']))
    ended = json.loads((env.previous / 'ended.json').read_text())
    live.check_ended(ended, started, relations)
    return relations, copy.deepcopy(relations.final['payload']['unresolved'][0]), ended['final_accounting']


@pytest.mark.parametrize('damage', ['attempt', 'hash', 'venue', 'unknown', 'reserved',
                                   'remaining', 'another_open'])
def test_exact_qualification_after_successful_audit(audited_relations, damage):
    relations, entry, accounting = audited_relations
    launch._check_incomplete_order(relations, entry, accounting)
    attempt = relations.attempts[entry['attempt_id']]
    assert attempt['venue'] == '8882120994'
    if damage == 'attempt':
        entry['attempt_id'] = 'unrelated-attempt'
    elif damage == 'hash':
        entry['signed_hash'] = '0x' + 'f' * 64
    elif damage == 'venue':
        attempt['venue'] = None
    elif damage == 'unknown':
        attempt['submission'] = 'UNKNOWN'
    elif damage == 'reserved':
        entry['reserved_atoms'] = str(int(entry['reserved_atoms']) + 1)
    elif damage == 'remaining':
        attempt['remaining'] -= 1
    else:
        relations.attempts['another-open'] = copy.deepcopy(attempt)
    with pytest.raises(launch.Refused, match='HISTORY_REQUIRES_TAKEOVER'):
        launch._check_incomplete_order(relations, entry, accounting)


def test_multiple_audited_closed_accepted_attempts_do_not_disqualify_one_open(audited_relations):
    """Qualification unit test combines indices only after both audits validate."""
    from test_btc5m_live_events import native_stream, metadata
    relations, entry, accounting = audited_relations
    frames = native_stream(1664, 1690)
    started, _, _ = metadata(frames[0])
    closed_relations = live.LiveRelations(started['run_id'], started['risk_window_id'])
    for frame in frames:
        closed_relations = closed_relations.stage(live.validate_live_event(frame))
    closed = {key: value for key, value in closed_relations.attempts.items()
              if value['submission'] == 'ACCEPTED' and closed_relations._closed(value)}
    assert len(closed) >= 2
    # This is an index-level qualification test, not a forged combined wire audit.
    relations.attempts.update({'closed-' + key: value for key, value in closed.items()})
    launch._check_incomplete_order(relations, entry, accounting)


@pytest.mark.parametrize('damage', ['attempt', 'hash', 'unknown', 'venue'])
def test_main_audit_relation_faults_never_read_account(takeover, capsys, damage):
    env = takeover
    path = env.previous / 'events.jsonl'
    events = [json.loads(line) for line in path.read_text().splitlines()]
    if damage in ('attempt', 'hash'):
        # These deliberately test LiveRelations, not the later qualification gate.
        events[-1]['payload']['unresolved'][0][
            'attempt_id' if damage == 'attempt' else 'signed_hash'] = (
                'another-attempt' if damage == 'attempt' else '0x' + 'f' * 64)
    else:
        for event in events:
            if event['event_kind'] in ('order', 'execution'):
                payload = event['payload']
                if damage == 'venue':
                    payload['venue_order_id'] = None
                elif event['event_kind'] == 'order' and payload['submission'] == 'ACCEPTED':
                    payload['submission'] = 'UNKNOWN'
    path.write_bytes(b''.join(live.canonical(event) + b'\n' for event in events))
    env.audit_bytes[path] = path.read_bytes()
    assert launch.main(env.argv) == 1
    result = json.loads(capsys.readouterr().out)
    if damage in ('unknown', 'venue'):
        # Otherwise-valid audit reaches the exact qualification gate.
        assert env.audit_checked == [True]
        assert result['reason'] == 'HISTORY_REQUIRES_TAKEOVER'
    else:
        assert env.audit_checked == []
    assert env.reads == [] and env.controllers == [] and env.calls == []
    assert env.activation.read_bytes() == env.old_bytes
    assert env.collection.record == env.old
    unchanged_audit(env)


def test_public_identity_binds_final_takeover_sources_and_regression():
    """Exercise the existing identity consumer, not the omitted digest probe."""
    import hashlib
    identity = control.public_identity({}, live=True)
    assert identity['binary_sha256'] == control.LIVE_BINARY_SHA256
    for name in ('tools/predictfun/btc5m_launch.py', 'tools/predictfun/btc5m_control.py',
                 'tools/predictfun/tests/test_btc5m_takeover.py',
                 'tools/predictfun/tests/test_btc5m_launch.py',
                 'tools/predictfun/tests/test_btc5m_control.py'):
        assert identity['files'][name] == hashlib.sha256((control.BOT / name).read_bytes()).hexdigest()
