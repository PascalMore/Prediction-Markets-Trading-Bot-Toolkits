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


UNSENT_RUN = ('2026-11-25-btc5m-controlled-paper/evidence/'
               'btc5m-live-7d3b59a09d384b8e')
UNSENT_C0 = 94488600000000000000
UNSENT_ORDER = '0xb397375bcf727a357f0635b40ba661ed1e0ddea1614d36ab076c3749448d559f'


@pytest.fixture
def unsent_edge(native_main, monkeypatch):
    """The real stopped window: one edge stop that was proven never sent.

    The audit, the launch seed and the native read are the frozen production
    evidence, copied into isolation. Only the external read seam is
    synthetic, and it replays the window's own saved native pair.
    """
    env = native_main
    real_root = control.BOT.parents[4]
    original = real_root / 'data/reports' / UNSENT_RUN
    env.previous = env.root / original.name
    env.previous.mkdir(parents=True)
    env.audit_bytes = {}
    for name in ('started.json', 'public-config.json', 'live-context.json',
                 'ended.json', 'events.jsonl'):
        path = env.previous / name
        path.write_bytes((original / name).read_bytes())
        env.audit_bytes[path] = path.read_bytes()
    started = json.loads((env.previous / 'started.json').read_text())
    env.old.update({key: started[key] for key in ('run_id', 'risk_window_id',
                                                  'authorization_ref')})
    env.old['candidate_digest'] = live.candidate_digest(started)
    env.collection.record = copy.deepcopy(env.old)
    activation = json.loads(env.activation.read_text())
    activation.update({key: env.old[key] for key in ('risk_window_id',
                                                      'authorization_ref',
                                                      'candidate_digest')})
    env.activation.write_text(json.dumps(activation))
    env.old_bytes = env.activation.read_bytes()
    seed_dir = env.root / 'launch-attempts' / original.name
    seed_dir.mkdir(parents=True)
    for name in ('attempt-reserved.json', 'reconciliation.json'):
        (seed_dir / name).write_bytes(
            (original.parent / 'launch-attempts' / original.name / name).read_bytes())
        env.audit_bytes[seed_dir / name] = (seed_dir / name).read_bytes()
    env.seed_path = seed_dir / 'attempt-reserved.json'
    saved = original.parent / 'launch-attempts' / original.name
    env.projections = [json.loads((saved / f'reconciliation-pass-{n}.json').read_text())
                       for n in (1, 2)]
    env.orders = list(env.projections[0]['reconciliation']['declared_orders'])
    assert len(env.orders) == 7 and UNSENT_ORDER not in env.orders
    env.native_exit = [0, 0]
    env.reads = []
    env.refresh_clock = True
    env.argv = ['--config', str(env.config), '--auto-prepare',
                '--run-id', 'btc5m-unattended-live-012',
                '--authorization-ref', 'offline-unsent-edge-recovery',
                '--evidence-root', str(env.root)]

    def reconcile(config, rpc_url, orders, output, *, environment):
        index = len(env.reads)
        assert index < 2
        data = control.read_public_config(Path(config))
        assert control.parameters(data)['budget'] == UNSENT_C0
        assert orders == env.orders and len(orders) == 7
        env.reads.append(copy.deepcopy(data))
        env.calls.append(f'read-{index + 1}')
        value = copy.deepcopy(env.projections[index])
        if env.refresh_clock:
            value['observation']['finished_at'] = dt.datetime.now(dt.timezone.utc).isoformat()
        Path(output).write_text(json.dumps(value))
        return {'exit_code': env.native_exit[index], 'projection': value}

    monkeypatch.setattr(control.local_signer, 'reconcile_account', reconcile)
    return env


def test_unsent_edge_recovers_with_two_reads_then_one_rotation(unsent_edge):
    """GREEN: the proven-unsent window opens a new window at the same C0."""
    env = unsent_edge
    assert launch.main(env.argv) == 0
    assert env.calls == ['read-1', 'read-2', 'delete', 'arm', 'child']
    assert len(env.children) == 1
    command = env.children[0][0]
    orders = [command[index + 1] for index, name in enumerate(command)
              if name == '--predict-reconciled-order']
    # The window's own signed hash was proven never sent: it is not venue
    # history and must not be carried into the new window.
    assert orders == env.orders and UNSENT_ORDER not in orders
    assert all(read['controlled_run']['budget_usdt'] == '94.4886' for read in env.reads)
    attempt = env.root / 'launch-attempts/btc5m-unattended-live-012'
    seed = json.loads((attempt / 'attempt-reserved.json').read_text())
    assert seed['committed_budget_atoms'] == UNSENT_C0
    assert seed['declared_orders'] == env.orders
    proof = json.loads((attempt / 'unsent-edge-recovery.json').read_text())
    assert proof['old_run_id'] == env.old['run_id']
    assert proof['prior_budget_atoms'] == str(UNSENT_C0)
    assert proof['next_budget_atoms'] == str(UNSENT_C0)
    assert proof['net_loss_atoms'] == '0'
    assert proof['unsent_hashes'] == [UNSENT_ORDER]
    assert len(proof['fresh_read_sha256']) == 2
    unchanged_audit(env)


def test_unsent_edge_classifier_rejects_every_other_stop(unsent_edge):
    """The branch is narrow: every other stop keeps its takeover refusal."""
    env = unsent_edge
    parsed = live.read_live_run(env.previous)
    final = copy.deepcopy(parsed['events'][-1]['payload'])
    ended = copy.deepcopy(parsed['ended'])
    accounting = ended['final_accounting']
    exits = ended['exit_facts']
    orders = [event['payload'] for event in parsed['events']
              if event['event_kind'] == 'order']
    kinds = {event['event_kind'] for event in parsed['events']}
    assert launch._unsent_edge_kind(ended, final, accounting, exits, orders, kinds) == 'UNSENT_EDGE'
    for damage in ('loss_latch', 'audit', 'unresolved', 'reserved', 'cost',
                   'stop_reason', 'stop_detail', 'run_status', 'forced_cleanup',
                   'child_exit', 'launcher_exit', 'child_signal', 'may_have_submitted',
                   'submitted', 'unknown_submission', 'other_detail', 'other_stage',
                   'execution_event', 'payout_event', 'no_order'):
        value = copy.deepcopy(final)
        book = copy.deepcopy(ended)
        fact = copy.deepcopy(accounting)
        wire = copy.deepcopy(orders)
        present = set(kinds)
        if damage == 'loss_latch':
            fact['loss_latched'] = True
        elif damage == 'audit':
            value['audit_complete'] = False
        elif damage == 'unresolved':
            value['unresolved'] = [{'closure': 'UNPROVEN'}]
        elif damage == 'reserved':
            fact['reserved'] = '1'
        elif damage == 'cost':
            fact['cost'] = '1'
        elif damage == 'stop_reason':
            value['stop_reason'] = 'OUTPUT_INVALID'
        elif damage == 'stop_detail':
            value['stop_detail'] = 'CASH_CONFLICT'
        elif damage == 'run_status':
            value['run_status'] = 'INCOMPLETE'
        elif damage == 'forced_cleanup':
            book['exit_facts']['forced_cleanup'] = True
        elif damage == 'child_exit':
            book['exit_facts']['child_returncode'] = 1
        elif damage == 'launcher_exit':
            book['exit_facts']['launcher_exit'] = 1
        elif damage == 'child_signal':
            book['exit_facts']['child_signal'] = 9
        elif damage == 'may_have_submitted':
            wire[0]['attempt_phase'] = 'MAY_HAVE_SUBMITTED'
        elif damage == 'submitted':
            wire[0]['attempt_phase'] = 'SUBMITTED'
        elif damage == 'unknown_submission':
            wire[0]['submission'] = 'UNKNOWN'
        elif damage == 'other_detail':
            wire[0]['diagnostic']['detail'] = 'BOOK_STALE'
        elif damage == 'other_stage':
            wire[0]['diagnostic']['stage'] = 'POST'
        elif damage == 'execution_event':
            present = present | {'execution'}
        elif damage == 'payout_event':
            present = present | {'payout'}
        else:
            wire = []
        assert launch._unsent_edge_kind(book, value, fact, book['exit_facts'],
                                        wire, present) is None, damage


@pytest.mark.parametrize('damage', [
    'fake_unresolved', 'may_submit', 'unknown', 'accepted', 'venue_id',
    'execution', 'other_diagnostic', 'other_reason', 'loss_latch', 'reserved',
    'cost', 'stop_detail', 'seed_missing',
    'seed_budget', 'seed_native_hash', 'hash_collision', 'first_exit', 'second_exit',
    'first_partial', 'second_partial', 'cash_plus', 'cash_minus', 'identity',
    'history', 'pagination', 'open_order', 'second_position', 'stale',
    'first_claim_removed', 'claim_removed', 'zero_removed', 'guard_drift',
])
def test_unsent_edge_counterexamples_refuse_without_dispatch(unsent_edge, monkeypatch, damage):
    """Every counterexample refuses before any durable write or child."""
    env = unsent_edge
    first, second = env.projections
    path = env.previous / 'events.jsonl'
    events = [json.loads(line) for line in path.read_text().splitlines()]
    if damage == 'fake_unresolved':
        event = next(e for e in reversed(events) if e['event_kind'] == 'final')
        event['payload']['unresolved'] = [{'attempt_id': '1', 'closure': 'UNPROVEN',
                                           'reason': 'FACT_INCOMPLETE',
                                           'reserved_atoms': '0',
                                           'signed_hash': UNSENT_ORDER}]
    elif damage == 'may_submit':
        event = next(e for e in events if e['event_kind'] == 'order')
        event['payload']['attempt_phase'] = 'MAY_HAVE_SUBMITTED'
    elif damage == 'unknown':
        event = next(e for e in events if e['event_kind'] == 'order')
        event['payload']['submission'] = 'UNKNOWN'
    elif damage == 'accepted':
        event = next(e for e in events if e['event_kind'] == 'order')
        event['payload']['submission'] = 'ACCEPTED'
    elif damage == 'venue_id':
        event = next(e for e in events if e['event_kind'] == 'order')
        event['payload']['venue_order_id'] = '8994654499'
    elif damage == 'execution':
        event = next(e for e in events if e['event_kind'] == 'order')
        event['payload']['submission'] = 'ACCEPTED'
        event['payload']['venue_order_id'] = '8994654499'
    elif damage == 'other_diagnostic':
        event = next(e for e in events if e['event_kind'] == 'order')
        event['payload']['diagnostic']['detail'] = 'BOOK_STALE'
    elif damage == 'other_reason':
        event = next(e for e in events if e['event_kind'] == 'order')
        event['payload']['diagnostic']['stage'] = 'POST'
    elif damage == 'loss_latch':
        ended = json.loads((env.previous / 'ended.json').read_text())
        ended['final_accounting']['loss_latched'] = True
        (env.previous / 'ended.json').write_text(json.dumps(ended))
    elif damage == 'reserved':
        ended = json.loads((env.previous / 'ended.json').read_text())
        ended['final_accounting']['reserved'] = '1'
        (env.previous / 'ended.json').write_text(json.dumps(ended))
    elif damage == 'cost':
        ended = json.loads((env.previous / 'ended.json').read_text())
        ended['final_accounting']['cost'] = '1'
        (env.previous / 'ended.json').write_text(json.dumps(ended))
    elif damage == 'stop_detail':
        ended = json.loads((env.previous / 'ended.json').read_text())
        ended['incomplete_detail'] = 'CASH_CONFLICT'
        (env.previous / 'ended.json').write_text(json.dumps(ended))
    elif damage == 'seed_missing':
        env.seed_path.unlink()
    elif damage == 'seed_budget':
        seed = json.loads(env.seed_path.read_text())
        seed['committed_budget_atoms'] = UNSENT_C0 + 1
        env.seed_path.write_text(json.dumps(seed))
    elif damage == 'seed_native_hash':
        seed = json.loads(env.seed_path.read_text())
        seed['reconciliation_sha256'] = 'f' * 64
        env.seed_path.write_text(json.dumps(seed))
    elif damage == 'hash_collision':
        # The window's own signed hash must never enter the venue history.
        seed = json.loads(env.seed_path.read_text())
        seed['declared_orders'] = list(seed['declared_orders']) + [UNSENT_ORDER]
        env.seed_path.write_text(json.dumps(seed))
    elif damage == 'first_exit':
        env.native_exit[0] = 4
    elif damage == 'second_exit':
        env.native_exit[1] = 4
    elif damage == 'first_partial':
        first['reconciliation']['complete'] = False
    elif damage == 'second_partial':
        second['reconciliation']['complete'] = False
    elif damage == 'cash_plus':
        first['account']['collateral_atoms'] = str(
            int(first['account']['collateral_atoms']) + 1)
    elif damage == 'cash_minus':
        first['account']['collateral_atoms'] = str(
            int(first['account']['collateral_atoms']) - 1)
    elif damage == 'identity':
        first['account_identity']['trading_address'] = '0x' + '1' * 40
    elif damage == 'history':
        first['reconciliation']['declared_orders'] = env.orders[:-1]
    elif damage == 'pagination':
        first['account']['pagination_complete'] = False
    elif damage == 'open_order':
        first['account']['open_orders'] = [env.orders[-1]]
    elif damage == 'second_position':
        second['account']['positions'] = [{'token': '1', 'shares_atoms': '1'}]
    elif damage == 'stale':
        env.refresh_clock = False
    elif damage == 'first_claim_removed':
        first['account']['claimed_win'].pop()
    elif damage == 'claim_removed':
        second['account']['claimed_win'].pop()
    elif damage == 'zero_removed':
        second['account']['settled_zero'].pop()
    elif damage == 'guard_drift':
        original = env.collection.find_one
        calls = []

        def drift(query, *args):
            calls.append(query)
            if len(calls) == 2:
                env.collection.record['scope']['host'] = 'concurrent-host'
            return original(query, *args)

        monkeypatch.setattr(env.collection, 'find_one', drift)
    if damage in ('fake_unresolved', 'may_submit', 'unknown', 'accepted',
                  'venue_id', 'execution', 'other_diagnostic', 'other_reason'):
        path.write_bytes(b''.join(live.canonical(event) + b'\n' for event in events))
    before = env.activation.read_bytes()
    assert launch.main(env.argv) == 1
    assert env.children == []
    if damage != 'guard_drift':
        assert env.collection.record == env.old
    assert env.activation.read_bytes() == before
    # No durable write and no child in any counterexample.
    assert 'delete' not in env.calls and 'arm' not in env.calls
    if damage in ('first_exit', 'second_exit', 'first_partial', 'second_partial',
                  'cash_plus', 'cash_minus', 'identity', 'history', 'pagination',
                  'open_order', 'second_position', 'stale', 'first_claim_removed',
                  'claim_removed', 'zero_removed', 'guard_drift'):
        # These are found by the two fresh reads, before any durable write.
        assert env.calls.count('read-1') == 1
    else:
        assert env.reads == []


@pytest.fixture
def filled_loss(native_main, monkeypatch):
    """Unmodified real audit; fresh-reader responses are explicitly synthetic.

    The saved poststop read has exit4 at old C0. Only this external read seam
    supplies complete flags for the independently anchored new C0 and current
    test receipt time. The saved source bytes and their flags are never changed.
    """
    env = native_main
    real_root = control.BOT.parents[4]
    original = real_root / 'data/reports/2026-11-25-btc5m-controlled-paper/evidence/btc5m-live-9c6c5e3a346748d4'
    env.previous = env.root / original.name
    env.previous.mkdir(parents=True)
    env.audit_bytes = {}
    for name in ('started.json', 'public-config.json', 'live-context.json', 'ended.json', 'events.jsonl'):
        path = env.previous / name
        path.write_bytes((original / name).read_bytes())
        env.audit_bytes[path] = path.read_bytes()
    started = json.loads((env.previous / 'started.json').read_text())
    env.old.update({key: started[key] for key in ('run_id', 'risk_window_id', 'authorization_ref')})
    env.old['candidate_digest'] = live.candidate_digest(started)
    env.collection.record = copy.deepcopy(env.old)
    activation = json.loads(env.activation.read_text())
    activation.update({key: env.old[key] for key in ('risk_window_id', 'authorization_ref', 'candidate_digest')})
    env.activation.write_text(json.dumps(activation))
    env.old_bytes = env.activation.read_bytes()
    seed_dir = env.root / 'launch-attempts' / original.name
    seed_dir.mkdir(parents=True)
    for name in ('attempt-reserved.json', 'reconciliation.json'):
        (seed_dir / name).write_bytes((original.parent / 'launch-attempts' / original.name / name).read_bytes())
        env.audit_bytes[seed_dir / name] = (seed_dir / name).read_bytes()
    env.seed_path = seed_dir / 'attempt-reserved.json'
    saved = real_root / 'data/reports/2026-11-25-btc5m-inventory-mismatch/evidence/poststop-20261124T222348Z/reconciliation-pass-1.json'
    env.source_projection = json.loads(saved.read_text())
    assert env.source_projection['reconciliation']['complete'] is False
    env.projections = [copy.deepcopy(env.source_projection), copy.deepcopy(env.source_projection)]
    for value in env.projections:
        value['account']['positions_explained'] = True
        value['reconciliation']['complete'] = True
        value['reconciliation']['positions_match_history'] = True
    env.orders = list(env.source_projection['reconciliation']['declared_orders'])
    env.native_exit = [0, 0]
    env.reads = []
    env.refresh_clock = True
    env.argv = ['--config', str(env.config), '--auto-prepare',
                '--run-id', 'btc5m-unattended-live-012',
                '--authorization-ref', 'offline-settled-loss-recovery',
                '--evidence-root', str(env.root)]

    def reconcile(config, rpc_url, orders, output, *, environment):
        index = len(env.reads)
        assert index < 2
        data = control.read_public_config(Path(config))
        assert control.parameters(data)['budget'] == 94488600000000000000
        assert orders == env.orders and len(orders) == 7
        env.reads.append(copy.deepcopy(data))
        env.calls.append(f'read-{index + 1}')
        value = copy.deepcopy(env.projections[index])
        if env.refresh_clock:
            value['observation']['finished_at'] = dt.datetime.now(dt.timezone.utc).isoformat()
        Path(output).write_text(json.dumps(value))
        return {'exit_code': env.native_exit[index], 'projection': value}

    monkeypatch.setattr(control.local_signer, 'reconcile_account', reconcile)
    return env


def test_filled_loss_original_main_proves_two_reads_before_one_real_controller_dispatch(filled_loss):
    env = filled_loss
    assert launch.main(env.argv) == 0
    assert env.calls == ['read-1', 'read-2', 'delete', 'arm', 'child']
    assert len(env.children) == 1
    command = env.children[0][0]
    assert command[0] == str(control.LIVE_BINARY)
    orders = [command[index + 1] for index, name in enumerate(command)
              if name == '--predict-reconciled-order']
    assert orders == env.orders
    attempt = env.root / 'launch-attempts/btc5m-unattended-live-012'
    seed = json.loads((attempt / 'attempt-reserved.json').read_text())
    assert seed['committed_budget_atoms'] == 94488600000000000000
    assert seed['declared_orders'] == env.orders
    assert (attempt / 'superseded-activation.json').read_bytes() == env.old_bytes
    proof = json.loads((attempt / 'settled-loss-recovery.json').read_text())
    assert proof['old_run_id'] == env.old['run_id']
    assert proof['net_loss_atoms'] == '999900000000000000'
    assert proof['prior_budget_atoms'] == '95488500000000000000'
    assert proof['next_budget_atoms'] == '94488600000000000000'
    unchanged_audit(env)



@pytest.mark.parametrize('damage', [
    'first_exit4', 'second_exit4', 'first_partial', 'second_partial',
    'cash_plus', 'cash_minus', 'second_cash', 'identity', 'history', 'pagination',
    'open_order', 'position', 'stale', 'zero_missing', 'zero_token', 'zero_market',
    'zero_positive', 'zero_unknown', 'zero_duplicate', 'zero_field_missing',
    'claims_same_total', 'claims_swapped_identity', 'extra_claim', 'claim_missing',
    'second_drift', 'seed_missing', 'seed_budget', 'seed_native_hash',
    'native_claim_and_cash_offset', 'wire_missing_lf', 'wire_generation',
    'other_stop', 'new_loss_limit', 'guard_drift', 'ambiguous_delete',
])
def test_filled_loss_main_faults_preserve_barrier_and_do_not_dispatch(filled_loss, monkeypatch, damage):
    import hashlib
    env = filled_loss
    first, second = env.projections
    if damage.endswith('exit4'):
        env.native_exit[0 if damage.startswith('first') else 1] = 4
    elif damage.endswith('partial'):
        (first if damage.startswith('first') else second)['reconciliation']['complete'] = False
    elif damage in ('cash_plus', 'cash_minus', 'second_cash'):
        value = second if damage == 'second_cash' else first
        value['account']['collateral_atoms'] = str(int(value['account']['collateral_atoms'])
                                                 + (-1 if damage == 'cash_minus' else 1))
    elif damage == 'identity':
        first['account_identity']['trading_address'] = '0x' + '1' * 40
    elif damage == 'history':
        first['reconciliation']['declared_orders'] = env.orders[:-1]
    elif damage == 'pagination':
        first['account']['pagination_complete'] = False
    elif damage == 'open_order':
        first['account']['open_orders'] = [env.orders[-1]]
    elif damage == 'position':
        first['account']['positions'] = [{'token': '1', 'shares_atoms': '1'}]
    elif damage == 'stale':
        env.refresh_clock = False
    elif damage.startswith('zero_'):
        records = first['account']['settled_zero']
        target = records[-1]
        if damage == 'zero_missing': records.pop()
        elif damage == 'zero_token': target['token'] = '1'
        elif damage == 'zero_market': target['market_id'] += 1
        elif damage == 'zero_positive': target.update(outcome_status='WON', payout_millionths=1000000)
        elif damage == 'zero_unknown': target['oracle_status'] = 'REGISTERED'
        elif damage == 'zero_duplicate': records.append(copy.deepcopy(target))
        else: target.pop('trading_status')
    elif damage.startswith('claims_'):
        claims = first['account']['claimed_win']
        if damage == 'claims_same_total':
            claims[0]['claim_atoms'] = str(int(claims[0]['claim_atoms']) + 1)
            claims[1]['claim_atoms'] = str(int(claims[1]['claim_atoms']) - 1)
        else:
            claims[0]['token'], claims[1]['token'] = claims[1]['token'], claims[0]['token']
    elif damage == 'extra_claim':
        extra = copy.deepcopy(first['account']['claimed_win'][0])
        extra.update(token='1', claim_atoms='0')
        first['account']['claimed_win'].append(extra)
    elif damage == 'claim_missing':
        first['account'].pop('claimed_win')
    elif damage == 'second_drift':
        second['account']['gas_atoms'] = str(int(second['account']['gas_atoms']) + 1)
    elif damage.startswith('seed_'):
        if damage == 'seed_missing':
            env.seed_path.unlink()
            env.audit_bytes.pop(env.seed_path)
        else:
            seed = json.loads(env.seed_path.read_text())
            if damage == 'seed_budget': seed['committed_budget_atoms'] += 1
            else: seed['reconciliation_sha256'] = 'f' * 64
            env.seed_path.write_text(json.dumps(seed))
            env.audit_bytes[env.seed_path] = env.seed_path.read_bytes()
    elif damage == 'native_claim_and_cash_offset':
        path = env.seed_path.parent / 'reconciliation.json'
        value = json.loads(path.read_text())
        value['account']['collateral_atoms'] = str(int(value['account']['collateral_atoms']) + 1)
        claim = value['account']['claimed_win'][0]
        claim['claim_atoms'] = str(int(claim['claim_atoms']) + 1)
        path.write_text(json.dumps(value))
        seed = json.loads(env.seed_path.read_text())
        seed['reconciliation_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        env.seed_path.write_text(json.dumps(seed))
        for item in (path, env.seed_path): env.audit_bytes[item] = item.read_bytes()
    elif damage in ('wire_missing_lf', 'wire_generation', 'other_stop'):
        path = env.previous / 'events.jsonl'
        if damage == 'wire_missing_lf': path.write_bytes(path.read_bytes()[:-1])
        else:
            events = [json.loads(line) for line in path.read_bytes().splitlines()]
            if damage == 'wire_generation':
                next(event for event in reversed(events) if event['event_kind'] == 'execution')['payload']['generation'] = 0
            else:
                for event in events:
                    if event['event_kind'] in ('stop', 'final'):
                        event['payload']['stop_detail'] = 'CASH_CONFLICT'
                ended = env.previous / 'ended.json'
                value = json.loads(ended.read_text())
                value['incomplete_detail'] = 'CASH_CONFLICT'
                ended.write_text(json.dumps(value))
                env.audit_bytes[ended] = ended.read_bytes()
            path.write_bytes(b''.join(live.canonical(event) + b'\n' for event in events))
        env.audit_bytes[path] = path.read_bytes()
    elif damage == 'new_loss_limit':
        value = json.loads(env.config.read_text())
        value['strategies']['limits']['max_loss_usd'] = 0.9999
        env.config.write_text(json.dumps(value))
    elif damage == 'guard_drift':
        original = env.collection.find_one
        calls = []
        def drift(query, *args):
            calls.append(query)
            if len(calls) == 2: env.collection.record['scope']['host'] = 'concurrent-change'
            return original(query, *args)
        monkeypatch.setattr(env.collection, 'find_one', drift)
    else:
        def ambiguous(query):
            env.calls.append('ambiguous-delete')
            raise RuntimeError('synthetic write result unknown')
        monkeypatch.setattr(env.collection, 'delete_one', ambiguous)
    assert launch.main(env.argv) == 1
    assert env.children == [] and env.calls.count('ambiguous-delete') <= 1
    if damage != 'ambiguous_delete':
        assert env.activation.read_bytes() == env.old_bytes
        assert 'delete' not in env.calls and 'arm' not in env.calls
    if damage not in ('guard_drift', 'ambiguous_delete'):
        assert env.collection.record == env.old
    if damage.startswith(('seed_', 'wire_')) or damage in ('native_claim_and_cash_offset', 'other_stop', 'new_loss_limit'):
        assert env.reads == []
    unchanged_audit(env)


@pytest.mark.parametrize('damage', ['hash', 'venue', 'remaining', 'net_cash', 'net_inventory',
                                   'unknown', 'another_accepted', 'not_final', 'unknown_fee'])
def test_filled_loss_qualification_after_validated_canonical_audit(filled_loss, damage):
    env = filled_loss
    started = json.loads((env.previous / 'started.json').read_text())
    public = json.loads((env.previous / 'public-config.json').read_text())
    context = json.loads((env.previous / 'live-context.json').read_text())
    live.check_metadata(started, public, context)
    relations = live.LiveRelations(started['run_id'], started['risk_window_id'], started['public_config_identity'])
    initial = None
    for line in (env.previous / 'events.jsonl').read_bytes().splitlines(keepends=True):
        event = live.validate_live_event(live.EVENT_PREFIX + line, started['public_config_identity'])
        relations = relations.stage(event)
        if initial is None and event['event_kind'] == 'reconciliation': initial = event
    ended = json.loads((env.previous / 'ended.json').read_text())
    live.check_ended(ended, started, relations)
    seed = json.loads(env.seed_path.read_text())
    arguments = (relations, relations.final['payload'], ended['final_accounting'], started,
                 env.previous, seed, env.seed_path.parent, initial)
    assert launch._filled_inventory_basis(*arguments)['next_budget_atoms'] == '94488600000000000000'
    owner = relations.attempts['1']
    if damage == 'hash': owner['hash'] = '0x' + 'e' * 64
    elif damage == 'venue': owner['venue'] = None
    elif damage == 'remaining': owner['remaining'] = 1
    elif damage in ('net_cash', 'net_inventory'): owner['applied'][damage + '_atoms'] += 1
    elif damage == 'unknown': owner['submission'] = 'UNKNOWN'
    elif damage == 'another_accepted': relations.attempts['other'] = copy.deepcopy(owner)
    elif damage == 'not_final': owner['execution']['closure'] = 'UNPROVEN'
    else: owner['execution']['facts']['share_fee']['status'] = 'UNKNOWN'
    with pytest.raises(launch.Refused, match='HISTORY_REQUIRES_TAKEOVER'):
        launch._filled_inventory_basis(*arguments)
    assert env.reads == [] and env.children == []



@pytest.mark.parametrize('damage', ['loss_latch', 'audit', 'reserved', 'unresolved',
                                   'forced_cleanup', 'child_exit', 'stop_detail'])
def test_filled_loss_classifier_preserves_first_hard_stop_boundaries(filled_loss, damage):
    parsed = live.read_live_run(filled_loss.previous)
    final = copy.deepcopy(parsed['events'][-1]['payload'])
    ended = copy.deepcopy(parsed['ended'])
    accounting = ended['final_accounting']
    exits = ended['exit_facts']
    assert launch._stopped_window_kind(ended, final, accounting, exits) == 'FILLED_INVENTORY'
    if damage == 'loss_latch': accounting['loss_latched'] = True
    elif damage == 'audit': final['audit_complete'] = False
    elif damage == 'reserved': accounting['reserved'] = '1'
    elif damage == 'unresolved': final['unresolved'] = [{'closure': 'UNPROVEN'}]
    elif damage == 'forced_cleanup': exits['forced_cleanup'] = True
    elif damage == 'child_exit': exits['child_returncode'] = 1
    else: final['stop_detail'] = 'CASH_CONFLICT'
    assert launch._stopped_window_kind(ended, final, accounting, exits) == 'REFUSE'



@pytest.mark.parametrize('location', ['old', 'first', 'second', 'both'])
@pytest.mark.parametrize('shape', ['missing', None, {}, False, 0, '', [{'token': '1'}]])
def test_filled_loss_positions_must_be_present_empty_list(filled_loss, location, shape):
    """Missing is unknown, even when both reads omit it or an old seed hash matches."""
    import hashlib
    env = filled_loss
    def change(account):
        if shape == 'missing': account.pop('positions')
        else: account['positions'] = copy.deepcopy(shape)
    if location == 'old':
        path = env.seed_path.parent / 'reconciliation.json'
        value = json.loads(path.read_text())
        change(value['account'])
        path.write_text(json.dumps(value))
        seed = json.loads(env.seed_path.read_text())
        seed['reconciliation_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        env.seed_path.write_text(json.dumps(seed))
        for item in (path, env.seed_path): env.audit_bytes[item] = item.read_bytes()
    else:
        indices = [0, 1] if location == 'both' else [0 if location == 'first' else 1]
        for index in indices: change(env.projections[index]['account'])
    assert launch.main(env.argv) == 1
    assert not env.children and not any(call in env.calls for call in ('delete', 'arm', 'child'))
    assert env.activation.read_bytes() == env.old_bytes
    assert env.collection.record == env.old
    if location == 'old': assert env.reads == []
    unchanged_audit(env)


@pytest.mark.parametrize('submission', ['UNKNOWN', 'ACCEPTED', 'REJECTED_FINAL'])
def test_unsent_edge_prior_submission_cannot_be_hidden_by_final_not_submitted(unsent_edge, submission):
    """Qualification rejects the entire input trace, not just the final order."""
    parsed = live.read_live_run(unsent_edge.previous)
    ended = parsed['ended']
    final = parsed['events'][-1]['payload']
    orders = [event['payload'] for event in parsed['events'] if event['event_kind'] == 'order']
    prior = {**copy.deepcopy(orders[0]), 'attempt_phase': 'PREPARED',
             'submission': submission}
    assert launch._unsent_edge_kind(ended, final, ended['final_accounting'],
                                   ended['exit_facts'], [prior, *orders],
                                   {event['event_kind'] for event in parsed['events']}) is None


@pytest.mark.parametrize('location', ['first', 'second', 'both'])
@pytest.mark.parametrize('record', ['duplicate_zero', 'extra_zero', 'offset_claims'])
def test_unsent_edge_fresh_history_must_equal_old_multiset(unsent_edge, location, record):
    """No-send cannot justify adding history or offsetting cash facts."""
    env = unsent_edge
    indices = [0, 1] if location == 'both' else [0 if location == 'first' else 1]
    template = copy.deepcopy(env.projections[0]['account']['settled_zero'][0])
    for index in indices:
        account = env.projections[index]['account']
        if record == 'offset_claims':
            account['claimed_win'][0]['claim_atoms'] = str(
                int(account['claimed_win'][0]['claim_atoms']) + 1)
            account['claimed_win'][1]['claim_atoms'] = str(
                int(account['claimed_win'][1]['claim_atoms']) - 1)
        else:
            extra = copy.deepcopy(template)
            if record == 'extra_zero':
                extra['token'] = '1'
            account['settled_zero'].append(extra)
    assert launch.main(env.argv) == 1
    assert len(env.reads) == (2 if location == 'second' else 1)
    assert not env.children and not any(call in env.calls for call in ('delete', 'arm', 'child'))
    assert env.activation.read_bytes() == env.old_bytes
    assert env.collection.record == env.old
    unchanged_audit(env)


@pytest.mark.parametrize('location', ['old', 'first', 'second', 'both'])
@pytest.mark.parametrize('shape', ['missing', None, {}, False, 0, '', [{'token': '1'}]])
def test_unsent_edge_positions_are_explicitly_empty(unsent_edge, location, shape):
    """The old native anchor and both fresh reads reject unknown positions."""
    import hashlib
    env = unsent_edge

    def change(account):
        if shape == 'missing':
            account.pop('positions')
        else:
            account['positions'] = copy.deepcopy(shape)

    if location == 'old':
        path = env.seed_path.parent / 'reconciliation.json'
        value = json.loads(path.read_text())
        change(value['account'])
        path.write_text(json.dumps(value))
        seed = json.loads(env.seed_path.read_text())
        seed['reconciliation_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        env.seed_path.write_text(json.dumps(seed))
        for item in (path, env.seed_path):
            env.audit_bytes[item] = item.read_bytes()
    else:
        indices = [0, 1] if location == 'both' else [0 if location == 'first' else 1]
        for index in indices:
            change(env.projections[index]['account'])
    assert launch.main(env.argv) == 1
    assert len(env.reads) == {'old': 0, 'first': 1, 'second': 2, 'both': 1}[location]
    assert not env.children and not any(call in env.calls for call in ('delete', 'arm', 'child'))
    assert env.activation.read_bytes() == env.old_bytes
    assert env.collection.record == env.old
    unchanged_audit(env)
