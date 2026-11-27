"""R21 through the original launcher and controller, with no external execution."""
import copy
import datetime as dt
import hashlib
import json
import os
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_btc5m_launch import launch, native_main  # noqa: F401 (pytest fixture)
import btc5m_control as control
import btc5m_live_events as live
import btc5m_live_guard as guard

RUN = 'btc5m-live-4d0577d73dee4106'
PREVIOUS = 'btc5m-live-6baf1ef8d6724dfb'
NEW = 'btc5m-unattended-live-012'
E = Path('data/reports/2026-11-27-btc5m-controlled-paper/evidence')
R = E / RUN
A = E / 'launch-attempts' / RUN
F = Path('data/reports/2026-11-27-btc5m-fact-incomplete-stop/evidence')
INTAKE = F / 'pre-send-exit-recovery-intake.json'
BINDING = F / 'context-root-production-binding.json'
BOT = control.BOT.relative_to(control.BOT.parents[4])
WRAPPER = E / 'wrapper/20261127T141559-2447-13708-btc5m-auth-20261127141559-1795760159667967068-e9ef582f/dispatch.json'
C0 = '80.760100000000000001'
EXTRA = {
    INTAKE: 'a98ce32fa6b45395ed79f1e46e7dbccfc95e8ce07099ffdf8f4ea2833456e729',
    A / 'superseded-activation.json': '70b61dba63eaa16b23dcfa8fe0adad062315be725bd24521d55b4c4f30793d5b',
    A / 'barrier-before-rotation.json': '8c495c297332114815286fbec6dea1f64b264c36851fcaa62fde3ff69d44ff51',
    E / 'launch-attempts' / PREVIOUS / 'attempt-reserved.json': '7d834efcb31b2f640da05cf8a33292095266a0b5b9a59aba352008a0d1e95276',
    **{A / name: '04f0d01e1abc9ceb42687bc4041a2afb6403ed998af5e507e4f879c938424ff6'
       for name in ('account-read-config.json', 'public-config.json')},
    WRAPPER: '5bd1cd098de01cdd73bd9011cd68752bc17de79994d61a0ae61117fc0001e05a',
    F / 'closed-reread-bound-candidate/polymarket-toolkits': '06954cb544d700c5c18933018aca949d897e67182c5a28668a2cb9b8834e8543',
    F / 'closed-reread-bound-candidate/btc5m_reconcile': '178b251f0e23fdbc6a282471418b75aeecccc20a155ecbce316bd7c23b259f08',
    F / 'context-root-fixed-candidate/polymarket-toolkits': '66f1286dae49bfc9b4ce2d7e66897c42d1034d20cccd341bfeac8f65d3482f21',
    F / 'context-root-fixed-candidate/validation.md': 'c4147bc64ac4dc6e587bb966ba79a83ec49f42e3bf392416de9dc981f64189fc',
    BOT / 'src/main.rs': '87d586b342e99ca9a7fab620cb62b96209d8a5b482c8f3cbb853c88651a79d3b',
    BOT / 'src/config/controlled_live.rs': '3f4b297a207c65207766d4ad961289bd6812b9bc7298c087d370eeb026138883',
    Path('scripts/start-btc5m-live.sh'): 'd34fc27601941c242f3c9dce34c4b98c307b80c7de45891d0e4f89dbbf30cd66',
}


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def raw(data):
    return live.canonical(data) + b'\n'


def load(path):
    return live.strict_json(path.read_bytes())


@pytest.fixture
def frozen(native_main, tmp_path, monkeypatch):
    env = native_main
    source = Path(os.environ['BTC5M_TEST_PROJECT_ROOT'])
    assert sha(source / INTAKE) == EXTRA[INTAKE]
    hashes = {Path(name): digest for name, digest in load(source / INTAKE)['source_sha256'].items()}
    hashes.update(EXTRA)
    for path, digest in hashes.items():
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2((control.BOT.parents[4] if path.is_relative_to(BOT) else source) / path, target)
        assert sha(target) == digest
    context = load(tmp_path / R / 'live-context.json')
    prior = load(tmp_path / A / 'superseded-activation.json')
    activation = {key: context[key] for key in ('authorization_ref', 'risk_window_id',
        'candidate_digest', 'account_identity', 'deadline_at', 'gas_policy', 'operations_mode')}
    activation.update(schema_version='btc5m-live-activation/1', not_before=context['started_at'],
                      takeover_ref=context['authorization_ref'], storage_binding=prior['storage_binding'])
    env.old_bytes = raw(activation)
    assert hashlib.sha256(env.old_bytes).hexdigest() == load(tmp_path / BINDING)['activation_sha256_unchanged']
    env.activation.write_bytes(env.old_bytes)
    env.old.update({key: context[key] for key in ('run_id', 'risk_window_id',
                                                  'authorization_ref', 'candidate_digest')},
                   armed_at='2026-11-27T06:17:17.000Z')
    env.collection.record = copy.deepcopy(env.old)
    env.argv = ['--auto-prepare', '--config', str(env.config), '--run-id', NEW,
                '--authorization-ref', 'offline-r21', '--evidence-root', str(env.root)]
    env.attempt = env.root / 'launch-attempts' / NEW
    env.orders = load(tmp_path / A / 'attempt-reserved.json')['declared_orders']
    env.frozen = tmp_path
    env.hashes = hashes
    env.inputs, env.readbacks, env.cas = [], [], []
    env.mutate = lambda index, value: 0
    env.now = dt.datetime.now(dt.timezone.utc)
    env.started_reads = []

    def reader(config, rpc_url, orders, output, *, environment):
        index = len(env.inputs) + 1
        env.inputs.append((load(config)['controlled_run']['budget_usdt'], orders[:], output))
        env.calls.append(f'read{index}')
        assert env.collection.record == env.old and env.activation.read_bytes() == env.old_bytes
        value = load(tmp_path / A / f'reconciliation-pass-{index}.json')
        start = dt.datetime.now(dt.timezone.utc)
        value['observation'].update(started_at=start.isoformat(),
                                    finished_at=dt.datetime.now(dt.timezone.utc).isoformat())
        env.started_reads.append(copy.deepcopy(value))
        status = env.mutate(index, value)
        live._create_file(output, raw(value))
        return {'projection': value, 'exit_code': status}

    monkeypatch.setattr(control.local_signer, 'reconcile_account', reader)
    env.published_bytes, env.activation_readbacks = [], []
    replace, read = os.replace, live._read_file

    def observe_replace(source, target):
        if target != env.activation:
            return replace(source, target)
        staged = Path(source).read_bytes()
        replace(source, target)
        env.published_bytes.append(staged)
        env.calls.append('publish')

    def observe_activation_read(path, limit):
        data = read(path, limit)
        # Count launcher readback only, not the pre-publication reads or the
        # controller's own activation validation after the handoff.
        if path == env.activation and env.published_bytes and 'dispatch' not in env.calls:
            env.activation_readbacks.append(data)
            env.calls.append('activation-readback')
        return data

    monkeypatch.setattr(os, 'replace', observe_replace)
    monkeypatch.setattr(live, '_read_file', observe_activation_read)
    delete, find = env.collection.delete_one, env.collection.find_one

    def full_cas(query):
        env.calls.append('cas')
        env.cas.append(copy.deepcopy(query))
        assert query == env.old
        assert load(env.activation)['risk_window_id'] == NEW
        return delete(query)

    def observe(query, *args):
        record = find(query, *args)
        env.readbacks.append(None if record is None else record['run_id'])
        return record

    monkeypatch.setattr(env.collection, 'delete_one', full_cas)
    monkeypatch.setattr(env.collection, 'find_one', observe)
    dispatch = control.run

    def original_controller(argv, **kwargs):
        env.calls.append('dispatch')
        assert env.readbacks[-1] is None
        return dispatch(argv, **kwargs)

    monkeypatch.setattr(control, 'run', original_controller)
    yield env
    assert all(sha(source / path) == digest for path, digest in hashes.items())


def test_frozen_window_reaches_original_main_once(frozen, capsys):
    env = frozen
    exit_code = launch.main(env.argv)
    report = json.loads(capsys.readouterr().out)
    assert exit_code == 0, (report.get('status'), report.get('reason'), env.calls)
    assert report['status'] == 'ORIGINAL_CONTROLLER_RETURNED_INSPECT_FINAL'
    assert env.calls == ['read1', 'read2', 'publish', 'activation-readback', 'cas',
                         'delete', 'dispatch', 'arm', 'child']
    assert env.published_bytes == env.activation_readbacks == [env.activation.read_bytes()]
    assert env.cas == [env.old]
    assert env.readbacks[:3] == [RUN, RUN, None]
    assert env.inputs == [(C0, env.orders, env.attempt / f'reconciliation-pass-{i}.json') for i in (1, 2)]
    assert len(env.orders) == len(set(env.orders)) == 25
    assert (env.attempt / 'superseded-activation.json').read_bytes() == env.old_bytes
    assert load(env.attempt / 'barrier-rotation.json')['readback_absent'] is True
    proof = load(env.attempt / 'pre-send-exit-recovery.json')
    assert proof['fresh_read_sha256'] == [sha(output) for _, _, output in env.inputs]
    assert load(env.attempt / 'launch-dispatch.json')['controller_invocations'] == 1
    manifest = env.root / NEW / 'live-context.json'
    command, _ = env.children[0]
    assert command[command.index('--controlled-live-manifest') + 1] == str(manifest)
    started = load(manifest.with_name('started.json'))
    assert live.candidate_digest(started) == load(env.activation)['candidate_digest']
    assert started['candidate_identity'] == control.public_identity({}, live=True)
    assert load(env.frozen / R / 'ended.json')['audit_complete'] is False
    assert all(sha(env.frozen / path) == digest for path, digest in env.hashes.items())


def refuse(env, capsys, reason, *, reads=0, published=False, cas=0, guard_state=RUN,
           fsync_failed=False):
    status = launch.main(env.argv)
    result = json.loads(capsys.readouterr().out)
    assert status == 1 and result['reason'] == reason, (result, env.calls)
    assert result['status'] == ('STOPPED_NO_RETRY' if reason == 'LAUNCH_PREPARATION_FAILED'
                                else 'REFUSED_NO_RETRY')
    assert len(env.inputs) == reads and len(env.cas) == cas
    assert not env.children and 'dispatch' not in env.calls
    assert (env.activation.read_bytes() != env.old_bytes) == published
    expected = [f'read{i}' for i in range(1, reads + 1)]
    assert env.published_bytes == ([env.activation.read_bytes()] if published else [])
    if published:
        assert load(env.activation)['risk_window_id'] == NEW
        expected.append('publish')
    # The readback fault injects a mismatch after the real read; the fsync
    # fault stops after replace and before even attempting that read.
    assert env.activation_readbacks == ([] if fsync_failed else env.published_bytes)
    if published and not fsync_failed:
        expected.append('activation-readback')
    assert [call for call in env.calls if call != 'delete'] == expected + ['cas'] * cas
    assert (None if env.collection.record is None else env.collection.record['run_id']) == guard_state
    assert not (env.attempt / 'launch-dispatch.json').exists()
    assert not (env.root / NEW).exists()


def change(path, update):
    data = load(path)
    update(data)
    path.write_bytes(raw(data))


def event_variant(case):
    return {
        'event': {'event_kind': 'reconciliation'}, 'permit': {'event_kind': 'permit'},
        'ack': {'event_kind': 'ack'}, 'unknown': {'event_kind': 'order', 'submission': 'UNKNOWN'},
        'late_fill': {'event_kind': 'fill'},
    }[case]


@pytest.mark.parametrize('case', ['run', 'risk', 'auth', 'account', 'digest', 'missing',
    'duplicate', 'binary', 'source', 'truncated', 'extra_log', 'event', 'signal',
    'forced', 'permit', 'ack', 'unknown', 'late_fill', 'symlink', 'hardlink', 'missing-events'])
def test_refuses_changed_window_or_exit_before_reads(frozen, capsys, case):
    env, root = frozen, frozen.frozen
    reason = 'HISTORY_IDENTITY'
    if case == 'missing-events':
        archive = root / R
        reports = root / 'data/reports'
        assert list(reports.glob(f'*/evidence/{RUN}/started.json')) == [archive / 'started.json']
        assert list(reports.glob(f'*/evidence/launch-attempts/{RUN}/attempt-reserved.json')) == [
            root / A / 'attempt-reserved.json']
        events = archive / 'events.jsonl'
        assert events.read_bytes() == b''
        events.unlink()
        remaining = {path.name: sha(path) for path in archive.iterdir()}
        refuse(env, capsys, reason)
        assert not env.attempt.exists()
        # Includes unchanged old ended.json and no newly written Final artifact.
        assert {path.name: sha(path) for path in archive.iterdir()} == remaining
        return
    if case in ('run', 'risk', 'auth', 'digest'):
        flag = {'run': 'run', 'risk': 'risk-window', 'auth': 'authorization-ref', 'digest': 'digest'}[case]
        env.argv += ['--old-' + flag, '0' * 64]
        reason = 'UNEXPECTED_BARRIER'
    elif case == 'account':
        env.collection.record['scope']['chain_id'] = 1
        reason = 'LAUNCH_PREPARATION_FAILED'
    elif case in ('missing', 'duplicate'):
        path = root / R / 'started.json'
        if case == 'missing':
            path.unlink()
        else:
            duplicate = root / 'data/reports/dup/evidence' / RUN / path.name
            duplicate.parent.mkdir(parents=True)
            shutil.copy2(path, duplicate)
        reason = 'HISTORY_MISSING_OR_AMBIGUOUS'
    elif case in ('binary', 'source'):
        path = root / (F / 'closed-reread-bound-candidate/polymarket-toolkits'
                       if case == 'binary' else BOT / 'src/main.rs')
        mode = path.stat().st_mode & 0o777
        path.chmod(0o600)
        with path.open('r+b') as stream:
            original = stream.read(1)
            stream.seek(0)
            stream.write(bytes([original[0] ^ 1]))
        path.chmod(mode)
    elif case in ('truncated', 'extra_log'):
        path = root / R / 'run-log.txt'
        original = path.read_bytes()
        path.write_bytes(original[:-1] if case == 'truncated' else original + b'extra\n')
    elif case in ('signal', 'forced'):
        change(root / R / 'ended.json', lambda d: d['exit_facts'].update(
            {'child_signal': 15} if case == 'signal' else {'forced_cleanup': True}))
    elif case in ('symlink', 'hardlink'):
        path = root / R / 'run-log.txt'
        alias = path.with_name('log-copy')
        path.rename(alias)
        if case == 'symlink':
            path.symlink_to(alias)
        else:
            os.link(alias, path)
    else:
        (root / R / 'events.jsonl').write_bytes(raw(event_variant(case)))
    refuse(env, capsys, reason)
    assert not env.attempt.exists()


SEED_MUTATIONS = [
    ('rotation-old', 'barrier-rotation.json', 'previous_run_id', 'wrong'),
    ('rotation-new', 'barrier-rotation.json', 'new_run_id', 'wrong'),
    ('rotation-delete', 'barrier-rotation.json', 'deleted_count', 0),
    ('rotation-readback', 'barrier-rotation.json', 'readback_absent', False),
    ('dispatch-argv', 'launch-dispatch.json', 'controller_argv', []),
    ('dispatch-repeat', 'launch-dispatch.json', 'controller_invocations', 2),
    ('dispatch-retry', 'launch-dispatch.json', 'automatic_retry', True),
    ('seed-cash', 'attempt-reserved.json', 'committed_budget_atoms', 1),
    ('seed-sha', 'attempt-reserved.json', 'reconciliation_sha256', '0' * 64),
    ('seed-orders', 'attempt-reserved.json', 'declared_orders', []),
    ('proof-hashes', 'account-read-recovery.json', 'fresh_read_sha256', None),
    ('summary-time', 'reconciliation.json', 'finished_at', '2026-01-01T00:00:00.000Z'),
    ('old-read-time', 'reconciliation-pass-2.json', 'observation', {}),
    ('old-read-declarations', 'reconciliation-pass-1.json', 'reconciliation', {}),
    ('first-config', 'account-read-config.json', 'controlled_run', {'budget_usdt': '0'}),
    ('second-config', 'public-config.json', 'controlled_run', {'budget_usdt': '0'}),
]


@pytest.mark.parametrize('case', [row[0] for row in SEED_MUTATIONS] + ['missing-seed',
    'duplicate-seed', 'caller-order', 'caller-reordered', 'caller-duplicated'])
def test_refuses_changed_seed_and_historical_reads_before_reads(frozen, capsys, case):
    env, root = frozen, frozen.frozen
    reason = 'HISTORY_IDENTITY'
    if case.startswith('caller-'):
        orders = {'caller-order': env.orders[:-1], 'caller-reordered': env.orders[::-1],
                  'caller-duplicated': env.orders + env.orders[:1]}[case]
        env.argv += [arg for order in orders for arg in ('--declared-order', order)]
        reason = 'DECLARED_ORDERS_MISMATCH'
    elif case in ('missing-seed', 'duplicate-seed'):
        seed = root / A / 'attempt-reserved.json'
        if case == 'missing-seed':
            seed.unlink()
        else:
            duplicate = root / 'data/reports/dup/evidence/launch-attempts' / RUN / seed.name
            duplicate.parent.mkdir(parents=True)
            shutil.copy2(seed, duplicate)
        reason = 'HISTORY_MISSING_OR_AMBIGUOUS'
    else:
        _, name, key, value = next(row for row in SEED_MUTATIONS if row[0] == case)
        change(root / A / name, lambda data: data.update(
            {key: data[key][::-1] if case == 'proof-hashes' else value}))
    refuse(env, capsys, reason)
    assert not env.attempt.exists()


FRESH_REFUSALS = {
    'native1': 'RECONCILIATION_NOT_COMPLETE', 'native4': 'RECONCILIATION_NOT_COMPLETE',
    'incomplete': 'RECONCILIATION_NOT_COMPLETE', 'unexplained': 'RECONCILIATION_NOT_COMPLETE',
    'history-open': 'RECONCILIATION_NOT_COMPLETE', 'stale': 'STALE_RECONCILIATION',
    'account': 'RECONCILIATION_FACTS', 'declarations': 'RECONCILIATION_FACTS',
    'pagination': 'RECONCILIATION_FACTS', 'not-read-only': 'RECONCILIATION_FACTS',
    **{f'{key}-{variant}': 'RECONCILIATION_FACTS' for key in ('positions', 'open_orders')
       for variant in ('nonempty', 'missing', 'null')},
    'gas': 'CASH_OR_GAS_UNAVAILABLE', 'cash-plus': 'CASH_BUDGET_MISMATCH',
    'cash-minus': 'CASH_BUDGET_MISMATCH',
}


@pytest.mark.parametrize('boundary', [1, 2])
@pytest.mark.parametrize('case', FRESH_REFUSALS)
def test_each_fresh_read_fails_closed(frozen, capsys, case, boundary):
    def mutate(index, value):
        if index != boundary:
            return 0
        account, recon = value['account'], value['reconciliation']
        if case.startswith('native'):
            return int(case[-1])
        if case == 'incomplete':
            recon['complete'] = False
        elif case == 'history-open':
            recon['positions_match_history'] = False
        elif case == 'unexplained':
            account['positions_explained'] = False
        elif case == 'stale':
            value['observation'] = load(frozen.frozen / A / 'reconciliation-pass-1.json')['observation']
        elif case == 'account':
            value['account_identity']['chain_id'] = 1
        elif case == 'declarations':
            recon['declared_orders'].reverse()
        elif case == 'pagination':
            account['pagination_complete'] = False
        elif case == 'not-read-only':
            value['read_only'] = False
        elif case == 'gas':
            account['gas_atoms'] = '1999999999999999'
        elif case.startswith('cash-'):
            delta = 1 if case == 'cash-plus' else -1
            account['collateral_atoms'] = str(int(account['collateral_atoms']) + delta)
        else:
            key, kind = case.split('-')
            if kind == 'missing':
                account.pop(key)
            else:
                account[key] = None if kind == 'null' else [{}]
        return 0

    frozen.mutate = mutate
    refuse(frozen, capsys, FRESH_REFUSALS[case], reads=boundary)
    assert not (frozen.attempt / 'pre-send-exit-recovery.json').exists()


@pytest.mark.parametrize('case,boundary,reason', [
    ('gas-drift', 2, 'RECONCILIATION_DRIFT'), ('duplicate-read', 2, 'STALE_RECONCILIATION'),
    ('removed', 1, 'HISTORY_REQUIRES_TAKEOVER'), ('duplicate', 1, 'HISTORY_REQUIRES_TAKEOVER'),
    ('unseen', 2, 'HISTORY_REQUIRES_TAKEOVER'), ('changed-record', 2, 'HISTORY_REQUIRES_TAKEOVER'),
    ('equal-sum', 1, 'HISTORY_REQUIRES_TAKEOVER'), ('equal-sum', 2, 'HISTORY_REQUIRES_TAKEOVER'),
    ('missing-history', 1, 'RECONCILIATION_FACTS'), ('null-history', 2, 'RECONCILIATION_FACTS'),
    ('reordered', 1, None),
])
def test_exact_fresh_history_and_account_drift(frozen, capsys, case, boundary, reason):
    def mutate(index, value):
        if index < boundary:
            return 0
        account = value['account']
        zero, claims = account['settled_zero'], account['claimed_win']
        if case == 'gas-drift':
            account['gas_atoms'] = str(int(account['gas_atoms']) + 1)
        elif case == 'duplicate-read':
            value.clear()
            value.update(frozen.started_reads[0])
        elif case == 'removed':
            zero.pop()
        elif case == 'duplicate':
            zero.append(zero[0])
        elif case == 'unseen':
            zero.append({**zero[0], 'token': '99'})
        elif case == 'changed-record':
            claims[0]['inventory_atoms'] = '1'
        elif case == 'equal-sum':
            for claim, change_by in zip(claims[:2], (1, -1)):
                claim['claim_atoms'] = str(int(claim['claim_atoms']) + change_by)
        elif case == 'missing-history':
            account.pop('claimed_win')
        elif case == 'null-history':
            account['settled_zero'] = None
        elif case == 'reordered':
            if index == 1:
                zero.reverse()
            else:
                claims.reverse()
        return 0

    frozen.mutate = mutate
    if reason is None:
        test_frozen_window_reaches_original_main_once(frozen, capsys)
    else:
        refuse(frozen, capsys, reason, reads=boundary)


HANDOFF = {
    'lock': ('ALREADY_RUNNING', 0, False, 0, RUN),
    'guard': ('UNEXPECTED_BARRIER', 0, False, 0, None),
    'activation': ('UNEXPECTED_ACTIVATION', 0, False, 0, RUN),
    'host-drift': ('BARRIER_DRIFT', 2, False, 0, RUN),
    'armed-drift': ('BARRIER_DRIFT', 2, False, 0, RUN),
    'activation-drift': ('ACTIVATION_DRIFT', 2, False, 0, RUN),
    'pending-readback': ('ACTIVATION_WRITE', 2, False, 0, RUN),
    'replace': ('LAUNCH_PREPARATION_FAILED', 2, False, 0, RUN),
    'fsync': ('LAUNCH_PREPARATION_FAILED', 2, True, 0, RUN),
    'activation-readback': ('ACTIVATION_WRITE', 2, True, 0, RUN),
    'before-cas': ('LAUNCH_PREPARATION_FAILED', 2, True, 0, RUN),
    'cas-miss': ('BARRIER_ROTATION', 2, True, 1, RUN),
    'cas-unknown': ('LAUNCH_PREPARATION_FAILED', 2, True, 1, None),
    'cas-readback-present': ('BARRIER_ROTATION', 2, True, 1, RUN),
    'cas-readback-error': ('LAUNCH_PREPARATION_FAILED', 2, True, 1, None),
    'rotation-evidence': ('LAUNCH_PREPARATION_FAILED', 2, True, 1, None),
    'dispatch-evidence': ('LAUNCH_PREPARATION_FAILED', 2, True, 1, None),
}


@pytest.mark.parametrize('case', HANDOFF)
def test_original_handoff_failures_report_reached_side_effects(frozen, capsys, monkeypatch, case):
    env = frozen
    reason, reads, published, cas, state = HANDOFF[case]
    def fail(*args):
        raise OSError('isolated failure')

    if case == 'lock':
        with guard.AccountLock(env.account, 'signed-future-live'):
            refuse(env, capsys, reason)
        return
    if case == 'guard':
        env.collection.record = None
    elif case == 'activation':
        change(env.activation, lambda data: data.update(authorization_ref='different'))
        env.old_bytes = env.activation.read_bytes()
    elif case in ('host-drift', 'armed-drift'):
        def drift(index, value):
            if index == 2:
                if case == 'host-drift':
                    env.collection.record['scope']['host'] = 'other-host'
                else:
                    env.collection.record['armed_at'] = '2026-11-27T06:17:18.000Z'
            return 0
        env.mutate = drift
    elif case == 'pending-readback':
        create = live._create_file
        pending = env.attempt / 'next-activation.json'
        monkeypatch.setattr(live, '_create_file', lambda path, data:
                            create(path, data + b'\n' if path == pending else data))
    elif case in ('activation-drift', 'activation-readback'):
        read = live._read_file
        def readback(path, limit):
            data = read(path, limit)
            if path == env.activation and (
                    (case == 'activation-drift' and len(env.inputs) == 2)
                    or (case == 'activation-readback' and data != env.old_bytes)):
                return data + b'\n'
            return data
        monkeypatch.setattr(live, '_read_file', readback)
    elif case == 'replace':
        replace = os.replace
        monkeypatch.setattr(os, 'replace', lambda source, target: fail() if target == env.activation
                            else replace(source, target))
    elif case == 'fsync':
        sync = live._sync_directory
        def sync_after_replace(directory):
            if directory == env.activation.parent and load(env.activation)['risk_window_id'] == NEW:
                fail()
            sync(directory)
        monkeypatch.setattr(live, '_sync_directory', sync_after_replace)
    elif case in ('before-cas', 'rotation-evidence', 'dispatch-evidence'):
        create = live._create_file
        filename = {'before-cas': 'barrier-before-rotation.json', 'rotation-evidence': 'barrier-rotation.json',
                    'dispatch-evidence': 'launch-dispatch.json'}[case]
        monkeypatch.setattr(live, '_create_file', lambda path, data: fail() if path.name == filename
                            else create(path, data))
    else:
        delete, find = env.collection.delete_one, env.collection.find_one
        def broken_cas(query):
            if case == 'cas-miss':
                env.calls.append('cas')
                env.cas.append(copy.deepcopy(query))
                assert query == env.old and load(env.activation)['risk_window_id'] == NEW
                return SimpleNamespace(deleted_count=0)
            result = delete(query)
            if case == 'cas-unknown':
                fail()
            if case == 'cas-readback-present':
                env.collection.record = copy.deepcopy(env.old)
            return result
        def broken_readback(query, *args):
            if case == 'cas-readback-error' and env.cas:
                env.readbacks.append('error')
                fail()
            return find(query, *args)
        monkeypatch.setattr(env.collection, 'delete_one', broken_cas)
        monkeypatch.setattr(env.collection, 'find_one', broken_readback)
    refuse(env, capsys, reason, reads=reads, published=published, cas=cas,
           guard_state=state, fsync_failed=case == 'fsync')
    if cas:
        assert env.cas == [env.old]
    if case in ('cas-miss', 'cas-unknown'):
        assert env.readbacks == [RUN, RUN]
    elif case == 'cas-readback-present':
        assert env.readbacks == [RUN, RUN, RUN]
    elif case == 'cas-readback-error':
        assert env.readbacks == [RUN, RUN, 'error']
    elif case in ('rotation-evidence', 'dispatch-evidence'):
        assert env.readbacks == [RUN, RUN, None]


@pytest.mark.parametrize('case,reason', [('stderr', 'HISTORY_IDENTITY'), ('exit-type', 'HISTORY_IDENTITY'),
    ('dispatch-extra', 'HISTORY_IDENTITY'), ('proof-order', 'HISTORY_REQUIRES_TAKEOVER'),
    ('seed-budget', 'HISTORY_REQUIRES_TAKEOVER'), ('history-set', 'HISTORY_REQUIRES_TAKEOVER'),
    ('old-canonical-probe-reason', 'HISTORY_IDENTITY')])
def test_semantic_checks_after_frozen_digest_gate(frozen, case, reason):
    # Exercise the second (semantic) gate on isolated inputs already passed to
    # that pure layer. All CLI cases above still use the real digest gate.
    import btc5m_pre_send_exit_recovery as recovery

    hashes = {str(path): digest for path, digest in frozen.hashes.items()}
    evidence = {path: (frozen.frozen / path).read_bytes() for path in recovery.BYTE_PATHS}
    existing = frozen.old
    recovery._qualify_bound(evidence, hashes, existing, frozen.old_bytes, frozen.now, 900)
    if case == 'stderr':
        evidence[str(R / 'run-log.txt')] = b'[stderr] Error: CONTROLLED_MODE: LIVE_CONTEXT_PATH\n'
    elif case == 'old-canonical-probe-reason':
        binding = live.strict_json(evidence[str(BINDING)])
        probe = binding['native_probe']['cases']['old_canonical']
        assert probe == {'native_exit': 1, 'reason': 'LIVE_CONTEXT_PATH'}
        probe['reason'] = 'CONTROL_CHANNEL'
        evidence[str(BINDING)] = raw(binding)
    else:
        path = {'exit-type': R / 'ended.json', 'dispatch-extra': A / 'launch-dispatch.json',
                'proof-order': A / 'account-read-recovery.json', 'seed-budget': A / 'attempt-reserved.json',
                'history-set': A / 'account-read-recovery.json'}[case]
        key = str(path)
        obj = live.strict_json(evidence[key])
        if case == 'exit-type':
            obj['exit_facts']['launcher_exit'] = True
        elif case == 'dispatch-extra':
            obj['controller_argv'] += ['--mode', 'signed-future-live']
        elif case == 'proof-order':
            obj['fresh_read_sha256'].reverse()
        elif case == 'seed-budget':
            obj['committed_budget_atoms'] += 1
        else:
            obj['expected_history']['settled_zero'].pop()
        evidence[key] = raw(obj)
    with pytest.raises(recovery.Rejected) as refused:
        recovery._qualify_bound(evidence, hashes, existing, frozen.old_bytes, frozen.now, 900)
    assert refused.value.reason == reason


def test_presend_branch_does_not_claim_other_windows(frozen, capsys):
    env = frozen
    other = 'different-window'
    shutil.copytree(env.frozen / R, env.frozen / R.with_name(other))
    env.collection.record['run_id'] = other
    refuse(env, capsys, 'HISTORY_IDENTITY', guard_state=other)
    assert not env.attempt.exists()
