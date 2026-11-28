"""Offline proof that the launch wrapper is import-safe and ordered."""
import copy
import datetime as dt
import json
import sys
from pathlib import Path
from types import SimpleNamespace

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
             'reconciliation': {'complete': True, 'positions_match_history': True,
                                'declared_orders': []},
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


@pytest.fixture
def native_main(tmp_path, monkeypatch):
    """Real files/parser/controller/lock/guard, fake only external service and child."""
    import os
    from types import SimpleNamespace
    import btc5m_control as control
    import btc5m_live_guard as guard
    import pymongo
    from pymongo.write_concern import WriteConcern

    now = dt.datetime.now(dt.timezone.utc)
    account = {'venue': 'PREDICT_FUN', 'chain_id': 56, 'account_mode': 'predict-account',
               'trading_address': control.local_signer.TRADING_ADDRESS.lower(),
               'signer_address': control.local_signer.SIGNER.lower()}
    barrier = {'_id': guard.scope_id(account), 'state': 'ARMED',
               'schema_version': guard.GUARD_SCHEMA, 'scope': {**account, 'host': 'offline-host'},
               'armed_at': (now - dt.timedelta(minutes=1)).isoformat(timespec='milliseconds'),
               'run_id': 'btc5m-unattended-live-011',
               'risk_window_id': 'btc5m-unattended-24h-20261122-011',
               'authorization_ref': 'pascal-unattended-24h-20261122-011',
               'candidate_digest': 'a' * 64}
    observed = reconciliation(finished_at=(now - dt.timedelta(seconds=1)).isoformat())
    observed['account_identity'] = {key: value for key, value in account.items() if key != 'venue'}
    config, record = write(tmp_path, facts=observed)
    calls = []

    class Collection:
        def __init__(self):
            self.record = copy.deepcopy(barrier)

        def with_options(self, **kwargs):
            return self

        def find_one(self, query, *args):
            return copy.deepcopy(self.record)

        def delete_one(self, query):
            assert self.record and all(self.record.get(k) == v for k, v in query.items())
            calls.append('delete')
            self.record = None
            return SimpleNamespace(deleted_count=1)

        def insert_one(self, value):
            assert self.record is None
            calls.append('arm')
            self.record = copy.deepcopy(value)
            return SimpleNamespace(acknowledged=True)

    collection = Collection()

    class Database:
        def __getitem__(self, name):
            assert name == '07_trading_btc5m_live_guard'
            return collection

        def get_collection(self, name, **kwargs):
            return self[name]

        def list_collection_names(self, **kwargs):
            return ['07_trading_btc5m_live_guard']

    class Client:
        def get_database(self, *args, **kwargs):
            return Database()

        def close(self):
            pass

    uri = 'mongodb://synthetic:synthetic@172.25.240.1:27017/tradingagents?authSource=admin&tls=false'
    monkeypatch.setattr(launch, 'ROOT', tmp_path)
    monkeypatch.setattr(launch, '_barrier_collection',
                        lambda: (Client(), uri, Database(), guard, pymongo, WriteConcern))
    monkeypatch.setattr(pymongo, 'MongoClient', lambda *a, **k: Client())
    monkeypatch.setattr(guard, 'LOCK_DIRECTORY', tmp_path / 'locks')
    activation = tmp_path / 'activation.json'
    monkeypatch.setattr(guard, 'ACTIVATION_PATH', activation)
    old_activation = {'schema_version': 'btc5m-live-activation/1',
                      'authorization_ref': barrier['authorization_ref'],
                      'risk_window_id': barrier['risk_window_id'],
                      'candidate_digest': barrier['candidate_digest'], 'account_identity': account,
                      'storage_binding': {'deployment_id': 'dep-1', 'database': 'tradingagents',
                          'collection': '07_trading_btc5m_live_guard',
                          'credential_env': guard.CREDENTIAL_ENV,
                          'endpoint_id': {'scheme': 'mongodb', 'hosts': ['172.25.240.1:27017'],
                                          'replica_set': None, 'tls': False}}}
    activation.write_bytes(json.dumps(old_activation, indent=1).encode() + b'\n')
    old_bytes = activation.read_bytes()
    monkeypatch.setattr(control, 'check_environment', lambda env: 'synthetic-api-only')
    monkeypatch.setattr(control.local_signer, '_read_owned', lambda: b'synthetic-never-signed')
    child_calls = []

    def child(command, **kwargs):
        calls.append('child')
        assert collection.record['run_id'] == 'btc5m-unattended-live-012'
        assert os.environ.get(guard.CREDENTIAL_ENV) == uri
        assert kwargs['env']['PM_PREDICT_API_KEY'] == 'synthetic-api-only'
        child_calls.append((command, kwargs))
        return SimpleNamespace(poll=lambda: 0)

    real_run = control.run
    monkeypatch.setattr(control, 'run', lambda argv, **kwargs: real_run(argv, popen=child, **kwargs))
    monkeypatch.setattr(control, 'supervise', lambda *a, **k: (
        {'launcher_exit': 0, 'child_returncode': 0, 'child_signal': None,
         'forced_cleanup': False, 'stop_reason': 'DEADLINE'}, None))
    args = arguments(config, record)
    argv = [value for key, value in vars(args).items()
            if key not in ('params', 'auto_prepare') and value is not None
            for value in ('--' + key.replace('_', '-'), str(value))]
    root = tmp_path / 'data/reports/current/evidence'
    argv += ['--evidence-root', str(root)]
    return SimpleNamespace(argv=argv, root=root, config=config, record=record, calls=calls,
                           children=child_calls, collection=collection, old=barrier,
                           activation=activation, old_bytes=old_bytes, account=account, uri=uri)


def test_main_real_files_guard_and_controller_reach_one_no_send_child(native_main):
    env = native_main
    assert launch.main(env.argv) == 0
    assert env.calls == ['delete', 'arm', 'child']
    assert len(env.children) == 1
    attempt = env.root / 'launch-attempts/btc5m-unattended-live-012'
    assert (attempt / 'superseded-activation.json').read_bytes() == env.old_bytes
    assert json.loads((attempt / 'launch-dispatch.json').read_text())['controller_invocations'] == 1
    started = json.loads((env.root / 'btc5m-unattended-live-012/started.json').read_text())
    duration = launch._parse_time(started['deadline_at']) - launch._parse_time(started['started_at'])
    assert duration == dt.timedelta(hours=24)
    assert json.loads(env.activation.read_text())['authorization_ref'].endswith('-012')


def test_main_bad_old_identity_never_writes_or_reads_key(native_main, monkeypatch):
    import btc5m_control as control
    env = native_main
    bad = json.loads(env.activation.read_text())
    bad['authorization_ref'] = 'someone-else'
    env.activation.write_text(json.dumps(bad))
    before = env.activation.read_bytes()
    monkeypatch.setattr(control.local_signer, '_read_owned', lambda: pytest.fail('secret read'))
    assert launch.main(env.argv) == 1
    assert env.activation.read_bytes() == before
    assert env.calls == []
    assert env.collection.record == env.old


def test_main_new_run_never_replays(native_main):
    env = native_main
    assert launch.main(env.argv) == 0
    before = env.activation.read_bytes()
    assert launch.main(env.argv) == 1
    assert len(env.children) == 1
    assert env.activation.read_bytes() == before


def test_main_passes_every_order_to_real_controller_child(native_main):
    env = native_main
    orders = ['0x' + letter * 64 for letter in 'abcd']
    facts = json.loads(env.record.read_text())
    facts['reconciliation']['declared_orders'] = orders
    env.record.write_text(json.dumps(facts))
    for value in orders:
        env.argv += ['--declared-order', value]
    assert launch.main(env.argv) == 0
    command = env.children[0][0]
    assert [command[i + 1] for i, value in enumerate(command)
            if value == '--predict-reconciled-order'] == orders


@pytest.fixture
def automatic_main(native_main, monkeypatch):
    import btc5m_control as control
    import btc5m_live_events as live
    from test_btc5m_live_events import native_stream, metadata, ended_from_native
    from contextlib import closing
    env = native_main
    previous = env.root / env.old['run_id']
    previous.parent.mkdir(parents=True, exist_ok=True)
    frames = native_stream('claim_wait::the_real_producer_pays_the_claim_and_the_reader_can_follow_it')
    started, public, context = metadata(frames[0])
    for value in (started, context):
        value.update(run_id=env.old['run_id'], risk_window_id=env.old['risk_window_id'],
                     authorization_ref=env.old['authorization_ref'])
    env.old['candidate_digest'] = live.candidate_digest(started)
    context['candidate_digest'] = env.old['candidate_digest']
    env.collection.record = copy.deepcopy(env.old)
    old_activation = json.loads(env.activation.read_text())
    old_activation.update(candidate_digest=env.old['candidate_digest'])
    env.activation.write_text(json.dumps(old_activation))
    accepted = []
    with closing(live.LiveEventStore(previous, started, public, context)) as store:
        for raw in frames:
            event = live.validate_live_event(raw)
            event.update(run_id=env.old['run_id'], risk_window_id=env.old['risk_window_id'])
            store.append(live.EVENT_PREFIX + live.canonical(event) + b'\n', ack=lambda _: None)
            if event['event_kind'] == 'order' and event['payload']['submission'] == 'ACCEPTED':
                accepted.append(event['payload']['signed_hash'])
        assert store.relations.final is not None
        ended = ended_from_native(store.relations.final)
        ended['incomplete_reason'] = None
        store.finish(ended)
    env.orders = ['0x' + f'{index:064x}' for index in (1, 2, 3)] + list(dict.fromkeys(accepted))
    assert len(env.orders) == 4
    old_attempt = env.root / 'launch-attempts' / env.old['run_id']
    old_attempt.mkdir(parents=True)
    (old_attempt / 'attempt-reserved.json').write_text(json.dumps(
        {'run_id': env.old['run_id'], 'declared_orders': env.orders[:3]}))
    template = json.loads(env.config.read_text())
    template.pop('controlled_run')
    template['bot'].update(enable_trading=False, mock_trading=True)
    template['controlled_paper'] = {'fee_model': 'paper_net_shares', 'initial_cash': 100}
    env.config.write_text(json.dumps(template))
    env.argv = ['--config', str(env.config), '--auto-prepare',
                '--run-id', 'btc5m-unattended-live-012',
                '--authorization-ref', 'pascal-unattended-24h-20261122-012',
                '--evidence-root', str(env.root)]
    env.reads = []

    def reconcile(config, rpc_url, orders, output, *, environment):
        env.reads.append((json.loads(Path(config).read_text()), list(orders)))
        facts = reconciliation(finished_at=dt.datetime.now(dt.timezone.utc).isoformat())
        facts['account_identity'] = {k: v for k, v in env.account.items() if k != 'venue'}
        facts['reconciliation']['declared_orders'] = list(orders)
        if getattr(env, 'claims', False):
            facts['account']['collateral_atoms'] = '98148096491228070176'
            facts['account']['claimed_win'] = [{'claim_atoms': '1723596491228070176'}]
        facts['observation'] = {'finished_at': facts.pop('finished_at')}
        if len(env.reads) == 2 and getattr(env, 'drift', False):
            facts['account']['collateral_atoms'] = '97422000000000000001'
        Path(output).write_text(json.dumps(facts))
        return {'exit_code': 0, 'projection': facts}

    env.real_reconcile = control.local_signer.reconcile_account
    env.fake_reconcile = reconcile
    monkeypatch.setattr(control.local_signer, 'reconcile_account', reconcile, raising=False)
    env.previous = previous
    return env


def test_auto_main_derives_cash_history_guard_and_window_without_manual_inputs(automatic_main):
    env = automatic_main
    assert launch.main(env.argv) == 0
    assert len(env.reads) == 2
    assert [orders for _, orders in env.reads] == [env.orders, env.orders]
    assert env.reads[1][0]['controlled_run']['budget_usdt'] == '97.422'
    assert env.reads[1][0]['controlled_run']['duration_seconds'] == 86400
    command = env.children[0][0]
    assert [command[i + 1] for i, value in enumerate(command)
            if value == '--predict-reconciled-order'] == env.orders
    assert len(env.children) == 1
    assert json.loads(env.activation.read_text())['risk_window_id'] != env.old['risk_window_id']


def test_auto_main_balance_drift_refuses_without_rotation(automatic_main):
    env = automatic_main
    env.drift = True
    before = env.activation.read_bytes()
    assert launch.main(env.argv) == 1
    assert env.activation.read_bytes() == before
    assert env.calls == []
    assert env.collection.record == env.old


def test_auto_main_unknown_submission_never_reads_account(automatic_main):
    env = automatic_main
    event_path = env.previous / 'events.jsonl'
    events = [json.loads(line) for line in event_path.read_text().splitlines()]
    event = next(value for value in reversed(events) if value['event_kind'] == 'order')
    event['payload']['submission'] = 'UNKNOWN'
    event_path.write_text(''.join(json.dumps(value) + '\n' for value in events))
    assert launch.main(env.argv) == 1
    assert env.reads == []
    assert env.calls == []


def test_fixed_reconciler_checks_artifact_and_invokes_only_read_binary(tmp_path, monkeypatch):
    import hashlib
    import local_signer as signer
    from types import SimpleNamespace
    binary = tmp_path / 'btc5m_reconcile'
    binary.write_bytes(b'offline fixed read-only fixture')
    binary.chmod(0o500)
    monkeypatch.setattr(signer, 'RECONCILE_BINARY', binary, raising=False)
    monkeypatch.setattr(signer, 'RECONCILE_SHA256', hashlib.sha256(binary.read_bytes()).hexdigest(),
                        raising=False)
    monkeypatch.setattr(signer, 'REPORT_ROOT', tmp_path, raising=False)
    monkeypatch.setattr(signer, '_read_owned', lambda: b'fake-never-signed')
    config = tmp_path / 'public-config.json'
    config.write_text(json.dumps(configured()))
    output = tmp_path / 'reconciliation-pass-1.json'
    calls = []

    def read(command, **kwargs):
        calls.append((command, kwargs))
        assert command[0] == str(binary)
        assert set(command[1::2]) == {'--config', '--rpc-url', '--output',
                                      '--predict-reconciled-order'}
        assert kwargs['env']['PM_PRIVATE_KEY'] == 'fake-never-signed'
        output.write_text(json.dumps({'read_only': True}))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(signer.subprocess, 'run', read)
    result = signer.reconcile_account(config, 'https://rpc.example.invalid', ['0x' + 'a' * 64],
                                      output, environment={'YQUANT_PREDICT_FUN_TOKEN': 'fake-api'})
    assert result['exit_code'] == 0
    assert len(calls) == 1
    assert calls[0][1]['shell'] is False
    assert calls[0][1]['env'] == {}  # Child secrets are cleared after the fixed invocation.
    with pytest.raises(signer.Failure):
        signer.reconcile_account(config, 'https://rpc.example.invalid', [],
                                 tmp_path.parent / 'outside.json',
                                 environment={'YQUANT_PREDICT_FUN_TOKEN': 'fake-api'})
    assert len(calls) == 1


@pytest.mark.parametrize('kind', ['stale', 'cash_plus_atom', 'cash_minus_atom', 'account',
                                  'open_order', 'open_position', 'bad_hash', 'wrong_old_run', 'bad_run'])
def test_main_invalid_inputs_preserve_activation_barrier_and_no_secret(native_main, monkeypatch, kind):
    import local_signer
    env = native_main
    facts = json.loads(env.record.read_text())
    if kind == 'stale':
        facts['finished_at'] = NOW.isoformat()
    elif kind.startswith('cash_'):
        facts['account']['collateral_atoms'] = str(97422000000000000000 +
                                                   (1 if kind == 'cash_plus_atom' else -1))
    elif kind == 'account':
        facts['account_identity']['trading_address'] = '0x' + '0' * 40
    elif kind == 'open_order':
        facts['account']['open_orders'] = ['unknown']
    elif kind == 'open_position':
        facts['account']['positions'] = [{'token': '7', 'shares_atoms': '1'}]
    elif kind == 'bad_hash':
        env.argv += ['--declared-order', '0x123']
    elif kind == 'wrong_old_run':
        env.argv[env.argv.index('--old-run') + 1] = 'wrong-run'
    else:
        env.argv[env.argv.index('--run-id') + 1] = '../escape'
    env.record.write_text(json.dumps(facts))
    before = env.activation.read_bytes()
    monkeypatch.setattr(local_signer, '_read_owned', lambda: pytest.fail('secret read'))
    assert launch.main(env.argv) == 1
    assert env.activation.read_bytes() == before
    assert env.collection.record == env.old
    assert env.calls == []


def test_main_activation_write_failure_preserves_old_bytes(native_main, monkeypatch):
    import btc5m_live_events as live
    env = native_main
    create = live._create_file

    def fail_pending(path, content):
        if Path(path).name == 'next-activation.json':
            raise OSError('offline write failure')
        return create(path, content)

    monkeypatch.setattr(live, '_create_file', fail_pending)
    assert launch.main(env.argv) == 1
    assert env.activation.read_bytes() == env.old_bytes
    assert env.collection.record == env.old
    assert env.calls == []
    archive = env.root / 'launch-attempts/btc5m-unattended-live-012/superseded-activation.json'
    assert archive.read_bytes() == env.old_bytes


def test_main_compare_and_delete_failure_never_starts_child(native_main, monkeypatch):
    from types import SimpleNamespace
    env = native_main
    monkeypatch.setattr(env.collection, 'delete_one', lambda query: SimpleNamespace(deleted_count=0))
    assert launch.main(env.argv) == 1
    assert env.collection.record == env.old
    assert env.calls == []
    archive = env.root / 'launch-attempts/btc5m-unattended-live-012/superseded-activation.json'
    assert archive.read_bytes() == env.old_bytes


def test_main_respects_an_already_held_real_account_lock(native_main, monkeypatch):
    import btc5m_live_guard as guard
    import local_signer
    env = native_main
    monkeypatch.setattr(local_signer, '_read_owned', lambda: pytest.fail('secret read'))
    with guard.AccountLock(env.account, 'signed-future-live'):
        assert launch.main(env.argv) == 1
    assert env.calls == []
    assert env.activation.read_bytes() == env.old_bytes


@pytest.mark.parametrize('kind', ['audit', 'sequence', 'loss_latch', 'takeover', 'hash',
                                  'declaration', 'reused_authorization'])
def test_auto_invalid_history_has_zero_reads_and_zero_rotation(automatic_main, kind):
    env = automatic_main
    ended_path = env.previous / 'ended.json'
    ended = json.loads(ended_path.read_text())
    if kind == 'audit':
        ended['audit_complete'] = False
    elif kind == 'sequence':
        ended['final_event_seq'] = 2
    elif kind == 'loss_latch':
        ended['final_accounting'] = {'loss_latched': True}
    elif kind == 'takeover':
        ended['exit_facts']['stop_reason'] = 'OUTPUT_INVALID'
    elif kind == 'hash':
        path = env.previous / 'events.jsonl'
        events = [json.loads(line) for line in path.read_text().splitlines()]
        event = next(value for value in events if value['event_kind'] == 'order')
        event['payload']['signed_hash'] = '0x123'
        path.write_text(''.join(json.dumps(value) + '\n' for value in events))
    elif kind == 'declaration':
        env.argv += ['--declared-order', env.orders[0]]
    else:
        env.argv[env.argv.index('--authorization-ref') + 1] = env.old['authorization_ref']
    ended_path.write_text(json.dumps(ended))
    before = env.activation.read_bytes()
    assert launch.main(env.argv) == 1
    assert env.reads == []
    assert env.calls == []
    assert env.activation.read_bytes() == before


def test_auto_claims_are_subtracted_once_at_atom_precision(automatic_main):
    env = automatic_main
    env.claims = True
    assert launch.main(env.argv) == 0
    assert env.reads[1][0]['controlled_run']['budget_usdt'] == '96.4245'
    run = env.root / 'btc5m-unattended-live-012/public-config.json'
    assert json.loads(run.read_text())['controlled_run']['budget_usdt'] == '96.4245'


def test_auto_main_fixed_credential_reader_and_real_controller_in_one_chain(automatic_main, monkeypatch):
    import hashlib
    from types import SimpleNamespace
    import local_signer as signer
    import subprocess
    env = automatic_main
    monkeypatch.setattr(signer, 'reconcile_account', env.real_reconcile)
    binary = env.config.parent / 'btc5m_reconcile'
    binary.write_bytes(b'fixed read-only transport fixture')
    binary.chmod(0o500)
    monkeypatch.setattr(signer, 'RECONCILE_BINARY', binary)
    monkeypatch.setattr(signer, 'RECONCILE_SHA256', hashlib.sha256(binary.read_bytes()).hexdigest())
    monkeypatch.setattr(signer, 'REPORT_ROOT', env.root.parents[1])
    native_reads = []

    def transport(argv, **kwargs):
        assert argv[0] == str(binary)
        orders = [argv[index + 1] for index, name in enumerate(argv)
                  if name == '--predict-reconciled-order']
        assert orders == env.orders
        result = env.fake_reconcile(Path(argv[argv.index('--config') + 1]),
                                    argv[argv.index('--rpc-url') + 1], orders,
                                    Path(argv[argv.index('--output') + 1]), environment={})
        native_reads.append(argv)
        return SimpleNamespace(returncode=result['exit_code'])

    monkeypatch.setattr(signer, 'subprocess', SimpleNamespace(
        run=transport, DEVNULL=subprocess.DEVNULL, SubprocessError=subprocess.SubprocessError))
    assert launch.main(env.argv) == 0
    assert len(native_reads) == 2
    assert env.calls == ['delete', 'arm', 'child']
    assert len(env.children) == 1


@pytest.mark.parametrize('kind', ['binary_hash', 'binary_symlink', 'config_symlink',
                                  'output_exists', 'output_escape', 'rpc_credentials',
                                  'bad_order', 'paper_config'])
def test_fixed_reconciler_invalid_inputs_never_read_secret(tmp_path, monkeypatch, kind):
    import hashlib
    import local_signer as signer
    binary = tmp_path / 'btc5m_reconcile'
    binary.write_bytes(b'fixed read fixture')
    binary.chmod(0o500)
    monkeypatch.setattr(signer, 'RECONCILE_BINARY', binary)
    monkeypatch.setattr(signer, 'RECONCILE_SHA256', hashlib.sha256(binary.read_bytes()).hexdigest())
    monkeypatch.setattr(signer, 'REPORT_ROOT', tmp_path)
    monkeypatch.setattr(signer, '_read_owned', lambda: pytest.fail('secret read'))
    config = tmp_path / 'public-config.json'
    config.write_text(json.dumps(configured()))
    output = tmp_path / 'reconciliation-pass-1.json'
    orders, rpc = [], 'https://rpc.example.invalid'
    if kind == 'binary_hash':
        binary.chmod(0o700)
        binary.write_bytes(b'changed')
    elif kind == 'binary_symlink':
        link = tmp_path / 'binary-link'
        link.symlink_to(binary)
        monkeypatch.setattr(signer, 'RECONCILE_BINARY', link)
    elif kind == 'config_symlink':
        link = tmp_path / 'config-link.json'
        link.symlink_to(config)
        config = link
    elif kind == 'output_exists':
        output.write_text('do not replace')
    elif kind == 'output_escape':
        output = tmp_path.parent / 'reconciliation-pass-1.json'
    elif kind == 'rpc_credentials':
        rpc = 'https://user:synthetic@rpc.example.invalid'
    elif kind == 'bad_order':
        orders = ['0x' + 'a' * 63]
    else:
        data = configured()
        data['bot']['mock_trading'] = True
        config.write_text(json.dumps(data))
    with pytest.raises(signer.Failure):
        signer.reconcile_account(config, rpc, orders, output,
                                 environment={'YQUANT_PREDICT_FUN_TOKEN': 'synthetic'})


def test_launcher_root_is_the_business_project():
    assert launch.ROOT == Path(__file__).resolve().parents[8]


@pytest.fixture
def native_order_main(automatic_main, monkeypatch):
    """Replay saved native account facts; adapt only fixture time/history in memory."""
    import local_signer
    env = automatic_main
    source = (Path(__file__).resolve().parents[8] /
        'data/reports/2026-11-24-btc5m-controlled-paper/evidence/launch-attempts/'
        'btc5m-live-7a95eeeb5a04411d')
    env.native_pair = [json.loads((source / f'reconciliation-pass-{n}.json').read_text())
                       for n in (1, 2)]
    env.native_reads = []
    env.returned_payloads = []

    def replay(config, rpc_url, orders, output, *, environment):
        index = len(env.native_reads)
        assert index < 2
        value = copy.deepcopy(env.native_pair[index])
        value['observation']['finished_at'] = dt.datetime.now(dt.timezone.utc).isoformat()
        value['reconciliation']['declared_orders'] = list(orders)
        env.native_reads.append(value)
        env.returned_payloads.append(copy.deepcopy(value))
        Path(output).write_text(json.dumps(value))
        return {'exit_code': 4 if index == 0 else 0, 'projection': value}

    monkeypatch.setattr(local_signer, 'reconcile_account', replay)
    return env


@pytest.mark.parametrize('variant', ['native_pair', 'object_key_order'])
def test_native_order_only_changes_reach_one_child_without_rewriting(native_order_main, variant):
    env = native_order_main
    if variant == 'object_key_order':
        for key in ('claimed_win', 'settled_zero'):
            env.native_pair[1]['account'][key] = [dict(reversed(list(row.items())))
                                                  for row in env.native_pair[0]['account'][key]]
    before = copy.deepcopy(env.native_pair)
    assert launch.main(env.argv) == 0
    assert len(env.native_reads) == 2 and len(env.children) == 1
    assert env.native_pair == before and env.native_reads == env.returned_payloads
    attempt = env.root / 'launch-attempts/btc5m-unattended-live-012'
    for index, expected in enumerate(before, 1):
        saved = json.loads((attempt / f'reconciliation-pass-{index}.json').read_text())
        assert saved['account'] == expected['account']


def test_native_order_original_pair_replays_with_frozen_clock(native_order_main, monkeypatch):
    import local_signer
    from types import SimpleNamespace
    env = native_order_main
    before = copy.deepcopy(env.native_pair)
    observed = launch._parse_time(before[1]['observation']['finished_at'])

    class ReplayClock(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return observed

    calls = []

    def original(config, rpc_url, orders, output, *, environment):
        index = len(calls)
        calls.append(str(output))
        value = copy.deepcopy(before[index])
        Path(output).write_text(json.dumps(value))
        return {'exit_code': 4 if index == 0 else 0, 'projection': value}

    monkeypatch.setattr(launch, 'dt', SimpleNamespace(datetime=ReplayClock, timezone=dt.timezone))
    monkeypatch.setattr(local_signer, 'reconcile_account', original)
    args = launch.build_parser().parse_args(env.argv)
    args.declared_order = before[0]['reconciliation']['declared_orders']
    launch.prepare_account(args, env.root / 'launch-attempts/original-pair-replay', {})
    assert len(calls) == 2 and env.native_pair == before
    assert [json.loads(Path(path).read_text()) for path in calls] == before
    assert env.children == [] and env.calls == []


@pytest.mark.parametrize('change', ['claim_amount', 'claim_token', 'claim_market', 'claim_status',
    'claim_extra', 'claim_duplicate', 'claim_remove', 'zero_market', 'zero_status',
    'zero_duplicate', 'zero_remove', 'gas', 'cash', 'extra_field', 'missing_zero',
    'empty_vs_missing', 'zero_non_list'])
def test_native_order_real_fact_changes_refuse_before_publish(native_order_main, change):
    env = native_order_main
    first, second = [value['account'] for value in env.native_pair]
    if change.startswith('claim_') or change.startswith('zero_') and change != 'zero_non_list':
        records = second['claimed_win' if change.startswith('claim_') else 'settled_zero']
        field = change.split('_', 1)[1]
        if field == 'duplicate':
            records.append(copy.deepcopy(records[0]))
        elif field == 'remove':
            records.pop()
        elif field == 'amount':
            records[0]['claim_atoms'] = str(int(records[0]['claim_atoms']) + 1)
        elif field == 'market':
            records[0]['market_id'] += 1
        elif field == 'token':
            records[0]['token'] = str(int(records[0]['token']) + 1)
        elif field == 'status':
            records[0]['outcome_status'] = 'CHANGED'
        else:
            records[0]['extra'] = True
    elif change in ('gas', 'cash'):
        field = 'gas_atoms' if change == 'gas' else 'collateral_atoms'
        second[field] = str(int(second[field]) + 1)
    elif change == 'extra_field':
        second['unexpected'] = True
    elif change in ('missing_zero', 'empty_vs_missing'):
        second.pop('settled_zero')
        if change == 'empty_vs_missing':
            first['settled_zero'] = []
    else:
        second['settled_zero'] = {'invalid': 'not-a-list'}
    before = env.activation.read_bytes()
    assert launch.main(env.argv) == 1
    assert len(env.native_reads) == 2 and env.children == [] and env.calls == []
    assert env.activation.read_bytes() == before and env.collection.record == env.old


@pytest.fixture
def saved_native_projection():
    source = (Path(__file__).resolve().parents[8] /
        'data/reports/2026-11-24-btc5m-controlled-paper/evidence/launch-attempts/'
        'btc5m-live-55bf9782c36040a4/reconciliation-pass-1.json')
    return json.loads(source.read_text())


@pytest.mark.parametrize('spelling', ['native', 'lower', 'upper'])
def test_native_address_saved_projection_replays_without_rewriting(saved_native_projection, monkeypatch, spelling):
    from types import SimpleNamespace
    value = saved_native_projection
    if spelling != 'native':
        for key in ('trading_address', 'signer_address'):
            suffix = value['account_identity'][key][2:]
            value['account_identity'][key] = '0x' + (suffix.lower() if spelling == 'lower' else suffix.upper())
    before = copy.deepcopy(value)
    observed = launch._parse_time(value['observation']['finished_at'])

    class ReplayClock(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return observed

    monkeypatch.setattr(launch, 'dt', SimpleNamespace(datetime=ReplayClock, timezone=dt.timezone))
    orders = value['reconciliation']['declared_orders']
    cash, claims = launch._cash_snapshot(value, orders, require_complete=False)
    assert cash == int(value['account']['collateral_atoms']) and cash > claims
    assert value == before
    with pytest.raises(launch.Refused, match='RECONCILIATION_NOT_COMPLETE'):
        launch._cash_snapshot(value, orders, require_complete=True)


def test_native_address_saved_projection_still_rejects_stale_facts(saved_native_projection, monkeypatch):
    from types import SimpleNamespace
    value = saved_native_projection
    observed = launch._parse_time(value['observation']['finished_at'])

    class StaleClock(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return observed + dt.timedelta(seconds=launch.MAX_RECONCILIATION_AGE_SECONDS + 1)

    monkeypatch.setattr(launch, 'dt', SimpleNamespace(datetime=StaleClock, timezone=dt.timezone))
    with pytest.raises(launch.Refused, match='STALE_RECONCILIATION'):
        launch._cash_snapshot(value, value['reconciliation']['declared_orders'], require_complete=False)


@pytest.mark.parametrize('shape', ['missing', 'null', 'extra'])
def test_native_address_bad_identity_shape_refuses(saved_native_projection, shape):
    value = saved_native_projection
    if shape == 'missing':
        value['account_identity'].pop('signer_address')
    elif shape == 'null':
        value['account_identity'] = None
    else:
        value['account_identity']['unexpected'] = True
    with pytest.raises(launch.Refused, match='RECONCILIATION_FACTS'):
        launch._cash_snapshot(value, value['reconciliation']['declared_orders'], require_complete=False)


@pytest.mark.parametrize('field,replacement', [
    ('trading_address', '0x' + '1' * 40), ('signer_address', '0x' + '1' * 40),
    ('trading_address', '0x123'), ('signer_address', '0x' + 'g' * 40),
    ('chain_id', '56'), ('chain_id', 56.0), ('chain_id', True), ('chain_id', 1),
    ('account_mode', 'eoa'), ('venue', 'PREDICT_FUN')])
def test_native_address_wrong_identity_stays_refused(saved_native_projection, field, replacement):
    value = saved_native_projection
    value['account_identity'][field] = replacement
    with pytest.raises(launch.Refused, match='RECONCILIATION_FACTS'):
        launch._cash_snapshot(value, value['reconciliation']['declared_orders'], require_complete=False)


def test_native_address_auto_main_accepts_both_checksum_reads(automatic_main, saved_native_projection, monkeypatch):
    import local_signer
    env = automatic_main
    payloads = []

    def native_format(config, rpc_url, orders, output, *, environment):
        result = env.fake_reconcile(config, rpc_url, orders, output, environment=environment)
        result['projection']['account_identity'] = copy.deepcopy(saved_native_projection['account_identity'])
        payloads.append(copy.deepcopy(result['projection']))
        Path(output).write_text(json.dumps(result['projection']))
        return result

    monkeypatch.setattr(local_signer, 'reconcile_account', native_format)
    assert launch.main(env.argv) == 0
    assert len(env.reads) == 2 and len(env.children) == 1
    assert all(value['account_identity'] == saved_native_projection['account_identity'] for value in payloads)
    attempt = env.root / 'launch-attempts/btc5m-unattended-live-012'
    assert all(json.loads((attempt / f'reconciliation-pass-{index}.json').read_text())['account_identity']
               == saved_native_projection['account_identity'] for index in (1, 2))


def test_native_address_explicit_main_accepts_checksum_identity(native_main, saved_native_projection):
    env = native_main
    facts = json.loads(env.record.read_text())
    facts['account_identity'] = saved_native_projection['account_identity']
    env.record.write_text(json.dumps(facts))
    assert launch.main(env.argv) == 0
    assert len(env.children) == 1


@pytest.mark.parametrize('framing', ['unterminated_final', 'crlf', 'prefix_overflow', 'exact_boundary'])
def test_history_raw_frames_match_canonical_reader_before_launch(automatic_main, framing):
    import btc5m_live_events as live
    env = automatic_main
    path = env.previous / 'events.jsonl'
    raw = path.read_bytes()
    assert raw.endswith(b'\n')
    if framing == 'unterminated_final':
        raw = raw[:-1]
    elif framing == 'crlf':
        raw = raw.replace(b'\n', b'\r\n', 1)
    else:
        lines = raw.splitlines(keepends=True)
        size = live.MAX_LINE_BYTES - (len(live.EVENT_PREFIX) if framing == 'exact_boundary' else 0)
        lines[-1] = lines[-1][:-1] + b' ' * (size - len(lines[-1])) + b'\n'
        assert len(lines[-1]) == size
        raw = b''.join(lines)
    path.write_bytes(raw)
    canonical = live.read_live_run(env.previous)
    before = env.activation.read_bytes()
    if framing == 'exact_boundary':
        assert canonical['status'] == 'COMPLETE'
        assert launch.main(env.argv) == 0
        assert len(env.children) == 1
    else:
        assert canonical['status'] == 'INCOMPLETE' and 'OUTPUT_INVALID' in canonical['reasons']
        assert launch.main(env.argv) == 1
        assert env.reads == [] and env.calls == [] and env.children == []
        assert env.activation.read_bytes() == before


@pytest.mark.parametrize('damage', ['sparse_audit', 'missing_final', 'missing_accounting',
                                  'metadata', 'ended_cash', 'exit', 'mixed_cash_fault'])
def test_m123_strict_history_refuses_before_any_account_read(automatic_main, damage):
    env = automatic_main
    path = env.previous / 'events.jsonl'
    events = [json.loads(line) for line in path.read_text().splitlines()]
    ended_path = env.previous / 'ended.json'
    ended = json.loads(ended_path.read_text())
    if damage == 'sparse_audit':
        started_path = env.previous / 'started.json'
        started = json.loads(started_path.read_text())
        started_path.write_text(json.dumps({key: started[key] for key in (
            'run_id', 'risk_window_id', 'authorization_ref', 'account_identity',
            'candidate_identity', 'public_config_identity')}))
    elif damage == 'missing_final':
        events = events[:-1]
        ended['final_event_seq'] = events[-1]['event_seq']
    elif damage == 'missing_accounting':
        ended.pop('final_accounting')
    elif damage == 'metadata':
        context_path = env.previous / 'live-context.json'
        context = json.loads(context_path.read_text())
        context['candidate_digest'] = 'f' * 64
        context_path.write_text(json.dumps(context))
    elif damage == 'ended_cash':
        ended['cash_complete'] = False
    elif damage == 'exit':
        ended['exit_facts']['child_returncode'] = 1
    else:
        ended.update(takeover_required=True, cash_complete=False, incomplete_reason='OUTPUT_INVALID')
        ended['exit_facts']['stop_reason'] = 'CASH_PENDING'
    path.write_text(''.join(json.dumps(value) + '\n' for value in events))
    ended_path.write_text(json.dumps(ended))
    before = env.activation.read_bytes()
    assert launch.main(env.argv) == 1
    assert env.reads == [] and env.calls == []
    assert env.activation.read_bytes() == before


@pytest.mark.parametrize('field', ['schema_version', 'scope', 'armed_at'])
def test_m123_full_barrier_cas_preserves_concurrent_drift(native_main, monkeypatch, field):
    from types import SimpleNamespace
    env = native_main
    queries = []

    def drift(query):
        env.collection.record[field] = ({**env.old['scope'], 'host': 'another-host'}
                                        if field == 'scope' else 'changed')
        queries.append(copy.deepcopy(query))
        matched = all(env.collection.record.get(key) == value for key, value in query.items())
        if matched:
            env.collection.record = None
        return SimpleNamespace(deleted_count=int(matched))

    monkeypatch.setattr(env.collection, 'delete_one', drift)
    assert launch.main(env.argv) == 1
    assert len(queries) == 1 and set(queries[0]) == set(env.old)
    assert env.collection.record is not None
    assert env.calls == [] and env.children == []


@pytest.mark.parametrize('field', ['schema_version', 'scope', 'armed_at'])
def test_m123_malformed_old_guard_never_writes(native_main, field):
    env = native_main
    env.collection.record[field] = {'unexpected': True} if field == 'scope' else 'invalid'
    before = env.activation.read_bytes()
    assert launch.main(env.argv) == 1
    assert env.activation.read_bytes() == before
    assert env.calls == []


def test_m123_rotation_client_explicitly_disables_driver_retries(tmp_path, monkeypatch):
    import pymongo
    from types import SimpleNamespace
    root = Path(__file__).resolve().parents[8]
    monkeypatch.syspath_prepend(str(root / 'skills/data/data-pipeline/scripts'))
    monkeypatch.setattr(launch, 'ROOT', tmp_path)
    (tmp_path / 'skills').mkdir()
    values = {'MONGODB_HOST': 'offline.invalid', 'MONGODB_PORT': '27017',
              'MONGODB_USERNAME': 'synthetic', 'MONGODB_PASSWORD': 'synthetic-not-a-secret',
              'MONGODB_DATABASE': 'tradingagents'}
    (tmp_path / 'skills/.env').write_text(''.join(f'{key}={value}\n' for key, value in values.items()))
    for key in values:
        monkeypatch.delenv(key, raising=False)
    constructors, databases = [], []

    def database(name, **kwargs):
        databases.append((name, kwargs))
        return object()

    def client(*args, **kwargs):
        constructors.append(kwargs)
        return SimpleNamespace(get_database=database)

    monkeypatch.setattr(pymongo, 'MongoClient', client)
    launch._barrier_collection()
    assert len(constructors) == 1
    options = constructors[0]
    assert options.get('retryWrites') is False
    assert options.get('connect') is False
    assert all(0 < options.get(key, 0) <= 5000 for key in (
        'serverSelectionTimeoutMS', 'connectTimeoutMS', 'socketTimeoutMS'))
    assert databases[0][1]['read_concern'].document == {'level': 'majority'}


def test_m123_ambiguous_delete_is_not_reissued_or_dispatched(native_main, monkeypatch):
    from pymongo.errors import AutoReconnect
    env = native_main
    attempts = []

    def ambiguous(query):
        attempts.append(query)
        raise AutoReconnect('offline ambiguous write')

    monkeypatch.setattr(env.collection, 'delete_one', ambiguous)
    assert launch.main(env.argv) == 1
    assert len(attempts) == 1
    assert env.calls == [] and env.children == []


@pytest.mark.parametrize('fault', [None, 'mixed_reason', 'forced_cleanup', 'missing_accounting'])
def test_m123_frozen_run018_cash_pending_is_not_blanket_rejected(tmp_path, monkeypatch, fault):
    import btc5m_live_events as live
    source = (Path(__file__).resolve().parents[8] /
              'data/reports/2026-11-23-btc5m-controlled-paper/evidence/btc5m-unattended-live-018')
    previous = tmp_path / 'data/reports/offline/evidence' / source.name
    previous.mkdir(parents=True)
    for name in ('started.json', 'ended.json', 'public-config.json', 'live-context.json', 'events.jsonl'):
        (previous / name).write_bytes((source / name).read_bytes())
    started = json.loads((previous / 'started.json').read_text())
    old = {key: started[key] for key in ('run_id', 'risk_window_id', 'authorization_ref')}
    old['candidate_digest'] = live.candidate_digest(started)
    dispatch_path = (Path(__file__).resolve().parents[8] /
        'data/reports/2026-11-22-btc5m-minimal-remediation/evidence/'
        'unattended-prefly/launch-018/launch-dispatch.json')
    argv = json.loads(dispatch_path.read_text())['controller_argv']
    seed = [argv[index + 1] for index, value in enumerate(argv) if value == '--predict-reconciled-order']
    attempt = previous.parent / 'launch-attempts' / source.name
    attempt.mkdir(parents=True)
    (attempt / 'attempt-reserved.json').write_text(json.dumps({'run_id': source.name, 'declared_orders': seed}))
    ended_path = previous / 'ended.json'
    ended = json.loads(ended_path.read_text())
    if fault == 'mixed_reason':
        ended['incomplete_reason'] = 'OUTPUT_INVALID'
    elif fault == 'forced_cleanup':
        ended['exit_facts']['forced_cleanup'] = True
    elif fault == 'missing_accounting':
        ended.pop('final_accounting')
    ended_path.write_text(json.dumps(ended))
    monkeypatch.setattr(launch, 'ROOT', tmp_path)
    if fault is None:
        orders = launch.history_for_window(old, started['account_identity'])
        assert orders[:len(seed)] == seed and len(orders) == 4
    else:
        with pytest.raises((launch.Refused, live.LiveInvalid)):
            launch.history_for_window(old, started['account_identity'])


@pytest.mark.parametrize('field', ['schema_version', 'scope', 'armed_at'])
def test_m123_barrier_reread_drift_never_publishes_activation(native_main, monkeypatch, field):
    env = native_main
    calls = []
    find = env.collection.find_one

    def drift(query, *args):
        calls.append(query)
        if len(calls) == 2:
            env.collection.record[field] = ({**env.old['scope'], 'host': 'another-host'}
                                            if field == 'scope' else 'changed')
        return find(query, *args)

    monkeypatch.setattr(env.collection, 'find_one', drift)
    before = env.activation.read_bytes()
    assert launch.main(env.argv) == 1
    assert len(calls) == 2
    assert env.activation.read_bytes() == before and env.calls == []


UNFILLED_ORDER = '0xf2b0bd91f5fece54686cd26ca0cdebcabf339136a146f5d5c3d282fd02cda202'
REAL_STOP = ('2026-11-24-btc5m-controlled-paper/evidence/btc5m-live-5201fd7ac6c1425b')
REAL_FRESH = ('2026-11-24-btc5m-order-closure/evidence/'
              'poststop-20261124T082516Z/reconciliation-pass-1.json')
REAL_ROOT = Path(__file__).resolve().parents[8]


@pytest.fixture
def unfilled_stop(tmp_path, monkeypatch):
    """The real stopped window: one accepted order with UNPROVEN terminal facts.

    The audit and the closing read are the frozen production evidence, not a
    hand-written imitation: a forged audit line would break the reader's own
    cross-check and prove nothing.
    """
    import btc5m_live_events as live
    source = REAL_ROOT / 'data/reports' / REAL_STOP
    previous = tmp_path / 'data/reports/offline/evidence' / source.name
    previous.mkdir(parents=True)
    for name in ('started.json', 'ended.json', 'public-config.json',
                 'live-context.json', 'events.jsonl'):
        (previous / name).write_bytes((source / name).read_bytes())
    attempt = tmp_path / 'data/reports/offline/evidence/launch-attempts' / source.name
    attempt.mkdir(parents=True)
    (attempt / 'attempt-reserved.json').write_bytes(
        (REAL_ROOT / 'data/reports/2026-11-24-btc5m-controlled-paper/evidence/'
         'launch-attempts/btc5m-live-5201fd7ac6c1425b/attempt-reserved.json').read_bytes())
    started = json.loads((previous / 'started.json').read_text())
    old = {key: started[key] for key in ('run_id', 'risk_window_id', 'authorization_ref')}
    old['candidate_digest'] = live.candidate_digest(started)
    monkeypatch.setattr(launch, 'ROOT', tmp_path)
    env = SimpleNamespace(root=tmp_path, previous=previous, old=old,
                          account=started['account_identity'],
                          fresh=json.loads((REAL_ROOT / 'data/reports' / REAL_FRESH).read_text()))
    return env


@pytest.fixture
def settled_data_unavailable_archive(tmp_path, monkeypatch):
    import btc5m_live_events as live

    run = 'btc5m-live-6f81c6cf5f724702'
    reports = Path('data/reports')
    evidence = reports / '2026-11-27-btc5m-controlled-paper/evidence'
    archive = evidence / run
    attempt = evidence / 'launch-attempts' / run
    gas = (reports / '2026-11-27-btc5m-data-unavailable-stop/evidence'
           / 'takeover-gas-6f81c6cf5f724702/chain-audit-projection.json')
    inputs = [archive / name for name in ('started.json', 'ended.json', 'events.jsonl',
                                          'public-config.json', 'live-context.json')]
    inputs.extend((attempt / 'attempt-reserved.json', attempt / 'reconciliation.json', gas))
    for path in inputs:
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((REAL_ROOT / path).read_bytes())
    started = json.loads((tmp_path / archive / 'started.json').read_bytes())
    old = {key: started[key] for key in ('run_id', 'risk_window_id', 'authorization_ref')}
    old['candidate_digest'] = live.candidate_digest(started)
    monkeypatch.setattr(launch, 'ROOT', tmp_path)
    return SimpleNamespace(root=tmp_path, old=old, account=started['account_identity'],
                           gas=tmp_path / gas, archive=tmp_path / archive,
                           attempt=tmp_path / attempt)


def test_settled_data_unavailable_enters_anchored_read_only_preparation(
        settled_data_unavailable_archive):
    env = settled_data_unavailable_archive
    result = launch._stopped_window(env.old, env.account)
    assert result['kind'] == 'ACCOUNT_READ'
    assert result['budget_atoms'] == 74839800000000000001
    assert len(set(result['orders'])) == len(result['orders']) == 32
    assert result['unresolved'] == []
    proof = result['recovery']
    assert proof['net_loss_atoms'] == '4599166666666666666'
    assert [len(proof['expected_history'][key]) for key in ('claimed_win', 'settled_zero')] == [9, 16]
    assert proof['historical_gas'] == {'original_status': 'NOT_MEASURED',
                                      'original_paid_atoms': None, 'direct_fee_atoms': '0'}


@pytest.mark.parametrize('damage', ['missing_gas', 'changed_gas', 'modified_native'])
def test_settled_data_unavailable_refuses_unbound_proof(settled_data_unavailable_archive, damage):
    env = settled_data_unavailable_archive
    if damage == 'missing_gas':
        env.gas.unlink()
    else:
        target = env.gas if damage == 'changed_gas' else env.attempt / 'reconciliation.json'
        target.write_bytes(target.read_bytes() + b' ')
    with pytest.raises(launch.Refused, match='HISTORY_REQUIRES_TAKEOVER'):
        launch._stopped_window(env.old, env.account)


@pytest.mark.parametrize('fault,read_count', [(None, 2), ('gas_missing', 0),
                                            ('first_partial', 1), ('history_drift', 2)])
def test_settled_data_unavailable_real_main(native_main, settled_data_unavailable_archive,
                                             monkeypatch, fault, read_count):
    import hashlib
    import btc5m_control as control

    env, archive = native_main, settled_data_unavailable_archive
    env.old.update(archive.old)
    env.collection.record = copy.deepcopy(env.old)
    original = json.loads(env.activation.read_bytes())
    original.update({key: env.old[key] for key in ('risk_window_id', 'authorization_ref', 'candidate_digest')})
    env.activation.write_text(json.dumps(original))
    activation_before = env.activation.read_bytes()
    replay = launch._stopped_window(env.old, env.account)
    calls = []
    digests = []

    def fake_read(config, rpc_url, orders, output, *, environment):
        assert env.collection.record == env.old and env.activation.read_bytes() == activation_before
        assert orders == replay['orders']
        assert json.loads(Path(config).read_bytes())['controlled_run']['budget_usdt'] == '74.839800000000000001'
        calls.append(output)
        facts = reconciliation(cash='92652512986227323799')
        facts['account_identity'] = {key: value for key, value in env.account.items() if key != 'venue'}
        facts['reconciliation']['declared_orders'] = list(orders)
        facts['account'].update(positions=[], **copy.deepcopy(replay['recovery']['expected_history']))
        facts['observation'] = {'finished_at': dt.datetime.now(dt.timezone.utc).isoformat()}
        if fault == 'history_drift' and len(calls) == 2:
            facts['account']['settled_zero'].pop()
        raw = json.dumps(facts).encode()
        output.write_bytes(raw)
        digests.append(hashlib.sha256(raw).hexdigest())
        return {'projection': facts, 'exit_code': 4 if fault == 'first_partial' else 0}

    monkeypatch.setattr(control.local_signer, 'reconcile_account', fake_read)
    if fault == 'gas_missing':
        archive.gas.unlink()
    args = ['--config', str(env.config), '--auto-prepare', '--run-id', 'btc5m-unattended-live-012',
            '--authorization-ref', 'pascal-unattended-24h-20261122-012', '--evidence-root', str(env.root)]
    assert launch.main(args) == (1 if fault else 0)
    assert len(calls) == read_count
    if fault:
        assert env.calls == [] and env.children == []
        assert env.activation.read_bytes() == activation_before and env.collection.record == env.old
    else:
        assert env.calls == ['delete', 'arm', 'child'] and len(env.children) == 1
        proof = json.loads((calls[0].parent / 'account-read-recovery.json').read_bytes())
        assert proof['fresh_read_sha256'] == digests
        command = env.children[0][0]
        assert [command[i + 1] for i, part in enumerate(command)
                if part == '--predict-reconciled-order'] == replay['orders']


def test_unfilled_stop_is_read_as_an_unfilled_order_not_a_takeover_refusal(unfilled_stop):
    """The real stopped window classifies as one unfilled order, not a refusal."""
    env = unfilled_stop
    stopped = launch._stopped_window(env.old, env.account)
    assert stopped['kind'] == 'UNFILLED_ORDER'
    assert stopped['budget_atoms'] == 95488500000000000000
    assert len(stopped['orders']) == 5
    assert UNFILLED_ORDER in stopped['orders']
    # The real post-stop read closes it: the order is proven and the cash
    # returns to the committed budget to the atom.
    assert launch._fresh_read_closes_window(stopped, env.fresh)


@pytest.mark.parametrize('damage', ['unproven_order', 'cash_short', 'cash_long',
                                    'open_order', 'position', 'incomplete'])
def test_unfilled_stop_refuses_a_read_that_does_not_close_it(unfilled_stop, damage):
    """A read that does not prove the order final never closes the window."""
    env = unfilled_stop
    stopped = launch._stopped_window(env.old, env.account)
    fresh = copy.deepcopy(env.fresh)
    if damage == 'unproven_order':
        fresh['reconciliation']['declared_orders'] = [
            value for value in fresh['reconciliation']['declared_orders']
            if value != UNFILLED_ORDER]
    elif damage == 'cash_short':
        fresh['account']['collateral_atoms'] = '99250496491228070175'
    elif damage == 'cash_long':
        fresh['account']['collateral_atoms'] = '99250496491228070177'
    elif damage == 'open_order':
        fresh['account']['open_orders'] = ['0x' + 'a' * 64]
    elif damage == 'position':
        fresh['account']['positions'] = [{'token': '7', 'shares_atoms': '1'}]
    else:
        fresh['reconciliation']['complete'] = False
    assert not launch._fresh_read_closes_window(stopped, fresh)


def test_other_stopped_windows_keep_their_takeover_refusal(tmp_path, monkeypatch):
    """Only an unfilled order may continue; every other fault still refuses."""
    source = REAL_ROOT / 'data/reports' / REAL_STOP
    previous = tmp_path / 'data/reports/offline/evidence' / source.name
    previous.mkdir(parents=True)
    for name in ('started.json', 'ended.json', 'public-config.json',
                 'live-context.json', 'events.jsonl'):
        (previous / name).write_bytes((source / name).read_bytes())
    attempt = tmp_path / 'data/reports/offline/evidence/launch-attempts' / source.name
    attempt.mkdir(parents=True)
    (attempt / 'attempt-reserved.json').write_bytes(
        (REAL_ROOT / 'data/reports/2026-11-24-btc5m-controlled-paper/evidence/'
         'launch-attempts/btc5m-live-5201fd7ac6c1425b/attempt-reserved.json').read_bytes())
    import btc5m_live_events as live
    started = json.loads((previous / 'started.json').read_text())
    old = {key: started[key] for key in ('run_id', 'risk_window_id', 'authorization_ref')}
    old['candidate_digest'] = live.candidate_digest(started)
    monkeypatch.setattr(launch, 'ROOT', tmp_path)
    ended_path = previous / 'ended.json'
    original = ended_path.read_bytes()
    for fault in ('loss_latch', 'forced_cleanup', 'child_exit', 'mixed_reason',
                  'audit_incomplete', 'two_orders'):
        ended = json.loads(original)
        if fault == 'loss_latch':
            ended['final_accounting']['loss_latched'] = True
        elif fault == 'forced_cleanup':
            ended['exit_facts']['forced_cleanup'] = True
        elif fault == 'child_exit':
            ended['exit_facts']['child_returncode'] = 1
        elif fault == 'mixed_reason':
            ended['incomplete_reason'] = 'OUTPUT_INVALID'
        elif fault == 'audit_incomplete':
            ended['audit_complete'] = False
        else:
            ended['final_accounting']['reserved'] = '1996800000000000000'
        ended_path.write_bytes(json.dumps(ended).encode())
        with pytest.raises((launch.Refused, live.LiveInvalid)):
            launch._stopped_window(old, started['account_identity'])
    ended_path.write_bytes(original)
    assert launch._stopped_window(old, started['account_identity'])['kind'] == 'UNFILLED_ORDER'


UNFILLED_FINAL = {'audit_complete': True, 'cash_complete': False, 'execution_complete': False,
                   'positions': [], 'run_status': 'INCOMPLETE', 'stop_detail': None,
                   'stop_reason': 'FACT_INCOMPLETE', 'takeover_required': True,
                   'unresolved': [{'attempt_id': '1', 'closure': 'UNPROVEN',
                                   'reason': 'FACT_INCOMPLETE',
                                   'reserved_atoms': '998400000000000000',
                                   'signed_hash': UNFILLED_ORDER}]}
UNFILLED_ENDED = {'incomplete_reason': 'FACT_INCOMPLETE', 'incomplete_detail': None,
                  'final_accounting': {'loss_latched': False, 'reserved': '998400000000000000',
                                       'status': 'INCOMPLETE', 'reason': 'FACT_INCOMPLETE',
                                       'pending_claim_value': None},
                  'exit_facts': {'launcher_exit': 0, 'child_returncode': 0,
                                 'child_signal': None, 'forced_cleanup': False,
                                 'stop_reason': 'FACT_INCOMPLETE'}}


def _kind(ended_overrides=None, final_overrides=None, exit_overrides=None):
    ended = copy.deepcopy(UNFILLED_ENDED)
    final = copy.deepcopy(UNFILLED_FINAL)
    ended.update(ended_overrides or {})
    final.update(final_overrides or {})
    ended['exit_facts'].update(exit_overrides or {})
    return launch._stopped_window_kind(ended, final, ended['final_accounting'],
                                       ended['exit_facts'])


@pytest.mark.parametrize('overrides,expected', [
    ({}, 'UNFILLED_ORDER'),
    # A loss latch, a forced cleanup or a failed child is never continued.
    ({'exit_facts': {'forced_cleanup': True}}, 'REFUSE'),
    ({'exit_facts': {'child_returncode': 1}}, 'REFUSE'),
    ({'exit_facts': {'launcher_exit': 1}}, 'REFUSE'),
    ({'exit_facts': {'child_signal': 9}}, 'REFUSE'),
    ({'final_accounting': {'loss_latched': True}}, 'REFUSE'),
    # An incomplete audit is never continued.
    ({'final': {'audit_complete': False}}, 'REFUSE'),
    # Another stop reason is never disguised as an unfilled order.
    ({'final': {'stop_reason': 'OUTPUT_INVALID'}}, 'REFUSE'),
    ({'final': {'stop_detail': 'DETAIL'}}, 'REFUSE'),
    ({'ended': {'incomplete_reason': 'OUTPUT_INVALID'}}, 'REFUSE'),
    ({'ended': {'incomplete_detail': 'DETAIL'}}, 'REFUSE'),
    # Two open orders, or an order of another kind, are never continued.
    ({'final': {'unresolved': UNFILLED_FINAL['unresolved'] * 2}}, 'REFUSE'),
    ({'final': {'unresolved': [{**UNFILLED_FINAL['unresolved'][0], 'closure': 'OPEN'}]}}, 'REFUSE'),
    ({'final': {'unresolved': [{**UNFILLED_FINAL['unresolved'][0], 'reason': 'OTHER'}]}}, 'REFUSE'),
    # A settled window with no open item is the healthy kind, not this one.
    ({'final_accounting': {'reserved': '0', 'status': 'COMPLETE', 'reason': 'NONE'},
      'final': {'unresolved': [], 'takeover_required': False, 'execution_complete': True,
                'cash_complete': True},
      'ended': {'incomplete_reason': None}}, 'HEALTHY'),
])
def test_stopped_window_kind_matrix(overrides, expected):
    """The classification decides by its own fields, never by the audit alone."""
    ended_overrides = overrides.get('ended', {})
    final_overrides = overrides.get('final', {})
    exit_overrides = overrides.get('exit_facts', {})
    accounting_overrides = overrides.get('final_accounting', {})
    ended = copy.deepcopy(UNFILLED_ENDED)
    final = copy.deepcopy(UNFILLED_FINAL)
    ended.update(ended_overrides)
    final.update(final_overrides)
    ended['final_accounting'].update(accounting_overrides)
    ended['exit_facts'].update(exit_overrides)
    assert launch._stopped_window_kind(ended, final, ended['final_accounting'],
                                       ended['exit_facts']) == expected
