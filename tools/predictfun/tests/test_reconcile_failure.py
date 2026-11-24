"""Offline proof that a native reader failure keeps its safe classification.

The pinned ``btc5m_reconcile`` writes its failure envelope on **stderr**
(``btc5m_reconcile.rs:670-676``) and only writes stdout on success. These
regressions therefore drive every native failure through stderr and never
through stdout, so a green run proves the real channel is read.

Nothing here touches the network, a credential, the keyring, the production
binary or a live account: the reader is an offline fixture and the child is a
fake process.
"""
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import local_signer as signer


SENTINEL = "SENTINEL-PRIVATE-KEY-MUST-NEVER-LEAK"


def public_config():
    """The public live config shape the reconciler accepts, built offline."""
    data = json.loads((Path(__file__).resolve().parents[1] /
                        'btc5m-controlled-paper.json').read_text())
    data['bot'].update(enable_trading=True, mock_trading=False)
    data.pop('controlled_paper', None)
    data['controlled_run'] = dict(budget_usdt=97.422, duration_seconds=86400,
                                  operations_mode='unattended')
    data['strategies']['btc_arb']['order_usd'] = 1
    data['strategies']['limits'].update(max_order_usd=1, max_market_usd=1,
                                        max_total_usd=10, max_loss_usd=20)
    return data


def envelope(code, step, reason):
    """The exact single-line envelope the native reader prints on stderr."""
    return json.dumps({"code": code, "reason": reason, "step": step}) + "\n"


def reader(tmp_path, monkeypatch):
    """An offline fixed reader fixture plus its public config and output path."""
    binary = tmp_path / 'btc5m_reconcile'
    binary.write_bytes(b'offline fixed read-only fixture')
    binary.chmod(0o500)
    monkeypatch.setattr(signer, 'RECONCILE_BINARY', binary)
    monkeypatch.setattr(signer, 'RECONCILE_SHA256',
                        hashlib.sha256(binary.read_bytes()).hexdigest())
    monkeypatch.setattr(signer, 'REPORT_ROOT', tmp_path)
    monkeypatch.setattr(signer, '_read_owned', lambda: b'fake-never-signed')
    config = tmp_path / 'public-config.json'
    config.write_text(json.dumps(public_config()))
    output = tmp_path / 'reconciliation-pass-1.json'
    return binary, config, output


def call(config, output, child):
    return signer.reconcile_account(config, 'https://rpc.example.invalid',
                                    ['0x' + 'a' * 64], output,
                                    environment={'YQUANT_PREDICT_FUN_TOKEN': 'synthetic'})


def fake_reader(stderr=b"", returncode=2, stdout=b"", projection=None, calls=None):
    """A fake child that behaves like the native reader's streams."""
    def run(command, **kwargs):
        if calls is not None:
            calls.append((command, kwargs))
        if projection is not None:
            Path(kwargs['argv_output']).write_text(json.dumps(projection))
        stderr_file = kwargs['stderr']
        stderr_file.write(stderr)
        stderr_file.flush()
        return SimpleNamespace(returncode=returncode, stdout=stdout)
    return run


# --- the native failure channel ------------------------------------------------


@pytest.mark.parametrize('code,step,reason,exit_code', [
    ('INVALID_INPUT', 'input', 'RPC_URL_INVALID', 2),
    ('READ_UNAVAILABLE', 'rpc', 'RPC_HTTP', 3),
    ('FACT_INCOMPLETE', 'account_snapshot', 'FACT_INCOMPLETE:DUPLICATE_ORDER', 4),
    ('OUTPUT_UNAVAILABLE', 'output', 'OUTPUT_EXISTS', 5),
])
def test_native_failure_on_stderr_keeps_its_classification(tmp_path, monkeypatch,
                                                            code, step, reason, exit_code):
    """A native failure on stderr is classified, not flattened to READ_FAILED."""
    binary, config, output = reader(tmp_path, monkeypatch)
    monkeypatch.setattr(signer.subprocess, 'run',
                        fake_reader(stderr=envelope(code, step, reason).encode(),
                                    returncode=exit_code))
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    result = failure.value.result
    assert result["code"] == "LAUNCH_FAILED"
    assert result["step"] == "reconcile"
    assert result["reason"] == "READ_FAILED"
    assert result["native_error"] == {"code": code, "step": step, "reason": reason,
                                      "native_exit": exit_code}


def test_native_failure_without_a_projection_never_becomes_success(tmp_path, monkeypatch):
    """Exit 4 without a projection is a failure, not an explained reconciliation."""
    binary, config, output = reader(tmp_path, monkeypatch)
    monkeypatch.setattr(signer.subprocess, 'run',
                        fake_reader(stderr=envelope('FACT_INCOMPLETE', 'account_snapshot',
                                                    'FACT_INCOMPLETE:DUPLICATE_ORDER').encode(),
                                    returncode=4))
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    assert failure.value.result["reason"] == "READ_FAILED"
    assert not output.exists()


# --- the success channel stays unchanged ----------------------------------------


@pytest.mark.parametrize('exit_code', [0, 4])
def test_successful_reads_keep_their_projection_unchanged(tmp_path, monkeypatch, exit_code):
    """Exit 0 and exit 4 with a valid read-only projection stay exactly as they were."""
    binary, config, output = reader(tmp_path, monkeypatch)
    projection = {"read_only": True, "account": {"collateral_atoms": "1"}}
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        output.write_text(json.dumps(projection))
        return SimpleNamespace(returncode=exit_code, stdout=b'', stderr=b'')

    monkeypatch.setattr(signer.subprocess, 'run', run)
    result = call(config, output, None)
    assert result == {"exit_code": exit_code, "projection": projection}
    assert "native_error" not in result
    assert len(calls) == 1


# --- every other stderr shape degrades to the generic failure ------------------


@pytest.mark.parametrize('stderr', [
    b'',
    b'not json at all\n',
    b'{"code":"INVALID_INPUT","code":"READ_UNAVAILABLE","reason":"X","step":"input"}\n',
    b'{"code":"INVALID_INPUT","reason":"PUBLIC_CONFIG","step":"input","extra":"x"}\n',
    b'{"code":"INVALID_INPUT","reason":"PUBLIC_CONFIG"}\n',
    b'["INVALID_INPUT","input","PUBLIC_CONFIG"]\n',
    b'{"code":1,"reason":"PUBLIC_CONFIG","step":"input"}\n',
    b'{"code":"NOT_A_NATIVE_CODE","reason":"PUBLIC_CONFIG","step":"input"}\n',
    b'{"code":"INVALID_INPUT","reason":"PUBLIC_CONFIG","step":"not_a_native_step"}\n',
    b'tracing noise only\nanother line\n',
])
def test_unapproved_stderr_keeps_the_generic_failure_and_leaks_nothing(tmp_path, monkeypatch,
                                                                       stderr):
    """Anything outside the strict envelope falls back and never carries text out."""
    binary, config, output = reader(tmp_path, monkeypatch)
    monkeypatch.setattr(signer.subprocess, 'run',
                        fake_reader(stderr=stderr + SENTINEL.encode() + b'\n', returncode=2))
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    result = failure.value.result
    assert result["code"] == "LAUNCH_FAILED"
    assert result["step"] == "reconcile"
    assert result["reason"] == "READ_FAILED"
    assert "native_error" not in result
    assert SENTINEL not in json.dumps(result)


def test_an_unknown_reason_becomes_unclassified_without_leaking(tmp_path, monkeypatch):
    """An unlisted reason is reduced to UNCLASSIFIED; its text never crosses."""
    binary, config, output = reader(tmp_path, monkeypatch)
    monkeypatch.setattr(signer.subprocess, 'run',
                        fake_reader(stderr=envelope('INVALID_INPUT', 'input',
                                                    'PM_PRIVATE_KEY_' + SENTINEL).encode(),
                                    returncode=2))
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    native = failure.value.result["native_error"]
    assert native["reason"] == "UNCLASSIFIED"
    assert native["code"] == "INVALID_INPUT"
    assert native["step"] == "input"
    assert SENTINEL not in json.dumps(failure.value.result)


def test_tracing_noise_around_the_envelope_is_refused(tmp_path, monkeypatch):
    """A tracing line next to the envelope makes the whole stream ambiguous."""
    binary, config, output = reader(tmp_path, monkeypatch)
    stderr = (b'2026-11-24T01:00:00Z WARN predict_fun http status\n'
              + envelope('RECONCILIATION_REQUIRED', 'reconciliation',
                         'RECONCILIATION_REQUIRED:UNEXPLAINED_POSITIONS').encode()
              + b'2026-11-24T01:00:01Z INFO done\n')
    monkeypatch.setattr(signer.subprocess, 'run', fake_reader(stderr=stderr, returncode=4))
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    result = failure.value.result
    assert (result["code"], result["step"], result["reason"]) == (
        "LAUNCH_FAILED", "reconcile", "READ_FAILED")
    assert "native_error" not in result
    assert 'WARN' not in json.dumps(result)


def test_only_the_whole_stream_may_be_the_envelope(tmp_path, monkeypatch):
    """One envelope and nothing else is the only stream that classifies."""
    binary, config, output = reader(tmp_path, monkeypatch)
    monkeypatch.setattr(signer.subprocess, 'run',
                        fake_reader(stderr=envelope('RECONCILIATION_REQUIRED', 'reconciliation',
                                                    'RECONCILIATION_REQUIRED:UNEXPLAINED_POSITIONS').encode(),
                                    returncode=4))
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    native = failure.value.result["native_error"]
    assert native["code"] == "RECONCILIATION_REQUIRED"
    assert native["step"] == "reconciliation"
    assert native["reason"] == "RECONCILIATION_REQUIRED:UNEXPLAINED_POSITIONS"
    assert native["native_exit"] == 4


def test_two_envelopes_are_ambiguous_and_refused(tmp_path, monkeypatch):
    """More than one candidate envelope is never resolved by guessing."""
    binary, config, output = reader(tmp_path, monkeypatch)
    stderr = (envelope('INVALID_INPUT', 'input', 'RPC_URL_INVALID').encode()
              + envelope('READ_UNAVAILABLE', 'rpc', 'RPC_HTTP').encode())
    monkeypatch.setattr(signer.subprocess, 'run', fake_reader(stderr=stderr, returncode=2))
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    assert "native_error" not in failure.value.result


def test_a_failure_on_stdout_only_is_not_classified(tmp_path, monkeypatch):
    """The native reader writes failures to stderr; stdout alone proves nothing."""
    binary, config, output = reader(tmp_path, monkeypatch)
    monkeypatch.setattr(signer.subprocess, 'run',
                        fake_reader(stdout=envelope('INVALID_INPUT', 'input',
                                                    'RPC_URL_INVALID').encode(),
                                    returncode=2))
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    assert "native_error" not in failure.value.result
    assert failure.value.result["reason"] == "READ_FAILED"


def test_a_timeout_fails_closed_without_leaking(tmp_path, monkeypatch):
    """A timed-out reader is a generic failure; the timeout text never escapes."""
    binary, config, output = reader(tmp_path, monkeypatch)

    def run(command, **kwargs):
        raise subprocess.TimeoutExpired(command, 300, output=SENTINEL.encode(),
                                        stderr=SENTINEL.encode())

    monkeypatch.setattr(signer.subprocess, 'run', run)
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    assert failure.value.result["reason"] == "READ_FAILED"
    assert "native_error" not in failure.value.result
    assert SENTINEL not in json.dumps(failure.value.result)


def test_a_signaled_reader_fails_closed(tmp_path, monkeypatch):
    """A killed reader is a generic failure, never a classified one."""
    binary, config, output = reader(tmp_path, monkeypatch)
    monkeypatch.setattr(signer.subprocess, 'run',
                        fake_reader(stderr=envelope('INVALID_INPUT', 'input',
                                                    'RPC_URL_INVALID').encode(),
                                    returncode=-9))
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    assert failure.value.result["reason"] == "READ_FAILED"
    assert "native_error" not in failure.value.result


def test_a_trailing_newline_is_the_only_permitted_surrounding(tmp_path, monkeypatch):
    """A single trailing newline still classifies; anything else does not."""
    binary, config, output = reader(tmp_path, monkeypatch)
    line = envelope('READ_UNAVAILABLE', 'rpc', 'RPC_HTTP').encode()
    monkeypatch.setattr(signer.subprocess, 'run',
                        fake_reader(stderr=line[:-1], returncode=3))
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    assert failure.value.result["native_error"]["native_exit"] == 3


def test_an_oversized_stream_is_refused_whole(tmp_path, monkeypatch):
    """A stream past the limit is refused, not truncated into a valid prefix."""
    binary, config, output = reader(tmp_path, monkeypatch)
    payload = (envelope('READ_UNAVAILABLE', 'rpc', 'RPC_HTTP').encode()
               + b' ' * signer.NATIVE_STDERR_LIMIT)
    assert len(payload) > signer.NATIVE_STDERR_LIMIT
    monkeypatch.setattr(signer.subprocess, 'run',
                        fake_reader(stderr=payload, returncode=3))
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    assert "native_error" not in failure.value.result


def test_a_capture_setup_failure_is_generic_and_clears_the_child(tmp_path, monkeypatch):
    """A capture file that cannot be created fails closed and keeps no secret."""
    binary, config, output = reader(tmp_path, monkeypatch)
    seen = {}
    original = signer.build_child_env

    def build(*args, **kwargs):
        child = original(*args, **kwargs)
        seen['child'] = child
        return child

    def unavailable(*args, **kwargs):
        raise OSError('synthetic capture failure')

    monkeypatch.setattr(signer, 'build_child_env', build)
    monkeypatch.setattr(signer.tempfile, 'TemporaryFile', unavailable)
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    assert (failure.value.result["code"], failure.value.result["step"],
            failure.value.result["reason"]) == ("LAUNCH_FAILED", "reconcile", "READ_FAILED")
    assert "native_error" not in failure.value.result
    assert seen['child'] == {}


@pytest.mark.parametrize('exit_code', [0, 1, 6, 127, 255, -9, -15])
def test_only_the_known_native_failure_exits_classify(tmp_path, monkeypatch, exit_code):
    """An exit code the reader never publishes is never published here either."""
    binary, config, output = reader(tmp_path, monkeypatch)
    monkeypatch.setattr(signer.subprocess, 'run',
                        fake_reader(stderr=envelope('READ_UNAVAILABLE', 'rpc',
                                                    'RPC_HTTP').encode(),
                                    returncode=exit_code))
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    assert "native_error" not in failure.value.result


@pytest.mark.parametrize('exit_code', [2, 3, 4, 5, 64])
def test_the_known_native_failure_exits_classify(tmp_path, monkeypatch, exit_code):
    """Each of the reader's own failure exits keeps its classification."""
    binary, config, output = reader(tmp_path, monkeypatch)
    monkeypatch.setattr(signer.subprocess, 'run',
                        fake_reader(stderr=envelope('READ_UNAVAILABLE', 'rpc',
                                                    'RPC_HTTP').encode(),
                                    returncode=exit_code))
    with pytest.raises(signer.Failure) as failure:
        call(config, output, None)
    assert failure.value.result["native_error"] == {
        "code": "READ_UNAVAILABLE", "step": "rpc", "reason": "RPC_HTTP",
        "native_exit": exit_code}


# --- the frozen reader contract stays frozen -----------------------------------


def test_the_reader_is_invoked_once_with_the_fixed_contract(tmp_path, monkeypatch):
    """One call, the pinned path, the fixed argv, env, cwd, timeout and shell."""
    binary, config, output = reader(tmp_path, monkeypatch)
    calls = []
    projection = {"read_only": True}

    def run(command, **kwargs):
        calls.append((command, dict(kwargs), dict(kwargs['env'])))
        output.write_text(json.dumps(projection))
        return SimpleNamespace(returncode=0, stdout=b'')

    monkeypatch.setattr(signer.subprocess, 'run', run)
    assert call(config, output, None)["exit_code"] == 0
    assert len(calls) == 1
    command, kwargs, child_env = calls[0]
    assert command[0] == str(binary)
    assert set(command[1::2]) == {'--config', '--rpc-url', '--output',
                                  '--predict-reconciled-order'}
    assert kwargs['cwd'] == signer.BOT
    assert kwargs['shell'] is False
    assert kwargs['timeout'] == 300
    assert child_env['PM_PRIVATE_KEY'] == 'fake-never-signed'
    assert kwargs['stdin'] == subprocess.DEVNULL
    assert kwargs['stdout'] == subprocess.DEVNULL
    assert kwargs['stderr'] is not subprocess.DEVNULL
    # the child secret is cleared once the call is over
    assert kwargs['env'] == {}


def test_the_child_environment_is_cleared_after_the_call(tmp_path, monkeypatch):
    """The child's secret is cleared once the fixed invocation is over."""
    binary, config, output = reader(tmp_path, monkeypatch)
    seen = {}

    def run(command, **kwargs):
        seen['env'] = kwargs['env']
        output.write_text(json.dumps({"read_only": True}))
        return SimpleNamespace(returncode=0, stdout=b'')

    monkeypatch.setattr(signer.subprocess, 'run', run)
    assert call(config, output, None)["exit_code"] == 0
    assert seen['env'] == {}


def test_a_drifted_reader_is_refused_before_any_key_is_read(tmp_path, monkeypatch):
    """A changed reader never reaches the credential read or a child process."""
    binary, config, output = reader(tmp_path, monkeypatch)
    binary.chmod(0o700)
    binary.write_bytes(b'changed reader')
    monkeypatch.setattr(signer, '_read_owned', lambda: pytest.fail('secret read'))

    def spawn(*args, **kwargs):
        pytest.fail('reader spawned')

    monkeypatch.setattr(signer.subprocess, 'run', spawn)
    with pytest.raises(signer.Failure):
        call(config, output, None)


# --- the frozen native vocabulary stays frozen ---------------------------------


def source_reasons():
    """Every reason the pinned native sources can publish, read from them."""
    import re
    source = Path(__file__).resolve().parents[3] / 'src'

    def strip_tests(text):
        kept, index = [], 0
        for marker in re.finditer(r'#\[cfg\(test\)\]', text):
            start = text.find('{', marker.end())
            depth, position = 0, start
            while position < len(text):
                if text[position] == '{':
                    depth += 1
                elif text[position] == '}':
                    depth -= 1
                    if depth == 0:
                        break
                position += 1
            kept.append(text[index:marker.start()])
            index = position + 1
        kept.append(text[index:])
        return "".join(kept)

    paths = [source / 'bin/btc5m_reconcile.rs',
             source / 'service/predict_fun/account.rs',
             source / 'service/venue.rs', source / 'config.rs']
    paths += sorted((source / 'service/predict_fun/account').rglob('*.rs'))
    reasons = set()
    for path in paths:
        if not path.exists():
            continue
        text = strip_tests(path.read_text(errors='ignore'))
        for pattern in (r'(?:bail!|anyhow!)\("([A-Z][A-Z0-9_:]{2,})"\)',
                        r'VenueError::new\(\s*VenueErrorCode::[A-Za-z]+\s*,\s*"[a-z_]+"\s*,'
                        r'\s*"([A-Z][A-Z0-9_:]*)"',
                        r'(?:read_error|mismatch)\("([A-Z][A-Z0-9_:]{2,})"\)',
                        r'Failure::(?:usage|input|credentials|read|reconcile|output)\('
                        r'"([A-Z][A-Z0-9_:]*)"',
                        r'Failure::new\(\s*"[A-Z_]+"\s*,\s*"[a-z_]+"\s*,\s*"([A-Z][A-Z0-9_:]*)"',
                        r'"(PUBLIC_CONFIG|CONTROLLED_MODE):[A-Z][A-Z0-9_]*"'):
            reasons.update(match.group(1) for match in re.finditer(pattern, text))
    reasons |= {"UNCLASSIFIED", "READ_FAILED"}
    secret = ("PRIVATE_KEY", "API_KEY", "API_SECRET", "API_PASSPHRASE", "PASSWORD",
              "PASSPHRASE", "JWT", "BEARER", "SECRET", "SIGNATURE_TYPE", "MNEMONIC",
              "SEED", "CREDENTIAL", "NONCE", "TIMESTAMP")
    return {reason for reason in reasons if not any(part in reason for part in secret)}


def test_the_native_reason_vocabulary_matches_the_pinned_sources():
    """The allowlist is the sources' own vocabulary, not a guess at its shape."""
    assert signer.NATIVE_REASONS == source_reasons()


def test_the_native_vocabulary_carries_no_secret_shaped_token():
    """No allowlist entry names a credential, a key or a signature."""
    secret = ("PRIVATE_KEY", "API_KEY", "API_SECRET", "PASSWORD", "PASSPHRASE", "JWT",
              "BEARER", "SECRET", "MNEMONIC", "SEED", "CREDENTIAL", "NONCE")
    for entry in (signer.NATIVE_CODES | signer.NATIVE_STEPS | signer.NATIVE_REASONS):
        assert not any(part in entry for part in secret), entry


def test_the_allowlists_are_finite_and_have_no_wildcards():
    """Every entry is an exact token: no prefix, glob or case pattern."""
    for entry in signer.NATIVE_CODES | signer.NATIVE_REASONS:
        assert entry and entry.strip() == entry
        assert not any(char in entry for char in "*?[]^$\\")
        assert entry == entry.upper()
    for entry in signer.NATIVE_STEPS:
        assert entry and entry.strip() == entry
        assert not any(char in entry for char in "*?[]^$\\")
        assert entry == entry.lower()
