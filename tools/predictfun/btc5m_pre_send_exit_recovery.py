"""Pure qualification of the one frozen pre-send exit. This is not a Final."""
from __future__ import annotations

import datetime as dt
import hashlib
import re
from pathlib import PurePosixPath

import btc5m_live_events as live

RUN = "btc5m-live-4d0577d73dee4106"
PRIOR = "btc5m-live-6baf1ef8d6724dfb"
KIND = "PRE_SEND_EXIT"
C0 = 80760100000000000001
MIN_GAS = 2_000_000_000_000_000
SHA = re.compile(r"[0-9a-f]{64}")
ORDER = re.compile(r"0x[0-9a-fA-F]{64}")
HISTORY = "HISTORY_REQUIRES_TAKEOVER"
IDENTITY = "HISTORY_IDENTITY"
E = "data/reports/2026-11-27-btc5m-controlled-paper/evidence"
R = f"{E}/{RUN}"
A = f"{E}/launch-attempts/{RUN}"
F = "data/reports/2026-11-27-btc5m-fact-incomplete-stop/evidence"
BOT = "skills/trading/auto-trading/prediction-markets/Prediction-Markets-Trading-Bot-Toolkits"
AUTH = "btc5m-auth-20261127141559-1795760159667967068-e9ef582f"
WRAPPER = f"{E}/wrapper/20261127T141559-2447-13708-{AUTH}/dispatch.json"
PROD = PurePosixPath("/home/pascal/workspace/yquant-investment")  # archived argv only
OLD_BINDING = f"{F}/closed-reread-bound-candidate/binding.json"
NEW_BINDING = f"{F}/context-root-production-binding.json"
VALIDATION = f"{F}/context-root-fixed-candidate/validation.md"
INTAKE = f"{F}/pre-send-exit-recovery-intake.json"
INTAKE_SHA = "a98ce32fa6b45395ed79f1e46e7dbccfc95e8ce07099ffdf8f4ea2833456e729"
OLD_TRADE = f"{F}/closed-reread-bound-candidate/polymarket-toolkits"
READER = f"{F}/closed-reread-bound-candidate/btc5m_reconcile"
NEW_TRADE = f"{F}/context-root-fixed-candidate/polymarket-toolkits"
ARTIFACTS = {
    OLD_TRADE: "06954cb544d700c5c18933018aca949d897e67182c5a28668a2cb9b8834e8543",
    READER: "178b251f0e23fdbc6a282471418b75aeecccc20a155ecbce316bd7c23b259f08",
    NEW_TRADE: "66f1286dae49bfc9b4ce2d7e66897c42d1034d20cccd341bfeac8f65d3482f21",
}
SHELL = "scripts/start-btc5m-live.sh"
CAUSAL_SOURCES = {
    "src/main.rs": "87d586b342e99ca9a7fab620cb62b96209d8a5b482c8f3cbb853c88651a79d3b",
    "src/config/controlled_live.rs": "3f4b297a207c65207766d4ad961289bd6812b9bc7298c087d370eeb026138883",
}
EXTRA = {
    f"{A}/superseded-activation.json": "70b61dba63eaa16b23dcfa8fe0adad062315be725bd24521d55b4c4f30793d5b",
    f"{A}/barrier-before-rotation.json": "8c495c297332114815286fbec6dea1f64b264c36851fcaa62fde3ff69d44ff51",
    f"{E}/launch-attempts/{PRIOR}/attempt-reserved.json": "7d834efcb31b2f640da05cf8a33292095266a0b5b9a59aba352008a0d1e95276",
    **{f"{A}/{name}": "04f0d01e1abc9ceb42687bc4041a2afb6403ed998af5e507e4f879c938424ff6"
       for name in ("account-read-config.json", "public-config.json")},
    WRAPPER: "5bd1cd098de01cdd73bd9011cd68752bc17de79994d61a0ae61117fc0001e05a",
    VALIDATION: "c4147bc64ac4dc6e587bb966ba79a83ec49f42e3bf392416de9dc981f64189fc",
    SHELL: "d34fc27601941c242f3c9dce34c4b98c307b80c7de45891d0e4f89dbbf30cd66",
    **{f"{BOT}/{path}": digest for path, digest in CAUSAL_SOURCES.items()},
}
METADATA = ("started.json", "ended.json", "public-config.json", "live-context.json")
FROZEN = ({f"{R}/{name}" for name in (*METADATA, "events.jsonl", "run-log.txt")}
          | {f"{A}/{name}.json" for name in ("attempt-reserved", "barrier-rotation", "launch-dispatch",
             "account-read-recovery", "reconciliation-pass-1", "reconciliation-pass-2", "reconciliation")}
          | {OLD_BINDING, NEW_BINDING})
BYTE_PATHS = FROZEN | EXTRA.keys() | {INTAKE}
START = "2026-11-27T06:17:16.121Z"
DEADLINE = "2026-11-28T06:17:16.121Z"
DIGEST = "dcd196c3a0bfd80bac619b48d57676783dcfe10fc8b8e7af1f48d24e794f7c0d"
ACTIVATION_SHA = "beb2fc8d07a82ee950484e24520a084f9e47225245f04703e27f43d95aee6f13"
HISTORY_KEYS = ("claimed_win", "settled_zero")


class Rejected(Exception):
    """An existing public reason, without evidence values."""

    @property
    def reason(self) -> str:
        return self.args[0]


def _require(condition: bool, reason: str = IDENTITY) -> None:
    if not condition:
        raise Rejected(reason)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _equal(left: object, right: object) -> bool:
    # Unlike Python container equality, distinguish true from 1 and 1 from 1.0.
    return live.canonical(left) == live.canonical(right)


def _time(text: str) -> dt.datetime:
    value = dt.datetime.fromisoformat(text)
    if value.tzinfo is None:
        raise ValueError("missing timezone")
    return value


def _millis(value: dt.datetime) -> dt.datetime:
    return value.replace(microsecond=value.microsecond // 1000 * 1000)


def _atoms(text: str) -> int:
    if type(text) is not str or not re.fullmatch(r"[0-9]+", text):
        raise ValueError("not atoms")
    return int(text)


def history(account: object) -> dict[str, list[bytes]]:
    """Canonical records with multiplicity preserved; never default missing lists."""
    if type(account) is not dict or any(type(account.get(key)) is not list
            or any(type(row) is not dict for row in account[key]) for key in HISTORY_KEYS):
        raise Rejected("RECONCILIATION_FACTS")
    return {key: sorted(live.canonical(row) for row in account[key]) for key in HISTORY_KEYS}


def fresh_history_matches(basis: dict, value: dict) -> bool:
    """Compare both complete multisets against the frozen history."""
    return history(value.get("account")) == basis["history"]


def sample(value: dict, *, earliest: dt.datetime, now: dt.datetime,
           max_age: int) -> tuple[dt.datetime, dt.datetime]:
    """Validate a complete sample envelope and bind observation to its call."""
    fact = "RECONCILIATION_FACTS"
    try:
        _require(value["schema_version"] == "btc5m-reconciliation-read.v2"
                 and value["venue"] == "predict_fun", fact)
        account = value["account"]
        _require(account.get("positions") == [] and account.get("open_orders") == [], fact)
        obs = value["observation"]
        _require(type(obs["chain_id"]) is int and obs["chain_id"] == 56
                 and obs["anchor_semantics"] == "NATIVE_FIXED_BLOCK_WITHIN_OBSERVED_RANGE", fact)
        before, after = obs["block_before"], obs["block_after"]
        for point in (before, after):
            _require(type(point["number"]) is int and point["number"] > 0
                     and ORDER.fullmatch(point["hash"]) is not None, fact)
        start, finish = _time(obs["started_at"]), _time(obs["finished_at"])
        _require(before["number"] <= after["number"]
                 and _time(before["time_utc"]) <= _time(after["time_utc"]) <= finish, fact)
    except (LookupError, TypeError, ValueError, AttributeError):
        raise Rejected(fact) from None
    _require(max(_millis(earliest), now - dt.timedelta(seconds=max_age)) <= start <= finish <= now,
             "STALE_RECONCILIATION")
    return start, finish


def _bound_digests(evidence: dict[str, bytes], artifacts: dict[str, tuple[str, int]]) -> dict:
    _require(evidence.keys() == BYTE_PATHS and artifacts.keys() == ARTIFACTS.keys())
    _require(_sha(evidence[INTAKE]) == INTAKE_SHA)
    intake = live.strict_json(evidence[INTAKE])
    frozen = intake["source_sha256"]
    _require(type(frozen) is dict and frozen.keys() == FROZEN
             and all(type(digest) is str and SHA.fullmatch(digest) for digest in frozen.values())
             and intake["run_id"] == RUN and intake["archived_audit_status"] == "INCOMPLETE")
    expected = {**frozen, **EXTRA, INTAKE: INTAKE_SHA}
    _require(all(_sha(evidence[path]) == digest for path, digest in expected.items()))
    _require(all(artifacts[path] == (digest, 0o500) for path, digest in ARTIFACTS.items()))
    return {**expected, **ARTIFACTS}


def _window(started: dict, context: dict, ended: dict, existing: dict, old_bytes: bytes,
            old_activation: dict, binding: dict, previous_activation: dict, now: dt.datetime,
            dispatched: dt.datetime) -> None:
    account = started["account_identity"]
    mode = "unattended"
    _require(live.candidate_digest(started) == DIGEST and _sha(old_bytes) == ACTIVATION_SHA
             and binding["activation_sha256_unchanged"] == ACTIVATION_SHA)
    common = dict(run_id=RUN, risk_window_id=RUN, authorization_ref=AUTH,
                  started_at=START, deadline_at=DEADLINE, operations_mode=mode,
                  recovery_policy="COLD_START_REFUSE", cash_recovery_policy="MANUAL_PAUSE")
    _require(all(started[key] == context[key] == value for key, value in common.items())
             and context["candidate_digest"] == DIGEST
             and started["strategy_contract"] == "phase1_net_shares"
             and _equal(context["asset_units"], started["asset_units"]))
    _require(_equal({key: existing[key] for key in ("run_id", "risk_window_id", "authorization_ref",
                    "candidate_digest", "state")}, dict(run_id=RUN, risk_window_id=RUN,
                    authorization_ref=AUTH, candidate_digest=DIGEST, state="ARMED"))
             and _equal({key: v for key, v in existing["scope"].items() if key != "host"}, account)
             and _millis(dispatched) <= _time(existing["armed_at"]) <= min(now, _time(DEADLINE)))
    _require(_equal(old_activation, dict(schema_version="btc5m-live-activation/1",
        authorization_ref=AUTH, risk_window_id=RUN, candidate_digest=DIGEST,
        not_before=START, deadline_at=DEADLINE, operations_mode=mode,
        account_identity=account, gas_policy=context["gas_policy"], takeover_ref=AUTH,
        storage_binding=previous_activation["storage_binding"])))
    _require(_equal(ended, dict(schema_version="btc5m-controlled-live.v1", run_id=RUN,
        audit_complete=False, cash_complete=False, execution_complete=False, takeover_required=True,
        final_accounting=None, final_event_seq=None, incomplete_detail=None,
        incomplete_reason="AUDIT_UNAVAILABLE", exit_facts=dict(stop_reason="PROCESS_FAILED",
        launcher_exit=1, child_returncode=1, child_signal=None, forced_cleanup=False))))


def _causal_binding(started: dict, old: dict, new: dict) -> None:
    identity = started["candidate_identity"]
    files = identity["files"]
    _require(_equal(identity, old["public_identity"])
             and identity["main_head"] == "08beb4d11092192f87a89352f32f0233c9cb9558"
             and identity["bot_head"] == "5616a56e8d7540783ebdc40d6245230f1a7729bb"
             and identity["binary_sha256"] == ARTIFACTS[OLD_TRADE]
             and all(files[path] == digest for path, digest in CAUSAL_SOURCES.items())
             and all(SHA.fullmatch(files[name]) for name in
                     ("Cargo.lock", "Cargo.toml", "tools/predictfun/btc5m_control.py"))
             and old["source_identity_sha256"] == "c513f25437e59d323b7f9828e8b338f7e1e77e0f2e8537ef0e3fa1e8e37aefdc"
             and old["shell_entrypoint_sha256"] == EXTRA[SHELL])
    for path in (OLD_TRADE, READER):
        record = old["selected"][PurePosixPath(path).name]
        _require((record["path"], record["sha256"], record["mode"])
                 == (str(PROD / path), ARTIFACTS[path], "0500"))
    trade = new["trade_binary"]
    _require((trade["previous_path"], trade["previous_sha256"], trade["selected_path"],
              trade["selected_sha256"], trade["mode"], trade["cargo_manifest_dir"])
             == (str(PROD / OLD_TRADE), ARTIFACTS[OLD_TRADE], str(PROD / NEW_TRADE),
                 ARTIFACTS[NEW_TRADE], "0500", str(PROD / BOT))
             and new["failed_window"] == RUN
             and _equal(new["reconciler_unchanged"], dict(path=str(PROD / READER), sha256=ARTIFACTS[READER]))
             and _equal(new["entrypoint"], dict(path=str(PROD / SHELL), sha256=EXTRA[SHELL])))
    _require(_equal(new["independent_offline_validate"], dict(task="t_6b2c9b97", run=4179,
                    sha256=EXTRA[VALIDATION], report=str(PROD / VALIDATION)))
             and _equal(new["native_probe"]["exit"], 0)
             and _equal(new["native_probe"]["cases"], {
                 label: dict(native_exit=1, reason=reason) for label, reason in
                 (("old_canonical", "LIVE_CONTEXT_PATH"), ("fixed_canonical", "CONTROL_CHANNEL"),
                  ("fixed_wrong", "LIVE_CONTEXT_PATH"))}))


def _invocation(dispatch: dict, wrapper: dict, seed: dict, account: dict,
                orders: list[str], start: dt.datetime) -> dt.datetime:
    fixed = ["--mode", "signed-future-live", "--config", str(PROD / A / "public-config.json"),
             "--run-id", RUN, "--predict-budget-confirmation",
             f"56:{account['trading_address']}:predict-account", "--predict-rpc-url",
             "https://bsc-dataseed.bnbchain.org", "--live-contract", "phase1_net_shares",
             "--operations-mode", "unattended", "--risk-window-id", RUN,
             "--authorization-ref", AUTH, "--deadline-at", DEADLINE]
    argv = fixed + [arg for order in orders for arg in ("--predict-reconciled-order", order)]
    _require(_equal(dispatch, dict(controller_argv=argv, automatic_retry=False, run_id=RUN,
              launcher_pid=dispatch["launcher_pid"], dispatched_utc=dispatch["dispatched_utc"],
              operations_mode="unattended", deadline_at=DEADLINE, controller_invocations=1,
              binary_sha256=ARTIFACTS[OLD_TRADE]))
             and type(dispatch["launcher_pid"]) is int and dispatch["launcher_pid"] > 0)
    prefix = PROD / BOT / "tools/predictfun"
    _require(wrapper["cwd"] == str(PROD) and wrapper["authorization_ref"] == AUTH
             and _equal(wrapper["pid"], dispatch["launcher_pid"])
             and wrapper["argv"] == [str(PROD / ".venv/bin/python"), str(prefix / "btc5m_launch.py"),
                 "--auto-prepare", "--operations-mode", "unattended", "--authorization-ref", AUTH,
                 "--config", str(prefix / "btc5m-controlled-paper.json")])
    reserve, dispatched = _time(seed["reserved_utc"]), _time(dispatch["dispatched_utc"])
    # The controller constructs the child manifest from UTC+08 and the fixed run.
    date = dispatched.astimezone(dt.timezone(dt.timedelta(hours=8))).date()
    manifest = PROD / f"data/reports/{date}-btc5m-controlled-paper/evidence/{RUN}/live-context.json"
    _require(_time(wrapper["dispatched_at"]) < reserve <= dispatched and _millis(reserve) == start
             and manifest == PROD / R / "live-context.json")
    return dispatched


def _old_reads(first: dict, second: dict, summary: dict, proof: dict, orders: list[str],
               account: dict, hashes: dict, reserve: dt.datetime, earliest: dt.datetime,
               max_age: int) -> dict:
    _require(_equal(summary, {**second, "finished_at": second["observation"]["finished_at"]})
             and proof["old_run_id"] == PRIOR and _atoms(proof["next_budget_atoms"]) == C0
             and proof["fresh_read_sha256"] == [hashes[f"{A}/reconciliation-pass-{i}.json"]
                                                for i in (1, 2)], HISTORY)
    expected = history(proof["expected_history"])
    try:
        for value in (first, second):
            _, earliest = sample(value, earliest=earliest, now=reserve, max_age=max_age)
            identity = value["account_identity"]
            native_scope = {**identity, "venue": "PREDICT_FUN"}
            live.check_account_identity(native_scope)
            for key in ("trading_address", "signer_address"):
                native_scope[key] = native_scope[key].lower()
            facts = value["account"]
            _require(_equal(native_scope, account) and value["read_only"] is True
                     and _equal(value["reconciliation"], dict(complete=True,
                          positions_match_history=True, declared_orders=orders))
                     and facts["pagination_complete"] is True and facts["positions_explained"] is True
                     and _atoms(facts["gas_atoms"]) >= MIN_GAS and history(facts) == expected
                     and _atoms(facts["collateral_atoms"]) == C0 + sum(
                         _atoms(row["claim_atoms"]) for row in facts["claimed_win"]), HISTORY)
    except (Rejected, live.LiveInvalid):
        raise Rejected(HISTORY) from None
    left, right = (value["account"] for value in (first, second))
    _require(_equal({k: v for k, v in left.items() if k not in HISTORY_KEYS},
                    {k: v for k, v in right.items() if k not in HISTORY_KEYS}), HISTORY)
    return expected


def qualify(evidence: dict[str, bytes], artifacts: dict[str, tuple[str, int]], *,
            existing: dict, old_bytes: bytes, now: dt.datetime, max_age: int) -> dict:
    """Return the internal anchored-read basis or refuse without any external I/O."""
    try:
        hashes = _bound_digests(evidence, artifacts)
        return _qualify_bound(evidence, hashes, existing, old_bytes, now, max_age)
    except (live.LiveInvalid, LookupError, TypeError, ValueError, AttributeError, OverflowError):
        raise Rejected(IDENTITY) from None


def _qualify_bound(evidence, hashes, existing, old_bytes, now, max_age):
    def load(path: str) -> dict:
        return live.strict_json(evidence[path])

    started, ended, public, context = (load(f"{R}/{name}") for name in METADATA)
    live.check_metadata(started, public, context)
    new_binding = load(NEW_BINDING)
    _causal_binding(started, load(OLD_BINDING), new_binding)
    seed, dispatch, wrapper = load(f"{A}/attempt-reserved.json"), load(f"{A}/launch-dispatch.json"), load(WRAPPER)
    orders = seed["declared_orders"]
    _require(type(orders) is list and len(orders) == 25
             and all(type(order) is str and ORDER.fullmatch(order) for order in orders)
             and len({order.lower() for order in orders}) == 25, HISTORY)
    dispatched = _invocation(dispatch, wrapper, seed, started["account_identity"], orders, _time(START))
    _window(started, context, ended, existing, old_bytes, live.strict_json(old_bytes), new_binding,
            load(f"{A}/superseded-activation.json"), now, dispatched)
    # Exact stderr is only admissible after the old binary/source, wrapper and
    # controller path have been bound. In that source, read_live_context's path
    # error returns through main's `?` before pipe/activation/config/bot::run.
    _require(evidence[f"{R}/events.jsonl"] == b"" and evidence[f"{R}/run-log.txt"]
             == b"[stderr] Error: CONTROLLED_MODE: LIVE_CONTEXT_PATH\n\n")
    rotation, old_guard = load(f"{A}/barrier-rotation.json"), load(f"{A}/barrier-before-rotation.json")
    _require(_equal(rotation, dict(previous_run_id=PRIOR, new_run_id=RUN,
                      deleted_count=1, readback_absent=True))
             and old_guard["_id"] == existing["_id"] and old_guard["state"] == "ARMED"
             and old_guard["run_id"] == PRIOR and old_guard["schema_version"] == existing["schema_version"]
             and all(old_guard["scope"][key] == value for key, value in started["account_identity"].items()))
    proof = load(f"{A}/account-read-recovery.json")
    prior_seed = load(f"{E}/launch-attempts/{PRIOR}/attempt-reserved.json")
    _require(prior_seed["run_id"] == PRIOR and orders[:17] == prior_seed["declared_orders"]
             and orders[17:] == proof["window_order_hashes"]
             and public["controlled_run"]["budget_usdt"] == "80.760100000000000001"
             and all(evidence[f"{A}/{name}"] == evidence[f"{R}/public-config.json"]
                     for name in ("account-read-config.json", "public-config.json"))
             and _equal(seed, dict(run_id=RUN, reserved_utc=seed["reserved_utc"],
                    committed_budget_atoms=C0, declared_orders=orders, maximum_controller_invocations=1,
                    automatic_retry=False, reconciliation_sha256=hashes[f"{A}/reconciliation.json"])), HISTORY)
    basis = _old_reads(load(f"{A}/reconciliation-pass-1.json"), load(f"{A}/reconciliation-pass-2.json"),
        load(f"{A}/reconciliation.json"), proof, orders, started["account_identity"], hashes,
        _time(seed["reserved_utc"]), _time(wrapper["dispatched_at"]), max_age)
    return {"kind": KIND, "orders": orders, "budget_atoms": C0, "unresolved": [], "recovery": {
        "history": basis, "old_run_id": RUN, "old_risk_window_id": RUN, "old_authorization_ref": AUTH,
        "old_candidate_digest": DIGEST, "budget_atoms": str(C0), "declared_orders": orders,
        "source_sha256": hashes, "old_activation_sha256": ACTIVATION_SHA,
        "old_read_sha256": proof["fresh_read_sha256"], "old_audit_status": "INCOMPLETE"}}
