"""Offline CLI/SDK regressions: synthetic contracts, never an account or network.

FakeSession only replaces the read context. signed_builder returns the installed
OrderBuilder, so even the CLI success path runs its actual funds methods.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

TOOL_ROOT = Path(__file__).resolve().parents[1]
if str(TOOL_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOL_ROOT))
import cli
import sdk_ops
from sdk_ops import ErrorCode, ToolError

OWNER = "0x" + "11" * 20
CONDITION = "0x" + "55" * 32
RAW = b"offline-fake-not-a-valid-transaction"


def arguments(command="plan", operation="approve", **changes):
    """Boolean flags are standalone switches, never key/value pairs."""
    values = {"--chain-id": "97", "--owner": OWNER, "--account-mode": "eoa",
              "--operation": operation, "--market-id": "7", "--side": "buy",
              "--is-neg-risk": "false", "--is-yield-bearing": "false",
              "--amount-atoms": "10"}
    if operation == "approve":
        values["--decimals"] = "18"
    else:
        values.update({"--condition-id": CONDITION, "--index-set": "1",
                       "--redeem-full-index-set": True})
    values.update(changes)
    argv = [command] if command else []
    for key, value in values.items():
        if value is None or value is False:
            continue
        argv.append(key)
        if value is not True:
            argv.append(str(value))
    return argv


class FakeSession:
    """Strict, per-test reads and lower transport beneath a real SDK builder."""
    def __init__(self, target, rpc_url, fault=None, allowance=0):
        self.target, self.fault, self.allowance = target, fault, allowance
        self.calls, self.reads, self.sends = [], 0, 0
        self.target_id = 123

    def identity(self):
        self.calls.append("identity")
        if self.fault == "identity":
            raise ToolError(ErrorCode.ACCOUNT_MISMATCH, "identity", "WRONG_CHAIN")
        return OWNER

    def market(self):
        self.calls.append("market")
        if self.fault == "market":
            raise TimeoutError("SECRET_SENTINEL")
        return {"id": 7}

    def gas_available(self, controller):
        if self.fault == "gas":
            raise ToolError(ErrorCode.READ_UNAVAILABLE, "gas", "NO_NATIVE_GAS")

    def approval_value(self, step):
        self.calls.append("read")
        if self.sends and self.fault == "readback":
            raise TimeoutError("SECRET_SENTINEL")
        return self.target.get("amount_atoms", True) if self.sends else self.allowance

    def redemption_before(self):
        self.reads += 1
        if self.fault == "partial":
            raise ToolError(ErrorCode.INVALID_INPUT, "redeem", "PARTIAL_OR_INSUFFICIENT_BALANCE")
        balance = 10 + int(self.fault == "changed" and self.reads > 1)
        return {"balance": balance, "cash": 100, "token_id": self.target_id, "payout": 5}

    def signed_builder(self, signer):
        import predict_sdk
        from web3 import Web3
        session = self

        class Method:
            def estimate_gas(self, args):
                return 21000

            def build_transaction(self, args):
                return {**args, "chainId": 97}

        class Functions:
            def __getattr__(self, name):
                def invoke(*args):
                    session.calls.append((name, args))
                    return Method()
                return invoke

        class Eth:
            def get_transaction_count(self, *args):
                return 0

            def send_raw_transaction(self, raw):
                session.sends += 1
                session.calls.append("send")
                if session.fault == "send":
                    raise TimeoutError("SECRET_SENTINEL")
                return Web3.keccak(raw)

            def wait_for_transaction_receipt(self, tx_hash, timeout):
                if session.fault == "receipt":
                    raise TimeoutError("SECRET_SENTINEL")
                return session.receipt(tx_hash)

        contract = SimpleNamespace(address=Web3.to_checksum_address(self.target.get("token", "0x" + "22" * 20)), functions=Functions())
        builder = predict_sdk.OrderBuilder.make(predict_sdk.ChainId.BNB_TESTNET)
        builder._contracts = SimpleNamespace(
            usdt=contract, conditional_tokens=contract, neg_risk_adapter=contract,
            neg_risk_conditional_tokens=contract, yield_bearing_neg_risk_conditional_tokens=contract,
            yield_bearing_conditional_tokens=contract, yield_bearing_neg_risk_adapter=contract,
        )
        builder._signer, builder._web3 = signer, SimpleNamespace(eth=Eth())
        return builder

    def receipt(self, tx_hash):
        if self.fault == "receipt":
            raise TimeoutError("SECRET_SENTINEL")
        status = None if self.fault == "pending" else int(self.fault != "revert")
        return {"status": status, "transactionHash": tx_hash}

    def verify_receipt(self, receipt, tx_hash):
        if receipt["status"] != 1:
            raise ToolError(ErrorCode.TX_UNCONFIRMED, "receipt", "HASH_OR_STATUS")

    def redemption_readback(self, receipt, before):
        return self.fault != "readback"


class AbiSession(FakeSession, sdk_ops.ReadSession):
    """Real SDK contracts and Web3 method lookup; only eth_call is fake."""
    def __init__(self, target, rpc_url, fault=None, allowance=0):
        super().__init__(target, rpc_url, fault, allowance)
        read = sdk_ops.ReadSession(target, rpc_url or "https://offline.invalid")
        self.web3, self.contracts, self.owner = read.web3, read.contracts, read.owner
        self.abi_calls = []
        self.web3.eth.call = self.fake_call

    def fake_call(self, transaction, *args, **kwargs):
        data = transaction["data"]
        self.abi_calls.append((transaction["to"].lower(), data[:10]))
        if data.startswith("0x313ce567"):
            if self.fault == "decimals_read":
                raise TimeoutError("SYNTHETIC_DECIMALS_UNAVAILABLE")
            value = 6 if self.fault == "decimals" else 18
        elif data.startswith("0xdd62ed3e"):
            value = self.target["amount_atoms"] if self.sends else self.allowance
        else:
            raise AssertionError(f"UNEXPECTED_ABI_CALL {data[:10]}")
        return value.to_bytes(32, "big")

    approval_value = sdk_ops.ReadSession.approval_value


class ToolTests(unittest.TestCase):
    def setUp(self):
        self.network = patch("socket.socket.connect", side_effect=AssertionError("NETWORK_FORBIDDEN"))
        self.real_signing = patch("eth_account.signers.local.LocalAccount.sign_transaction",
                                  side_effect=AssertionError("REAL_SIGNING_FORBIDDEN"))
        self.network.start()
        self.real_signing.start()
        self.addCleanup(self.network.stop)
        self.addCleanup(self.real_signing.stop)
        self.sessions, self.signs, self.loads = [], 0, 0

    def execute(self, operation="approve", fault=None, allowance=0, session_class=FakeSession, **changes):
        from web3 import Web3
        plan = cli.run(arguments(operation=operation, **changes))
        test = self

        class Signer:
            address = OWNER

            def sign_transaction(self, transaction):
                test.signs += 1
                test.sessions[-1].calls.append("sign")
                return SimpleNamespace(raw_transaction=RAW)

        def load(fd):
            self.loads += 1
            return Signer()

        def factory(target, rpc):
            session = session_class(target, rpc, fault, allowance)
            self.sessions.append(session)
            return session

        result = cli.run(arguments(operation, operation, **{
            **changes, "--execute": True, "--confirm-target": sdk_ops.target_digest(plan.target),
            "--rpc-url": "https://offline.invalid", "--signer-fd": "7",
        }), session_factory=factory, signer_loader=load)
        expected_hash = "0x" + Web3.keccak(RAW).hex().removeprefix("0x")
        return result, expected_hash

    def test_a002_plan_and_readonly_check_never_load_signer(self):
        plan = cli.run(arguments())
        self.assertTrue(plan.steps)
        self.assertFalse(plan.completed)
        with patch("sdk_ops.fd_signer_loader", side_effect=AssertionError("SECRET_READ")):
            with self.assertRaises(ToolError) as caught:
                cli.run(arguments("check"))
        self.assertEqual(caught.exception.code, ErrorCode.READ_UNAVAILABLE)
        target = plan.target
        session = FakeSession(target, None, allowance=10)
        checked = cli.run(arguments("check"), session_factory=lambda *_: session)
        self.assertTrue(checked.completed)
        self.assertEqual(session.sends, 0)
        self.assertEqual(self.loads, 0)

    def test_a002_missing_target_is_an_input_error(self):
        for argv in ([], ["plan"], ["check"], ["--owner", OWNER], ["approve", "--execute"]):
            with self.subTest(argv=argv), self.assertRaises(ToolError) as caught:
                cli.run(argv)
            self.assertEqual(caught.exception.code, ErrorCode.INVALID_INPUT)

    def test_a002_cli_plan_has_no_subcommand_defaults_to_plan(self):
        self.assertEqual(cli.run(arguments(None)).target, cli.run(arguments()).target)
        with self.assertRaises(ToolError) as caught:
            cli.run(arguments(None, **{"--market-id": None}))
        self.assertEqual(caught.exception.code, ErrorCode.INVALID_INPUT)

    def test_a002_finite_allowance_uses_raw_required_amount(self):
        target = cli.run(arguments()).target
        for value in (0, 9, 10, 11):
            session = AbiSession(target, None, allowance=value)
            step = sdk_ops.select_step(target, sdk_ops.approval_steps(target))
            reading = session.approval_value(step)
            self.assertEqual(reading, value)
            self.assertEqual(sdk_ops._satisfied(target, reading), value >= 10)
            self.assertEqual(session.abi_calls, [(step.token.lower(), "0x313ce567"),
                                                (step.token.lower(), "0xdd62ed3e")])

    def test_fix_r1_real_abi_check_and_approve_decimals(self):
        for fault in (None, "decimals", "decimals_read"):
            with self.subTest(fault=fault):
                target = cli.run(arguments()).target
                session = AbiSession(target, None, fault, allowance=10)
                if fault:
                    with self.assertRaises(ToolError):
                        cli.run(arguments("check"), session_factory=lambda *_: session)
                    with self.assertRaises(ToolError):
                        self.execute(session_class=AbiSession, fault=fault)
                    self.assertEqual(self.signs, 0)
                    self.assertEqual(self.loads, 0)
                    self.assertEqual(self.sessions[-1].sends, 0)
                else:
                    self.assertTrue(cli.run(arguments("check"), session_factory=lambda *_: session).completed)
        result, _ = self.execute(session_class=AbiSession)
        self.assertTrue(result.completed)
        self.assertEqual(self.signs, 1)
        self.assertEqual(self.sessions[-1].sends, 1)

    def test_a011_explicit_approval_and_redeem_routes(self):
        for operation in ("approve", "redeem"):
            result, expected = self.execute(operation)
            self.assertTrue(result.completed)
            self.assertEqual(result.expected_tx_hash, expected)
            self.assertEqual(self.sessions[-1].sends, 1)
            calls = self.sessions[-1].calls
            self.assertLess(calls.index("read") if operation == "approve" else 0, calls.index("sign"))
            self.assertLess(calls.index("sign"), calls.index("send"))
        for flags, code in (({}, ErrorCode.EXECUTION_NOT_ENABLED),
                            ({"--execute": True}, ErrorCode.EXECUTION_NOT_ENABLED),
                            ({"--execute": True, "--confirm-target": "bad"}, ErrorCode.INVALID_INPUT)):
            with self.assertRaises(ToolError) as caught:
                cli.run(arguments("approve", **flags))
            self.assertEqual(caught.exception.code, code)

    def test_a011_standard_redeem_amount_not_silently_ignored(self):
        with self.assertRaises(ToolError) as caught:
            self.execute("redeem", fault="partial")
        self.assertEqual(caught.exception.reason, "PARTIAL_OR_INSUFFICIENT_BALANCE")
        self.assertEqual(self.loads, 0)
        with self.assertRaises(ToolError) as caught:
            cli.run(arguments(operation="redeem", **{"--redeem-full-index-set": None}))
        self.assertEqual(caught.exception.reason, "MISSING_FULL_INDEX_SET_CONSENT")
        with self.assertRaises(ToolError) as caught:
            self.execute("redeem", fault="changed")
        self.assertEqual(caught.exception.reason, "BALANCE_CHANGED")
        self.assertEqual(self.signs, 0)

    def test_a011_unbounded_erc20_amount_is_refused(self):
        for raw in (str(sdk_ops.MAX_UINT256), str(sdk_ops.MAX_INT256), "0", "-1", "1.1", "١"):
            with self.subTest(raw=raw), self.assertRaises(ToolError):
                cli.run(arguments(**{"--amount-atoms": raw}))

    def test_a011_noop_does_not_load_a_signer(self):
        result, _ = self.execute(allowance=10)
        self.assertTrue(result.completed)
        self.assertEqual(result.readback_status, "already_satisfied_noop")
        self.assertEqual(self.loads, 0)
        self.assertEqual(self.sessions[-1].sends, 0)

    def test_a011_neg_risk_yield_routes_and_operator_consent(self):
        for neg_risk, yielding in (("true", "false"), ("false", "true"), ("true", "true")):
            result, _ = self.execute("redeem", **{
                "--is-neg-risk": neg_risk, "--is-yield-bearing": yielding,
                "--redeem-full-index-set": neg_risk == "false",
            })
            self.assertTrue(result.completed)
        base = {**cli.run(arguments()).target, "side": "sell"}
        operator_steps = [step for step in sdk_ops.approval_steps(base) if step.type == "ERC1155_APPROVAL"]
        self.assertTrue(operator_steps)
        chosen = operator_steps[0]
        changes = {"--side": "sell", "--amount-atoms": None, "--decimals": None,
                   "--operator": chosen.spender, "--token": chosen.token}
        with self.assertRaises(ToolError) as caught:
            cli.run(arguments(**changes))
        self.assertEqual(caught.exception.reason, "MISSING_OPERATOR_CONSENT")
        result, _ = self.execute(**{**changes, "--allow-operator": True})
        self.assertTrue(result.completed)

    def test_a012_receipt_readback_hash_and_redaction(self):
        for failure in ("send", "receipt", "pending", "revert", "readback"):
            with self.subTest(failure=failure):
                result, expected = self.execute(fault=failure)
                self.assertFalse(result.completed)
                self.assertEqual(result.expected_tx_hash, expected)
                self.assertEqual(self.sessions[-1].sends, 1)
                self.assertTrue(result.steps)
                self.assertNotIn("SECRET_SENTINEL", json.dumps(result.as_dict()))
                self.assertNotIn(RAW.decode(), json.dumps(result.as_dict()))

    def test_a012_before_read_failure_never_loads_signer(self):
        for fault in ("identity", "market", "gas"):
            with self.subTest(fault=fault), self.assertRaises(ToolError):
                self.execute(fault=fault)
        self.assertEqual(self.signs, 0)
        self.assertEqual(self.loads, 0)

    def test_a012_signer_identity_and_fd_refused(self):
        context = sdk_ops.ReadContext(OWNER, 97)
        for descriptor in (None, 0, 1, 2):
            with self.subTest(descriptor=descriptor), self.assertRaises(ToolError):
                sdk_ops.load_signer(lambda _: self.fail("must not load"), descriptor, context)
        with self.assertRaises(ToolError) as caught:
            sdk_ops.load_signer(lambda _: SimpleNamespace(address="0x" + "99" * 20), 7, context)
        self.assertEqual(caught.exception.code, ErrorCode.ACCOUNT_MISMATCH)

    def test_a012_receipt_recheck_cannot_send(self):
        tx_hash = "0x" + "66" * 32
        for fault in (None, "receipt", "pending", "revert"):
            target = cli.run(arguments()).target
            session = FakeSession(target, None, fault, allowance=10)
            result = cli.run(arguments("check", **{"--tx-hash": tx_hash}),
                             session_factory=lambda *_: session,
                             signer_loader=lambda _: self.fail("CHECK_LOADED_SIGNER"))
            self.assertEqual(result.expected_tx_hash, tx_hash)
            self.assertEqual(result.completed, fault is None)
            self.assertEqual(session.sends, 0)

    def test_target_digest_changes_with_scope(self):
        baseline = cli.run(arguments()).target
        digest = sdk_ops.target_digest(baseline)
        for changes in ({"--amount-atoms": "11"}, {"--owner": "0x" + "22" * 20},
                        {"--is-yield-bearing": "true"}, {"--market-id": "8"}):
            self.assertNotEqual(digest, sdk_ops.target_digest(cli.run(arguments(**changes)).target))
        self.assertEqual(sdk_ops.target_digest({"a": 1, "b": None}), sdk_ops.target_digest({"a": 1}))


class ContractReadTests(unittest.TestCase):
    """Exercise the actual ReadSession and SDK ABI decoders with synthetic chain logs."""

    def setUp(self):
        self.guard = patch("socket.socket.connect", side_effect=AssertionError("NETWORK_FORBIDDEN"))
        self.guard.start()
        self.addCleanup(self.guard.stop)

    def fixture(self, neg=False, yielding=False, batch=False):
        from eth_abi import encode
        from eth_utils import event_abi_to_log_topic
        from hexbytes import HexBytes
        from web3 import Web3

        target = cli.run(arguments(operation="redeem", **{
            "--is-neg-risk": str(neg).lower(), "--is-yield-bearing": str(yielding).lower(),
            "--redeem-full-index-set": not neg,
        })).target
        session = sdk_ops.ReadSession(target, "https://offline.invalid")
        actual_ct, actual_usdt = session.ct, session.contracts.usdt
        adapter = (session.contracts.yield_bearing_neg_risk_adapter if yielding
                   else session.contracts.neg_risk_adapter)
        state = {"balance": 10, "cash": 100, "denominator": 1 if neg else 2,
                 "payouts": [1, 0] if neg else [1, 1], "tokens": [123, 456], "collateral": None}

        def value(result):
            return SimpleNamespace(call=lambda: result)

        def position(collateral, collection):
            state["collateral"] = collateral.lower()
            return value(123)

        session.ct = SimpleNamespace(address=actual_ct.address, events=actual_ct.events, functions=SimpleNamespace(
            getOutcomeSlotCount=lambda *_: value(2), payoutDenominator=lambda *_: value(state["denominator"]),
            payoutNumerators=lambda _, index: value(state["payouts"][index]),
            getCollectionId=lambda *_: value(bytes(32)), getPositionId=position,
            balanceOf=lambda *_: value(state["balance"]),
        ))
        session.contracts.usdt = SimpleNamespace(address=actual_usdt.address, events=actual_usdt.events,
            functions=SimpleNamespace(balanceOf=lambda *_: value(state["cash"])))
        wrapped = "0x" + "77" * 20
        adapted = SimpleNamespace(address=adapter.address, events=adapter.events, functions=SimpleNamespace(
            col=lambda: value(actual_usdt.address), ctf=lambda: value(actual_ct.address),
            wcol=lambda: value(wrapped)))
        if yielding:
            session.contracts.yield_bearing_neg_risk_adapter = adapted
        else:
            session.contracts.neg_risk_adapter = adapted
        session.market = lambda: {"id": 7, "outcomes": [{"onChainId": str(token)} for token in state["tokens"]]}
        zero = "0x" + "00" * 20
        payout = 10 if neg else 7 if yielding else 5
        receipt = {"transactionHash": HexBytes("0x" + "66" * 32), "status": 1,
                   "from": OWNER, "to": adapter.address if neg else actual_ct.address, "logs": []}

        def add(event, emitter, values):
            abi = event.abi
            topics = [event_abi_to_log_topic(abi)]
            for entry in abi["inputs"]:
                if entry["indexed"]:
                    topics.append(encode([entry["type"]], [values[entry["name"]]]))
            nonindexed = [entry for entry in abi["inputs"] if not entry["indexed"]]
            data = encode([entry["type"] for entry in nonindexed],
                          [values[entry["name"]] for entry in nonindexed])
            receipt["logs"].append({"address": emitter, "topics": list(map(HexBytes, topics)),
                "data": HexBytes(data), "logIndex": len(receipt["logs"]), "transactionIndex": 0,
                "transactionHash": receipt["transactionHash"], "blockHash": HexBytes(bytes(32)), "blockNumber": 1})

        transfer_args = {"operator": OWNER, "from": OWNER, "to": adapter.address if neg else zero}
        if batch:
            add(actual_ct.events.TransferBatch(), actual_ct.address,
                {**transfer_args, "ids": [123], "values": [10]})
        else:
            add(actual_ct.events.TransferSingle(), actual_ct.address,
                {**transfer_args, "id": 123, "value": 10})
        if neg:
            add(adapter.events.PayoutRedemption(), adapter.address,
                {"redeemer": OWNER, "conditionId": bytes.fromhex(CONDITION[2:]),
                 "amounts": [10, 0], "payout": payout})
        else:
            add(actual_ct.events.PayoutRedemption(), actual_ct.address,
                {"redeemer": OWNER, "conditionId": bytes.fromhex(CONDITION[2:]),
                 "collateralToken": actual_usdt.address, "parentCollectionId": bytes(32),
                 "indexSets": [1], "payout": payout})
        add(actual_usdt.events.Transfer(), actual_usdt.address,
            {"from": adapter.address if neg else actual_ct.address, "to": OWNER, "value": payout})
        return session, state, receipt, add, payout

    def test_a011_actual_contract_payout_and_token_mapping(self):
        for neg, yielding in ((False, False), (True, False), (False, True), (True, True)):
            for batch in (False, True):
                with self.subTest(neg=neg, yielding=yielding, batch=batch):
                    session, state, receipt, _, payout = self.fixture(neg, yielding, batch)
                    before = session.redemption_before()
                    expected_collateral = "0x" + "77" * 20 if neg else session.contracts.usdt.address.lower()
                    self.assertEqual(state["collateral"], expected_collateral)
                    state.update(balance=0, cash=100 + payout)
                    self.assertTrue(session.redemption_readback(receipt, before))
                    self.assertTrue(session.redemption_readback(receipt, None))

    def test_a011_unsettled_partial_and_neg_risk_draw_refused(self):
        for fault in ("unsettled", "partial", "token", "neg-draw"):
            session, state, *_ = self.fixture(neg=fault == "neg-draw")
            if fault == "unsettled":
                state["denominator"] = 0
            elif fault == "partial":
                state["balance"] = 11
            elif fault == "token":
                state["tokens"] = [999]
            else:
                state.update(denominator=2, payouts=[1, 1])
            with self.subTest(fault=fault), self.assertRaises(ToolError):
                session.redemption_before()

    def test_a012_actual_receipt_scope_and_unrelated_credit_rejected(self):
        session, state, receipt, add, payout = self.fixture()
        before = session.redemption_before()
        state.update(balance=0, cash=100 + payout)
        self.assertTrue(session.redemption_readback(receipt, before))
        state["cash"] += 1
        self.assertFalse(session.redemption_readback(receipt, before))
        state["cash"] -= 1
        add(session.contracts.usdt.events.Transfer(), session.contracts.usdt.address,
            {"from": "0x" + "99" * 20, "to": OWNER, "value": 1})
        self.assertFalse(session.redemption_readback(receipt, before))

    def test_a012_approval_event_must_match_not_just_current_allowance(self):
        session, _, receipt, add, _ = self.fixture()
        session.target = cli.run(arguments()).target
        session.identity = lambda: OWNER
        receipt["to"] = session.contracts.usdt.address
        tx_hash = sdk_ops._hex_hash(receipt["transactionHash"])
        with self.assertRaises(ToolError):
            session.verify_receipt(receipt, tx_hash)
        add(session.contracts.usdt.events.Approval(), session.contracts.usdt.address,
            {"owner": OWNER, "spender": session.target["spender"], "value": 10})
        session.verify_receipt(receipt, tx_hash)
        session.target = {**session.target, "amount_atoms": 11}
        with self.assertRaises(ToolError):
            session.verify_receipt(receipt, tx_hash)

    def test_a012_actual_receipt_sender_destination_and_hash(self):
        session, _, receipt, _, _ = self.fixture()
        session.identity = lambda: OWNER
        tx_hash = sdk_ops._hex_hash(receipt["transactionHash"])
        session.verify_receipt(receipt, tx_hash)
        for key, value in (("to", "0x" + "99" * 20), ("from", "0x" + "99" * 20),
                           ("transactionHash", bytes(32)), ("status", 0)):
            with self.subTest(key=key), self.assertRaises(ToolError):
                session.verify_receipt({**receipt, key: value}, tx_hash)


if __name__ == "__main__":
    unittest.main()
