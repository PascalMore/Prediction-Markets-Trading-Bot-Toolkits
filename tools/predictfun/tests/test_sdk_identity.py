"""The installed package's identity and API.

These tests run against the real `predict-sdk==0.0.22` wheel in the isolated
environment. They never reach the network: every transport and signer is a
fake that raises if it is called.
"""

from __future__ import annotations

import importlib.metadata
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

TOOL_ROOT = Path(__file__).resolve().parents[1]
import sys

if str(TOOL_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOL_ROOT))

import sdk_ops  # noqa: E402


class ForbiddenTransport:
    """Any use of this is a failure: the tool must not reach the network."""

    def __getattr__(self, name: str):
        def forbidden(*_args, **_kwargs):
            raise AssertionError(f"the tool touched the network: {name}")

        return forbidden


class ForbiddenSigner:
    """Any use of this is a failure: a plan must not load a signer."""

    address = "0x0000000000000000000000000000000000000000"

    def __getattr__(self, name: str):
        def forbidden(*_args, **_kwargs):
            raise AssertionError(f"the tool touched the signer: {name}")

        return forbidden


class TestA001DistributionIdentity(unittest.TestCase):
    """A-001: the distribution's identity and API."""

    def test_a001_distribution_identity_and_api(self):
        # The version is exactly the one the contract fixes.
        self.assertEqual(importlib.metadata.version("predict-sdk"), "0.0.22")

        # The package is imported from the isolated environment, not from the
        # repository or a shared site-packages.
        import predict_sdk

        package_path = Path(predict_sdk.__file__).resolve()
        self.assertIn("tools/predictfun/.venv", package_path.as_posix())

        # The names the contract relies on are actually exported.
        for name in (
            "OrderBuilder",
            "ApprovalScope",
            "ApprovalStep",
            "ApprovalOperation",
            "ApprovalStepType",
            "OrderBuilderOptions",
            "TransactionResult",
            "TransactionSuccess",
            "TransactionFail",
            "ChainId",
            "Side",
        ):
            self.assertTrue(hasattr(predict_sdk, name), f"missing export: {name}")

        # The five methods the recovery evidence named, with their parameter
        # names.
        import inspect

        signatures = {
            "make": inspect.signature(predict_sdk.OrderBuilder.make),
            "get_approval_steps": inspect.signature(
                predict_sdk.OrderBuilder.get_approval_steps
            ),
            "check_approvals": inspect.signature(
                predict_sdk.OrderBuilder.check_approvals
            ),
            "set_approval": inspect.signature(predict_sdk.OrderBuilder.set_approval),
            "redeem_positions": inspect.signature(
                predict_sdk.OrderBuilder.redeem_positions
            ),
        }
        self.assertIn("chain_id", signatures["make"].parameters)
        self.assertIn("scope", signatures["get_approval_steps"].parameters)
        self.assertIn("steps", signatures["check_approvals"].parameters)
        self.assertIn("step", signatures["set_approval"].parameters)
        self.assertIn("amount", signatures["set_approval"].parameters)
        self.assertIn("condition_id", signatures["redeem_positions"].parameters)
        self.assertIn("index_set", signatures["redeem_positions"].parameters)
        self.assertIn("amount", signatures["redeem_positions"].parameters)
        self.assertIn("is_neg_risk", signatures["redeem_positions"].parameters)
        self.assertIn("is_yield_bearing", signatures["redeem_positions"].parameters)

        # The step's fields, so the tool can map them.
        step_fields = set(predict_sdk.ApprovalStep.__dataclass_fields__)
        self.assertTrue({"id", "type", "spender", "token"} <= step_fields)

        # The result's fields, so the tool can map them.
        self.assertTrue({"success", "receipt"} <= set(predict_sdk.TransactionSuccess.__annotations__))
        self.assertTrue({"success", "cause", "receipt"} <= set(predict_sdk.TransactionFail.__annotations__))


class TestA002RealSdkPlan(unittest.TestCase):
    """A-002: the plan path of the real package, with no transport."""

    def test_a002_real_sdk_plan_without_transport(self):
        import predict_sdk

        # A builder with no signer has no contracts and no provider.
        builder = predict_sdk.OrderBuilder.make(predict_sdk.ChainId.BNB_TESTNET)
        self.assertIsNone(builder.contracts)

        # The plan derives its steps without a signer and without a network.
        scope = predict_sdk.ApprovalScope(
            operation="TRADE",
            is_neg_risk=False,
            is_yield_bearing=False,
            side=predict_sdk.Side.BUY,
        )
        steps = builder.get_approval_steps(scope)
        self.assertTrue(steps)
        for step in steps:
            self.assertIn(step.type, ("ERC20_ALLOWANCE", "ERC1155_APPROVAL"))

        # The tool maps them onto its own shape.
        mapped = sdk_ops.normalize_steps(steps)
        self.assertEqual(len(mapped), len(steps))
        for step in mapped:
            self.assertTrue(step.id)
            self.assertTrue(step.token or step.spender)

    def test_a002_plan_never_loads_a_signer(self):
        import predict_sdk

        calls = []

        def factory(chain_id, signer=None, options=None):
            calls.append(("builder", chain_id, signer))
            return predict_sdk.OrderBuilder.make(predict_sdk.ChainId.BNB_TESTNET)

        target = {"operation": "approve", "chain_id": 97,
                  "owner": "0x" + "11" * 20, "account_mode": "eoa",
                  "market_id": "7", "side": "buy", "is_neg_risk": False,
                  "is_yield_bearing": False, "amount_atoms": 10, "decimals": 18}
        step = next(step for step in sdk_ops.approval_steps(target)
                    if step.type == "ERC20_ALLOWANCE")
        target.update(token=step.token.lower(), spender=step.spender.lower())
        with patch("socket.socket.connect", side_effect=AssertionError("NETWORK_FORBIDDEN")), patch(
            "sdk_ops.fd_signer_loader", side_effect=AssertionError("SIGNER_FORBIDDEN")
        ):
            result = sdk_ops.plan_target(target)
        self.assertTrue(result.steps)
        self.assertFalse(result.completed)


class TestA012Observer(unittest.TestCase):
    """Drive the installed SDK's real transaction methods, never real transport/signing."""

    def test_a012_real_sdk_broadcast_observer(self):
        import predict_sdk
        from web3 import Web3

        for operation in ("approve", "standard", "neg-risk", "yield"):
            for failure in (None, "send", "receipt", "revert", "pending", "hash-mismatch"):
                with self.subTest(operation=operation, failure=failure):
                    calls = []
                    raw = b"fake-signed-transaction-not-valid-on-any-chain"
                    expected = "0x" + Web3.keccak(raw).hex().removeprefix("0x")

                    class FakeMethod:
                        def estimate_gas(self, args):
                            return 21000

                        def build_transaction(self, args):
                            return {**args, "chainId": 97, "to": "0x" + "22" * 20}

                    class FakeFunctions:
                        def __getattr__(self, name):
                            def invoke(*args):
                                calls.append((name, args))
                                return FakeMethod()
                            return invoke

                    class FakeSigner:
                        address = "0x" + "11" * 20

                        def sign_transaction(self, transaction):
                            calls.append(("sign", transaction))
                            return SimpleNamespace(raw_transaction=raw)

                    class FakeEth:
                        def get_transaction_count(self, *args):
                            return 1

                        def send_raw_transaction(self, transaction):
                            calls.append(("send", transaction))
                            if failure == "send":
                                raise TimeoutError("[REDACTED]")
                            return bytes.fromhex("44" * 32) if failure == "hash-mismatch" else Web3.keccak(raw)

                        def wait_for_transaction_receipt(self, tx_hash, timeout):
                            if failure == "receipt":
                                raise TimeoutError("[REDACTED]")
                            status = None if failure == "pending" else int(failure != "revert")
                            return {"status": status, "transactionHash": tx_hash}

                    contract = SimpleNamespace(functions=FakeFunctions(), address="0x" + "22" * 20)
                    builder = predict_sdk.OrderBuilder.make(predict_sdk.ChainId.BNB_TESTNET)
                    builder._contracts = SimpleNamespace(
                        usdt=contract, conditional_tokens=contract,
                        yield_bearing_conditional_tokens=contract, neg_risk_adapter=contract,
                        yield_bearing_neg_risk_adapter=contract,
                    )
                    builder._signer = FakeSigner()
                    original_web3 = SimpleNamespace(eth=FakeEth())
                    builder._web3 = original_web3
                    # Any accidental escape to a real transport or signer aborts the test.
                    with patch("socket.socket.connect", side_effect=AssertionError("NETWORK_FORBIDDEN")), patch(
                        "eth_account.signers.local.LocalAccount.sign_transaction",
                        side_effect=AssertionError("REAL_SIGNING_FORBIDDEN"),
                    ):
                        with sdk_ops.observe_transaction(builder) as observed:
                            if operation == "approve":
                                step = predict_sdk.ApprovalStep(
                                    id="single", type="ERC20_ALLOWANCE", token=contract.address,
                                    spender="0x" + "33" * 20, label="finite", description="finite",
                                )
                                result = builder.set_approval(step, amount=10)
                            else:
                                result = builder.redeem_positions(
                                    "0x" + "55" * 32, 1, 10,
                                    is_neg_risk=operation == "neg-risk",
                                    is_yield_bearing=operation == "yield",
                                )
                            self.assertEqual(observed.expected_hash, expected)
                            self.assertTrue(observed.broadcast_attempted)
                            self.assertEqual(observed.send_count, 1)
                            normalized = sdk_ops.normalize_sdk_result(result)
                            if failure is None:
                                self.assertEqual(normalized["receipt_status"], "confirmed")
                                self.assertEqual(observed.broadcast_hash, expected)
                            else:
                                self.assertFalse(result.success)
                    self.assertIs(builder._web3, original_web3)
                    names = [name for name, _ in calls]
                    self.assertLess(names.index("sign"), names.index("send"))
                    self.assertEqual(names.count("send"), 1)
                    if operation == "approve":
                        self.assertIn(("approve", ("0x" + "33" * 20, 10)), calls)
                    elif operation == "neg-risk":
                        self.assertIn(("redeemPositions", ("0x" + "55" * 32, [10, 0])), calls)
                    else:
                        # Real standard SDK never forwards amount: full-selected consent is required.
                        arguments = next(args for name, args in calls if name == "redeemPositions")
                        self.assertEqual(arguments[-1], [1])
                        self.assertEqual(len(arguments), 4)


if __name__ == "__main__":
    unittest.main()
