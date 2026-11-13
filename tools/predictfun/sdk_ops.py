"""Single-target operations using predict-sdk==0.0.22; no credentials on import.

The SDK builds/signs transactions. This module supplies explicit read context,
observes its existing signer/Eth interfaces, and verifies receipt-scoped facts.
"""
from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable, Iterator

MAX_UINT256 = 2**256 - 1
MAX_INT256 = 2**255 - 1


class ErrorCode(str, Enum):
    INVALID_INPUT = "INVALID_INPUT"
    EXECUTION_NOT_ENABLED = "EXECUTION_NOT_ENABLED"
    ACCOUNT_MISMATCH = "ACCOUNT_MISMATCH"
    READ_UNAVAILABLE = "READ_UNAVAILABLE"
    FACT_INCOMPLETE = "FACT_INCOMPLETE"
    SUBMISSION_UNKNOWN = "SUBMISSION_UNKNOWN"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"
    TX_UNCONFIRMED = "TX_UNCONFIRMED"
    SDK_INCOMPATIBLE = "SDK_INCOMPATIBLE"


class ToolError(Exception):
    """A stable public code, never an upstream exception or credential payload."""

    def __init__(self, code: ErrorCode, step: str, reason: str) -> None:
        super().__init__(f"{code.value}:{step}:{reason}")
        self.code, self.step, self.reason = code, step, reason

    def as_dict(self) -> dict[str, str]:
        """Return only public, pre-defined diagnostics."""
        return {"code": self.code.value, "step": self.step, "reason": self.reason}


def parse_address(raw: str, field: str) -> str:
    """Validate an exact nonzero EVM address; normalize only after validation."""
    if not isinstance(raw, str) or len(raw) != 42 or not raw.startswith("0x"):
        raise ToolError(ErrorCode.INVALID_INPUT, field, "BAD_ADDRESS")
    if any(char not in "0123456789abcdefABCDEF" for char in raw[2:]) or int(raw, 16) == 0:
        raise ToolError(ErrorCode.INVALID_INPUT, field, "BAD_ADDRESS")
    return raw.lower()


def parse_hash(raw: str, field: str) -> str:
    """Validate an exact bytes32 hex identifier."""
    if not isinstance(raw, str) or len(raw) != 66 or not raw.startswith("0x"):
        raise ToolError(ErrorCode.INVALID_INPUT, field, "BAD_HASH")
    if any(char not in "0123456789abcdefABCDEF" for char in raw[2:]):
        raise ToolError(ErrorCode.INVALID_INPUT, field, "BAD_HASH")
    return raw.lower()


def parse_positive_int(raw: str, field: str) -> int:
    """Accept bounded ASCII integer atoms, not floats or Unicode digits."""
    if not isinstance(raw, str) or not raw.isascii() or not raw.isdecimal():
        raise ToolError(ErrorCode.INVALID_INPUT, field, "BAD_AMOUNT")
    if len(raw) > 78 or not 0 < int(raw) < MAX_INT256:
        raise ToolError(ErrorCode.INVALID_INPUT, field, "UNBOUNDED_AMOUNT")
    return int(raw)


def parse_amount_atoms(raw: str, field: str) -> int:
    """Parse an explicitly finite amount; MAX sentinels are never defaults."""
    return parse_positive_int(raw, field)


def parse_bool(raw: str | None, field: str) -> bool:
    """Require explicit true/false route flags."""
    if raw not in ("true", "false"):
        raise ToolError(ErrorCode.INVALID_INPUT, field, "MISSING_OR_BAD_BOOLEAN")
    return raw == "true"


def parse_index_set(raw: str) -> int:
    """Only binary-market singleton sets are supported."""
    if raw not in ("1", "2"):
        raise ToolError(ErrorCode.INVALID_INPUT, "index_set", "BAD_INDEX_SET")
    return int(raw)


def target_digest(parts: dict[str, Any]) -> str:
    """Hash the applicable public target, never RPC endpoints or secrets."""
    payload = json.dumps({key: value for key, value in parts.items() if value is not None},
                         sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("ascii")).hexdigest()


def _import_sdk() -> Any:
    from importlib.metadata import version

    try:
        import predict_sdk
        if version("predict-sdk") != "0.0.22":
            raise ValueError("VERSION")
    except Exception:
        raise ToolError(ErrorCode.SDK_INCOMPATIBLE, "import", "SDK_VERSION_OR_IMPORT") from None
    return predict_sdk


@dataclass
class ReadContext:
    """Explicit holder and RPC context; no environment/credential discovery."""
    owner: str
    chain_id: int
    rpc_url: str | None = None


@dataclass
class Step:
    """One official approval step."""
    id: str
    type: str
    token: str | None = None
    spender: str | None = None
    description: str = ""


def normalize_steps(raw_steps: Any) -> list[Step]:
    """Adapt the pinned SDK's dataclasses."""
    return [Step(item.id, item.type, item.token, item.spender, item.description)
            for item in raw_steps]


@dataclass
class OperationResult:
    """Stable operation result; receipt and target readback must both succeed."""
    operation: str
    target: dict[str, Any] = field(default_factory=dict)
    steps: list[Step] = field(default_factory=list)
    expected_tx_hash: str | None = None
    broadcast_tx_hash: str | None = None
    receipt_status: str | None = None
    readback_status: str | None = None
    completed: bool = False
    error: dict[str, str] | None = None

    def as_dict(self) -> dict[str, Any]:
        """Serialize only the fixed public output contract."""
        return asdict(self)


def _hex_hash(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        value = "0x" + bytes(value).hex()
    return parse_hash(value, "tx_hash")


def normalize_sdk_result(result: Any) -> dict[str, Any]:
    """Normalize actual SDK receipt fields without leaking its cause payload."""
    receipt = getattr(result, "receipt", None)
    status = receipt.get("status") if receipt is not None else None
    raw_hash = receipt.get("transactionHash") if receipt is not None else None
    cause = getattr(result, "cause", None)
    return {
        "success": bool(getattr(result, "success", False)),
        "receipt_status": "confirmed" if status == 1 else "reverted" if status == 0 else "pending",
        "tx_hash": _hex_hash(raw_hash) if raw_hash is not None else None,
        "cause": type(cause).__name__ if cause is not None else None,
    }


@dataclass
class ObservedTx:
    """One shared record survives SDK-caught send/receipt exceptions."""
    expected_hash: str | None = None
    broadcast_hash: str | None = None
    broadcast_attempted: bool = False
    send_count: int = 0


class _HashObserver:
    def observe_sign(self, raw: bytes) -> str:
        from web3 import Web3
        return _hex_hash(Web3.keccak(raw))

    def observe_broadcast(self, raw: bytes) -> None:
        pass


class ObservedSigner:
    """Delegate signing and record its returned raw hash before any send."""
    def __init__(self, signer: Any, observer: Any, state: ObservedTx | None = None) -> None:
        self._signer, self._observer = signer, observer
        self.observed = state if state is not None else ObservedTx()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._signer, name)

    def sign_transaction(self, transaction: Any) -> Any:
        """Do not allow the same operation to sign again after a known attempt."""
        if self.observed.expected_hash is not None:
            raise ToolError(ErrorCode.SDK_INCOMPATIBLE, "sign", "SECOND_SIGN_REFUSED")
        signed = self._signer.sign_transaction(transaction)
        self.observed.expected_hash = self._observer.observe_sign(signed.raw_transaction)
        return signed


class ObservedProvider:
    """Wrap the Eth instance actually used by the SDK, not a separate client."""
    def __init__(self, provider: Any, observer: Any, state: ObservedTx | None = None) -> None:
        self._provider, self._observer = provider, observer
        self.observed = state if state is not None else ObservedTx()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def send_raw_transaction(self, raw_transaction: bytes) -> Any:
        """Record uncertainty before sending; forbid replay and hash disagreement."""
        if self.observed.send_count:
            raise ToolError(ErrorCode.SDK_INCOMPATIBLE, "send", "SECOND_SEND_REFUSED")
        actual = _HashObserver().observe_sign(raw_transaction)
        if self.observed.expected_hash != actual:
            raise ToolError(ErrorCode.RECONCILIATION_REQUIRED, "send", "RAW_HASH_MISMATCH")
        self.observed.expected_hash = actual
        self.observed.broadcast_attempted = True
        self.observed.send_count = 1
        self._observer.observe_broadcast(raw_transaction)
        returned = self._provider.send_raw_transaction(raw_transaction)
        self.observed.broadcast_hash = _hex_hash(returned)
        if self.observed.broadcast_hash != actual:
            raise ToolError(ErrorCode.RECONCILIATION_REQUIRED, "send", "RPC_HASH_MISMATCH")
        return returned


class _ObservedWeb3:
    def __init__(self, web3: Any, observer: Any, state: ObservedTx) -> None:
        self._web3 = web3
        self.eth = ObservedProvider(web3.eth, observer, state)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._web3, name)


@contextmanager
def observe_transaction(builder: Any) -> Iterator[ObservedTx]:
    """Locally wrap pinned SDK instance dependencies; restore even on failure."""
    signer, web3 = builder._signer, builder._web3
    if signer is None or web3 is None:
        raise ToolError(ErrorCode.SDK_INCOMPATIBLE, "observe", "MISSING_SDK_SEAM")
    state, observer = ObservedTx(), _HashObserver()
    builder._signer = ObservedSigner(signer, observer, state)
    builder._web3 = _ObservedWeb3(web3, observer, state)
    try:
        yield state
    finally:
        builder._signer, builder._web3 = signer, web3


def approval_steps(target: dict[str, Any], builder: Any = None) -> list[Any]:
    """Ask the SDK for only this market/side/route's steps, never all approvals."""
    sdk = _import_sdk()
    builder = builder or sdk.OrderBuilder.make(sdk.ChainId(target["chain_id"]))
    scope = sdk.ApprovalScope(
        operation="TRADE" if target["operation"] == "approve" else "REDEEM",
        is_neg_risk=target["is_neg_risk"], is_yield_bearing=target["is_yield_bearing"],
        side=sdk.Side.BUY if target.get("side") == "buy" else sdk.Side.SELL,
    )
    return list(builder.get_approval_steps(scope))


def select_step(target: dict[str, Any], steps: list[Any]) -> Any:
    """Only an exact official token/spender pair may be approved."""
    operator = target.get("operator")
    candidates = [step for step in steps
                  if step.type == ("ERC1155_APPROVAL" if operator else "ERC20_ALLOWANCE")
                  and step.token.lower() == target.get("token", "").lower()
                  and step.spender.lower() == (operator or target.get("spender", "")).lower()]
    if len(candidates) != 1:
        raise ToolError(ErrorCode.INVALID_INPUT, "scope", "NOT_ONE_OFFICIAL_STEP")
    return candidates[0]


def plan_target(target: dict[str, Any]) -> OperationResult:
    """Derive official steps and digest with zero signer/provider/RPC access."""
    steps = approval_steps(target)
    if target["operation"] == "approve":
        select_step(target, steps)
    return OperationResult("plan:" + target["operation"], target, normalize_steps(steps))


def load_signer(loader: Any, fd: int | None, context: ReadContext) -> Any:
    """Only an explicit non-stdio fd is eligible, after all read-only checks."""
    if fd is None or fd < 3:
        raise ToolError(ErrorCode.EXECUTION_NOT_ENABLED, "signer", "MISSING_SIGNER_FD")
    if loader is None:
        raise ToolError(ErrorCode.EXECUTION_NOT_ENABLED, "signer", "NO_SIGNER_LOADER")
    signer = loader(fd)
    if parse_address(signer.address, "signer") != context.owner:
        raise ToolError(ErrorCode.ACCOUNT_MISMATCH, "signer", "SIGNER_ADDRESS_MISMATCH")
    return signer


def fd_signer_loader(fd: int) -> Any:
    """Future explicitly approved use only: no env/argv/default key discovery."""
    from eth_account import Account
    # Do not close the caller's descriptor or emit its contents on any error.
    try:
        key = os.read(fd, 1024).strip()
        if not key or len(key) > 66:
            raise ValueError("FORMAT")
        return Account.from_key(key.decode("ascii"))
    except Exception:
        raise ToolError(ErrorCode.INVALID_INPUT, "signer", "BAD_SIGNER_FD_CONTENT") from None


class ReadSession:
    """Read-only contracts from the pinned SDK; construction has no signer."""
    def __init__(self, target: dict[str, Any], rpc_url: str | None) -> None:
        if not rpc_url:
            raise ToolError(ErrorCode.READ_UNAVAILABLE, "read", "MISSING_RPC_URL")
        from urllib.parse import urlsplit
        if urlsplit(rpc_url).scheme not in ("http", "https"):
            raise ToolError(ErrorCode.INVALID_INPUT, "rpc", "BAD_RPC_SCHEME")
        from web3 import Web3
        from web3.middleware import ExtraDataToPOAMiddleware
        from predict_sdk._internal.contracts import make_contracts, get_conditional_tokens_contract

        sdk = _import_sdk()
        self.target = target
        self.builder = sdk.OrderBuilder.make(sdk.ChainId(target["chain_id"]))
        self.web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 15},
                                         exception_retry_configuration=None))
        self.web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        self.contracts = make_contracts(self.web3, self.builder._addresses)
        self.ct = get_conditional_tokens_contract(
            self.contracts, is_neg_risk=target["is_neg_risk"],
            is_yield_bearing=target["is_yield_bearing"],
        )
        self.owner = Web3.to_checksum_address(target["owner"])

    def identity(self) -> str:
        """Read chain/account mode/control before any signer can be loaded."""
        if self.web3.eth.chain_id != self.target["chain_id"]:
            raise ToolError(ErrorCode.ACCOUNT_MISMATCH, "identity", "WRONG_CHAIN")
        code = self.web3.eth.get_code(self.owner)
        if self.target["account_mode"] == "eoa":
            if code:
                raise ToolError(ErrorCode.ACCOUNT_MISMATCH, "identity", "EXPECTED_EOA")
            return self.owner.lower()
        if not code:
            raise ToolError(ErrorCode.ACCOUNT_MISMATCH, "identity", "EXPECTED_PREDICT_ACCOUNT")
        controller = self.contracts.ecdsa_validator.functions.ecdsaValidatorStorage(self.owner).call()
        return parse_address(controller, "controller")

    def approval_value(self, step: Any) -> int | bool:
        """Read raw finite allowance/operator state; never SDK MAX satisfaction."""
        from predict_sdk._internal.contracts import make_contract
        from predict_sdk.abis import CONDITIONAL_TOKENS_ABI
        from web3 import Web3
        spender = Web3.to_checksum_address(step.spender)
        if step.type == "ERC20_ALLOWANCE":
            decimals_contract = make_contract(self.web3, self.contracts.usdt.address, [{
                "type": "function", "name": "decimals", "stateMutability": "view",
                "inputs": [], "outputs": [{"name": "", "type": "uint8"}],
            }])
            decimals = decimals_contract.functions.decimals().call()
            if decimals != self.target["decimals"]:
                raise ToolError(ErrorCode.INVALID_INPUT, "decimals", "DECIMALS_MISMATCH")
            return int(self.contracts.usdt.functions.allowance(self.owner, spender).call())
        token = make_contract(self.web3, step.token, CONDITIONAL_TOKENS_ABI)
        return bool(token.functions.isApprovedForAll(self.owner, spender).call())

    def market(self) -> dict[str, Any]:
        """Read only the explicit official public market; auth/shape failure is unknown."""
        import requests
        host = "api.predict.fun" if self.target["chain_id"] == 56 else "api-testnet.predict.fun"
        response = requests.get(f"https://{host}/v1/markets/{self.target['market_id']}",
                                timeout=15, auth=lambda request: request, allow_redirects=False)
        response.raise_for_status()
        market = response.json()["data"]
        if str(market["id"]) != self.target["market_id"] or any(
            market[key] is not self.target[field] for key, field in
            (("isNegRisk", "is_neg_risk"), ("isYieldBearing", "is_yield_bearing"))
        ):
            raise ToolError(ErrorCode.RECONCILIATION_REQUIRED, "market", "ROUTE_MISMATCH")
        return market

    def position_id(self) -> int:
        """Use the route's actual collateral, including NegRisk wrapped collateral."""
        from web3 import Web3
        target = self.target
        collateral = self.contracts.usdt.address
        if target["is_neg_risk"]:
            adapter = (self.contracts.yield_bearing_neg_risk_adapter if target["is_yield_bearing"]
                       else self.contracts.neg_risk_adapter)
            if (adapter.functions.col().call().lower() != collateral.lower()
                    or adapter.functions.ctf().call().lower() != self.ct.address.lower()):
                raise ToolError(ErrorCode.RECONCILIATION_REQUIRED, "route", "ADAPTER_MISMATCH")
            collateral = parse_address(adapter.functions.wcol().call(), "wrapped_collateral")
        condition = bytes.fromhex(target["condition_id"][2:])
        collection = self.ct.functions.getCollectionId(bytes(32), condition, target["index_set"]).call()
        return int(self.ct.functions.getPositionId(Web3.to_checksum_address(collateral), collection).call())

    def gas_available(self, controller: str) -> None:
        """Require a readable, nonzero native balance for the actual transaction payer."""
        from web3 import Web3
        if self.web3.eth.get_balance(Web3.to_checksum_address(controller)) <= 0:
            raise ToolError(ErrorCode.READ_UNAVAILABLE, "gas", "NO_NATIVE_GAS")

    def redemption_before(self) -> dict[str, int]:
        """Bind market token, condition, selected set and recorded payouts via SDK ABI."""
        target, functions = self.target, self.ct.functions
        market = self.market()
        condition = bytes.fromhex(target["condition_id"][2:])
        slots = int(functions.getOutcomeSlotCount(condition).call())
        denominator = int(functions.payoutDenominator(condition).call())
        if slots != 2 or denominator <= 0:
            raise ToolError(ErrorCode.INVALID_INPUT, "settlement", "NOT_SETTLED_BINARY")
        numerators = [int(functions.payoutNumerators(condition, index).call()) for index in (0, 1)]
        if sum(numerators) != denominator or any(value < 0 for value in numerators):
            raise ToolError(ErrorCode.RECONCILIATION_REQUIRED, "settlement", "INVALID_PAYOUT")
        if target["is_neg_risk"] and all(value > 0 for value in numerators):
            raise ToolError(ErrorCode.INVALID_INPUT, "settlement", "NEG_RISK_DRAW_UNSUPPORTED")
        token_id = self.position_id()
        official_tokens = {int(outcome["onChainId"]) for outcome in market["outcomes"]}
        if token_id not in official_tokens:
            raise ToolError(ErrorCode.RECONCILIATION_REQUIRED, "condition", "MARKET_TOKEN_MISMATCH")
        balance = int(functions.balanceOf(self.owner, token_id).call())
        amount = target["amount_atoms"]
        if amount > balance or (not target["is_neg_risk"] and amount != balance):
            raise ToolError(ErrorCode.INVALID_INPUT, "redeem", "PARTIAL_OR_INSUFFICIENT_BALANCE")
        return {"token_id": token_id, "balance": balance,
                "cash": int(self.contracts.usdt.functions.balanceOf(self.owner).call()),
                "payout": amount * numerators[target["index_set"] - 1] // denominator}

    def signed_builder(self, signer: Any) -> Any:
        """Reuse one SDK instance and this explicit RPC, not SDK default RPC discovery."""
        self.builder._contracts, self.builder._web3 = self.contracts, self.web3
        self.builder._signer = signer
        self.builder._predict_account = self.owner if self.target["account_mode"] == "predict-account" else None
        return self.builder

    def receipt(self, tx_hash: str) -> Any:
        """Single transaction read, never resend or wait indefinitely."""
        return self.web3.eth.get_transaction_receipt(tx_hash)

    def verify_receipt(self, receipt: Any, tx_hash: str) -> None:
        """Tie receipt to confirmed target account and expected contract."""
        if _hex_hash(receipt["transactionHash"]) != tx_hash or receipt["status"] != 1:
            raise ToolError(ErrorCode.TX_UNCONFIRMED, "receipt", "HASH_OR_STATUS")
        controller = self.identity()
        if parse_address(receipt["from"], "from") != controller:
            raise ToolError(ErrorCode.TX_UNCONFIRMED, "receipt", "SENDER_MISMATCH")
        target = self.target
        destination = self.owner if target["account_mode"] == "predict-account" else (
            target["token"] if target["operation"] == "approve" else
            (self.contracts.yield_bearing_neg_risk_adapter.address if target["is_yield_bearing"]
             else self.contracts.neg_risk_adapter.address) if target["is_neg_risk"] else self.ct.address
        )
        if parse_address(receipt["to"], "to") != destination.lower():
            raise ToolError(ErrorCode.TX_UNCONFIRMED, "receipt", "DESTINATION_MISMATCH")
        if target["operation"] == "approve":
            from web3.logs import DISCARD
            operator = target.get("operator")
            contract = self.ct if operator else self.contracts.usdt
            event_type = contract.events.ApprovalForAll if operator else contract.events.Approval
            events = event_type().process_receipt(receipt, errors=DISCARD)
            matched = [event for event in events
                       if event["address"].lower() == target["token"]
                       and event["args"]["account" if operator else "owner"].lower() == self.owner.lower()
                       and event["args"]["operator" if operator else "spender"].lower()
                       == (operator or target["spender"])
                       and (event["args"]["approved"] is True if operator else
                            int(event["args"]["value"]) == target["amount_atoms"])]
            if len(matched) != 1:
                raise ToolError(ErrorCode.TX_UNCONFIRMED, "receipt", "APPROVAL_SCOPE_MISMATCH")

    def redemption_readback(self, receipt: Any, before: dict[str, int] | None) -> bool:
        """Decode official ABI events and require same-scope share/collateral movement."""
        from web3.logs import DISCARD
        owner, target = self.owner.lower(), self.target
        functions = self.ct.functions
        token_id = self.position_id()
        adapter = (self.contracts.yield_bearing_neg_risk_adapter if target["is_yield_bearing"]
                   else self.contracts.neg_risk_adapter)
        emitter = adapter if target["is_neg_risk"] else self.ct
        # ERC1155 may emit single or batch transfers; both are official ABI decoders.
        transfers = list(self.ct.events.TransferSingle().process_receipt(receipt, errors=DISCARD))
        batches = self.ct.events.TransferBatch().process_receipt(receipt, errors=DISCARD)
        for batch in batches:
            values = batch["args"]
            if len(values["ids"]) != len(values["values"]):
                return False
            transfers.extend({"address": batch["address"], "args": {
                "from": values["from"], "to": values["to"], "id": identifier, "value": value,
            }} for identifier, value in zip(values["ids"], values["values"]))
        sinks = {"0x" + "00" * 20}
        if target["is_neg_risk"]:
            sinks.add(adapter.address.lower())
        spent = 0
        for event in transfers:
            values = event["args"]
            if event["address"].lower() != self.ct.address.lower():
                continue
            if values["from"].lower() == owner:
                if int(values["id"]) != token_id or values["to"].lower() not in sinks:
                    return False
                spent += int(values["value"])
            if values["to"].lower() == owner:
                return False  # Concurrent/unrelated credit: do not guess allocation.
        if spent != target["amount_atoms"]:
            return False
        events = emitter.events.PayoutRedemption().process_receipt(receipt, errors=DISCARD)
        matching = []
        for event in events:
            values = event["args"]
            if (event["address"].lower() != emitter.address.lower()
                    or _hex_hash(values["conditionId"]) != target["condition_id"]
                    or values["redeemer"].lower() != owner):
                continue
            if target["is_neg_risk"]:
                expected = [spent, 0] if target["index_set"] == 1 else [0, spent]
                if list(values["amounts"]) != expected:
                    return False
            elif (list(values["indexSets"]) != [target["index_set"]]
                  or values["collateralToken"].lower() != self.contracts.usdt.address.lower()
                  or bytes(values["parentCollectionId"]) != bytes(32)):
                return False
            matching.append(values)
        if len(matching) != 1:
            return False
        cash_events = self.contracts.usdt.events.Transfer().process_receipt(receipt, errors=DISCARD)
        allowed_sources = {self.ct.address.lower(), adapter.address.lower()}
        credit = 0
        for event in cash_events:
            values = event["args"]
            if (event["address"].lower() == self.contracts.usdt.address.lower()
                    and values["to"].lower() == owner):
                if values["from"].lower() not in allowed_sources:
                    return False
                credit += int(values["value"])
        if int(matching[0]["payout"]) != credit:
            return False
        if before is not None:
            if before["token_id"] != token_id:
                return False
            remaining = int(functions.balanceOf(self.owner, token_id).call())
            cash = int(self.contracts.usdt.functions.balanceOf(self.owner).call())
            if before["balance"] - remaining != spent or cash - before["cash"] != credit:
                return False
            if not target["is_yield_bearing"] and credit != before["payout"]:
                return False
        return True


def _satisfied(target: dict[str, Any], reading: Any) -> bool:
    return reading is True if target.get("operator") else (
        isinstance(reading, int) and not isinstance(reading, bool) and reading >= target["amount_atoms"]
    )


def execute_target(
    target: dict[str, Any], rpc_url: str | None, *, command: str,
    execute: bool = False, confirmation: str | None = None, signer_fd: int | None = None,
    tx_hash: str | None = None, session_factory: Callable[..., Any] = ReadSession,
    signer_loader: Callable[[int], Any] = fd_signer_loader,
) -> OperationResult:
    """Execute one explicit target, or signer-free check of the original receipt.

    Read failures before submission raise stable errors; failures after possible
    submission return the retained attempt and never invoke the SDK twice.
    """
    if command != "check" and (not execute or not confirmation):
        raise ToolError(ErrorCode.EXECUTION_NOT_ENABLED, "execute", "MISSING_EXECUTE_OR_CONFIRM")
    if command != "check" and confirmation != target_digest(target):
        raise ToolError(ErrorCode.INVALID_INPUT, "execute", "TARGET_DIGEST_MISMATCH")
    if tx_hash is not None:
        tx_hash = parse_hash(tx_hash, "tx_hash")
    result = OperationResult(target["operation"], target)
    try:
        session = session_factory(target, rpc_url)
        controller = session.identity()
        session.market()
        step = None
        if target["operation"] == "approve":
            step = select_step(target, approval_steps(target))
            result.steps = normalize_steps([step])
        if command == "check":
            if tx_hash is None:
                if step is not None:
                    healthy = _satisfied(target, session.approval_value(step))
                    result.readback_status = "satisfied" if healthy else "insufficient"
                    result.completed = healthy
                else:
                    session.redemption_before()
                    result.readback_status = "settled_and_amount_checked"
                return result
            result.expected_tx_hash = tx_hash
            receipt = session.receipt(tx_hash)
            result.receipt_status = "confirmed" if receipt.get("status") == 1 else (
                "reverted" if receipt.get("status") == 0 else "pending")
            session.verify_receipt(receipt, tx_hash)
            result.completed = (_satisfied(target, session.approval_value(step)) if step is not None
                                else session.redemption_readback(receipt, None))
            result.readback_status = "matched" if result.completed else "mismatch"
            if not result.completed:
                result.error = ToolError(ErrorCode.TX_UNCONFIRMED, "readback", "SCOPE_NOT_MATCHED").as_dict()
            return result
        before = None
        if step is not None:
            if _satisfied(target, session.approval_value(step)):
                result.completed, result.readback_status = True, "already_satisfied_noop"
                return result
        else:
            before = session.redemption_before()
        session.gas_available(controller)
        signer = load_signer(signer_loader, signer_fd,
                             ReadContext(controller, target["chain_id"], rpc_url))
        # Standard SDK redeems all of the selected set: reread immediately before signing.
        if before is not None and session.redemption_before() != before:
            raise ToolError(ErrorCode.RECONCILIATION_REQUIRED, "before", "BALANCE_CHANGED")
        builder = session.signed_builder(signer)
    except Exception as error:
        if command == "check" and tx_hash is not None:
            result.expected_tx_hash = tx_hash
            result.error = (error.as_dict() if isinstance(error, ToolError) else
                            ToolError(ErrorCode.TX_UNCONFIRMED, "check", "REQUIRED_READ_FAILED").as_dict())
            return result
        if isinstance(error, ToolError):
            raise
        raise ToolError(ErrorCode.READ_UNAVAILABLE, "before", "REQUIRED_READ_FAILED") from None

    with observe_transaction(builder) as observed:
        try:
            sdk_result = (builder.set_approval(step, amount=target.get("amount_atoms", 1))
                          if step is not None else builder.redeem_positions(
                              target["condition_id"], target["index_set"], target["amount_atoms"],
                              is_neg_risk=target["is_neg_risk"],
                              is_yield_bearing=target["is_yield_bearing"]))
            normalized = normalize_sdk_result(sdk_result)
            result.receipt_status = normalized["receipt_status"]
            receipt = getattr(sdk_result, "receipt", None)
            if (not normalized["success"] or observed.broadcast_hash != observed.expected_hash
                    or normalized["tx_hash"] != observed.expected_hash):
                raise ToolError(ErrorCode.TX_UNCONFIRMED, "receipt", "TRANSACTION_NOT_CONFIRMED")
            session.verify_receipt(receipt, observed.expected_hash)
            result.completed = (_satisfied(target, session.approval_value(step)) if step is not None
                                else session.redemption_readback(receipt, before))
            result.readback_status = "matched" if result.completed else "mismatch"
            if not result.completed:
                raise ToolError(ErrorCode.TX_UNCONFIRMED, "readback", "SCOPE_NOT_MATCHED")
        except (Exception, KeyboardInterrupt) as error:
            result.completed = False
            result.error = (error.as_dict() if isinstance(error, ToolError) else
                            ToolError(ErrorCode.TX_UNCONFIRMED, "after_send", "READ_OR_SEND_FAILED").as_dict())
        finally:
            result.expected_tx_hash = observed.expected_hash
            result.broadcast_tx_hash = observed.broadcast_hash
    return result
