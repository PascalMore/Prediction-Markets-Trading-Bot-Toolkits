"""Explicit one-target Predict.fun SDK CLI; no credential discovery or default send."""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, NoReturn

import sdk_ops
from sdk_ops import ErrorCode, ToolError


class InputParser(argparse.ArgumentParser):
    """Parser refusals are stable public errors, not raw argparse payloads."""
    def error(self, message: str) -> NoReturn:
        raise ToolError(ErrorCode.INVALID_INPUT, "parse", "BAD_ARGUMENTS")


def build_parser() -> argparse.ArgumentParser:
    """Define the four fixed subcommands with a shared, unambiguous option set."""
    parser = InputParser(prog="predictfun", description=__doc__)
    parser.add_argument("command", choices=("plan", "check", "approve", "redeem"))
    parser.add_argument("--operation", choices=("approve", "redeem"))
    parser.add_argument("--chain-id", choices=("56", "97"))
    parser.add_argument("--owner")
    parser.add_argument("--account-mode", choices=("eoa", "predict-account"))
    parser.add_argument("--predict-account")
    parser.add_argument("--market-id")
    parser.add_argument("--side", choices=("buy", "sell"))
    parser.add_argument("--is-neg-risk", choices=("true", "false"))
    parser.add_argument("--is-yield-bearing", choices=("true", "false"))
    parser.add_argument("--token")
    parser.add_argument("--spender")
    parser.add_argument("--operator")
    parser.add_argument("--amount-atoms")
    parser.add_argument("--decimals")
    parser.add_argument("--condition-id")
    parser.add_argument("--index-set", choices=("1", "2"))
    parser.add_argument("--redeem-full-index-set", action="store_true")
    parser.add_argument("--allow-operator", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-target")
    parser.add_argument("--rpc-url")
    parser.add_argument("--tx-hash")
    parser.add_argument("--signer-fd", type=int)
    return parser


def normalize_target(args: argparse.Namespace) -> dict[str, Any]:
    """Validate formats/routes before execution permission, and canonicalize once."""
    operation = args.operation or (args.command if args.command in ("approve", "redeem") else None)
    if args.command in ("approve", "redeem") and operation != args.command:
        raise ToolError(ErrorCode.INVALID_INPUT, "operation", "OPERATION_MISMATCH")
    if not all((operation, args.chain_id, args.owner, args.account_mode, args.market_id, args.side)):
        raise ToolError(ErrorCode.INVALID_INPUT, "target", "MISSING_TARGET")
    market_id = str(sdk_ops.parse_positive_int(args.market_id, "market_id"))
    owner = sdk_ops.parse_address(args.owner, "owner")
    predict_account = None
    if args.account_mode == "predict-account":
        predict_account = sdk_ops.parse_address(args.predict_account, "predict_account")
        if owner != predict_account:
            raise ToolError(ErrorCode.ACCOUNT_MISMATCH, "target", "HOLDER_MISMATCH")
    elif args.predict_account:
        raise ToolError(ErrorCode.INVALID_INPUT, "target", "UNEXPECTED_PREDICT_ACCOUNT")
    target = {
        "operation": operation, "chain_id": int(args.chain_id), "owner": owner,
        "account_mode": args.account_mode, "predict_account": predict_account,
        "market_id": market_id, "side": args.side,
        "is_neg_risk": sdk_ops.parse_bool(args.is_neg_risk, "is_neg_risk"),
        "is_yield_bearing": sdk_ops.parse_bool(args.is_yield_bearing, "is_yield_bearing"),
    }
    if operation == "approve":
        if args.condition_id or args.index_set or args.redeem_full_index_set:
            raise ToolError(ErrorCode.INVALID_INPUT, "target", "UNEXPECTED_REDEEM_SCOPE")
        if args.operator:
            if not args.allow_operator:
                raise ToolError(ErrorCode.INVALID_INPUT, "approve", "MISSING_OPERATOR_CONSENT")
            if args.amount_atoms or args.decimals or args.spender:
                raise ToolError(ErrorCode.INVALID_INPUT, "approve", "OPERATOR_HAS_NO_AMOUNT")
            target.update(operator=sdk_ops.parse_address(args.operator, "operator"), allow_operator=True)
        else:
            if args.allow_operator:
                raise ToolError(ErrorCode.INVALID_INPUT, "approve", "UNEXPECTED_OPERATOR_CONSENT")
            amount = sdk_ops.parse_amount_atoms(args.amount_atoms, "amount_atoms")
            if args.decimals is None or not args.decimals.isascii() or not args.decimals.isdecimal():
                raise ToolError(ErrorCode.INVALID_INPUT, "decimals", "MISSING_OR_BAD_DECIMALS")
            decimals = int(args.decimals)
            if decimals > 36:
                raise ToolError(ErrorCode.INVALID_INPUT, "decimals", "DECIMALS_RANGE")
            target.update(amount_atoms=amount, decimals=decimals)
        # Derivation is pure SDK scope planning, not an address enumeration probe.
        steps = sdk_ops.approval_steps(target)
        candidates = [step for step in steps if step.type == (
            "ERC1155_APPROVAL" if args.operator else "ERC20_ALLOWANCE")]
        if args.token:
            token = sdk_ops.parse_address(args.token, "token")
            candidates = [step for step in candidates if step.token.lower() == token]
        if args.spender or args.operator:
            spender = sdk_ops.parse_address(args.operator or args.spender, "spender")
            candidates = [step for step in candidates if step.spender.lower() == spender]
        if len(candidates) != 1:
            raise ToolError(ErrorCode.INVALID_INPUT, "scope", "EXPLICIT_STEP_REQUIRED")
        target["token"] = candidates[0].token.lower()
        if not args.operator:
            target["spender"] = candidates[0].spender.lower()
    else:
        if args.token or args.spender or args.operator or args.allow_operator or args.decimals:
            raise ToolError(ErrorCode.INVALID_INPUT, "target", "UNEXPECTED_APPROVAL_SCOPE")
        target.update(
            condition_id=sdk_ops.parse_hash(args.condition_id, "condition_id"),
            index_set=sdk_ops.parse_index_set(args.index_set),
            amount_atoms=sdk_ops.parse_amount_atoms(args.amount_atoms, "amount_atoms"),
            redeem_full_index_set=args.redeem_full_index_set,
        )
        if not target["is_neg_risk"] and not args.redeem_full_index_set:
            raise ToolError(ErrorCode.INVALID_INPUT, "redeem", "MISSING_FULL_INDEX_SET_CONSENT")
        if target["is_neg_risk"] and args.redeem_full_index_set:
            raise ToolError(ErrorCode.INVALID_INPUT, "redeem", "NEG_RISK_FULL_SET_UNSUPPORTED")
    return target


def run(argv: list[str], *, session_factory: Any = None, signer_loader: Any = None) -> sdk_ops.OperationResult:
    """Run explicit public inputs; callers may inject strict offline test dependencies."""
    command_argv = argv if argv and argv[0] in ("plan", "check", "approve", "redeem") else ["plan", *argv]
    args = build_parser().parse_args(command_argv)
    target = normalize_target(args)
    if args.command == "plan":
        if args.execute or args.signer_fd is not None or args.tx_hash:
            raise ToolError(ErrorCode.INVALID_INPUT, "plan", "PLAN_CANNOT_EXECUTE")
        return sdk_ops.plan_target(target)
    if args.command == "check" and (args.execute or args.signer_fd is not None):
        raise ToolError(ErrorCode.INVALID_INPUT, "check", "CHECK_CANNOT_EXECUTE")
    if args.command != "check" and args.tx_hash:
        raise ToolError(ErrorCode.INVALID_INPUT, "execute", "HASH_RECOVERY_IS_READ_ONLY")
    return sdk_ops.execute_target(
        target, args.rpc_url, command=args.command, execute=args.execute,
        confirmation=args.confirm_target, signer_fd=args.signer_fd, tx_hash=args.tx_hash,
        session_factory=session_factory or sdk_ops.ReadSession,
        signer_loader=signer_loader or sdk_ops.fd_signer_loader,
    )


def main() -> int:
    """Emit fixed public JSON; errors and incomplete writes are nonzero."""
    try:
        result = run(sys.argv[1:])
        output = result.as_dict()
        output["target_digest"] = sdk_ops.target_digest(result.target)
        print(json.dumps(output, sort_keys=True))
        return 1 if result.error else 0
    except ToolError as error:
        print(json.dumps({"error": error.as_dict()}, sort_keys=True))
        return 2
    except Exception:
        print(json.dumps({"error": {"code": "READ_UNAVAILABLE", "step": "cli", "reason": "OPERATION_FAILED"}}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
