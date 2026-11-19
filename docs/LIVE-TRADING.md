# Going live

Every bot starts in dry run: it reads real markets, builds and signs orders, and sends nothing. This guide covers switching that off.

> ⚠️ **No live order has been sent through this engine on Predict.fun or on Polymarket's CLOB V2.** Signing, sign-in and order amounts are checked against each venue's official SDK, and dry runs have used real books and real on-chain fills. Sending an order, cancelling one and reading a real fill have not been run, so expect to find problems, and no strategy here has a track record. Start with money you can afford to lose.

## Before either venue

1. **Check that you're allowed to trade there.** Read the venue's terms. Don't try to get around a restriction.
2. **Use a new wallet** that holds only what you're prepared to lose. Never use your main wallet: its private key sits in `config.yaml` or an environment variable.
3. **Run a dry run first** for a full session, and read what it would have done.
4. **Set small limits** (see [Limits](#limits)).

## Predict.fun

**Terms:** [Predict's Terms of Service](https://docs.predict.fun/terms-of-service) bar several jurisdictions, including the US and UK. Signing in accepts those terms.

**You need:**

| Item | Where it goes |
|---|---|
| Wallet private key | `private_key` in `config.yaml`, or `PM_PRIVATE_KEY` |
| API key from Predict (mainnet only) | `predict_api_key`, or `PM_PREDICT_API_KEY` |
| Predict Account address, if you trade through the web app's smart wallet | `predict_account`, or `PM_PREDICT_ACCOUNT`. Set `private_key` to the Privy wallet exported from your account settings |
| USDT on BNB Chain, plus BNB for gas | The trading wallet |
| Approvals, set once | `setApprovals` in the official [`PredictDotFun/sdk`](https://github.com/PredictDotFun/sdk). The engine doesn't send on-chain transactions |

**Check the connection.** The first command signs nothing with Predict. The second signs in, which accepts the terms.

```bash
./polymarket-toolkits --venue predict-fun check-venue
./polymarket-toolkits --venue predict-fun check-venue --sign-in
```

**Testnet or mainnet:** `predict_fun.network` in `config.json` is `testnet` by default. Change it to `mainnet` only after a testnet order has worked.

**Good to know:**

- Removing an order on Predict doesn't cancel it on-chain. A removed order can stay fillable for a short time, and the bots keep watching it until the venue closes it.
- Cross-Market Arbitrage only reports gaps. It refuses to run live.
- BTC prediction uses Binance spot, with Coinbase as a paper-only fallback; these proxies are not the official settlement feed. Settlement requires the venue's verified round facts (provider CHAINLINK or UNKNOWN). The existing live source guard remains in force.
- The bots don't redeem winnings. Redeem them on Predict.

## Controlled BTC5m paper (07-004)

The fixed launcher is `tools/predictfun/btc5m_control.py`; see the exact command in
[the tool README](../tools/predictfun/README.md#07-004-固定-btc5m-受控入口).
Run it only after independent Validate PASS and a separately approved unique run-id.
This engineering change does not authorize a live run or start the six-hour observation.

Unlike ordinary mock mode, `unsigned-paper` never constructs a wallet, reads keyring,
loads credential YAML, or signs in. It does not load an entire .env file; only the fixed
single-key exception below is allowed. The existing market API key environment source takes priority;
wallet/JWT environment keys are refused. The frozen public snapshot fixes BTCUSDT,
exact 300-second rounds, 100 USDT initial virtual cash, 2 USDT order/market caps,
10 USDT total exposure and a latched 20 USDT cumulative net-loss stop including fees.
BUY fees reduce received shares; they are not charged to cash a second time.

The market API key comes from the environment first. When the parent environment has
no `YQUANT_PREDICT_FUN_TOKEN` key at all, exactly that one key is read from the fixed
project file `<project root>/skills/.env`; the path comes from the source location,
not the working directory, and cannot be overridden. A present but blank key is still
refused, and only single-line `KEY=value` assignments (optional `export`, quotes and
trailing comments) are accepted, with no substitution or evaluation. A missing,
duplicated, empty, unreadable or malformed entry fails with the fixed redacted
`API_SOURCE` error and exit code 2. No other variable from that file is imported,
and the parent environment, argv and public snapshots never change. This exception
applies to `unsigned-paper` only; signed mode, the SDK and the original
check/enroll/sign-in remain environment-only.

Deadline stops entries, cancels quotes, then permits at most 600 seconds of read-only
official-settlement tail checks for traded markets. Signal/parent EOF interrupts the
tail; parent cleanup, kill and reap share one 30-second deadline. No automatic restart.
Missing/invalid final accounting and native child failure remain explicitly incomplete.
Official paper payout releases virtual cash only once; live settlement is not redeem
or confirmed wallet cash. Model Binance/Coinbase spot is not the official oracle
provider (CHAINLINK/UNKNOWN). Gas, queueing, partial fills and redeem delay are not
measured or simulated, and a profitable window would not establish sustained alpha.

Phase1 (07-005) fixes new `unsigned-paper` runs to `phase1_net_shares` and the
`btc5m-controlled-paper.v2` protocol. Entry uses `EV = net_shares * fair - cost`
and `edge = EV / gross_shares`, not cash-fee EV or ROI as the gate. Defaults,
cash/exposure limits and the 20 USDT stop are unchanged. Ordinary paper, signed
mode and replay/history `/1` retain their prior semantics.

The original launcher validates each v2 diagnostic and its candidate/reservation/
fill/settlement relations before the first evidence write. Evaluations, submit
and pre-apply rechecks use distinct check times; missing values remain null.
Absent REST source timestamps mean RECEIPT_BOUND, not verified oracle freshness.
Binance/BTCUSDT and Coinbase/BTC-USD have separate source epochs and warmup.
Existing replay explicitly selects `btc-arb-replay/2` through its config; it does
not create a second ledger. Old v1 evidence is read-only with
DIAGNOSTICS_UNAVAILABLE, never silently upgraded or mixed into execution metrics.
Attempt direction, settled-entry direction, per-token net shares and final cash
PnL are separate metrics; DRAW/abstain/pending are excluded from the appropriate
direction denominators and pending final cash PnL is unavailable. This is an
offline correctness change, not a new paper/live authorization or an alpha claim.

`signed-future-live` requires separate action approval and the original account/budget
and signer gates. It has no paper ledger: unknown account amounts are null with
LIVE_ACCOUNTING_UNAVAILABLE and Gas NOT_MEASURED, not a fictitious 100 USDT balance
or NOT_INCURRED claim. Do not infer live readiness from offline synthetic tests.

## Polymarket

Copy Trading is the only strategy on Polymarket for now.

**Terms:** Polymarket [rejects orders from restricted regions](https://docs.polymarket.com/api-reference/geoblock), including the US and UK. `check-venue` shows what Polymarket reports for your connection, and a live run stops at startup if it's blocked.

**You need:**

| Item | Where it goes |
|---|---|
| Wallet private key | `private_key` in `config.yaml`, or `PM_PRIVATE_KEY` |
| The address holding your funds, if it isn't that key's own address | `funder_address`, or `PM_FUNDER_ADDRESS` |
| Your wallet type, if you set a funder: `proxy` (email login), `safe` (browser wallet) or `deposit_wallet` | `signature_type`, or `PM_SIGNATURE_TYPE` |
| pUSD on Polygon. It's Polymarket's collateral, wrapped from USDC.e | The funder wallet |
| POL for gas, if you trade from your own key | The same wallet |
| Approvals, set once | See below |
| A WebSocket RPC you trust | `site.polygon_ws_url` in `config.json` |

API credentials are created or derived for you on the first live run. To use your own, set `api_key`, `api_secret` and `api_passphrase` together.

**Approvals:** the Polymarket site sets these for accounts made there. If you trade from your own key, approve the CTF Exchange (`0xE111180000d2663C0091e4f400237545B87B996B`) and the Neg Risk Exchange (`0xe2222d279d744050d28e00520010520000310F59`) to spend your pUSD, and to move your outcome tokens on the Conditional Tokens contract. Neg-risk markets also need the Neg Risk Adapter. Polymarket's own `approvals.rs` example still lists older contracts and USDC.e, so don't run it unchanged.

**Check it.** The first command signs a sample order locally and sends nothing. The second creates or derives your API credentials, then shows your balance and whether each exchange is approved.

```bash
./polymarket-toolkits --venue polymarket check-venue
./polymarket-toolkits --venue polymarket check-venue --sign-in
```

**Good to know:**

- The shipped public RPC can miss events under load, and a missed event is a missed copy. Use your own for anything real.
- Prices snap to the market's tick in your favour, so a buy limit of 0.505 on a 0.01 tick becomes 0.50 and may not fill.
- Orders below the market's minimum, usually 5 shares, are refused.
- Copy Trading watches the CTF and Neg Risk exchanges. It doesn't yet see fills on Exchange V3, which handles position-order markets.
- Config files from before v0.2.0 are refused. Copy the `exchange` block from the current `config.json`.

## Limits

Set these before you switch anything on.

**Copy Trading**, in `config.json`:

| Setting | Shipped | For a first run |
|---|---|---|
| `strategy.max_order_size_usd` | 500 | 10 |
| `strategy.copy_size` | 20 (percent of the whale's trade) | 5 |
| `filters.per_category_max_open_usd` | 300 to 500 | 25 |

**The other nine**, under `strategies.limits` in `config.json`, hard caps in USD:

| Setting | Shipped |
|---|---|
| `max_order_usd` | 5 |
| `max_market_usd` | 25 |
| `max_total_usd` | 100 |
| `max_loss_usd` (halts new entries) | 25 |

Positions already in the wallet count toward these caps, and no strategy will sell them.

## Switch it on

Both flags in `config.json` have to change. Either one left alone keeps you in dry run.

```json
"bot": {
  "enable_trading": true,
  "mock_trading": false
}
```

Then run one strategy, and watch the first order on the venue's own site:

```bash
./polymarket-toolkits --venue predict-fun run market-making
./polymarket-toolkits --venue polymarket run copy-trading
```

The log shows `LIVE TRADING IS ON` at startup. If you don't see it, you're still in dry run.

## Stopping

Press **Ctrl-C** once.

- **Predict.fun strategies** finish the current step and stop. Check the venue afterwards for resting orders.
- **Copy Trading** stops following and stops its take-profit and stop-loss checks. Open positions stay in your wallet. They're tracked in memory, so a restart won't resume watching them: manage them on the venue.

To go back to dry run, set the flags back and restart.

## If something goes wrong

- **Orders rejected:** the venue's reason is in the log. Common causes are missing approvals, no balance, the wrong `signature_type`, or a restricted region.
- **A fill looks wrong:** stop adding risk and reconcile the original order. Unknown or incomplete fills must not be replaced with the planned size. The live-foundation changes are still awaiting independent verification; do not enable live trading based on compilation or this document alone.
- **Found a bug in the live path?** [Open an issue](https://github.com/tarpboi/Prediction-Markets-Trading-Bot-Toolkits/issues) with the log, with keys removed. Reports from real runs are especially useful, as none exist yet.

## Official SDK thin operations tool (live-foundation candidate)

`tools/predictfun/cli.py` reuses **predict-sdk==0.0.22** for one explicit
approval or redemption. It is not a second bot, wallet platform, daemon or
Rust execution bridge. See [tool instructions](../tools/predictfun/README.md)
for isolated hash-pinned dependencies, target-digest confirmation, finite
ERC20 amounts versus full ERC1155 operator permissions, and the standard
CT full-selected-index-set limitation.

- `plan` is signer/provider-free; `check` is signer-free and needs explicit
  RPC/account/market context. Missing or unreadable context is an error,
  never a healthy zero. No default key, business config or `.env` is read.
- `approve`/`redeem` require explicit execution, the matching plan digest,
  target/route/read checks and a caller-supplied non-stdio signer descriptor.
  This implementation cycle uses only fake signer/provider dependencies.
- Signing and broadcasting remain inside the official SDK. Local dependency
  wrappers retain the transaction hash before send and never replay a send;
  receipt success alone is not proof of the expected allowance or payout.
- Standard SDK redemption ignores the amount argument. The tool therefore
  rejects partial standard redemptions, requires explicit full-set consent
  and rereads the selected set balance before signing. This is not a protocol
  amount cap or protection against concurrent external deposits.
- Never run this tool concurrently with a bot on the same wallet. It has no
  cross-process locking, durable journal or automatic restart recovery.

**Status:** offline candidate implementation/self-tests only. Independent
Verify/Review, real account checks, real signing, orders, on-chain approvals,
redemptions and payout confirmation have not been performed or authorized.
The legacy switch-on examples above are not permission to activate this
candidate, nor evidence of profitability or live-trading readiness.

## BTC5m 07-007 explicit live candidate: offline only

The existing btc5m_control launcher now selects the new domain only with the
complete explicit live-contract/operations-mode/risk-window/authorization/deadline
argument set. It uses the same phase1_net_shares core and original Predict adapter,
not a paper-cash simulation or a second runner. Legacy signed-null evidence remains
unavailable; unsigned v2, paper72 and its unsigned-only observer keep their old paths.

The M-01 fixed candidate binary is ROOT/data/reports/2026-11-19-btc5m-live-parity-and-operations/evidence/candidate-v2/polymarket-toolkits
(SHA256 8d4bfa158011e9f15c52ab0a87eb284e86686347956aac3dc3ecc5366857ccaa).
Historical candidate-v1/cargo-target/debug/polymarket-toolkits (39f48ca7…) and its
R1 REVISE evidence remain unchanged; the new candidate is not authorized for activation.
No arbitrary binary override, automatic build or upgrade of an existing run exists.
The fixed public activation/storage deployment is not provisioned or authorized.
Missing or mismatched public binding fails before trading secrets or a child.

An account-wide OS lock and one persistent Mongo ARMED refusal precede durable
startup files and the original one-shot secret loader. The original supervisor
validates each new live frame, durably writes and reads it back, then acknowledges
only the matching prepared order. Failure retains the audit prefix and does not
fabricate cash, a final event or successful native exit. The disk reader reports
execution, cash and audit completeness separately.

COLD_START_REFUSE remains after normal exit or crash; changing run/window or
releasing the OS lock does not remove ARMED. There is no reset/resume command.
A positive official claim means WAIT_CASH/MANUAL_PAUSE, not redeemed cash or renewed
entry permission. Cash recovery uses existing SDK/manual operations only under a
separate explicit authorization, never concurrent with the bot on this account.

This candidate still requires independent Validate and separate production approval
for storage binding, exact candidate digest, credentials, account/Gas checks,
takeover/alert arrangements and every real transaction. The historical activation
examples elsewhere in this document do not authorize this candidate. No production
activation file, Mongo operation, signing/order/cancel, fund movement or external
notification was performed by this implementation.
