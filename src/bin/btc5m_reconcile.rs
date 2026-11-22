//! Read-only account/history reconciliation entry for a BTC5m recovery decision.
//!
//! This binary answers one question for a human taking over a stopped run: what
//! does the original account, order and history consumer see on this account
//! right now? It reuses the toolkit's native consumers unchanged:
//!
//! * [`PredictFunClient::new`] and [`PredictFunTrader::new`] build the same
//!   client as `check-venue --sign-in` (authenticated with the same sign-in
//!   exchange the live child uses), and
//! * [`OrderVenue::account_snapshot`] is the native
//!   `read_account_snapshot` (`src/service/predict_fun/account.rs:302`), which
//!   also runs the native `reconcile_account_history`
//!   (`src/service/predict_fun/account.rs:448`) and fails closed on any
//!   incomplete page, duplicate fact or unexplained historical order hash.
//!
//! It never signs an order, never posts to a venue write endpoint, never writes
//! a database and never touches the live barrier or activation. Its only
//! outbound traffic is:
//!
//! 1. the native snapshot's authenticated reads (the sign-in exchange that
//!    yields the token, then GETs of positions, orders, matches and the
//!    account), and
//! 2. two read-only JSON-RPC calls (`eth_chainId`, `eth_getBlockByNumber`) on
//!    the operator's explicit `--rpc-url`, to record the observed chain head
//!    around the native fixed-block read.
//!
//! The native snapshot reads balances at a fixed block but does not expose that
//! block, so this entry records the head immediately before and after the
//! native read. The native read's fixed block lies inside that observed range;
//! the projection states that range instead of claiming a block the native
//! consumer never published. Keeping every live-path source byte-identical is
//! deliberate: `btc5m_control.LIVE_BINARY_SHA256` and the live candidate file
//! hashes stay valid, and the live path gains no new capability.
//!
//! Output is a public-safe JSON projection: identities, atoms, token ids,
//! open order ids, the reconciliation verdict and the observation window. No
//! credential, JWT, API key, RPC URL or raw HTTP body is ever printed.

use anyhow::Error as AnyhowError;
use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use polymarket_toolkits::config::{AppConfig, ControlledMode};
use polymarket_toolkits::service::predict_fun::account::{
    parse_rpc_url, run_confirmation_slot, validate_order_hash, RunContext,
};
use polymarket_toolkits::service::predict_fun::{PredictFunClient, PredictFunTrader};
use polymarket_toolkits::service::venue::{
    AccountIdentity, AccountMode, AccountSnapshot, OrderVenue, VenueError, VenueErrorCode,
};
use serde_json::{json, Value};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

const SCHEMA_VERSION: &str = "btc5m-reconciliation-read.v2";
const EXIT_OK: u8 = 0;
const EXIT_INPUT: u8 = 2;
const EXIT_READ: u8 = 3;
const EXIT_RECONCILE: u8 = 4;
const EXIT_OUTPUT: u8 = 5;
const EXIT_USAGE: u8 = 64;

/// The only JSON-RPC methods this entry may send. Read-only by construction.
const RPC_READ_METHODS: [&str; 2] = ["eth_chainId", "eth_getBlockByNumber"];
const RPC_TIMEOUT: Duration = Duration::from_secs(12);

const USAGE: &str = "\
btc5m_reconcile — read-only BTC5m account/history reconciliation evidence

USAGE:
    btc5m_reconcile --config <PATH> --rpc-url <URL> --output <PATH> \\
                    [--predict-reconciled-order <HASH>]...

OPTIONS:
    --config <PATH>                     The run's public live config (JSON)
    --rpc-url <URL>                     Explicit read-only RPC endpoint
    --output <PATH>                     Where to create the projection (must not exist)
    --predict-reconciled-order <HASH>   An order hash a human explained; repeatable
    -h, --help                          Print this help
    -V, --version                       Print the version

EXIT CODES:
    0   The projection was written and the account is fully explained
    2   An input value or the public config violated its contract
    3   A required read was unavailable or incomplete
    4   The account, an order hash or the history could not be reconciled
    5   The projection could not be created
    64  A usage error

Reads only. It does not order, approve, redeem, or transfer funds, does not
write a database, and does not modify the live barrier or activation.";

/// A stable, public failure token set. Never carries raw causes.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Failure {
    code: &'static str,
    step: &'static str,
    reason: String,
    exit: u8,
}

impl Failure {
    fn new(code: &'static str, step: &'static str, reason: &str) -> Self {
        Self {
            code,
            step,
            reason: sanitize(reason),
            exit: exit_for_code(code),
        }
    }

    fn at_exit(mut self, exit: u8) -> Self {
        self.exit = exit;
        self
    }

    fn usage(reason: &str) -> Self {
        Self::new("INVALID_INPUT", "arguments", reason).at_exit(EXIT_USAGE)
    }

    fn input(reason: &str) -> Self {
        Self::new("INVALID_INPUT", "input", reason)
    }

    fn credentials(reason: &str) -> Self {
        Self::new("INVALID_INPUT", "credentials", reason)
    }

    fn read(reason: &str) -> Self {
        Self::new("READ_UNAVAILABLE", "rpc", reason)
    }

    fn reconcile(reason: &str) -> Self {
        Self::new("RECONCILIATION_REQUIRED", "reconciliation", reason)
    }

    fn output(reason: &str) -> Self {
        Self::new("OUTPUT_UNAVAILABLE", "output", reason)
    }

    fn exit_code(&self) -> u8 {
        self.exit
    }

    fn public_json(&self) -> Value {
        json!({"code": self.code, "step": self.step, "reason": self.reason})
    }
}

fn exit_for_code(code: &str) -> u8 {
    match code {
        "INVALID_INPUT" => EXIT_INPUT,
        "READ_UNAVAILABLE" | "DATA_UNAVAILABLE" => EXIT_READ,
        "ACCOUNT_MISMATCH" | "RECONCILIATION_REQUIRED" | "FACT_INCOMPLETE" => EXIT_RECONCILE,
        "OUTPUT_UNAVAILABLE" => EXIT_OUTPUT,
        _ => EXIT_READ,
    }
}

/// Only fixed token characters may cross the boundary.
fn sanitize(raw: &str) -> String {
    let trimmed = raw.trim();
    let allowed = !trimmed.is_empty()
        && trimmed.len() <= 96
        && trimmed.bytes().all(|b| {
            b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_' || b == b':' || b == b'.'
        });
    if allowed {
        trimmed.to_string()
    } else {
        "UNCLASSIFIED".to_string()
    }
}

/// Classify a native error without ever echoing its raw text.
///
/// `ApiError`'s display carries the HTTP body, so only typed venue errors and
/// bare token-shaped context messages may cross into the boundary.
fn classify(error: &AnyhowError, step: &'static str) -> Failure {
    if let Some(venue) = error.downcast_ref::<VenueError>() {
        let code = match venue.code {
            VenueErrorCode::InvalidInput => "INVALID_INPUT",
            VenueErrorCode::AccountMismatch => "ACCOUNT_MISMATCH",
            VenueErrorCode::ReadUnavailable => "READ_UNAVAILABLE",
            VenueErrorCode::FactIncomplete => "FACT_INCOMPLETE",
            VenueErrorCode::ReconciliationRequired => "RECONCILIATION_REQUIRED",
            VenueErrorCode::SubmissionUnknown => "SUBMISSION_UNKNOWN",
            VenueErrorCode::ExecutionNotEnabled => "EXECUTION_NOT_ENABLED",
            VenueErrorCode::TxUnconfirmed => "TX_UNCONFIRMED",
            VenueErrorCode::SdkIncompatible => "SDK_INCOMPATIBLE",
            VenueErrorCode::BudgetUnconfirmed => "BUDGET_UNCONFIRMED",
        };
        return Failure::new(code, step, &venue.reason);
    }
    for cause in error.chain() {
        let text = cause.to_string();
        // Fixed prefixes are safe to classify: only the prefix survives.
        if text.starts_with("CONTROLLED_MODE") {
            return Failure::new("INVALID_INPUT", step, "CONTROLLED_MODE");
        }
        if text.starts_with("PUBLIC_CONFIG") {
            return Failure::new("INVALID_INPUT", step, "PUBLIC_CONFIG");
        }
        if sanitize(&text) != "UNCLASSIFIED" {
            let code = match text.split(':').next().unwrap_or("") {
                "INVALID_INPUT" => "INVALID_INPUT",
                "ACCOUNT_MISMATCH" => "ACCOUNT_MISMATCH",
                "RECONCILIATION_REQUIRED" => "RECONCILIATION_REQUIRED",
                "FACT_INCOMPLETE" | "FACT_CONFLICT" => "FACT_INCOMPLETE",
                _ => "READ_UNAVAILABLE",
            };
            return Failure::new(code, step, &text);
        }
    }
    Failure::new("READ_UNAVAILABLE", step, "READ_FAILED")
}

// --- command line -------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
struct Args {
    config: PathBuf,
    rpc_url: String,
    output: PathBuf,
    reconciled_orders: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum Parsed {
    Help,
    Version,
    Run(Args),
}

fn parse_args<I: IntoIterator<Item = String>>(args: I) -> Result<Parsed, Failure> {
    let mut config: Option<PathBuf> = None;
    let mut rpc_url: Option<String> = None;
    let mut output: Option<PathBuf> = None;
    let mut reconciled_orders: Vec<String> = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "-V" | "--version" => return Ok(Parsed::Version),
            "--config" => {
                if config.is_some() {
                    return Err(Failure::usage("DUPLICATE_OPTION"));
                }
                config = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| Failure::usage("CONFIG_MISSING_VALUE"))?,
                ));
            }
            "--rpc-url" => {
                if rpc_url.is_some() {
                    return Err(Failure::usage("DUPLICATE_OPTION"));
                }
                rpc_url = Some(
                    args.next()
                        .ok_or_else(|| Failure::usage("RPC_URL_MISSING_VALUE"))?,
                );
            }
            "--output" => {
                if output.is_some() {
                    return Err(Failure::usage("DUPLICATE_OPTION"));
                }
                output = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| Failure::usage("OUTPUT_MISSING_VALUE"))?,
                ));
            }
            "--predict-reconciled-order" => reconciled_orders.push(
                args.next()
                    .ok_or_else(|| Failure::usage("ORDER_HASH_MISSING_VALUE"))?,
            ),
            _ => return Err(Failure::usage("UNKNOWN_OPTION")),
        }
    }
    Ok(Parsed::Run(Args {
        config: config.ok_or_else(|| Failure::usage("CONFIG_REQUIRED"))?,
        rpc_url: rpc_url.ok_or_else(|| Failure::usage("RPC_URL_REQUIRED"))?,
        output: output.ok_or_else(|| Failure::usage("OUTPUT_REQUIRED"))?,
        reconciled_orders,
    }))
}

/// Accept only an explicit, credential-free http(s) endpoint.
///
/// The native parser already rejects fragments, whitespace and non-http(s)
/// schemes; the live launcher additionally refuses userinfo and a query, and
/// this entry keeps that stricter shape so a key-bearing URL never becomes
/// evidence or a log line.
fn check_rpc_url(raw: &str) -> Result<(), Failure> {
    parse_rpc_url(raw)
        .map(|_| ())
        .map_err(|error| classify(&error.into(), "rpc_input"))?;
    let url = url::Url::parse(raw).map_err(|_| Failure::input("RPC_URL_INVALID"))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Failure::input("RPC_URL_INVALID"));
    }
    Ok(())
}

// --- read-only chain head -----------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockIdentity {
    number: u64,
    hash: String,
    timestamp: u64,
}

fn rpc_quantity(value: &Value) -> Result<u64, Failure> {
    let raw = value.as_str().ok_or_else(|| Failure::read("INVALID_HEX"))?;
    let digits = raw
        .strip_prefix("0x")
        .filter(|v| !v.is_empty() && v.len() <= 16 && v.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(|| Failure::read("INVALID_HEX"))?;
    u64::from_str_radix(digits, 16).map_err(|_| Failure::read("INVALID_HEX"))
}

/// One read-only JSON-RPC view of the operator's endpoint.
struct ChainHead<'a> {
    http: &'a reqwest::Client,
    url: &'a str,
}

impl ChainHead<'_> {
    async fn call(&self, method: &'static str, params: Value) -> Result<Value, Failure> {
        if !RPC_READ_METHODS.contains(&method) {
            return Err(Failure::read("RPC_METHOD_NOT_ALLOWED"));
        }
        let response = self
            .http
            .post(self.url)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()
            .await
            .map_err(|_| Failure::read("RPC_TRANSPORT"))?;
        if !response.status().is_success() {
            return Err(Failure::read("RPC_HTTP"));
        }
        let body: Value = response
            .json()
            .await
            .map_err(|_| Failure::read("RPC_JSON"))?;
        if body.get("error").is_some()
            || body.get("id") != Some(&json!(1))
            || body.get("jsonrpc") != Some(&json!("2.0"))
        {
            return Err(Failure::read("RPC_RESPONSE"));
        }
        body.get("result")
            .filter(|value| !value.is_null())
            .cloned()
            .ok_or_else(|| Failure::read("RPC_RESULT_MISSING"))
    }

    async fn chain_id(&self) -> Result<u64, Failure> {
        rpc_quantity(&self.call("eth_chainId", json!([])).await?)
    }

    async fn head(&self) -> Result<BlockIdentity, Failure> {
        let block = self
            .call("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        let hash = block["hash"]
            .as_str()
            .map(str::to_ascii_lowercase)
            .filter(|hash| {
                hash.len() == 66
                    && hash.starts_with("0x")
                    && hash[2..].bytes().all(|b| b.is_ascii_hexdigit())
            })
            .ok_or_else(|| Failure::read("BLOCK_HASH"))?;
        Ok(BlockIdentity {
            number: rpc_quantity(&block["number"])?,
            hash,
            timestamp: rpc_quantity(&block["timestamp"])?,
        })
    }
}

// --- projection ---------------------------------------------------------------

fn utc_text(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn block_time(seconds: u64) -> String {
    let seconds = i64::try_from(seconds).unwrap_or(i64::MAX);
    Utc.timestamp_opt(seconds, 0)
        .single()
        .map(utc_text)
        .unwrap_or_else(|| "UNKNOWN".to_string())
}

fn block_json(block: &BlockIdentity) -> Value {
    json!({"number": block.number, "hash": block.hash, "time_utc": block_time(block.timestamp)})
}

fn account_mode_text(mode: AccountMode) -> &'static str {
    match mode {
        AccountMode::Eoa => "eoa",
        AccountMode::PredictAccount => "predict-account",
    }
}

/// Build the public-safe projection of one native account read.
fn project(
    identity: &AccountIdentity,
    snapshot: &AccountSnapshot,
    reconciled_orders: &[String],
    before: &BlockIdentity,
    after: &BlockIdentity,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
) -> Value {
    let mut positions: Vec<(String, String)> = snapshot
        .positions
        .iter()
        .map(|(token, shares)| (token.clone(), shares.to_string()))
        .collect();
    positions.sort_by(|left, right| left.0.cmp(&right.0));
    let mut open_orders: Vec<String> = snapshot.open_orders.clone();
    open_orders.sort();
    open_orders.dedup();
    json!({
        "schema_version": SCHEMA_VERSION,
        "read_only": true,
        "venue": "predict_fun",
        "account_identity": {
            "chain_id": identity.chain_id,
            "trading_address": identity.trading_address,
            "signer_address": identity.signer_address,
            "account_mode": account_mode_text(identity.mode),
        },
        "account": {
            "collateral_atoms": snapshot.collateral_atoms.map(|atoms| atoms.to_string()),
            "gas_atoms": snapshot.gas_atoms.map(|atoms| atoms.to_string()),
            "positions": positions.iter().map(|(token, shares)| {
                json!({"token": token, "shares_atoms": shares})
            }).collect::<Vec<_>>(),
            "open_orders": open_orders,
            "positions_explained": snapshot.positions_explained,
            "settled_zero": snapshot.settled_zero.iter().map(|zero| {
                json!({
                    "token": zero.token,
                    "market_id": zero.market_id,
                    "oracle_status": zero.oracle_status,
                    "trading_status": zero.trading_status,
                    "outcome_status": zero.outcome_status,
                    "payout_millionths": zero.payout_millionths,
                })
            }).collect::<Vec<_>>(),
            "claimed_win": snapshot.claimed_win.iter().map(|win| {
                json!({
                    "token": win.token,
                    "market_id": win.market_id,
                    "oracle_status": win.oracle_status,
                    "trading_status": win.trading_status,
                    "outcome_status": win.outcome_status,
                    "payout_millionths": win.payout_millionths,
                    "inventory_atoms": win.inventory_atoms.to_string(),
                    "claim_atoms": win.claim_atoms.to_string(),
                })
            }).collect::<Vec<_>>(),
            "pagination_complete": snapshot.complete,
        },
        "reconciliation": {
            "declared_orders": reconciled_orders,
            "positions_match_history": snapshot.positions_explained,
            "complete": snapshot.complete && snapshot.positions_explained,
        },
        "observation": {
            "started_at": utc_text(started_at),
            "finished_at": utc_text(finished_at),
            "chain_id": identity.chain_id,
            "block_before": block_json(before),
            "block_after": block_json(after),
            "anchor_semantics": "NATIVE_FIXED_BLOCK_WITHIN_OBSERVED_RANGE",
        },
    })
}

fn write_projection(path: &Path, projection: &Value) -> Result<(), Failure> {
    if path.exists() {
        return Err(Failure::output("OUTPUT_EXISTS"));
    }
    let mut text =
        serde_json::to_vec_pretty(projection).map_err(|_| Failure::output("OUTPUT_ENCODE"))?;
    text.push(b'\n');
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| Failure::output("OUTPUT_CREATE"))?;
    file.write_all(&text)
        .and_then(|()| file.sync_all())
        .map_err(|_| Failure::output("OUTPUT_WRITE"))
}

// --- entry --------------------------------------------------------------------

enum ExitOutcome {
    Completed {
        projection_path: String,
        complete: bool,
    },
    Failed(Failure),
}

impl ExitOutcome {
    fn exit_code(&self) -> u8 {
        match self {
            Self::Completed { complete: true, .. } => EXIT_OK,
            Self::Completed {
                complete: false, ..
            } => EXIT_RECONCILE,
            Self::Failed(failure) => failure.exit_code(),
        }
    }

    fn public_json(&self) -> Value {
        match self {
            Self::Completed {
                projection_path,
                complete,
            } => json!({
                "schema_version": SCHEMA_VERSION,
                "ok": complete,
                "complete": complete,
                "projection": projection_path,
            }),
            Self::Failed(failure) => failure.public_json(),
        }
    }
}

/// Fail closed on anything the projection must never launder as truth.
fn verify_read(identity: &AccountIdentity, snapshot: &AccountSnapshot) -> Result<(), Failure> {
    if snapshot.identity != *identity {
        return Err(Failure::new(
            "ACCOUNT_MISMATCH",
            "account_snapshot",
            "IDENTITY_MISMATCH",
        ));
    }
    if !snapshot.complete {
        return Err(Failure::read("FACT_INCOMPLETE:PAGINATION"));
    }
    Ok(())
}

fn verify_heads(before: &BlockIdentity, after: &BlockIdentity) -> Result<(), Failure> {
    if after.number < before.number {
        return Err(Failure::read("RPC_HEAD_REGRESSED"));
    }
    Ok(())
}

async fn run(args: Args) -> Result<(String, bool), Failure> {
    check_rpc_url(&args.rpc_url)?;
    for hash in &args.reconciled_orders {
        validate_order_hash(hash).map_err(|error| classify(&error.into(), "reconciled_order"))?;
    }
    let config = AppConfig::load_controlled(&args.config, ControlledMode::SignedFutureLive)
        .map_err(|error| classify(&error, "public_config"))?;
    // The declared C0 lets the native reader explain a claimed payout against
    // the exact pre-claim cash. A file without a controlled_run declares none.
    let raw = std::fs::read_to_string(&args.config)
        .map_err(|_| Failure::input("PUBLIC_CONFIG"))?;
    let budget_atoms = polymarket_toolkits::config::RunParameters::declared_settings(&raw)
        .ok()
        .flatten()
        .and_then(|settings| settings.budget_atoms().ok());
    let rpc_url =
        parse_rpc_url(&args.rpc_url).map_err(|error| classify(&error.into(), "rpc_input"))?;
    run_confirmation_slot().install_context(RunContext {
        confirmation: None,
        reconciled_orders: args.reconciled_orders.clone(),
        rpc_url: Some(rpc_url),
        budget_atoms,
    });
    // Missing or unusable credentials are input problems, never reads: the
    // cause is not echoed so no key material can escape.
    let client =
        PredictFunClient::new(&config).map_err(|_| Failure::credentials("CONTEXT_MISSING"))?;
    let trader = PredictFunTrader::new(client, &config)
        .map_err(|_| Failure::credentials("CONTEXT_MISSING"))?;
    let identity = trader.identity();
    let http = reqwest::Client::builder()
        .timeout(RPC_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| Failure::read("RPC_CLIENT"))?;
    let chain = ChainHead {
        http: &http,
        url: &args.rpc_url,
    };
    if chain.chain_id().await? != identity.chain_id {
        return Err(Failure::new("ACCOUNT_MISMATCH", "chain", "CHAIN_MISMATCH"));
    }
    let started_at = Utc::now();
    let before = chain.head().await?;
    let snapshot = trader
        .account_snapshot()
        .await
        .map_err(|error| classify(&error, "account_snapshot"))?;
    let after = chain.head().await?;
    let finished_at = Utc::now();
    verify_read(&identity, &snapshot)?;
    verify_heads(&before, &after)?;
    let projection = project(
        &identity,
        &snapshot,
        &args.reconciled_orders,
        &before,
        &after,
        started_at,
        finished_at,
    );
    write_projection(&args.output, &projection)?;
    Ok((
        args.output.display().to_string(),
        snapshot.positions_explained,
    ))
}

#[tokio::main]
async fn main() -> ExitCode {
    let parsed = match parse_args(std::env::args().skip(1)) {
        Ok(parsed) => parsed,
        Err(failure) => {
            eprintln!("{}", failure.public_json());
            return ExitCode::from(failure.exit_code());
        }
    };
    let outcome = match parsed {
        Parsed::Help => {
            println!("{USAGE}");
            return ExitCode::from(EXIT_OK);
        }
        Parsed::Version => {
            println!("btc5m_reconcile {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::from(EXIT_OK);
        }
        Parsed::Run(args) => match run(args).await {
            Ok((projection_path, complete)) => ExitOutcome::Completed {
                projection_path,
                complete,
            },
            Err(failure) => ExitOutcome::Failed(failure),
        },
    };
    if let ExitOutcome::Completed {
        complete: false, ..
    } = &outcome
    {
        eprintln!(
            "{}",
            Failure::reconcile("RECONCILIATION_REQUIRED:UNEXPLAINED_POSITIONS").public_json()
        );
    }
    let exit = outcome.exit_code();
    let line = outcome.public_json();
    if exit == EXIT_OK {
        println!("{line}");
    } else {
        eprintln!("{line}");
    }
    ExitCode::from(exit)
}

#[cfg(test)]
mod tests {
    //! Offline only: no socket, credential, signer or venue is used here.
    use super::*;
    use alloy_primitives::U256;
    use polymarket_toolkits::models::VenueId;

    fn identity() -> AccountIdentity {
        AccountIdentity {
            chain_id: 56,
            trading_address: "0xFC9eB66e6F22f96608058C0A711Ed048eED8c2E2".to_string(),
            mode: AccountMode::PredictAccount,
            signer_address: "0x5129330159f4E6a99eF873f86901E3cafa7f9A7D".to_string(),
            venue: VenueId::PredictFun,
        }
    }

    fn snapshot() -> AccountSnapshot {
        AccountSnapshot {
            identity: identity(),
            collateral_atoms: Some(U256::from(100_000_000_000_000_000_000u128)),
            gas_atoms: Some(U256::from(4_990_000_000_000_000u128)),
            positions: vec![
                ("9002".to_string(), U256::from(2u8)),
                ("9001".to_string(), U256::from(1u8)),
            ],
            positions_explained: true,
            settled_zero: vec![],
            claimed_win: vec![],
            open_orders: vec!["order-b".to_string(), "order-a".to_string()],
            complete: true,
        }
    }

    fn block(number: u64) -> BlockIdentity {
        BlockIdentity {
            number,
            hash: format!("0x{}", "ab".repeat(32)),
            timestamp: 1_790_000_000,
        }
    }

    fn collect_keys(value: &Value, into: &mut Vec<String>) {
        match value {
            Value::Object(map) => {
                for (key, nested) in map {
                    into.push(key.clone());
                    collect_keys(nested, into);
                }
            }
            Value::Array(items) => items.iter().for_each(|item| collect_keys(item, into)),
            _ => {}
        }
    }

    #[test]
    fn projection_is_public_safe_and_allowlisted() {
        let started = Utc.timestamp_opt(1_790_000_000, 0).unwrap();
        let projection = project(
            &identity(),
            &snapshot(),
            &[format!("0x{}", "cd".repeat(32))],
            &block(100),
            &block(101),
            started,
            started,
        );
        let mut keys = Vec::new();
        collect_keys(&projection, &mut keys);
        let allowed = [
            "schema_version",
            "read_only",
            "venue",
            "account_identity",
            "chain_id",
            "trading_address",
            "signer_address",
            "account_mode",
            "account",
            "collateral_atoms",
            "gas_atoms",
            "positions",
            "token",
            "shares_atoms",
            "open_orders",
            "positions_explained",
            "settled_zero",
            "claimed_win",
            "inventory_atoms",
            "claim_atoms",
            "market_id",
            "oracle_status",
            "trading_status",
            "outcome_status",
            "payout_millionths",
            "pagination_complete",
            "reconciliation",
            "declared_orders",
            "positions_match_history",
            "complete",
            "observation",
            "started_at",
            "finished_at",
            "block_before",
            "block_after",
            "number",
            "hash",
            "time_utc",
            "anchor_semantics",
        ];
        for key in &keys {
            assert!(allowed.contains(&key.as_str()), "unexpected key {key}");
        }
        assert_eq!(
            projection["account"]["collateral_atoms"],
            "100000000000000000000"
        );
        assert_eq!(projection["account"]["gas_atoms"], "4990000000000000");
        assert_eq!(projection["account"]["positions"][0]["token"], "9001");
        assert_eq!(projection["account"]["open_orders"][0], "order-a");
        assert_eq!(projection["account"]["positions_explained"], true);
        assert_eq!(projection["reconciliation"]["complete"], true);
        assert_eq!(projection["observation"]["block_before"]["number"], 100);
        assert_eq!(projection["observation"]["block_after"]["number"], 101);
        assert_eq!(
            projection["observation"]["anchor_semantics"],
            "NATIVE_FIXED_BLOCK_WITHIN_OBSERVED_RANGE"
        );
    }

    #[test]
    fn projection_publishes_a_claimed_win_with_its_exact_atoms() {
        let mut snapshot = snapshot();
        snapshot.claimed_win.push(polymarket_toolkits::service::venue::ClaimedWin {
            token: "9001".into(),
            market_id: 7,
            oracle_status: "RESOLVED".into(),
            trading_status: "CLOSED".into(),
            outcome_status: "WON".into(),
            payout_millionths: 1_000_000,
            inventory_atoms: U256::from(5),
            claim_atoms: U256::from(5),
        });
        let started = Utc.timestamp_opt(1_790_000_000, 0).unwrap();
        let projection = project(
            &identity(),
            &snapshot,
            &[],
            &block(1),
            &block(2),
            started,
            started,
        );
        let win = &projection["account"]["claimed_win"][0];
        assert_eq!(win["token"], "9001");
        assert_eq!(win["payout_millionths"], 1_000_000);
        assert_eq!(win["inventory_atoms"], "5");
        assert_eq!(win["claim_atoms"], "5");
    }

    #[test]
    fn projection_marks_unexplained_positions_incomplete() {
        let mut snapshot = snapshot();
        snapshot.positions_explained = false;
        let started = Utc.timestamp_opt(1_790_000_000, 0).unwrap();
        let projection = project(
            &identity(),
            &snapshot,
            &[],
            &block(1),
            &block(2),
            started,
            started,
        );
        assert_eq!(projection["account"]["positions_explained"], false);
        assert_eq!(projection["reconciliation"]["complete"], false);
    }

    #[test]
    fn typed_venue_errors_keep_their_stable_code() {
        let error: AnyhowError = VenueError::new(
            VenueErrorCode::ReconciliationRequired,
            "account_snapshot",
            "UNEXPLAINED_HISTORY",
        )
        .into();
        let failure = classify(&error, "account_snapshot");
        assert_eq!(failure.code, "RECONCILIATION_REQUIRED");
        assert_eq!(failure.reason, "UNEXPLAINED_HISTORY");
        assert_eq!(failure.exit_code(), EXIT_RECONCILE);
    }

    #[test]
    fn raw_causes_never_cross_the_boundary() {
        let error = AnyhowError::msg("HTTP 401: token=SECRET-JWT-AND-KEY").context("GET /account");
        let failure = classify(&error, "account_snapshot");
        assert_eq!(failure.code, "READ_UNAVAILABLE");
        assert_eq!(failure.reason, "READ_FAILED");
        let line = failure.public_json().to_string();
        assert!(!line.contains("SECRET"));
        assert!(!line.contains("GET /account"));
    }

    #[test]
    fn native_token_errors_are_classified_without_text_leak() {
        let error = AnyhowError::msg("READ_UNAVAILABLE:PAGE_CAP");
        let failure = classify(&error, "account_snapshot");
        assert_eq!(failure.code, "READ_UNAVAILABLE");
        assert_eq!(failure.reason, "READ_UNAVAILABLE:PAGE_CAP");
        assert_eq!(failure.exit_code(), EXIT_READ);
    }

    #[test]
    fn contract_prefixes_are_input_failures_not_read_failures() {
        for (text, reason) in [
            ("CONTROLLED_MODE: ACCOUNT", "CONTROLLED_MODE"),
            ("PUBLIC_CONFIG: TRADING_SWITCH", "PUBLIC_CONFIG"),
        ] {
            let failure = classify(&AnyhowError::msg(text), "public_config");
            assert_eq!(failure.code, "INVALID_INPUT", "{text}");
            assert_eq!(failure.reason, reason, "{text}");
            assert_eq!(failure.exit_code(), EXIT_INPUT, "{text}");
        }
    }

    #[test]
    fn usage_requires_every_path_and_refuses_unknown_flags() {
        let args = |raw: &[&str]| parse_args(raw.iter().map(|value| value.to_string()));
        assert_eq!(
            args(&[
                "--config",
                "c.json",
                "--rpc-url",
                "https://rpc.invalid",
                "--output",
                "o.json"
            ]),
            Ok(Parsed::Run(Args {
                config: PathBuf::from("c.json"),
                rpc_url: "https://rpc.invalid".to_string(),
                output: PathBuf::from("o.json"),
                reconciled_orders: vec![],
            }))
        );
        assert_eq!(args(&["--help"]), Ok(Parsed::Help));
        for bad in [
            vec![],
            vec!["--config", "c.json", "--rpc-url", "https://rpc.invalid"],
            vec!["--config", "c.json", "--output", "o.json"],
            vec!["--rpc-url", "https://rpc.invalid", "--output", "o.json"],
            vec![
                "--config",
                "c.json",
                "--rpc-url",
                "https://rpc.invalid",
                "--output",
                "o.json",
                "--nope",
            ],
            vec!["--config"],
        ] {
            assert!(args(&bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn rpc_url_with_credentials_whitespace_or_query_is_refused() {
        for raw in [
            "https://user:pass@rpc.invalid",
            "https://rpc.invalid/?key=SECRET",
            "https://rpc.invalid/#fragment",
            "https://rpc.invalid/ path",
            "",
        ] {
            assert!(check_rpc_url(raw).is_err(), "{raw}");
        }
        assert!(check_rpc_url("https://rpc.invalid").is_ok());
    }

    #[test]
    fn order_hashes_are_validated_before_any_io() {
        assert!(validate_order_hash("0xnothex").is_err());
        assert!(validate_order_hash(&format!("0x{}", "ab".repeat(31))).is_err());
        assert!(validate_order_hash(&format!("0x{}", "ab".repeat(32))).is_ok());
    }

    #[test]
    fn exit_codes_are_distinct_per_failure_class() {
        assert_eq!(Failure::usage("X").exit_code(), EXIT_USAGE);
        assert_eq!(Failure::input("X").exit_code(), EXIT_INPUT);
        assert_eq!(Failure::read("READ_UNAVAILABLE").exit_code(), EXIT_READ);
        assert_eq!(
            Failure::reconcile("FACT_INCOMPLETE").exit_code(),
            EXIT_RECONCILE
        );
        assert_eq!(Failure::output("X").exit_code(), EXIT_OUTPUT);
    }

    #[test]
    fn rpc_allowlist_refuses_any_write_method() {
        assert!(RPC_READ_METHODS.contains(&"eth_chainId"));
        assert!(RPC_READ_METHODS.contains(&"eth_getBlockByNumber"));
        for method in ["eth_sendRawTransaction", "eth_sendTransaction", "eth_sign"] {
            assert!(!RPC_READ_METHODS.contains(&method));
        }
    }

    #[test]
    fn failed_or_mismatched_reads_are_never_complete() {
        let mut mismatched = snapshot();
        mismatched.identity.trading_address = "0x0000000000000000000000000000000000000001".into();
        let failure = verify_read(&identity(), &mismatched).unwrap_err();
        assert_eq!(failure.code, "ACCOUNT_MISMATCH");
        assert_eq!(failure.exit_code(), EXIT_RECONCILE);

        let mut incomplete = snapshot();
        incomplete.complete = false;
        let failure = verify_read(&identity(), &incomplete).unwrap_err();
        assert_eq!(failure.code, "READ_UNAVAILABLE");
        assert_eq!(failure.reason, "FACT_INCOMPLETE:PAGINATION");
        assert_eq!(failure.exit_code(), EXIT_READ);

        assert_eq!(verify_read(&identity(), &snapshot()), Ok(()));
        assert_eq!(verify_heads(&block(7), &block(7)), Ok(()));
        assert_eq!(
            verify_heads(&block(8), &block(7)).unwrap_err().reason,
            "RPC_HEAD_REGRESSED"
        );
    }

    #[test]
    fn projection_file_is_created_exclusively_and_never_overwritten() {
        let dir = std::env::temp_dir().join(format!("btc5m-reconcile-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("projection.json");
        let _ = std::fs::remove_file(&path);
        let started = Utc.timestamp_opt(1_790_000_000, 0).unwrap();
        let projection = project(
            &identity(),
            &snapshot(),
            &[],
            &block(1),
            &block(2),
            started,
            started,
        );
        assert_eq!(write_projection(&path, &projection), Ok(()));
        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(written["schema_version"], SCHEMA_VERSION);
        let second = write_projection(&path, &projection);
        assert!(second.is_err());
        assert_eq!(second.unwrap_err().reason, "OUTPUT_EXISTS");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Static read-only proof: this source may reuse the native snapshot but
    /// carries no venue write or database path of its own. Tokens are built from
    /// fragments so the guard never matches its own literals.
    #[test]
    fn source_has_no_venue_write_or_database_path() {
        let source = include_str!("btc5m_reconcile.rs");
        let forbidden: Vec<String> = [
            ("/", "orders"),
            ("authed", "_write"),
            ("ERC", "1155"),
            ("transfer", "From"),
            ("insert", "_one"),
            ("update", "_one"),
            ("delete", "_one"),
            ("approve", "("),
            ("redeem", "("),
        ]
        .iter()
        .map(|(head, tail)| format!("{head}{tail}"))
        .collect();
        for token in &forbidden {
            assert!(!source.contains(token.as_str()), "forbidden token present");
        }
        for required in ["OrderVenue", "account_snapshot", "PredictFunTrader::new"] {
            assert!(source.contains(required), "missing native reuse {required}");
        }
    }
}
