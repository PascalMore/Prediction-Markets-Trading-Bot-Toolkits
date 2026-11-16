//! Configuration loading and types.
//!
//! Two-file split:
//! - `config.json` — public settings (committed)
//! - `config.yaml` — credentials (gitignored, must never be committed)

use crate::models::VenueId;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// Which venue the copy-trading bot trades. Defaults to Polymarket.
    #[serde(default)]
    pub venue: VenueId,

    pub bot: BotConfig,
    pub site: SiteConfig,
    pub strategy: StrategyConfig,
    pub trading: TradingConfig,
    pub risk: RiskConfig,
    pub exchange: ExchangeConfig,

    /// Predict.fun settings, used when `venue` is `predict_fun`.
    #[serde(default)]
    pub predict_fun: PredictFunConfig,

    /// Market eligibility filter (allowlist + blocklist by category, tag, slug).
    #[serde(default)]
    pub filters: FiltersConfig,

    /// Take-profit / stop-loss configuration.
    #[serde(default)]
    pub tp_sl: TpSlConfig,

    /// Opt-in controlled paper cash and fee model. Absent in every ordinary
    /// run; present only in the controlled unsigned-paper configuration.
    #[serde(default)]
    pub controlled_paper: Option<ControlledPaperConfig>,

    /// The controlled run's identity and clock. Set by the launcher through the
    /// command line, never read from the public configuration file.
    #[serde(skip)]
    pub controlled: Option<ControlledRun>,

    /// Hard caps and tunables for the strategy bots other than copy trading.
    #[serde(default)]
    pub strategies: StrategiesConfig,

    /// Loaded from `config.yaml`. Not serialised back out.
    #[serde(skip)]
    pub credentials: Credentials,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotConfig {
    pub wallets_to_track: Vec<String>,

    /// When `false`, decisions are computed but no orders are sent.
    #[serde(default)]
    pub enable_trading: bool,

    /// When `true`, every order path returns early with a log line.
    /// Independent of `enable_trading` — both must be permissive for live trades.
    #[serde(default = "default_true")]
    pub mock_trading: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteConfig {
    pub gamma_api_base: String,
    pub data_api_base: String,
    pub clob_api_base: String,
    pub clob_wss_url: String,
    pub polygon_rpc_url: String,
    pub polygon_ws_url: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum CopyStrategy {
    Percentage,
    Fixed,
    Adaptive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub copy_strategy: CopyStrategy,

    /// For PERCENTAGE: percent of whale notional (e.g. 20.0 = 20%).
    /// For FIXED: USD per copy leg.
    /// For ADAPTIVE: ignored (uses adaptive_* below).
    pub copy_size: f64,

    pub trade_multiplier: f64,
    pub min_order_size_usd: f64,
    pub max_order_size_usd: f64,
    pub min_whale_shares_to_copy: f64,
    pub adaptive_threshold_usd: f64,
    pub adaptive_min_percent: f64,
    pub adaptive_max_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingConfig {
    /// Max API requests per `rate_window_secs`.
    pub rate_limit: u32,
    pub rate_window_secs: u64,
    pub poll_interval_secs: u64,

    /// Slippage tolerance applied to the order limit price (fractional, e.g. 0.005 = 0.5¢ at $1 scale).
    pub price_buffer: f64,

    pub order_expiration_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    pub large_trade_shares: f64,
    pub consecutive_trigger: u32,
    pub sequence_window_secs: u64,
    pub min_depth_usd: f64,
    pub trip_duration_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    pub ctf_exchange_address: String,
    pub neg_risk_exchange_address: String,
    /// Exchange for position-order books, which sign with domain version "3".
    #[serde(default = "default_exchange_v3")]
    pub exchange_v3_address: String,
    pub chain_id: u64,
    pub domain_name: String,
    pub domain_version: String,
}

fn default_exchange_v3() -> String {
    "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa".to_string()
}

impl ExchangeConfig {
    /// The exchange addresses are also what the copy-trading feed watches, so a
    /// config from before CLOB V2 would sign rejected orders and see no trades.
    pub fn ensure_clob_v2(&self) -> Result<()> {
        if self.domain_version != "2" {
            bail!(
                "config.json has exchange.domain_version {:?}, but Polymarket's CLOB V2 needs \"2\". \
                 Copy the exchange block from the current config.json.",
                self.domain_version
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PredictNetwork {
    #[default]
    Testnet,
    Mainnet,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PredictFunConfig {
    /// Selects the API host, chain and exchange contracts together, so they
    /// can't disagree.
    #[serde(default)]
    pub network: PredictNetwork,

    /// Predict.fun addresses to copy. Separate from `bot.wallets_to_track`,
    /// since a wallet's address differs between venues.
    #[serde(default)]
    pub wallets_to_track: Vec<String>,
}

/// Market eligibility filter.
///
/// Behavior:
/// 1. Block lists always win — anything matching is skipped.
/// 2. If *any* allow list is non-empty, the market must match at least one
///    allow rule for ANY of (slug / category / tag) to be eligible.
/// 3. If all allow lists are empty, the bot falls back to allow-everything-
///    minus-blocklist semantics (so users who haven't curated yet aren't
///    stranded with zero trades).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FiltersConfig {
    #[serde(default)]
    pub slug_allow: Vec<String>,
    #[serde(default)]
    pub slug_block: Vec<String>,
    #[serde(default)]
    pub categories_allow: Vec<String>,
    #[serde(default)]
    pub categories_block: Vec<String>,
    #[serde(default)]
    pub tags_allow: Vec<String>,
    #[serde(default)]
    pub tags_block: Vec<String>,

    /// Per-category cap on simultaneously open USD notional.
    /// e.g. `{ "Politics": 500, "Sports": 300 }`.
    #[serde(default)]
    pub per_category_max_open_usd: std::collections::HashMap<String, f64>,

    /// Per-tag cap on simultaneously open USD notional.
    #[serde(default)]
    pub per_tag_max_open_usd: std::collections::HashMap<String, f64>,
}

impl FiltersConfig {
    /// True when at least one allow list is populated — strict allowlist mode.
    pub fn is_strict(&self) -> bool {
        !self.slug_allow.is_empty()
            || !self.categories_allow.is_empty()
            || !self.tags_allow.is_empty()
    }
}

/// Limits every strategy bot runs under, enforced by [`crate::bot::runtime::Broker`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StrategyLimits {
    /// Largest single order.
    pub max_order_usd: f64,
    /// Open cost plus resting buys in one market.
    pub max_market_usd: f64,
    /// Open cost plus resting buys across all markets.
    pub max_total_usd: f64,
    /// Realised plus marked loss that halts new entries and pulls quotes.
    pub max_loss_usd: f64,
}

impl Default for StrategyLimits {
    fn default() -> Self {
        Self {
            max_order_usd: 5.0,
            max_market_usd: 25.0,
            max_total_usd: 100.0,
            max_loss_usd: 25.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StrategiesConfig {
    pub poll_secs: u64,
    /// How often the market list is read again.
    pub rescan_secs: u64,
    /// Lifetime signed into every resting order, so a crash can't leave one
    /// open forever.
    pub quote_ttl_secs: u64,
    pub limits: StrategyLimits,
    pub btc_arb: BtcArbConfig,
    pub cross_arb: CrossArbConfig,
    pub directional_arb: DirectionalArbConfig,
    pub spread_farming: SpreadFarmingConfig,
    pub sports: SportsConfig,
    pub resolution_sniper: ResolutionSniperConfig,
    pub orderbook_imbalance: OrderbookImbalanceConfig,
    pub market_making: MarketMakingConfig,
    pub whale_signal: WhaleSignalConfig,
}

impl Default for StrategiesConfig {
    fn default() -> Self {
        Self {
            poll_secs: 5,
            rescan_secs: 300,
            quote_ttl_secs: 300,
            limits: Default::default(),
            btc_arb: Default::default(),
            cross_arb: Default::default(),
            directional_arb: Default::default(),
            spread_farming: Default::default(),
            sports: Default::default(),
            resolution_sniper: Default::default(),
            orderbook_imbalance: Default::default(),
            market_making: Default::default(),
            whale_signal: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BtcArbConfig {
    /// Price feed symbols to trade, as the venue names them.
    pub symbols: Vec<String>,
    /// Exact round duration to trade. None preserves all venue cadences.
    pub round_interval_secs: Option<u32>,
    /// Model value minus cost and fee required to buy, in price units.
    pub min_edge: f64,
    pub order_usd: f64,
    /// No new entries this close to the end of a round.
    pub min_secs_left: i64,
    /// Lowest volatility the model will assume, per root second.
    pub vol_floor: f64,
}

impl BtcArbConfig {
    /// Reject zero before connecting to a venue or starting the strategy.
    pub fn ensure_round_interval(&self) -> Result<()> {
        if self.round_interval_secs == Some(0) {
            bail!("strategies.btc_arb.round_interval_secs must be a positive integer or null");
        }
        Ok(())
    }
}

impl Default for BtcArbConfig {
    fn default() -> Self {
        Self {
            symbols: vec!["BTCUSDT".into()],
            round_interval_secs: None,
            min_edge: 0.03,
            order_usd: 5.0,
            min_secs_left: 30,
            vol_floor: 0.00005,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CrossArbConfig {
    /// Locked profit per pair after fees required to report an opportunity.
    pub min_edge: f64,
    pub order_usd: f64,
    /// Assumed Polymarket taker fee, as `bps x min(p, 1 - p)` per share.
    pub polymarket_fee_bps: u32,
    /// Linked markets checked per tick.
    pub markets_per_tick: usize,
}

impl Default for CrossArbConfig {
    fn default() -> Self {
        Self {
            min_edge: 0.02,
            order_usd: 5.0,
            polymarket_fee_bps: 200,
            markets_per_tick: 20,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DirectionalArbConfig {
    /// Most the Yes and No bids may add up to. `0.99` requires a 1c floor.
    pub max_basket_cost: f64,
    /// Book-implied edge on a side, in cents, before tilting toward it.
    pub tilt_threshold_cents: f64,
    /// Largest size ratio of the tilted side to the hedge side.
    pub max_tilt_ratio: f64,
    /// Size of the smaller side.
    pub order_usd: f64,
    pub max_markets: usize,
    /// Stop quoting this close to a market's end.
    pub min_secs_left: i64,
}

impl Default for DirectionalArbConfig {
    fn default() -> Self {
        Self {
            max_basket_cost: 0.99,
            tilt_threshold_cents: 1.5,
            max_tilt_ratio: 3.0,
            order_usd: 2.0,
            max_markets: 5,
            min_secs_left: 3600,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SpreadFarmingConfig {
    /// Narrowest spread worth quoting into.
    pub min_spread: f64,
    pub order_usd: f64,
    pub max_markets: usize,
    /// A position still unsold after this long is sold at the bid.
    pub max_hold_secs: i64,
    /// Stop quoting this close to a market's end.
    pub min_secs_left: i64,
}

impl Default for SpreadFarmingConfig {
    fn default() -> Self {
        Self {
            min_spread: 0.04,
            order_usd: 5.0,
            max_markets: 5,
            max_hold_secs: 3600,
            min_secs_left: 3600,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SportsConfig {
    /// Player props outnumber fixtures by far, so they're hidden unless asked for.
    pub include_props: bool,
    /// Largest number of markets a listing prints.
    pub list_limit: usize,
}

impl Default for SportsConfig {
    fn default() -> Self {
        Self {
            include_props: false,
            list_limit: 25,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ResolutionSniperConfig {
    pub min_price: f64,
    pub max_price: f64,
    /// Payout minus price and fee required to buy.
    pub min_net_edge: f64,
    pub order_usd: f64,
    /// Only markets due to end within this long.
    pub max_secs_to_end: i64,
}

impl Default for ResolutionSniperConfig {
    fn default() -> Self {
        Self {
            min_price: 0.95,
            max_price: 0.99,
            min_net_edge: 0.005,
            order_usd: 5.0,
            max_secs_to_end: 86_400,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OrderbookImbalanceConfig {
    /// Levels from the touch counted on each side.
    pub levels: usize,
    /// Absolute imbalance in [0, 1] required to enter.
    pub threshold: f64,
    /// Trade against the heavy side rather than with it.
    pub fade: bool,
    pub max_spread: f64,
    pub order_usd: f64,
    pub take_profit: f64,
    pub stop_loss: f64,
    pub max_hold_secs: i64,
    pub max_markets: usize,
    /// Positions open at once. On a live book the signal fires across many markets
    /// at the same moment, and each entry pays the spread.
    pub max_positions: usize,
    /// Only trade prices in this band. A 3c spread is 60% of a 5c price.
    pub min_price: f64,
    pub max_price: f64,
}

impl Default for OrderbookImbalanceConfig {
    fn default() -> Self {
        Self {
            levels: 3,
            threshold: 0.6,
            fade: false,
            max_spread: 0.03,
            order_usd: 5.0,
            take_profit: 0.02,
            stop_loss: 0.02,
            max_hold_secs: 600,
            max_markets: 50,
            max_positions: 5,
            min_price: 0.15,
            max_price: 0.85,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MarketMakingConfig {
    /// Distance from the mid to each quote.
    pub half_spread: f64,
    pub order_usd: f64,
    /// Price shift per USD of net inventory.
    pub skew_per_usd: f64,
    /// Quotes further than this from where they should be are replaced.
    pub requote_distance: f64,
    pub max_markets: usize,
    /// Stop quoting this close to a market's end.
    pub min_secs_left: i64,
}

impl Default for MarketMakingConfig {
    fn default() -> Self {
        Self {
            half_spread: 0.02,
            order_usd: 5.0,
            skew_per_usd: 0.001,
            requote_distance: 0.01,
            max_markets: 5,
            min_secs_left: 3600,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WhaleSignalConfig {
    /// With no wallets in `predict_fun.wallets_to_track`, follow any taker this large.
    pub min_trade_usd: f64,
    pub order_usd: f64,
    /// Each wallet's own bucket: the most it can have opened through this bot.
    pub per_wallet_max_usd: f64,
    /// Never follow into a price above this.
    pub max_price: f64,
}

impl Default for WhaleSignalConfig {
    fn default() -> Self {
        Self {
            min_trade_usd: 500.0,
            order_usd: 5.0,
            per_wallet_max_usd: 25.0,
            max_price: 0.9,
        }
    }
}

/// Take-profit / stop-loss config (percent of entry price).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TpSlConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_tp_pct")]
    pub default_take_profit_pct: f64,
    #[serde(default = "default_sl_pct")]
    pub default_stop_loss_pct: f64,
    #[serde(default)]
    pub per_category_tp_pct: std::collections::HashMap<String, f64>,
    #[serde(default)]
    pub per_category_sl_pct: std::collections::HashMap<String, f64>,
    #[serde(default = "default_monitor_secs")]
    pub poll_interval_secs: u64,
}

impl Default for TpSlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_take_profit_pct: 50.0,
            default_stop_loss_pct: 30.0,
            per_category_tp_pct: Default::default(),
            per_category_sl_pct: Default::default(),
            poll_interval_secs: 15,
        }
    }
}

fn default_tp_pct() -> f64 {
    50.0
}
fn default_sl_pct() -> f64 {
    30.0
}
fn default_monitor_secs() -> u64 {
    15
}

#[derive(Debug, Clone, Default)]
pub struct Credentials {
    pub private_key: String,
    pub funder_address: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub api_passphrase: Option<String>,
    /// Polymarket wallet type when `funder_address` isn't the signer: proxy, safe
    /// or deposit_wallet.
    pub signature_type: Option<String>,
    pub predict_api_key: Option<String>,
    pub predict_account: Option<String>,
}

/// The controlled paper cash model. Opt-in only: an ordinary run has none of
/// this, and a controlled run must state it explicitly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlledPaperConfig {
    /// Virtual cash the run starts with, in USDT.
    pub initial_cash: f64,
    /// The only fee model this mode supports: fees are taken from the shares
    /// received, never from the cash twice.
    pub fee_model: String,
}

/// The mode a controlled run runs in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ControlledMode {
    /// Paper only: no wallet, no signer, no authenticated request.
    UnsignedPaper,
    /// A future live run. This build never assembles it from the paper path.
    SignedFutureLive,
}

impl ControlledMode {
    pub fn as_str(self) -> &'static str {
        match self {
            ControlledMode::UnsignedPaper => "unsigned-paper",
            ControlledMode::SignedFutureLive => "signed-future-live",
        }
    }
}

/// The controlled run's identity and deadline, fixed by the launcher.
#[derive(Debug, Clone)]
pub struct ControlledRun {
    pub mode: ControlledMode,
    pub run_id: String,
    pub started_at: DateTime<Utc>,
    pub deadline_at: DateTime<Utc>,
    /// The local monotonic base. A clock that goes backwards never extends the
    /// run: this elapsed time only ever grows.
    started_instant: Instant,
}

/// How long one controlled run may take, in seconds.
pub const CONTROLLED_MAX_SECS: i64 = 21_600;

/// Whether a run id is a short public identifier.
///
/// It is never a path: only lowercase letters, digits and dashes, and it can't
/// start with a dash.
fn valid_run_id(run_id: &str) -> bool {
    !run_id.is_empty()
        && run_id.len() <= 64
        && run_id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
        && !run_id.starts_with('-')
}

/// Parse one UTC timestamp.
fn parse_utc(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| anyhow::anyhow!("CONTROLLED_MODE: the timestamp must be RFC 3339 UTC"))
}

impl ControlledRun {
    /// Build a controlled run from the launcher's public arguments.
    ///
    /// The four arguments belong together: a half-specified run is refused
    /// rather than half assembled. The deadline must be exactly the start plus
    /// the fixed budget, so it can't be lengthened from the outside.
    pub fn from_arguments(
        mode: Option<&str>,
        run_id: Option<&str>,
        started_at: Option<&str>,
        deadline_at: Option<&str>,
    ) -> Result<Option<Self>> {
        let given = [
            mode.is_some(),
            run_id.is_some(),
            started_at.is_some(),
            deadline_at.is_some(),
        ];
        if !given.iter().any(|present| *present) {
            return Ok(None);
        }
        if !given.iter().all(|present| *present) {
            bail!("CONTROLLED_MODE: the controlled arguments belong together");
        }
        let mode = match mode.expect("the mode is present") {
            "unsigned-paper" => ControlledMode::UnsignedPaper,
            "signed-future-live" => ControlledMode::SignedFutureLive,
            _ => bail!("CONTROLLED_MODE"),
        };
        let run_id = run_id.expect("the run id is present");
        if !valid_run_id(run_id) {
            bail!("CONTROLLED_MODE: the run id must be a short public identifier");
        }
        let started_at = parse_utc(started_at.expect("the start is present"))?;
        let deadline_at = parse_utc(deadline_at.expect("the deadline is present"))?;
        if deadline_at != started_at + chrono::Duration::seconds(CONTROLLED_MAX_SECS) {
            bail!("CONTROLLED_MODE: the deadline must be the start plus the fixed budget");
        }
        Ok(Some(Self {
            mode,
            run_id: run_id.to_string(),
            started_at,
            deadline_at,
            started_instant: Instant::now(),
        }))
    }

    /// Fix the run's identity and deadline. The deadline is always the start
    /// plus the fixed budget: it can't be lengthened.
    pub fn new(mode: ControlledMode, run_id: String, started_at: DateTime<Utc>) -> Result<Self> {
        let deadline_at = started_at + chrono::Duration::seconds(CONTROLLED_MAX_SECS);
        Ok(Self {
            mode,
            run_id,
            started_at,
            deadline_at,
            started_instant: Instant::now(),
        })
    }

    /// Whether the run may still add risk.
    pub fn expired(&self) -> bool {
        self.elapsed() >= CONTROLLED_MAX_SECS as f64
    }

    /// Seconds since the run started, on the local monotonic clock.
    pub fn elapsed(&self) -> f64 {
        self.started_instant.elapsed().as_secs_f64()
    }

    /// Whether the absolute UTC deadline has passed.
    pub fn past_deadline(&self) -> bool {
        Utc::now() >= self.deadline_at
    }

    /// Both clocks must agree: a wall-clock jump can't extend the run, and a
    /// paused process can't skip it either.
    pub fn stopped(&self) -> bool {
        self.expired() || self.past_deadline()
    }
}

#[derive(Debug, Deserialize)]
struct CredentialsFile {
    bot: CredentialsBot,
}

#[derive(Debug, Deserialize)]
struct CredentialsBot {
    private_key: String,
    #[serde(default)]
    funder_address: String,
    api_key: Option<String>,
    api_secret: Option<String>,
    api_passphrase: Option<String>,
    signature_type: Option<String>,
    predict_api_key: Option<String>,
    predict_account: Option<String>,
}

/// The public configuration, read strictly.
///
/// A controlled run refuses a duplicate key, a non-finite number, a boolean
/// where an amount belongs and any field that could carry a secret. The checks
/// run on the raw JSON, before the ordinary configuration is deserialized.
#[derive(Debug)]
struct StrictJson(serde_json::Value);

impl<'de> Deserialize<'de> for StrictJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictVisitor).map(StrictJson)
    }
}

/// Builds a JSON value while refusing a duplicate object key.
struct StrictVisitor;

impl<'de> serde::de::Visitor<'de> for StrictVisitor {
    type Value = serde_json::Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut map = serde_json::Map::new();
        while let Some(key) = access.next_key::<String>()? {
            if map.contains_key(&key)
                || matches!(
                    key.as_str(),
                    "credentials"
                        | "private_key"
                        | "jwt"
                        | "seed"
                        | "mnemonic"
                        | "api_key"
                        | "api_secret"
                        | "api_passphrase"
                        | "predict_api_key"
                )
            {
                return Err(serde::de::Error::custom("PUBLIC_CONFIG"));
            }
            let value = access.next_value::<StrictJson>()?.0;
            map.insert(key, value);
        }
        Ok(serde_json::Value::Object(map))
    }

    fn visit_seq<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut items = Vec::new();
        while let Some(item) = access.next_element::<StrictJson>()? {
            items.push(item.0);
        }
        Ok(serde_json::Value::Array(items))
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(serde_json::Value::from(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(serde_json::Value::from(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if !value.is_finite() {
            return Err(serde::de::Error::custom("non-finite number"));
        }
        Ok(serde_json::Value::from(value))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(serde_json::Value::String(value.to_string()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(serde_json::Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::Deserialize::deserialize(deserializer)
    }
}

impl StrictJson {
    /// The fixed values a controlled unsigned-paper run must state.
    const REQUIRED_SYMBOL: &'static str = "BTCUSDT";
    const REQUIRED_ROUND_SECS: u64 = 300;
    const REQUIRED_INITIAL_CASH: f64 = 100.0;
    const REQUIRED_MIN_EDGE: f64 = 0.03;
    const REQUIRED_MIN_SECS_LEFT: i64 = 30;
    const REQUIRED_VOL_FLOOR: f64 = 0.00005;
    const REQUIRED_FEE_MODEL: &'static str = "paper_net_shares";

    fn check_controlled(&self, mode: ControlledMode) -> Result<()> {
        let root = self.0.as_object().ok_or_else(|| {
            anyhow::anyhow!("PUBLIC_CONFIG: the public configuration must be a JSON object")
        })?;
        for (name, _) in root {
            if name == "credentials" {
                bail!("PUBLIC_CONFIG: the public configuration must not carry credentials");
            }
        }
        let venue = root.get("venue").and_then(serde_json::Value::as_str);
        if venue != Some("predict_fun") {
            bail!("PUBLIC_CONFIG: venue must be predict_fun");
        }
        let network = root
            .get("predict_fun")
            .and_then(|v| v.get("network"))
            .and_then(serde_json::Value::as_str);
        if network != Some("mainnet") {
            bail!("PUBLIC_CONFIG: predict_fun.network must be mainnet");
        }
        let bot = root
            .get("bot")
            .ok_or_else(|| anyhow::anyhow!("PUBLIC_CONFIG: bot settings are required"))?;
        let unsigned = mode == ControlledMode::UnsignedPaper;
        if bot.get("enable_trading") != Some(&serde_json::Value::Bool(!unsigned))
            || bot.get("mock_trading") != Some(&serde_json::Value::Bool(unsigned))
        {
            bail!("PUBLIC_CONFIG: TRADING_SWITCH");
        }
        let controlled = root.get("controlled_paper");
        if unsigned {
            if controlled
                .and_then(|value| value.get("initial_cash"))
                .and_then(serde_json::Value::as_f64)
                != Some(Self::REQUIRED_INITIAL_CASH)
                || controlled
                    .and_then(|value| value.get("fee_model"))
                    .and_then(serde_json::Value::as_str)
                    != Some(Self::REQUIRED_FEE_MODEL)
            {
                bail!("PUBLIC_CONFIG: CONTROLLED_PAPER");
            }
        } else if controlled.is_some_and(|value| !value.is_null()) {
            bail!("PUBLIC_CONFIG: CONTROLLED_PAPER");
        }
        let strategies = root
            .get("strategies")
            .ok_or_else(|| anyhow::anyhow!("PUBLIC_CONFIG: strategy settings are required"))?;
        for (name, expected) in [
            ("poll_secs", 5),
            ("rescan_secs", 300),
            ("quote_ttl_secs", 300),
        ] {
            if strategies.get(name).and_then(serde_json::Value::as_u64) != Some(expected) {
                bail!("PUBLIC_CONFIG: PARAMETERS");
            }
        }
        let limits = strategies
            .get("limits")
            .ok_or_else(|| anyhow::anyhow!("PUBLIC_CONFIG: strategy limits are required"))?;
        for (name, expected) in [
            ("max_order_usd", 2.0),
            ("max_market_usd", 2.0),
            ("max_total_usd", 10.0),
            ("max_loss_usd", 20.0),
        ] {
            let value = limits.get(name).and_then(serde_json::Value::as_f64);
            if value != Some(expected) {
                bail!("PUBLIC_CONFIG: limits.{name} must be {expected}");
            }
        }
        let btc = strategies
            .get("btc_arb")
            .ok_or_else(|| anyhow::anyhow!("PUBLIC_CONFIG: btc_arb settings are required"))?;
        let symbols = btc
            .get("symbols")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("PUBLIC_CONFIG: btc_arb.symbols are required"))?;
        if symbols.len() != 1 || symbols[0].as_str() != Some(Self::REQUIRED_SYMBOL) {
            bail!("PUBLIC_CONFIG: btc_arb.symbols must be exactly [BTCUSDT]");
        }
        if btc
            .get("round_interval_secs")
            .and_then(serde_json::Value::as_u64)
            != Some(Self::REQUIRED_ROUND_SECS)
        {
            bail!("PUBLIC_CONFIG: btc_arb.round_interval_secs must be 300");
        }
        for (name, expected) in [
            ("min_edge", Self::REQUIRED_MIN_EDGE),
            ("vol_floor", Self::REQUIRED_VOL_FLOOR),
        ] {
            let value = btc.get(name).and_then(serde_json::Value::as_f64);
            if value != Some(expected) {
                bail!("PUBLIC_CONFIG: btc_arb.{name} must be {expected}");
            }
        }
        if btc.get("min_secs_left").and_then(serde_json::Value::as_i64)
            != Some(Self::REQUIRED_MIN_SECS_LEFT)
        {
            bail!("PUBLIC_CONFIG: btc_arb.min_secs_left must be 30");
        }
        let order = btc
            .get("order_usd")
            .and_then(serde_json::Value::as_f64)
            .ok_or_else(|| anyhow::anyhow!("PUBLIC_CONFIG: btc_arb.order_usd is required"))?;
        if order != 2.0 {
            bail!("PUBLIC_CONFIG: btc_arb.order_usd must be 2");
        }
        Ok(())
    }
}

impl AppConfig {
    /// Load a controlled unsigned-paper configuration.
    ///
    /// The public file is read once, checked for duplicate keys and for the
    /// fixed values this mode requires, and only then deserialized. No
    /// credentials file is opened and no environment variable is read: an
    /// unsigned paper run has no wallet.
    pub fn load_controlled_unsigned_paper(config_path: &Path) -> Result<Self> {
        Self::load_controlled(config_path, ControlledMode::UnsignedPaper)
    }

    /// Load only public JSON and the selected mode's allowlisted environment.
    pub fn load_controlled(config_path: &Path, mode: ControlledMode) -> Result<Self> {
        let raw =
            std::fs::read_to_string(config_path).map_err(|_| anyhow::anyhow!("PUBLIC_CONFIG"))?;
        let value: StrictJson =
            serde_json::from_str(&raw).map_err(|_| anyhow::anyhow!("PUBLIC_CONFIG"))?;
        value.check_controlled(mode)?;
        let mut cfg: AppConfig =
            serde_json::from_value(value.0).map_err(|_| anyhow::anyhow!("PUBLIC_CONFIG"))?;
        cfg.strategies.btc_arb.ensure_round_interval()?;
        cfg.credentials = Credentials::default();
        cfg.credentials.predict_api_key = std::env::var("PM_PREDICT_API_KEY").ok();
        if mode == ControlledMode::SignedFutureLive {
            cfg.credentials.private_key = std::env::var("PM_PRIVATE_KEY").unwrap_or_default();
            cfg.credentials.predict_account = std::env::var("PM_PREDICT_ACCOUNT").ok();
            if cfg
                .credentials
                .predict_account
                .as_deref()
                .map(str::to_ascii_lowercase)
                .as_deref()
                != Some("0xfc9eb66e6f22f96608058c0a711ed048eed8c2e2")
            {
                bail!("CONTROLLED_MODE: ACCOUNT");
            }
        }
        Ok(cfg)
    }

    pub fn load(config_path: &Path, credentials_path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(config_path)
            .with_context(|| format!("reading public config from {}", config_path.display()))?;
        let mut cfg: AppConfig = serde_json::from_str(&raw).context("parsing config.json")?;
        cfg.strategies.btc_arb.ensure_round_interval()?;

        // Credentials file is optional — without it we can still run in mock mode.
        if credentials_path.exists() {
            let raw = std::fs::read_to_string(credentials_path).with_context(|| {
                format!("reading credentials from {}", credentials_path.display())
            })?;
            let parsed: CredentialsFile =
                serde_yaml::from_str(&raw).context("parsing config.yaml")?;
            cfg.credentials = Credentials {
                private_key: parsed.bot.private_key,
                funder_address: parsed.bot.funder_address,
                api_key: parsed.bot.api_key,
                api_secret: parsed.bot.api_secret,
                api_passphrase: parsed.bot.api_passphrase,
                signature_type: parsed.bot.signature_type,
                predict_api_key: parsed.bot.predict_api_key,
                predict_account: parsed.bot.predict_account,
            };
        }

        // Environment-variable overrides (handy for CI / Docker).
        if let Ok(key) = std::env::var("PM_PRIVATE_KEY") {
            cfg.credentials.private_key = key;
        }
        if let Ok(addr) = std::env::var("PM_FUNDER_ADDRESS") {
            cfg.credentials.funder_address = addr;
        }
        if let Ok(kind) = std::env::var("PM_SIGNATURE_TYPE") {
            cfg.credentials.signature_type = Some(kind);
        }
        if let Ok(key) = std::env::var("PM_API_KEY") {
            cfg.credentials.api_key = Some(key);
        }
        if let Ok(secret) = std::env::var("PM_API_SECRET") {
            cfg.credentials.api_secret = Some(secret);
        }
        if let Ok(passphrase) = std::env::var("PM_API_PASSPHRASE") {
            cfg.credentials.api_passphrase = Some(passphrase);
        }
        if let Ok(key) = std::env::var("PM_PREDICT_API_KEY") {
            cfg.credentials.predict_api_key = Some(key);
        }
        if let Ok(account) = std::env::var("PM_PREDICT_ACCOUNT") {
            cfg.credentials.predict_account = Some(account);
        }

        Ok(cfg)
    }

    pub fn live_trading_allowed(&self) -> bool {
        self.bot.enable_trading && !self.bot.mock_trading
    }
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn btc_round_interval_defaults_to_all_and_accepts_positive_durations() {
        let omitted: BtcArbConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(omitted.round_interval_secs, None);
        assert_eq!(BtcArbConfig::default().round_interval_secs, None);
        for interval in [
            None,
            Some(1),
            Some(300),
            Some(900),
            Some(3600),
            Some(u32::MAX),
        ] {
            let parsed: BtcArbConfig = serde_json::from_value(serde_json::json!({
                "round_interval_secs": interval
            }))
            .unwrap();
            assert_eq!(parsed.round_interval_secs, interval);
            parsed.ensure_round_interval().unwrap();
        }
    }

    #[test]
    fn btc_round_interval_rejects_invalid_numbers_and_zero() {
        for invalid in [
            serde_json::json!(-1),
            serde_json::json!(1.5),
            serde_json::json!(4294967296_u64),
            serde_json::json!("300"),
        ] {
            assert!(serde_json::from_value::<BtcArbConfig>(serde_json::json!({
                "round_interval_secs": invalid
            }))
            .is_err());
        }
        let zero: BtcArbConfig = serde_json::from_str(r#"{"round_interval_secs":0}"#).unwrap();
        assert!(zero
            .ensure_round_interval()
            .unwrap_err()
            .to_string()
            .contains("round_interval_secs"));
    }

    #[test]
    fn app_load_rejects_zero_before_reading_credentials() {
        let mut public: serde_json::Value =
            serde_json::from_str(include_str!("../config.json")).unwrap();
        public["strategies"]["btc_arb"]["round_interval_secs"] = serde_json::json!(0);
        let path = std::env::temp_dir().join(format!(
            "btc-round-interval-zero-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, public.to_string()).unwrap();
        // This is JSON, not credentials YAML: reaching credential parsing would
        // produce a different error. No real credential file is opened.
        let error = AppConfig::load(&path, &path).unwrap_err();
        std::fs::remove_file(path).unwrap();
        assert!(
            error.to_string().contains("round_interval_secs"),
            "{error:#}"
        );
    }

    /// A controlled public configuration, with the fixed values this mode needs.
    fn controlled_public() -> serde_json::Value {
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../config.json")).unwrap();
        value["venue"] = serde_json::json!("predict_fun");
        value["bot"]["enable_trading"] = serde_json::json!(false);
        value["bot"]["mock_trading"] = serde_json::json!(true);
        value["predict_fun"]["network"] = serde_json::json!("mainnet");
        value["strategies"]["limits"] = serde_json::json!({
            "max_order_usd": 2.0,
            "max_market_usd": 2.0,
            "max_total_usd": 10.0,
            "max_loss_usd": 20.0
        });
        value["strategies"]["btc_arb"] = serde_json::json!({
            "symbols": ["BTCUSDT"],
            "round_interval_secs": 300,
            "min_edge": 0.03,
            "order_usd": 2.0,
            "min_secs_left": 30,
            "vol_floor": 0.00005
        });
        value["controlled_paper"] = serde_json::json!({
            "initial_cash": 100.0,
            "fee_model": "paper_net_shares"
        });
        value
    }

    fn write_public(value: &serde_json::Value, suffix: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "controlled-paper-{suffix}-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, value.to_string()).unwrap();
        path
    }

    #[test]
    fn controlled_paper_loads_with_the_fixed_values() {
        let path = write_public(&controlled_public(), "ok");
        let cfg = AppConfig::load_controlled_unsigned_paper(&path).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(cfg.venue, VenueId::PredictFun);
        assert!(!cfg.live_trading_allowed());
        let paper = cfg.controlled_paper.expect("the opt-in is present");
        assert_eq!(paper.initial_cash, 100.0);
        assert_eq!(paper.fee_model, "paper_net_shares");
        assert_eq!(cfg.strategies.limits.max_total_usd, 10.0);
        assert_eq!(cfg.strategies.limits.max_loss_usd, 20.0);
        assert_eq!(cfg.strategies.btc_arb.order_usd, 2.0);
        // No credential was read: the wallet stays empty.
        assert!(cfg.credentials.private_key.is_empty());
    }

    #[test]
    fn controlled_paper_refuses_a_changed_fixed_value() {
        let mut cases: Vec<(&str, Box<dyn Fn(&mut serde_json::Value)>)> = vec![
            (
                "initial_cash",
                Box::new(|v: &mut serde_json::Value| {
                    v["controlled_paper"]["initial_cash"] = serde_json::json!(500.0);
                }),
            ),
            (
                "fee_model",
                Box::new(|v: &mut serde_json::Value| {
                    v["controlled_paper"]["fee_model"] = serde_json::json!("gross");
                }),
            ),
            (
                "max_total_usd",
                Box::new(|v: &mut serde_json::Value| {
                    v["strategies"]["limits"]["max_total_usd"] = serde_json::json!(100.0);
                }),
            ),
            (
                "max_order_usd",
                Box::new(|v: &mut serde_json::Value| {
                    v["strategies"]["limits"]["max_order_usd"] = serde_json::json!(5.0);
                }),
            ),
            (
                "symbols",
                Box::new(|v: &mut serde_json::Value| {
                    v["strategies"]["btc_arb"]["symbols"] =
                        serde_json::json!(["BTCUSDT", "ETHUSDT"]);
                }),
            ),
            (
                "round_interval_secs",
                Box::new(|v: &mut serde_json::Value| {
                    v["strategies"]["btc_arb"]["round_interval_secs"] = serde_json::json!(900);
                }),
            ),
            (
                "min_edge",
                Box::new(|v: &mut serde_json::Value| {
                    v["strategies"]["btc_arb"]["min_edge"] = serde_json::json!(0.01);
                }),
            ),
            (
                "enable_trading",
                Box::new(|v: &mut serde_json::Value| {
                    v["bot"]["enable_trading"] = serde_json::json!(true);
                }),
            ),
            (
                "mock_trading",
                Box::new(|v: &mut serde_json::Value| {
                    v["bot"]["mock_trading"] = serde_json::json!(false);
                }),
            ),
            (
                "network",
                Box::new(|v: &mut serde_json::Value| {
                    v["predict_fun"]["network"] = serde_json::json!("testnet");
                }),
            ),
        ];
        for (name, mutate) in cases {
            let mut value = controlled_public();
            mutate(&mut value);
            let path = write_public(&value, name);
            let error = AppConfig::load_controlled_unsigned_paper(&path).unwrap_err();
            std::fs::remove_file(path).unwrap();
            assert!(
                error.to_string().contains("PUBLIC_CONFIG"),
                "{name}: {error:#}"
            );
        }
    }

    #[test]
    fn controlled_paper_refuses_missing_duplicate_and_malformed_values() {
        // A missing controlled block.
        let mut missing = controlled_public();
        missing.as_object_mut().unwrap().remove("controlled_paper");
        let path = write_public(&missing, "missing");
        assert!(AppConfig::load_controlled_unsigned_paper(&path).is_err());
        std::fs::remove_file(path).unwrap();

        // A boolean where an amount belongs.
        let mut boolean = controlled_public();
        boolean["controlled_paper"]["initial_cash"] = serde_json::json!(true);
        let path = write_public(&boolean, "boolean");
        assert!(AppConfig::load_controlled_unsigned_paper(&path).is_err());
        std::fs::remove_file(path).unwrap();

        // A duplicate key.
        let path = std::env::temp_dir().join(format!(
            "controlled-paper-duplicate-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            "{\"venue\":\"predict_fun\",\"venue\":\"predict_fun\"}",
        )
        .unwrap();
        assert!(AppConfig::load_controlled_unsigned_paper(&path).is_err());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_controlled_run_ends_at_its_fixed_deadline() {
        let run =
            ControlledRun::new(ControlledMode::UnsignedPaper, "run-1".into(), Utc::now()).unwrap();
        assert_eq!(run.mode.as_str(), "unsigned-paper");
        assert_eq!(
            (run.deadline_at - run.started_at).num_seconds(),
            CONTROLLED_MAX_SECS
        );
        assert!(!run.stopped());
    }
}

#[cfg(test)]
mod controlled_example_tests {
    use super::*;

    /// The shipped public example must load under the controlled rules.
    #[test]
    fn the_public_example_loads_under_the_controlled_rules() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tools/predictfun/btc5m-controlled-paper.json");
        let cfg = AppConfig::load_controlled_unsigned_paper(&path).unwrap();
        assert_eq!(cfg.venue, VenueId::PredictFun);
        assert!(!cfg.live_trading_allowed());
        assert_eq!(cfg.strategies.limits.max_order_usd, 2.0);
        assert_eq!(cfg.strategies.limits.max_market_usd, 2.0);
        assert_eq!(cfg.strategies.limits.max_total_usd, 10.0);
        assert_eq!(cfg.strategies.limits.max_loss_usd, 20.0);
        assert_eq!(cfg.strategies.btc_arb.order_usd, 2.0);
        assert_eq!(cfg.strategies.btc_arb.round_interval_secs, Some(300));
        let paper = cfg.controlled_paper.expect("the opt-in is present");
        assert_eq!(paper.initial_cash, 100.0);
        assert_eq!(paper.fee_model, "paper_net_shares");
        assert!(cfg.credentials.private_key.is_empty());
    }
}
