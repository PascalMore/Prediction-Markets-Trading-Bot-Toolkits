//! Read-only post-entry valuation from existing tick inputs and broker facts.
//! Reports use an inherited AF_UNIX datagram, never the trading stdout stream.
//! This module cannot evaluate candidates, execute, or mutate trading state.

#[cfg(test)]
mod tests;

use super::DecisionCore;
use crate::bot::arbitrage::phase1::{
    self, Phase1ObservationState, Reason as Phase1Reason, ResetReason, SettlementProvider,
    SourceState,
};
use crate::bot::runtime::observer::BrokerObservationSnapshot;
use crate::bot::runtime::paper;
use crate::bot::runtime::token_book;
use crate::bot::runtime::walk;
use crate::models::{BookSnapshot, Side};
use crate::service::observation::{
    serialize_millis_option, CheckTime, Clock, ObservationClock, ObservedBook, ObservedSpot,
};
use crate::service::spot::SpotSource;
use crate::service::venue::TradableMarket;
use anyhow::Result;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixDatagram;
use std::sync::Arc;

/// The wire version of the observation channel.
pub const WIRE_VERSION: &str = "btc5m-post-entry-wire/1";
/// The schema version of one observation record.
pub const RECORD_SCHEMA_VERSION: &str = "btc5m-post-entry-observer/1";
/// The environment variable that carries the inherited datagram's descriptor.
pub const FD_ENV: &str = "BTC5M_POST_ENTRY_FD";
/// The largest frame the channel accepts, framing included.
pub const MAX_FRAME_BYTES: usize = 64 * 1024;
pub const PAPER72_END_BYTES: usize = 256 * 1024;
/// The largest number of levels one side may carry.
pub const MAX_LEVELS: usize = 1000;
/// The largest number of series one run may observe.
pub const MAX_SERIES: usize = 1000;
/// The same epsilon the original book walk uses.
const EPS: f64 = 1e-9;

/// The state of one observed series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SampleState {
    Active,
    RoundEnded,
    Settled,
    RunStopped,
}

impl SampleState {
    /// Both valuation domains terminate together, with the same lifecycle reason.
    fn terminal_reasons(self) -> Option<(ModelReason, ExitReason)> {
        match self {
            SampleState::Active => None,
            SampleState::RoundEnded => Some((ModelReason::RoundEnded, ExitReason::RoundEnded)),
            SampleState::Settled => Some((ModelReason::Settled, ExitReason::Settled)),
            SampleState::RunStopped => Some((ModelReason::RunStopped, ExitReason::RunStopped)),
        }
    }
}

/// Model domain status: only OK carries a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ModelReason {
    Ok,
    MissingInput,
    MarketInvalid,
    InputInvalid,
    ClockInvalid,
    RequestTooSlow,
    SpotStale,
    InputChanged,
    Warmup,
    RoundEnded,
    Settled,
    RunStopped,
}

/// Exit domain status: only OK carries a full value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExitReason {
    Ok,
    NoPosition,
    MarketInvalid,
    MissingBook,
    InputInvalid,
    ClockInvalid,
    RequestTooSlow,
    BookStale,
    NoBid,
    InsufficientDepth,
    ObservationLimit,
    RoundEnded,
    Settled,
    RunStopped,
}

/// Which fee evidence the exit carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FeeBasis {
    /// The confirmed controlled paper rule, not a real venue's fee.
    PaperCashFee,
    /// The real fee is unknown, and stays unknown.
    Unknown,
}

/// Which book the Down side was read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DownQuoteBasis {
    /// The mirrored Yes book: the same depth, seen from the other side.
    MirroredYesBook,
}

/// Why a record could not be encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodingFailure {
    /// The identity of the observation is not a decimal id.
    InvalidIdentity,
    /// The record holds a number that cannot travel.
    NonFinite,
}

/// Complete flat record: unavailable values are explicit nulls, never omitted or zero-filled.
#[derive(Debug, Clone, Serialize)]
pub struct PostEntryObservation {
    pub schema_version: &'static str,
    pub run_id: String,
    pub observation_seq: u64,
    pub market_id: String,
    pub token: String,
    pub sample_state: SampleState,
    #[serde(serialize_with = "serialize_millis_option")]
    pub observed_at: Option<DateTime<Utc>>,
    pub observed_elapsed_ms: Option<u64>,
    #[serde(serialize_with = "serialize_millis_option")]
    pub starts_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "serialize_millis_option")]
    pub ends_at: Option<DateTime<Utc>>,
    pub secs_left: Option<f64>,
    pub strike: Option<f64>,
    pub position_net_shares: Option<f64>,
    pub position_cost: Option<f64>,
    pub prediction_source: Option<SpotSource>,
    pub actual_pair: Option<String>,
    pub source_state: SourceState,
    pub source_epoch: Option<u64>,
    pub reset_reason: ResetReason,
    pub settlement_provider: SettlementProvider,
    pub spot: Option<f64>,
    pub spot_clock: Option<ObservationClock>,
    pub book_clock: Option<ObservationClock>,
    pub samples: u32,
    pub sigma_raw: Option<f64>,
    pub sigma: Option<f64>,
    pub p_up: Option<f64>,
    pub p_down: Option<f64>,
    pub model_reason: ModelReason,
    pub up_bid: Option<f64>,
    pub up_ask: Option<f64>,
    pub down_bid: Option<f64>,
    pub down_ask: Option<f64>,
    pub down_quote_basis: DownQuoteBasis,
    pub exit_bid_levels: Option<Vec<phase1::Level>>,
    pub exit_covered_shares: Option<f64>,
    pub exit_depth_complete: Option<bool>,
    pub exit_vwap: Option<f64>,
    pub exit_gross_income: Option<f64>,
    pub exit_reason: ExitReason,
    pub fee_basis: FeeBasis,
    pub fee_rate_bps: Option<u32>,
    pub exit_fee: Option<f64>,
    pub exit_net_income: Option<f64>,
    pub exit_pnl_vs_cost: Option<f64>,
    pub model_hold_value: Option<f64>,
    pub model_hold_surplus: Option<f64>,
}

/// Borrowed tick inputs, actual net inventory, and a copied model state.
pub struct ObservationInput<'a> {
    pub run_id: &'a str,
    pub seq: u64,
    pub market: &'a TradableMarket,
    pub token: &'a str,
    /// The net shares and the cost, when the position is still held.
    pub position: Option<(f64, f64)>,
    pub sample_state: SampleState,
    /// The clock reading this tick proved.
    pub now: Option<CheckTime>,
    pub quote: Option<&'a ObservedSpot>,
    pub book: Option<&'a ObservedBook>,
    pub state: &'a Phase1ObservationState,
    /// When the official identity of the round became locally available.
    pub identity_available_at: Option<DateTime<Utc>>,
    /// When the official strike became locally available.
    pub strike_available_at: Option<DateTime<Utc>>,
    /// Whether the controlled run's cash fee rule is confirmed.
    pub paper_fee_confirmed: bool,
}

/// The model half of one observation.
struct ModelOutcome {
    reason: ModelReason,
    p_up: Option<f64>,
    sigma_raw: Option<f64>,
    sigma: Option<f64>,
}

/// The exit half of one observation.
struct ExitOutcome {
    reason: ExitReason,
    levels: Option<Vec<phase1::Level>>,
    covered: Option<f64>,
    complete: Option<bool>,
    vwap: Option<f64>,
    gross: Option<f64>,
    fee: Option<f64>,
    fee_bps: Option<u32>,
    net: Option<f64>,
    pnl: Option<f64>,
}

impl ExitOutcome {
    /// An exit that has no value at all.
    fn empty(reason: ExitReason) -> Self {
        Self {
            reason,
            levels: None,
            covered: None,
            complete: None,
            vwap: None,
            gross: None,
            fee: None,
            fee_bps: None,
            net: None,
            pnl: None,
        }
    }
}

impl ModelOutcome {
    /// A model that has no value at all.
    fn empty(reason: ModelReason) -> Self {
        Self {
            reason,
            p_up: None,
            sigma_raw: None,
            sigma: None,
        }
    }
}

/// Pure construction from tick facts; no account, core, or clock mutation.
pub(crate) fn build_observation(
    core: &DecisionCore,
    input: &ObservationInput<'_>,
) -> Result<PostEntryObservation, EncodingFailure> {
    let market_id = input.market.id.to_string();
    let token = input.token.to_string();
    if !decimal_id(&market_id) || !decimal_id(&token) {
        return Err(EncodingFailure::InvalidIdentity);
    }
    let now = input.now;
    let quote = input
        .quote
        .filter(|quote| valid_source(quote.source, &quote.actual_pair));

    let (model, exit) = match input.sample_state.terminal_reasons() {
        Some((model, exit)) => (ModelOutcome::empty(model), ExitOutcome::empty(exit)),
        None => (model_outcome(core, input), exit_outcome(input)),
    };

    let ends_at = input.market.ends_at;
    let secs_left = ends_at
        .zip(now.map(|now| now.utc))
        .map(|(end, at)| (end - at).num_milliseconds() as f64 / 1000.0);
    let strike = input
        .market
        .start_price
        .filter(|price| price.is_finite() && *price > 0.0);

    // The model value and the exit value are separate statements about the
    // same position. Neither one may erase the other.
    let p_up = model.p_up;
    let p_down = p_up.map(|value| 1.0 - value);
    let position = input.position;
    let token_probability = p_up.map(|value| {
        if token == input.market.yes_token {
            value
        } else {
            1.0 - value
        }
    });
    let model_hold_value = token_probability
        .zip(position.map(|(shares, _)| shares))
        .map(|(probability, shares)| shares * probability);
    let model_hold_surplus = model_hold_value
        .zip(position.map(|(_, cost)| cost))
        .map(|(value, cost)| value - cost);

    let record = PostEntryObservation {
        schema_version: RECORD_SCHEMA_VERSION,
        run_id: input.run_id.to_string(),
        observation_seq: input.seq,
        market_id,
        token,
        sample_state: input.sample_state,
        observed_at: now.map(|now| now.utc),
        observed_elapsed_ms: now.map(|now| now.elapsed_ms),
        starts_at: input.market.starts_at,
        ends_at,
        secs_left,
        strike,
        position_net_shares: position.map(|(shares, _)| shares),
        position_cost: position.map(|(_, cost)| cost),
        prediction_source: quote.map(|quote| quote.source),
        actual_pair: quote.map(|quote| quote.actual_pair.clone()),
        source_state: match quote.map(|quote| quote.source) {
            Some(SpotSource::Binance) => SourceState::Primary,
            Some(SpotSource::Coinbase) => SourceState::Degraded,
            None => SourceState::Unavailable,
        },
        source_epoch: input.state.epoch,
        reset_reason: input.state.reset_reason,
        settlement_provider: settlement_provider(input.market),
        spot: input
            .quote
            .map(|quote| quote.price)
            .filter(|price| price.is_finite() && *price > 0.0),
        spot_clock: input.quote.map(|quote| quote.clock.clone()),
        book_clock: input.book.map(|book| book.clock.clone()),
        samples: input.state.samples,
        sigma_raw: model.sigma_raw,
        sigma: model.sigma,
        p_up,
        p_down,
        model_reason: model.reason,
        up_bid: input
            .book
            .and_then(|book| touch(book.book.bids.first().map(|level| level.price))),
        up_ask: input
            .book
            .and_then(|book| touch(book.book.asks.first().map(|level| level.price))),
        down_bid: input
            .book
            .and_then(|book| touch(book.book.mirrored().bids.first().map(|level| level.price))),
        down_ask: input
            .book
            .and_then(|book| touch(book.book.mirrored().asks.first().map(|level| level.price))),
        down_quote_basis: DownQuoteBasis::MirroredYesBook,
        exit_bid_levels: exit.levels,
        exit_covered_shares: exit.covered,
        exit_depth_complete: exit.complete,
        exit_vwap: exit.vwap,
        exit_gross_income: exit.gross,
        exit_reason: exit.reason,
        fee_basis: if exit.fee.is_some() {
            FeeBasis::PaperCashFee
        } else {
            FeeBasis::Unknown
        },
        fee_rate_bps: exit.fee_bps,
        exit_fee: exit.fee,
        exit_net_income: exit.net,
        exit_pnl_vs_cost: exit.pnl,
        model_hold_value,
        model_hold_surplus,
    };
    if !record_is_finite(&record) {
        return Err(EncodingFailure::NonFinite);
    }
    Ok(record)
}

/// Whether a decimal id is the shape the protocol names.
fn decimal_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 78 && value.bytes().all(|byte| byte.is_ascii_digit())
}

/// The official settlement provider of a round.
fn settlement_provider(market: &TradableMarket) -> SettlementProvider {
    match market
        .round_facts
        .as_ref()
        .and_then(|facts| facts.price_feed_provider.as_deref())
    {
        Some("CHAINLINK") => SettlementProvider::Chainlink,
        _ => SettlementProvider::Unknown,
    }
}

/// A touch price, when it is a price.
fn touch(price: Option<f64>) -> Option<f64> {
    price.filter(|price| price.is_finite() && *price > 0.0 && *price < 1.0)
}

/// Validate identity → inputs → clock → accepted receipt → warm-up; use original valuation.
fn model_outcome(core: &DecisionCore, input: &ObservationInput<'_>) -> ModelOutcome {
    let Some(now) = input.now else {
        return ModelOutcome::empty(ModelReason::MissingInput);
    };
    let market = input.market;
    if !market_identity_is_valid(market, input.token) {
        return ModelOutcome::empty(ModelReason::MarketInvalid);
    }
    // Proven Some/finite by the same immutable identity check above.
    let (strike, starts_at, ends_at) = (
        market.start_price.unwrap(),
        market.starts_at.unwrap(),
        market.ends_at.unwrap(),
    );
    if starts_at > now.utc {
        return ModelOutcome::empty(ModelReason::MarketInvalid);
    }
    if input.identity_available_at.is_none_or(|at| at > now.utc)
        || input.strike_available_at.is_none_or(|at| at > now.utc)
    {
        return ModelOutcome::empty(ModelReason::MarketInvalid);
    }
    let Some(quote) = input.quote else {
        return ModelOutcome::empty(ModelReason::MissingInput);
    };
    if !quote.price.is_finite()
        || quote.price <= 0.0
        || !valid_source(quote.source, &quote.actual_pair)
    {
        return ModelOutcome::empty(ModelReason::InputInvalid);
    }
    let clock = &quote.clock;
    if let Some(reason) = clock_error(
        clock,
        now,
        ModelReason::ClockInvalid,
        ModelReason::RequestTooSlow,
        ModelReason::SpotStale,
    ) {
        return ModelOutcome::empty(reason);
    }
    // The model may only be valued on the quote the core actually accepted.
    // A rejected or repeated quote cannot borrow the old epoch's warmth.
    let state = input.state;
    let Some(key) = state.key.as_ref() else {
        return ModelOutcome::empty(ModelReason::Warmup);
    };
    if key.source != quote.source
        || key.actual_pair != quote.actual_pair
        || state.last_receive != Some((clock.received_at, clock.received_elapsed_ms))
    {
        return ModelOutcome::empty(ModelReason::InputChanged);
    }
    let secs_left = (ends_at - now.utc).num_milliseconds() as f64 / 1000.0;
    if secs_left <= 0.0 {
        return ModelOutcome::empty(ModelReason::RoundEnded);
    }
    let symbol = market.feed_symbol.as_deref().unwrap_or("BTCUSDT");
    // The core's own valuation, with its own floor: the observer never
    // re-implements the model.
    match core.phase1_valuation(symbol, quote.price, strike, secs_left, input.state.floor) {
        Ok(value) if value.p_up.is_finite() => ModelOutcome {
            reason: ModelReason::Ok,
            p_up: Some(value.p_up),
            sigma_raw: Some(value.sigma_raw),
            sigma: Some(value.sigma),
        },
        Ok(_) => ModelOutcome::empty(ModelReason::InputInvalid),
        Err(reason) => ModelOutcome::empty(map_phase1_reason(reason)),
    }
}

/// The observer's reason for the core's refusal.
fn map_phase1_reason(reason: Phase1Reason) -> ModelReason {
    match reason {
        Phase1Reason::Warmup => ModelReason::Warmup,
        Phase1Reason::InputInvalid => ModelReason::InputInvalid,
        Phase1Reason::ClockInvalid => ModelReason::ClockInvalid,
        Phase1Reason::MissingInput => ModelReason::MissingInput,
        Phase1Reason::MarketInvalid => ModelReason::MarketInvalid,
        Phase1Reason::RequestTooSlow => ModelReason::RequestTooSlow,
        Phase1Reason::SpotStale => ModelReason::SpotStale,
        Phase1Reason::InputChanged => ModelReason::InputChanged,
        Phase1Reason::RoundTooLate => ModelReason::RoundEnded,
        Phase1Reason::RunStopped => ModelReason::RunStopped,
        _ => ModelReason::InputInvalid,
    }
}

/// Whether the source and the pair belong together.
fn valid_source(source: SpotSource, pair: &str) -> bool {
    matches!(
        (source, pair),
        (SpotSource::Binance, "BTCUSDT") | (SpotSource::Coinbase, "BTC-USD")
    )
}

/// Validate inventory → input levels → clock → depth; walk original held-token bids.
fn exit_outcome(input: &ObservationInput<'_>) -> ExitOutcome {
    let market = input.market;
    if !market_identity_is_valid(market, input.token) {
        return ExitOutcome::empty(ExitReason::MarketInvalid);
    }
    let Some((shares, cost)) = input.position else {
        return ExitOutcome::empty(ExitReason::NoPosition);
    };
    if !shares.is_finite() || shares <= 0.0 {
        return ExitOutcome::empty(ExitReason::NoPosition);
    }
    if !cost.is_finite() || cost < 0.0 {
        return ExitOutcome::empty(ExitReason::InputInvalid);
    }
    let Some(book) = input.book else {
        return ExitOutcome::empty(ExitReason::MissingBook);
    };
    let Some(now) = input.now else {
        return ExitOutcome::empty(ExitReason::MissingBook);
    };
    let yes_book = &book.book;
    if yes_book.bids.len() > MAX_LEVELS || yes_book.asks.len() > MAX_LEVELS {
        return ExitOutcome::empty(ExitReason::ObservationLimit);
    }
    // The book's own validity, level by level: a level that is not a level
    // cannot be walked, and an empty book is not the same as an invalid one.
    if !levels_are_valid(yes_book) {
        return ExitOutcome::empty(ExitReason::InputInvalid);
    }
    if yes_book
        .bids
        .windows(2)
        .any(|levels| levels[0].price < levels[1].price)
        || yes_book
            .asks
            .windows(2)
            .any(|levels| levels[0].price > levels[1].price)
    {
        return ExitOutcome::empty(ExitReason::InputInvalid);
    }
    if !yes_book.bids.is_empty()
        && (yes_book.asks.is_empty() || yes_book.bids[0].price > yes_book.asks[0].price)
    {
        return ExitOutcome::empty(ExitReason::InputInvalid);
    }
    if let Some(reason) = clock_error(
        &book.clock,
        now,
        ExitReason::ClockInvalid,
        ExitReason::RequestTooSlow,
        ExitReason::BookStale,
    ) {
        return ExitOutcome::empty(reason);
    }
    // The held token's own side of the book: the No side is the mirrored one.
    let held = token_book(market, input.token, yes_book);
    if held.bids.is_empty() {
        return ExitOutcome {
            reason: ExitReason::NoBid,
            levels: Some(Vec::new()),
            covered: Some(0.0),
            complete: Some(false),
            ..ExitOutcome::empty(ExitReason::NoBid)
        };
    }
    // The original walk's own epsilon: an inventory that is smaller than it
    // has no walk, and a walk that lands on zero is not an exit price.
    if shares <= EPS {
        return ExitOutcome::empty(ExitReason::InputInvalid);
    }
    let mut remaining = shares;
    let mut consumed = 0.0;
    let mut levels = Vec::new();
    for level in &held.bids {
        if remaining <= EPS {
            break;
        }
        levels.push(phase1::Level {
            price: level.price,
            size: level.size,
        });
        let take = remaining.min(level.size);
        consumed += take;
        remaining -= take;
    }
    let complete = remaining <= EPS;
    let covered = consumed.min(shares);
    if !complete {
        return ExitOutcome {
            reason: ExitReason::InsufficientDepth,
            levels: Some(levels),
            covered: Some(covered),
            complete: Some(false),
            ..ExitOutcome::empty(ExitReason::InsufficientDepth)
        };
    }
    let Some(vwap) = walk(&held.bids, Side::Sell, shares, 0.0) else {
        return ExitOutcome::empty(ExitReason::InputInvalid);
    };
    if !vwap.is_finite() || vwap <= 0.0 || vwap >= 1.0 {
        return ExitOutcome::empty(ExitReason::InputInvalid);
    }
    let gross = shares * vwap;
    if !gross.is_finite() || gross < 0.0 {
        return ExitOutcome::empty(ExitReason::InputInvalid);
    }
    // The confirmed paper rule takes the fee out of the cash at the walk's
    // average price. The real venue's fee is not this number.
    let mut outcome = ExitOutcome {
        reason: ExitReason::Ok,
        levels: Some(levels),
        covered: Some(covered),
        complete: Some(true),
        vwap: Some(vwap),
        gross: Some(gross),
        ..ExitOutcome::empty(ExitReason::Ok)
    };
    if input.paper_fee_confirmed && market.fee_rate_bps <= 10_000 {
        if let Some((_net_shares, fee)) = paper::net_shares(shares, vwap, market.fee_rate_bps) {
            let net = gross - fee;
            outcome.fee = Some(fee);
            outcome.fee_bps = Some(market.fee_rate_bps);
            outcome.net = Some(net);
            outcome.pnl = Some(net - cost);
        }
    }
    outcome
}

/// Whether every level of the book is a level.
fn clock_error<T>(
    clock: &ObservationClock,
    now: CheckTime,
    invalid: T,
    slow: T,
    stale: T,
) -> Option<T> {
    if !clock.is_consistent()
        || clock.received_at > now.utc
        || clock.received_elapsed_ms > now.elapsed_ms
    {
        Some(invalid)
    } else if clock.request_span_ms() > phase1::MAX_REQUEST_SPAN_MS {
        Some(slow)
    } else if clock.acquisition_age_ms(now) > phase1::MAX_ACQUISITION_AGE_MS
        || clock.source_at.is_some_and(|source| {
            now.utc.timestamp_millis() - source.timestamp_millis() > phase1::MAX_ACQUISITION_AGE_MS
        })
    {
        Some(stale)
    } else {
        None
    }
}

/// Validate the official fixed identity before either valuation domain.
fn market_identity_is_valid(market: &TradableMarket, token: &str) -> bool {
    market.open
        && !market.contested
        && market.feed_symbol.as_deref() == Some("BTCUSDT")
        && decimal_id(&market.yes_token)
        && decimal_id(&market.no_token)
        && market.yes_token != market.no_token
        && (token == market.yes_token || token == market.no_token)
        && market
            .start_price
            .is_some_and(|strike| strike.is_finite() && strike > 0.0)
        && market
            .starts_at
            .zip(market.ends_at)
            .is_some_and(|(start, end)| end - start == chrono::Duration::seconds(300))
}

/// Validate all actual levels, including single-sided books.
fn levels_are_valid(book: &BookSnapshot) -> bool {
    book.bids.iter().chain(book.asks.iter()).all(|level| {
        level.price.is_finite()
            && level.price > 0.0
            && level.price < 1.0
            && level.size.is_finite()
            && level.size > 0.0
    })
}

/// Whether every number of a record can travel.
fn record_is_finite(record: &PostEntryObservation) -> bool {
    let finite = |value: &Option<f64>| value.is_none_or(|number| number.is_finite());
    finite(&record.secs_left)
        && finite(&record.strike)
        && finite(&record.position_net_shares)
        && finite(&record.position_cost)
        && finite(&record.spot)
        && finite(&record.sigma_raw)
        && finite(&record.sigma)
        && finite(&record.p_up)
        && finite(&record.p_down)
        && finite(&record.up_bid)
        && finite(&record.up_ask)
        && finite(&record.down_bid)
        && finite(&record.down_ask)
        && finite(&record.exit_covered_shares)
        && finite(&record.exit_vwap)
        && finite(&record.exit_gross_income)
        && finite(&record.exit_fee)
        && finite(&record.exit_net_income)
        && finite(&record.exit_pnl_vs_cost)
        && finite(&record.model_hold_value)
        && finite(&record.model_hold_surplus)
        && record.exit_bid_levels.as_ref().is_none_or(|levels| {
            levels
                .iter()
                .all(|level| level.price.is_finite() && level.size.is_finite())
        })
}

/// Forward original clock calls unchanged; retain only a valid current-tick reading.
pub struct TickClock {
    inner: Arc<dyn Clock>,
    last: Mutex<Option<CheckTime>>,
    valid: std::sync::atomic::AtomicBool,
}

impl TickClock {
    /// Wrap a clock without changing what it answers.
    pub fn new(inner: Arc<dyn Clock>) -> Self {
        Self {
            inner,
            last: Mutex::new(None),
            valid: true.into(),
        }
    }

    /// The last reading that succeeded.
    pub fn last(&self) -> Option<CheckTime> {
        self.valid
            .load(std::sync::atomic::Ordering::Relaxed)
            .then(|| *self.last.lock())
            .flatten()
            .filter(|at| at.elapsed_ms <= i64::MAX as u64 && at.utc.timestamp_millis() >= 0)
    }
}

impl Clock for TickClock {
    fn now(&self) -> Result<CheckTime> {
        let reading = self.inner.now().inspect_err(|_| {
            self.valid
                .store(false, std::sync::atomic::Ordering::Relaxed);
        })?;
        let mut last = self.last.lock();
        if last.is_some_and(|last| reading.utc < last.utc || reading.elapsed_ms < last.elapsed_ms) {
            self.valid
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
        *last = Some(reading);
        Ok(reading)
    }
}

/// Why the observation channel stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DisabledReason {
    /// The inherited descriptor was not a usable datagram.
    TransportInvalid,
    /// The channel refused a frame.
    TransportFailed,
    /// The observation grew past what a frame may carry.
    ObservationLimit,
    /// A record could not be encoded.
    EncodingFailed,
}

/// What the run's samples amounted to.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ProducerCounters {
    pub attempted_samples: u64,
    pub encoded_samples: u64,
    pub sent_samples: u64,
    pub dropped_samples: u64,
    pub clock_unavailable_samples: u64,
    pub encode_failed_samples: u64,
    pub send_failed_samples: u64,
    pub last_observation_seq: u64,
}

/// One observed series' terminal state.
#[derive(Debug, Clone, Serialize)]
pub struct SeriesTerminal {
    pub market_id: String,
    pub token: String,
    pub terminal_state: Option<SampleState>,
    pub terminal_observation_seq: Option<u64>,
}

/// The producer's closing report.
#[derive(Debug, Clone, Serialize)]
pub struct ProducerReport {
    #[serde(flatten)]
    pub counters: ProducerCounters,
    pub disabled_reason: Option<DisabledReason>,
    pub series: Vec<SeriesTerminal>,
}

/// One observed series.
struct Series {
    market: TradableMarket,
    token: String,
    terminal: Option<SampleState>,
    terminal_seq: Option<u64>,
}

/// One non-blocking endpoint and bounded series metadata, with no trading authority.
pub struct PostEntryObserver {
    socket: Option<UnixDatagram>,
    run_id: String,
    seq: u64,
    paper_fee_confirmed: bool,
    series: BTreeMap<(u64, String), Series>,
    counters: ProducerCounters,
    disabled: Option<DisabledReason>,
}

impl PostEntryObserver {
    /// Attach to a validated inherited datagram; failure leaves trading unaffected.
    pub fn attach(run_id: &str, paper_fee_confirmed: bool) -> Option<Self> {
        let socket = inherited_socket().ok()?;
        let mut observer = Self {
            socket: Some(socket),
            run_id: run_id.to_string(),
            seq: 0,
            paper_fee_confirmed,
            series: BTreeMap::new(),
            counters: ProducerCounters::default(),
            disabled: None,
        };
        observer.send_ready();
        Some(observer)
    }

    /// Observe eligible holdings after trading, without acquiring or changing inputs.
    pub(crate) fn after_tick(
        &mut self,
        core: &DecisionCore,
        snapshot: &BrokerObservationSnapshot,
        eligibility: &[String],
        now: Option<CheckTime>,
        quotes: &HashMap<String, ObservedSpot>,
        books: &HashMap<u64, ObservedBook>,
        markets: &BTreeMap<u64, TradableMarket>,
        availability: &BTreeMap<u64, (Option<DateTime<Utc>>, Option<DateTime<Utc>>)>,
        accepted: &std::collections::HashSet<String>,
    ) {
        if self.disabled.is_some() {
            return;
        }
        let tokens: std::collections::BTreeSet<_> = eligibility
            .iter()
            .chain(self.series.values().map(|series| &series.token))
            .cloned()
            .collect();
        for token in tokens {
            let Some(market) = snapshot
                .market_of(&token)
                .and_then(|market_id| markets.get(&market_id).cloned())
                .or_else(|| self.market_of(&token))
            else {
                continue;
            };
            if self
                .series
                .get(&(market.id, token.clone()))
                .is_some_and(|series| series.terminal.is_some())
            {
                continue;
            }
            if !self.series.contains_key(&(market.id, token.clone()))
                && self.series.len() >= MAX_SERIES
            {
                self.disabled = Some(DisabledReason::ObservationLimit);
                break;
            }
            let position = snapshot.position_of(&token);
            let sample_state = terminal_state(snapshot, &market, position, now);
            // A position that is gone and a round that has not ended is not an
            // observation: there is nothing to observe.
            if position.is_none() && sample_state == SampleState::Active {
                continue;
            }
            let symbol = market.feed_symbol.as_deref().unwrap_or("BTCUSDT");
            let mut state = core.phase1_observation_state(symbol);
            if !accepted.contains(symbol) {
                // Invalidate only this copied receipt, never the model itself.
                state.last_receive = None;
            }
            let (identity_at, strike_at) = availability
                .get(&market.id)
                .copied()
                .unwrap_or((None, None));
            let quote = quotes.get(symbol);
            let book = books.get(&market.id);
            self.seq += 1;
            let seq = self.seq;
            let input = ObservationInput {
                run_id: &self.run_id,
                seq,
                market: &market,
                token: &token,
                position,
                sample_state,
                now,
                quote,
                book,
                state: &state,
                identity_available_at: identity_at,
                strike_available_at: strike_at,
                paper_fee_confirmed: self.paper_fee_confirmed,
            };
            self.counters.attempted_samples += 1;
            if now.is_none() {
                self.counters.clock_unavailable_samples += 1;
                self.counters.dropped_samples += 1;
                self.counters.last_observation_seq = seq;
                continue;
            }
            let mut record = match build_observation(core, &input) {
                Ok(record) => record,
                Err(_) => {
                    self.counters.encode_failed_samples += 1;
                    self.counters.dropped_samples += 1;
                    self.counters.last_observation_seq = seq;
                    continue;
                }
            };
            self.counters.encoded_samples += 1;
            let sent = self.send_sample(snapshot.after_event_seq, &mut record);
            if sent {
                self.counters.sent_samples += 1;
            } else {
                self.counters.send_failed_samples += 1;
                self.counters.dropped_samples += 1;
            }
            self.counters.last_observation_seq = seq;
            self.remember(&market, &token, sample_state, seq);
            if !sent {
                if let Some(series) = self.series.get_mut(&(market.id, token.clone())) {
                    series.terminal_seq = None;
                }
            }
        }
    }

    /// The market a token belongs to, when it is known.
    fn market_of(&self, token: &str) -> Option<TradableMarket> {
        self.series
            .values()
            .find(|series| series.token == token)
            .map(|series| series.market.clone())
    }

    /// Record what happened to a series.
    fn remember(
        &mut self,
        market: &TradableMarket,
        token: &str,
        sample_state: SampleState,
        seq: u64,
    ) {
        let key = (market.id, token.to_string());
        if !self.series.contains_key(&key) && self.series.len() >= MAX_SERIES {
            self.disabled = Some(DisabledReason::ObservationLimit);
            return;
        }
        let series = self.series.entry(key).or_insert_with(|| Series {
            market: market.clone(),
            token: token.to_string(),
            terminal: None,
            terminal_seq: None,
        });
        if sample_state != SampleState::Active {
            series.terminal = Some(sample_state);
            series.terminal_seq = Some(seq);
        }
    }

    /// Say the channel is open.
    fn send_ready(&mut self) {
        let frame = serde_json::json!({
            "wire_version": WIRE_VERSION,
            "kind": "READY",
            "run_id": self.run_id,
        });
        self.send_frame(&frame);
    }

    /// Send one sample.
    fn send_sample(&mut self, after_event_seq: u64, record: &mut PostEntryObservation) -> bool {
        let frame = |record: &PostEntryObservation| {
            serde_json::json!({
                "wire_version": WIRE_VERSION,
                "kind": "SAMPLE",
                "run_id": self.run_id,
                "after_event_seq": after_event_seq,
                "record": record,
            })
        };
        if serde_json::to_vec(&frame(record))
            .map_or(true, |bytes| bytes.len() + 1 > MAX_FRAME_BYTES)
        {
            record.exit_reason = ExitReason::ObservationLimit;
            record.exit_bid_levels = None;
            record.exit_covered_shares = None;
            record.exit_depth_complete = None;
            record.exit_vwap = None;
            record.exit_gross_income = None;
            record.fee_basis = FeeBasis::Unknown;
            record.fee_rate_bps = None;
            record.exit_fee = None;
            record.exit_net_income = None;
            record.exit_pnl_vs_cost = None;
        }
        let frame = frame(record);
        self.send_frame(&frame)
    }

    /// Send one frame, whole or not at all.
    fn send_frame(&mut self, frame: &serde_json::Value) -> bool {
        let Some(socket) = self.socket.as_ref() else {
            return false;
        };
        let Ok(body) = serde_json::to_string(frame) else {
            self.disabled = Some(DisabledReason::EncodingFailed);
            return false;
        };
        let mut bytes = body.into_bytes();
        bytes.push(b'\n');
        // Only the unsigned 72h launcher sets this output-only capability.
        let limit = if frame["kind"] == "END"
            && std::env::var("BTC5M_POST_ENTRY_RUN_SECONDS").as_deref() == Ok("259200")
        {
            PAPER72_END_BYTES
        } else {
            MAX_FRAME_BYTES
        };
        if bytes.len() > limit {
            self.disabled = Some(DisabledReason::ObservationLimit);
            return false;
        }
        match socket.send(&bytes) {
            Ok(_) => true,
            Err(_) => {
                // A refused frame disables the channel: the run keeps trading
                // and the gap is reported in the closing counters.
                self.disabled = Some(DisabledReason::TransportFailed);
                false
            }
        }
    }

    /// Tick-external termination has no reusable clock, so notify but never sample.
    pub fn finish_with_snapshot(&mut self, snapshot: &BrokerObservationSnapshot) {
        for series in self
            .series
            .values_mut()
            .filter(|series| series.terminal.is_none())
        {
            series.terminal = if snapshot.settled_markets.contains(&series.market.id) {
                Some(SampleState::Settled)
            } else if snapshot.entries_stopped {
                Some(SampleState::RunStopped)
            } else {
                None
            };
        }
        self.finish();
    }

    /// Close the channel and report what it saw.
    pub fn finish(&mut self) {
        let series = self
            .series
            .values()
            .map(|series| SeriesTerminal {
                market_id: series.market.id.to_string(),
                token: series.token.clone(),
                terminal_state: series.terminal,
                terminal_observation_seq: series.terminal_seq,
            })
            .collect();
        let report = ProducerReport {
            counters: self.counters.clone(),
            disabled_reason: self.disabled,
            series,
        };
        let frame = serde_json::json!({
            "wire_version": WIRE_VERSION,
            "kind": "END",
            "run_id": self.run_id,
            "producer": report,
        });
        self.send_frame(&frame);
        self.socket = None;
    }
}

/// The state a series ends in, when it ends.
fn terminal_state(
    snapshot: &BrokerObservationSnapshot,
    market: &TradableMarket,
    position: Option<(f64, f64)>,
    now: Option<CheckTime>,
) -> SampleState {
    if position.is_none() && snapshot.settled_markets.contains(&market.id) {
        return SampleState::Settled;
    }
    if snapshot.entries_stopped {
        return SampleState::RunStopped;
    }
    if let Some(now) = now {
        if market.ends_at.is_some_and(|end| end <= now.utc) {
            return SampleState::RoundEnded;
        }
    }
    SampleState::Active
}

/// The inherited datagram, when the launcher passed a usable one.
fn inherited_socket() -> Result<UnixDatagram> {
    let raw = std::env::var(FD_ENV).ok().unwrap_or_default();
    let fd: i32 = raw
        .parse()
        .map_err(|_| anyhow::anyhow!("TRANSPORT_INVALID"))?;
    if fd < 3 || raw.starts_with('+') {
        return Err(anyhow::anyhow!("TRANSPORT_INVALID"));
    }
    // The descriptor is checked before it is taken: a descriptor this process
    // does not own must never be closed here.
    if !is_datagram(fd) {
        return Err(anyhow::anyhow!("TRANSPORT_INVALID"));
    }
    // SAFETY: the descriptor is owned by this process from the moment the
    // launcher hands it over, and it is turned into an owned socket exactly
    // once, here.
    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
    let socket = UnixDatagram::from(owned);
    socket
        .set_nonblocking(true)
        .map_err(|_| anyhow::anyhow!("TRANSPORT_INVALID"))?;
    Ok(socket)
}

/// Whether the descriptor is a connected `AF_UNIX` datagram.
fn is_datagram(fd: RawFd) -> bool {
    let mut kind: i32 = 0;
    let mut length = std::mem::size_of::<i32>() as u32;
    let ok = unsafe {
        getsockopt(
            fd,
            SOL_SOCKET,
            SO_TYPE,
            &mut kind as *mut i32 as *mut std::os::raw::c_void,
            &mut length,
        )
    } == 0
        && kind == SOCK_DGRAM;
    if !ok {
        return false;
    }
    let mut address = SockAddrUnix {
        sun_family: 0,
        sun_path: [0; 108],
    };
    let mut length = std::mem::size_of::<SockAddrUnix>() as u32;
    // SAFETY: address and length point to writable, correctly sized storage.
    // getpeername also proves the anonymous endpoint is connected.
    let named = unsafe { getpeername(fd, &mut address, &mut length) == 0 };
    named && address.sun_family == AF_UNIX
}

const SOL_SOCKET: i32 = 1;
const SO_TYPE: i32 = 3;
const SOCK_DGRAM: i32 = 2;
const AF_UNIX: u16 = 1;

#[repr(C)]
struct SockAddrUnix {
    sun_family: u16,
    sun_path: [u8; 108],
}

extern "C" {
    fn getsockopt(
        fd: i32,
        level: i32,
        name: i32,
        value: *mut std::os::raw::c_void,
        length: *mut u32,
    ) -> i32;
    fn getpeername(fd: i32, address: *mut SockAddrUnix, length: *mut u32) -> i32;
}
