//! BTC Up/Down rounds: buy when the book lags the underlying.
//!
//! A round is a digital option on the price feed. Its value follows from spot, the
//! round's start price and the time left, so when the book is slower than spot a
//! taker can buy below that value. Despite the name this isn't riskless: a position
//! is held to the end of the round and can lose everything it cost. The edge is
//! being right on average, and the model is a plain driftless lognormal with
//! trailing volatility, not a fitted one.
//!
//! Rounds settle on Binance's top of book. Where Binance isn't reachable the bot
//! still runs on a Coinbase price to exercise the path, but only ever on paper:
//! the feeds differ by enough to swamp a small edge.

pub mod history;
pub mod phase1;
pub mod replay;

use crate::bot::quoting::Known;
use crate::bot::runtime::{drive, shares_for, Ctx, Strategy, TakeResult};
use crate::config::{AppConfig, BtcArbConfig};
use crate::models::{BookSnapshot, Side};
use crate::service::observation::{CheckTime, Clock, ObservedBook, ObservedSpot};
use crate::service::spot::{digital_up, EwmaVol, SpotFeed, SpotQuote, SpotSource};
use crate::service::venue::TradableMarket;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use phase1::{Phase1Core, StrategyContract};

const VOL_HALF_LIFE_SECS: f64 = 300.0;
/// Spot samples before the volatility estimate is trusted.
const MIN_VOL_SAMPLES: u32 = 20;
/// A round's start price appears when it starts, so the list can't go stale for long.
const ROUND_RESCAN: Duration = Duration::from_secs(15);

pub async fn run(cfg: AppConfig) -> Result<()> {
    cfg.strategies.btc_arb.ensure_round_interval()?;
    let ctx = Ctx::connect(cfg, "BTC arbitrage")?;
    let arb_cfg = ctx.cfg.strategies.btc_arb.clone();
    let mut bot = RoundTaker {
        cfg: arb_cfg.clone(),
        core: DecisionCore::new(arb_cfg.clone()),
        spot: SpotFeed::new()?,
        known: Known::default(),
        rounds: Vec::new(),
        last_scan: None,
        phase1: None,
        #[cfg(test)]
        inputs: None,
    };
    // Only an unsigned controlled run speaks the phase1 protocol. Every other
    // path keeps the legacy cash-fee core.
    if ctx.broker.is_phase1() {
        bot.phase1 = Some(Phase1Core::new(StrategyContract::Phase1NetShares));
    }
    drive(&mut bot, &ctx).await
}

struct RoundTaker {
    cfg: BtcArbConfig,
    /// The decision core: volatility state and the markets already bought.
    core: DecisionCore,
    spot: SpotFeed,
    known: Known,
    rounds: Vec<TradableMarket>,
    last_scan: Option<Instant>,
    /// The phase1 core, when this run speaks the phase1 protocol.
    phase1: Option<Phase1Core>,
    #[cfg(test)]
    inputs: Option<(Vec<TradableMarket>, SpotQuote, DateTime<Utc>)>,
}

/// The decision core shared by the live tick and the offline replay driver.
///
/// It owns the volatility estimate and the set of markets already bought. The
/// live path feeds it quotes from the spot feed; the replay path feeds it
/// events on an explicit clock. Neither path re-implements the model.
struct DecisionCore {
    cfg: BtcArbConfig,
    /// The volatility half-life, in seconds.
    half_life_secs: f64,
    /// The number of return samples before the estimate is trusted.
    min_vol_samples: u32,
    vols: HashMap<String, EwmaVol>,
    /// Markets already bought: one entry a round, so a wrong model can't average
    /// into a losing round.
    entered: HashSet<u64>,
}

impl DecisionCore {
    fn new(cfg: BtcArbConfig) -> Self {
        Self::with_warmup(cfg, VOL_HALF_LIFE_SECS, MIN_VOL_SAMPLES)
    }

    fn with_warmup(cfg: BtcArbConfig, half_life_secs: f64, min_vol_samples: u32) -> Self {
        Self {
            cfg,
            half_life_secs,
            min_vol_samples,
            vols: HashMap::new(),
            entered: HashSet::new(),
        }
    }

    /// Record one spot observation on the given clock (seconds since the epoch).
    fn observe_spot(&mut self, symbol: &str, price: f64, at_secs: f64) {
        self.vols
            .entry(symbol.to_string())
            .or_insert_with(|| EwmaVol::new(self.half_life_secs))
            .update(price, at_secs);
    }

    /// Drop a symbol's volatility history so it warms up again from scratch.
    /// Only the replay driver does this, at a gap in the observed series.
    fn forget_vol(&mut self, symbol: &str) {
        self.vols.remove(symbol);
    }

    fn vol_samples(&self, symbol: &str) -> Option<u32> {
        self.vols.get(symbol).map(EwmaVol::samples)
    }

    fn sigma(&self, symbol: &str) -> Option<f64> {
        self.vols.get(symbol).map(|vol| vol.vol(self.cfg.vol_floor))
    }

    fn is_entered(&self, market_id: u64) -> bool {
        self.entered.contains(&market_id)
    }

    /// The model value of `symbol` against `strike`, if the volatility is warm.
    fn valuation(&self, symbol: &str, spot: f64, strike: f64, secs_left: f64) -> Option<Valuation> {
        let vol = self.vols.get(symbol)?;
        if vol.samples() < self.min_vol_samples {
            return None;
        }
        let sigma = vol.vol(self.cfg.vol_floor);
        Some(Valuation {
            samples: vol.samples(),
            sigma,
            p_up: digital_up(spot, strike, sigma, secs_left),
        })
    }

    /// The side worth buying in a round, if either clears `min_edge` after fees.
    fn entry(
        &self,
        market: &TradableMarket,
        book: &BookSnapshot,
        spot: f64,
        now: DateTime<Utc>,
    ) -> Option<RoundEntry> {
        let symbol = market.feed_symbol.as_ref()?;
        let vol = self.vols.get(symbol)?;
        if vol.samples() < self.min_vol_samples {
            return None;
        }
        let sigma = vol.vol(self.cfg.vol_floor);
        round_entry(market, book, spot, sigma, &self.cfg, now)
    }

    /// Record a fill. Only a positive fill counts as an entry.
    fn confirm_fill(&mut self, market_id: u64, shares: f64) -> bool {
        if shares > 0.0 {
            self.entered.insert(market_id)
        } else {
            false
        }
    }
}

/// A model value, without any trading state.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Valuation {
    samples: u32,
    sigma: f64,
    p_up: f64,
}

/// The live tick's adapter to the shared core.
///
/// It repeats the tick's own source guard so the seam can be falsified
/// offline: on a live account only the settlement feed may be traded.
fn live_entry(
    core: &DecisionCore,
    market: &TradableMarket,
    book: &BookSnapshot,
    quote: SpotQuote,
    now: DateTime<Utc>,
    is_live: bool,
) -> Option<RoundEntry> {
    if is_live && quote.source != SpotSource::Binance {
        return None;
    }
    core.entry(market, book, quote.price, now)
}

impl RoundTaker {
    fn now(&self) -> DateTime<Utc> {
        #[cfg(test)]
        if let Some((_, _, now)) = &self.inputs {
            return *now;
        }
        Utc::now()
    }

    async fn read_rounds(&self, ctx: &Ctx) -> Result<Vec<TradableMarket>> {
        #[cfg(test)]
        if let Some((rounds, _, _)) = &self.inputs {
            return Ok(rounds.clone());
        }
        ctx.client.live_rounds().await
    }

    async fn read_quote(&mut self, symbol: &str) -> Result<SpotQuote> {
        #[cfg(test)]
        if let Some((_, quote, _)) = &self.inputs {
            return Ok(*quote);
        }
        self.spot.quote(symbol).await
    }

    /// Apply the configured symbol and optional cadence together on every rescan.
    fn select_rounds(&mut self, rounds: Vec<TradableMarket>, now: DateTime<Utc>) {
        self.rounds = rounds
            .into_iter()
            .filter(|m| {
                m.open
                    && !m.contested
                    && m.start_price.is_some()
                    && m.starts_at.is_some_and(|s| s <= now)
                    && m.feed_symbol
                        .as_ref()
                        .is_some_and(|s| self.cfg.symbols.contains(s))
                    && self.cfg.round_interval_secs.is_none_or(|seconds| {
                        seconds > 0
                            && m.starts_at.zip(m.ends_at).is_some_and(|(start, end)| {
                                end - start == chrono::Duration::seconds(i64::from(seconds))
                            })
                    })
            })
            .collect();
    }
}

#[async_trait]
impl Strategy for RoundTaker {
    fn name(&self) -> &'static str {
        "btc-arb"
    }

    async fn tick(&mut self, ctx: &Ctx) -> Result<()> {
        // A controlled run starts each tick with no usable mark: only the books
        // seen in this tick can make one valid again.
        ctx.broker.invalidate_paper_marks();
        if self.last_scan.is_none_or(|t| t.elapsed() >= ROUND_RESCAN) {
            let rounds = self.read_rounds(ctx).await?;
            self.known.remember(&rounds);
            self.select_rounds(rounds, self.now());
            self.last_scan = Some(Instant::now());
            info!(live_rounds = self.rounds.len(), "up/down rounds read");
        }

        let ids: Vec<u64> = self.rounds.iter().map(|m| m.id).collect();
        if self.phase1.is_some() {
            return self.tick_phase1(ctx, ids).await;
        }

        let at_secs = self.now().timestamp_millis() as f64 / 1000.0;
        let mut quotes = HashMap::new();
        for symbol in self.cfg.symbols.clone() {
            let quote = self.read_quote(&symbol).await?;
            if ctx.broker.paper_snapshot().is_some()
                && (!quote.price.is_finite() || quote.price <= 0.0)
            {
                anyhow::bail!("DATA_INCOMPLETE");
            }
            self.core.observe_spot(&symbol, quote.price, at_secs);
            quotes.insert(symbol, quote);
        }

        let books = self.known.observe(ctx, &ids).await?;
        if ctx.broker.is_halted() {
            return Ok(());
        }

        self.trade_quotes(ctx.broker.as_ref(), &quotes, &books, self.now())
            .await
    }
}

impl RoundTaker {
    /// The phase1 tick: every input carries the clock that received it.
    ///
    /// The inputs are acquired the same way the legacy path acquires them,
    /// but the clock of each quote and each book batch travels with them, so a
    /// decision can prove how old its inputs are.
    async fn tick_phase1(&mut self, ctx: &Ctx, ids: Vec<u64>) -> Result<()> {
        let Some(clock) = ctx.broker.phase1_clock() else {
            anyhow::bail!("CONTROLLED_MODE: the phase1 run has no clock");
        };
        let mut quotes: HashMap<String, ObservedSpot> = HashMap::new();
        for symbol in self.cfg.symbols.clone() {
            let quote = self.spot.quote_observed(&symbol, clock.as_ref()).await?;
            if let Some(core) = self.phase1.as_mut() {
                let now = clock.now()?;
                if let Err(reason) = core.observe(
                    &symbol,
                    quote.price,
                    quote.source,
                    &quote.actual_pair,
                    &quote.clock,
                    now,
                ) {
                    debug!(symbol = %symbol, ?reason, "phase1 observation refused");
                }
            }
            quotes.insert(symbol, quote);
        }

        let mut all: Vec<u64> = ids.to_vec();
        for id in ctx.broker.tracked_markets() {
            if !all.contains(&id) {
                all.push(id);
            }
        }
        for id in &all {
            if self.known.get(*id).is_none() {
                let market = ctx.data.market(*id).await?;
                self.known.remember(&[market]);
            }
        }
        let books = ctx.data.observed_books(&all, clock.as_ref()).await?;
        for (id, observed) in &books {
            if let Some(market) = self.known.get(*id) {
                ctx.broker.observe(market, &observed.book);
            }
        }
        if ctx.broker.is_halted() {
            return Ok(());
        }
        self.trade_phase1(ctx.broker.as_ref(), &quotes, &books, clock.as_ref())
            .await
    }
}

impl RoundTaker {
    /// The real tick's decision-to-Broker consumer, with input acquisition separated.
    async fn trade_quotes(
        &mut self,
        broker: &crate::bot::runtime::Broker,
        quotes: &HashMap<String, SpotQuote>,
        books: &HashMap<u64, BookSnapshot>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        for market in self.rounds.clone() {
            if self.core.is_entered(market.id) {
                continue;
            }
            let (Some(symbol), Some(book)) = (market.feed_symbol.as_ref(), books.get(&market.id))
            else {
                continue;
            };
            let (Some(quote), Some(vol)) = (quotes.get(symbol), self.core.vol_samples(symbol))
            else {
                continue;
            };
            if vol < MIN_VOL_SAMPLES {
                continue;
            }
            if broker.is_live() && quote.source != SpotSource::Binance {
                warn!("no Binance price: not trading live on a substitute feed");
                continue;
            }
            let sigma = self.core.sigma(symbol).unwrap_or(self.cfg.vol_floor);
            debug!(
                market = market.id,
                spot = quote.price,
                strike = market.start_price,
                sigma,
                secs_left = market.secs_left(now),
                fair_up = market
                    .secs_left(now)
                    .zip(market.start_price)
                    .map(|(left, strike)| { digital_up(quote.price, strike, sigma, left as f64) }),
                bid = book.best_bid().map(|l| l.price),
                ask = book.best_ask().map(|l| l.price),
                "round valued"
            );
            let Some(entry) = live_entry(&self.core, &market, book, *quote, now, broker.is_live())
            else {
                continue;
            };
            let shares = shares_for(self.cfg.order_usd, entry.price);
            // The price source belongs to this decision: a Coinbase fallback is
            // recorded separately and is not the same evidence as Binance.
            broker.record_source(&entry.token, &format!("{:?}", quote.source), now);
            info!(
                market = market.id,
                fair = entry.fair,
                price = entry.price,
                edge = entry.edge,
                spot = quote.price,
                source = ?quote.source,
                "book below model value"
            );
            match broker
                .take(&market, &entry.token, Side::Buy, shares, entry.price, book)
                .await?
            {
                TakeResult::Filled { shares, .. } if shares > 0.0 => {
                    self.core.confirm_fill(market.id, shares);
                }
                TakeResult::Filled { .. } => {}
                TakeResult::Blocked(reason) => info!(market = market.id, ?reason, "entry blocked"),
            }
        }
        Ok(())
    }

    /// The phase1 decision-to-Broker consumer.
    ///
    /// Every selected market is evaluated once, so a refusal leaves a snapshot
    /// too. Only a candidate that clears the checks reaches the broker, and it
    /// reaches it with the context the evaluation produced.
    async fn trade_phase1(
        &mut self,
        broker: &crate::bot::runtime::Broker,
        quotes: &HashMap<String, ObservedSpot>,
        books: &HashMap<u64, ObservedBook>,
        clock: &dyn Clock,
    ) -> Result<()> {
        let Some(core) = self.phase1.as_mut() else {
            return Ok(());
        };
        for market in self.rounds.clone() {
            let Some(symbol) = market.feed_symbol.clone() else {
                continue;
            };
            let Some(quote) = quotes.get(&symbol) else {
                continue;
            };
            let Some(observed) = books.get(&market.id) else {
                continue;
            };
            let now = clock.now()?;
            let inputs = phase1::EvaluationInputs {
                market: &market,
                book: &observed.book,
                spot_price: quote.price,
                spot_source: quote.source,
                actual_pair: quote.actual_pair.clone(),
                spot_clock: Some(quote.clock.clone()),
                book_clock: Some(observed.clock.clone()),
                now,
                order_usd: self.cfg.order_usd,
                min_edge: self.cfg.min_edge,
                min_secs_left: self.cfg.min_secs_left as f64,
                entered: self.core.is_entered(market.id),
                identity_available_at: None,
                strike_available_at: None,
                settlement_provider: phase1::SettlementProvider::Unknown,
            };
            let evaluation = core.evaluate(&inputs);
            let Some(candidate) = evaluation.candidate else {
                debug!(
                    market = market.id,
                    reason = ?evaluation.diagnostic.reason,
                    "phase1 evaluation refused"
                );
                continue;
            };
            let token = candidate.token().to_string();
            broker.record_source(&token, &format!("{:?}", quote.source), now.utc);
            info!(
                market = market.id,
                fair = candidate.fair(),
                price = candidate.limit_price(),
                edge = evaluation
                    .diagnostic
                    .up
                    .as_ref()
                    .and_then(|side| side.edge)
                    .or_else(|| {
                        evaluation
                            .diagnostic
                            .down
                            .as_ref()
                            .and_then(|side| side.edge)
                    }),
                spot = quote.price,
                source = ?quote.source,
                "phase1 book below model value"
            );
            match broker.take_candidate(&market, &observed.book, &candidate)? {
                TakeResult::Filled { shares, .. } if shares > 0.0 => {
                    self.core.confirm_fill(market.id, shares);
                }
                TakeResult::Filled { .. } => {}
                TakeResult::Blocked(reason) => {
                    info!(market = market.id, ?reason, "phase1 entry blocked")
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RoundEntry {
    token: String,
    price: f64,
    fair: f64,
    edge: f64,
}

/// The side worth buying in a live round, if either clears `min_edge` after fees.
fn round_entry(
    market: &TradableMarket,
    book: &BookSnapshot,
    spot: f64,
    vol: f64,
    cfg: &BtcArbConfig,
    now: DateTime<Utc>,
) -> Option<RoundEntry> {
    let secs_left = market.secs_left(now)?;
    if secs_left < cfg.min_secs_left {
        return None;
    }
    let strike = market.start_price?;
    let fair_up = digital_up(spot, strike, vol, secs_left as f64);

    let up = book.best_ask().map(|ask| RoundEntry {
        token: market.yes_token.clone(),
        price: ask.price,
        fair: fair_up,
        edge: fair_up - ask.price - market.taker_fee(ask.price),
    });
    let down = book.best_bid().map(|bid| {
        let price = 1.0 - bid.price;
        RoundEntry {
            token: market.no_token.clone(),
            price,
            fair: 1.0 - fair_up,
            edge: 1.0 - fair_up - price - market.taker_fee(price),
        }
    });
    [up, down]
        .into_iter()
        .flatten()
        .filter(|e| e.edge >= cfg.min_edge)
        .max_by(|a, b| a.edge.total_cmp(&b.edge))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bot::runtime::test_support::{book, market};

    fn cfg() -> BtcArbConfig {
        BtcArbConfig {
            min_edge: 0.03,
            min_secs_left: 30,
            ..Default::default()
        }
    }

    fn round(now: DateTime<Utc>, secs: i64) -> TradableMarket {
        TradableMarket {
            start_price: Some(100.0),
            ends_at: Some(now + chrono::Duration::seconds(secs)),
            ..market(1)
        }
    }

    fn selection_bot(interval: Option<u32>) -> RoundTaker {
        // Deserialize the real application shape, then use exactly the config
        // field that run() passes to RoundTaker in production.
        let mut public: serde_json::Value =
            serde_json::from_str(include_str!("../../config.json")).unwrap();
        public["strategies"]["btc_arb"]["symbols"] = serde_json::json!(["BTCUSDT"]);
        if let Some(seconds) = interval {
            public["strategies"]["btc_arb"]["round_interval_secs"] = seconds.into();
        } else {
            public["strategies"]["btc_arb"]
                .as_object_mut()
                .unwrap()
                .remove("round_interval_secs");
        }
        let app: AppConfig = serde_json::from_value(public).unwrap();
        let cfg = app.strategies.btc_arb;
        RoundTaker {
            core: DecisionCore::new(cfg.clone()),
            cfg,
            spot: SpotFeed::new().unwrap(),
            known: Known::default(),
            rounds: Vec::new(),
            last_scan: None,
            phase1: None,
            #[cfg(test)]
            inputs: None,
        }
    }

    fn selectable_round(id: u64, symbol: &str, seconds: i64, now: DateTime<Utc>) -> TradableMarket {
        TradableMarket {
            feed_symbol: Some(symbol.into()),
            starts_at: Some(now - chrono::Duration::seconds(1)),
            ends_at: Some(now + chrono::Duration::seconds(seconds - 1)),
            start_price: Some(100.0),
            ..market(id)
        }
    }

    #[tokio::test]
    async fn controlled_round_taker_consumes_real_core_and_broker_once() {
        use crate::bot::runtime::Broker;
        use crate::config::{ControlledMode, ControlledRun, StrategyLimits};
        use crate::service::venue::RoundFacts;
        let now = Utc::now();
        for source in [SpotSource::Binance, SpotSource::Coinbase] {
            let mut bot = selection_bot(Some(300));
            bot.cfg.order_usd = 2.0;
            let mut round = selectable_round(1, "BTCUSDT", 300, now);
            round.round_facts = Some(RoundFacts {
                decimal_precision: Some(2),
                ..Default::default()
            });
            bot.select_rounds(vec![round.clone()], now);
            for sample in 0..=20 {
                bot.core.observe_spot("BTCUSDT", 101.0, sample as f64);
            }
            let broker = Broker::new(
                StrategyLimits {
                    max_order_usd: 2.0,
                    max_market_usd: 2.0,
                    max_total_usd: 10.0,
                    max_loss_usd: 20.0,
                },
                false,
                None,
                None,
            )
            .unwrap()
            .with_controlled_paper(
                ControlledRun::new(ControlledMode::UnsignedPaper, "round-taker".into(), now)
                    .unwrap(),
                100.0,
            )
            .unwrap();
            let book = crate::bot::runtime::test_support::book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
            broker.observe(&round, &book);
            let books = HashMap::from([(1, book)]);
            let quotes = HashMap::from([(
                "BTCUSDT".into(),
                SpotQuote {
                    price: 101.0,
                    source,
                },
            )]);
            bot.trade_quotes(&broker, &quotes, &books, now)
                .await
                .unwrap();
            assert_eq!(broker.holdings().len(), 1);
            assert!(bot.core.is_entered(1));
            assert_eq!(
                broker.source_of(&round.yes_token).unwrap().0,
                format!("{source:?}")
            );
            let before = broker.paper_snapshot().unwrap();
            bot.trade_quotes(&broker, &quotes, &books, now)
                .await
                .unwrap();
            assert_eq!(broker.paper_snapshot().unwrap(), before);
        }
    }

    #[tokio::test]
    async fn real_tick_acquires_inputs_warms_core_and_obeys_29_30_boundary() {
        use crate::bot::runtime::Broker;
        use crate::config::{ControlledMode, ControlledRun};
        use crate::service::predict_fun::PredictFunClient;
        use crate::service::venue::{MarketData, MarketQuery, RoundFacts};
        use std::sync::Arc;
        struct Data(BookSnapshot);
        #[async_trait]
        impl MarketData for Data {
            async fn books(&self, ids: &[u64]) -> Result<HashMap<u64, BookSnapshot>> {
                Ok(ids.iter().map(|id| (*id, self.0.clone())).collect())
            }
            async fn market(&self, _: u64) -> Result<TradableMarket> {
                panic!("UNEXPECTED_MARKET_READ")
            }
            async fn markets(&self, _: &MarketQuery) -> Result<Vec<TradableMarket>> {
                panic!("NETWORK_FORBIDDEN")
            }
            async fn with_schedule(&self, _: &TradableMarket) -> Result<TradableMarket> {
                panic!("NETWORK_FORBIDDEN")
            }
        }
        for (ticks, left, interval, has_start, expected) in [
            (20, 30, 300, true, false),
            (21, 29, 300, true, false),
            (21, 30, 300, true, true),
            (21, 30, 900, true, false),
            (21, 30, 300, false, false),
        ] {
            for source in [SpotSource::Binance, SpotSource::Coinbase] {
                let now = Utc::now();
                let mut round = selectable_round(
                    1,
                    "BTCUSDT",
                    interval,
                    now - chrono::Duration::seconds(interval - left),
                );
                round.starts_at = Some(now - chrono::Duration::seconds(interval - left));
                round.ends_at = Some(now + chrono::Duration::seconds(left));
                round.start_price = has_start.then_some(100.0);
                round.round_facts = Some(RoundFacts {
                    decimal_precision: Some(2),
                    ..Default::default()
                });
                let mut cfg: AppConfig = serde_json::from_str(include_str!(
                    "../../tools/predictfun/btc5m-controlled-paper.json"
                ))
                .unwrap();
                cfg.credentials.predict_api_key = Some("synthetic-market-only".into());
                let client = PredictFunClient::new(&cfg).unwrap();
                let broker = Broker::new(cfg.strategies.limits.clone(), false, None, None)
                    .unwrap()
                    .with_controlled_paper(
                        ControlledRun::new(ControlledMode::UnsignedPaper, "tick-test".into(), now)
                            .unwrap(),
                        100.0,
                    )
                    .unwrap();
                let ctx = Ctx {
                    cfg: cfg.clone(),
                    data: Arc::new(Data(book(&[(0.4, 100.0)], &[(0.4, 100.0)]))),
                    client,
                    broker: Arc::new(broker),
                };
                let mut bot = selection_bot(Some(300));
                bot.cfg = cfg.strategies.btc_arb;
                bot.core = DecisionCore::new(bot.cfg.clone());
                for index in 0..ticks {
                    let at = now - chrono::Duration::seconds((ticks - 1 - index) * 5);
                    bot.inputs = Some((
                        vec![round.clone()],
                        SpotQuote {
                            price: 101.0,
                            source,
                        },
                        at,
                    ));
                    bot.tick(&ctx).await.unwrap();
                }
                assert_eq!(
                    !ctx.broker.holdings().is_empty(),
                    expected,
                    "ticks={ticks}, left={left}"
                );
                if expected {
                    assert_eq!(
                        ctx.broker.source_of(&round.yes_token).unwrap().0,
                        format!("{source:?}")
                    );
                    let cash = ctx.broker.paper_snapshot().unwrap().cash;
                    bot.tick(&ctx).await.unwrap();
                    assert_eq!(ctx.broker.paper_snapshot().unwrap().cash, cash);
                }
            }
        }
    }

    #[test]
    fn rescan_uses_app_config_cadence_and_symbol_together() {
        let now = Utc::now();
        let mut rounds: Vec<_> = [300, 900, 3600, 86400]
            .into_iter()
            .enumerate()
            .map(|(index, seconds)| selectable_round(index as u64 + 1, "BTCUSDT", seconds, now))
            .collect();
        rounds.push(selectable_round(5, "BNBUSDT", 300, now));
        rounds.push(selectable_round(6, "ETHUSDT", 900, now));
        let mut unknown_end = selectable_round(7, "BTCUSDT", 300, now);
        unknown_end.ends_at = None;
        rounds.push(unknown_end);
        for (interval, expected) in [
            (None, vec![1, 2, 3, 4, 7]),
            (Some(300), vec![1]),
            (Some(900), vec![2]),
            (Some(3600), vec![3]),
            (Some(86400), vec![4]),
            (Some(1), vec![]),
        ] {
            let mut bot = selection_bot(interval);
            // This is the rescan method called directly by Strategy::tick,
            // operating on the instance's deserialized production config.
            bot.select_rounds(rounds.clone(), now);
            assert_eq!(
                bot.rounds.iter().map(|m| m.id).collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn explicit_cadence_requires_known_exact_positive_window() {
        let now = Utc::now();
        let valid = selectable_round(1, "BTCUSDT", 300, now);
        let mut cases = vec![valid.clone()];
        let mut unknown_start = valid.clone();
        unknown_start.starts_at = None;
        cases.push(unknown_start);
        let mut unknown_end = valid.clone();
        unknown_end.ends_at = None;
        cases.push(unknown_end);
        let mut fractional = valid.clone();
        fractional.ends_at = fractional
            .ends_at
            .map(|end| end + chrono::Duration::milliseconds(1));
        cases.push(fractional);
        cases.push(selectable_round(5, "BTCUSDT", -300, now));
        cases.push(selectable_round(6, "BTCUSDT", 0, now));
        for (index, candidate) in cases.into_iter().enumerate() {
            let mut bot = selection_bot(Some(300));
            bot.select_rounds(vec![candidate], now);
            assert_eq!(bot.rounds.len(), usize::from(index == 0));
        }
        let mut zero = selection_bot(Some(0));
        zero.select_rounds(vec![selectable_round(1, "BTCUSDT", 0, now)], now);
        assert!(zero.rounds.is_empty());
    }

    #[test]
    fn cadence_does_not_bypass_existing_round_eligibility() {
        let now = Utc::now();
        let valid = selectable_round(1, "BTCUSDT", 300, now);
        let mut closed = valid.clone();
        closed.open = false;
        let mut contested = valid.clone();
        contested.contested = true;
        let mut no_price = valid.clone();
        no_price.start_price = None;
        let future = selectable_round(2, "BTCUSDT", 300, now + chrono::Duration::seconds(60));
        let mut no_symbol = valid;
        no_symbol.feed_symbol = None;
        for interval in [None, Some(300)] {
            let mut bot = selection_bot(interval);
            bot.select_rounds(
                vec![
                    closed.clone(),
                    contested.clone(),
                    no_price.clone(),
                    future.clone(),
                    no_symbol.clone(),
                ],
                now,
            );
            assert!(bot.rounds.is_empty());
        }
    }

    #[tokio::test]
    async fn zero_cadence_cannot_start_btc_arb_even_without_app_load() {
        let mut app: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
        app.strategies.btc_arb.round_interval_secs = Some(0);
        let error = run(app).await.unwrap_err();
        assert!(
            error.to_string().contains("round_interval_secs"),
            "{error:#}"
        );
    }

    #[test]
    fn buys_up_when_spot_has_moved_and_the_book_hasnt() {
        let now = Utc::now();
        let stale = book(&[(0.48, 50.0)], &[(0.52, 50.0)]);
        let e = round_entry(&round(now, 300), &stale, 100.1, 0.0001, &cfg(), now).unwrap();
        assert_eq!(e.token, "yes-1");
        assert_eq!(e.price, 0.52);
        assert!(e.fair > 0.7 && e.edge > 0.03);
    }

    #[test]
    fn buys_down_through_the_mirrored_book_when_spot_has_fallen() {
        let now = Utc::now();
        let stale = book(&[(0.48, 50.0)], &[(0.52, 50.0)]);
        let e = round_entry(&round(now, 300), &stale, 99.9, 0.0001, &cfg(), now).unwrap();
        assert_eq!(e.token, "no-1");
        assert!((e.price - 0.52).abs() < 1e-9);
    }

    #[test]
    fn a_book_in_line_with_spot_is_left_alone() {
        let now = Utc::now();
        let fair = book(&[(0.48, 50.0)], &[(0.52, 50.0)]);
        assert!(round_entry(&round(now, 300), &fair, 100.0, 0.0001, &cfg(), now).is_none());
    }

    #[test]
    fn fees_count_against_the_edge() {
        let now = Utc::now();
        let book = book(&[(0.3, 50.0)], &[(0.5, 50.0)]);
        let free = TradableMarket {
            fee_rate_bps: 0,
            ..round(now, 300)
        };
        let dear = TradableMarket {
            fee_rate_bps: 2000,
            ..round(now, 300)
        };
        // Value about 0.533 against a 0.50 ask: only clears without the fee.
        let spot = 100.0 + 0.000_01 * 100.0 * 100.0;
        let vol = 0.0005;
        let fair = digital_up(spot, 100.0, vol, 300.0);
        assert!(fair - 0.5 > 0.03 && fair - 0.5 < 0.1, "{fair}");
        assert!(round_entry(&free, &book, spot, vol, &cfg(), now).is_some());
        assert!(round_entry(&dear, &book, spot, vol, &cfg(), now).is_none());
    }

    #[test]
    fn stays_out_near_the_end_and_before_the_start_price_is_known() {
        let now = Utc::now();
        let stale = book(&[(0.48, 50.0)], &[(0.52, 50.0)]);
        assert!(round_entry(&round(now, 10), &stale, 100.1, 0.0001, &cfg(), now).is_none());
        let unstarted = TradableMarket {
            start_price: None,
            ..round(now, 300)
        };
        assert!(round_entry(&unstarted, &stale, 100.1, 0.0001, &cfg(), now).is_none());
    }

    /// The golden values below were recorded from the original code at
    /// 71c0e4f, before the decision core was extracted. The live seam and the
    /// replay driver must both reproduce them.
    const GOLDEN_NOW_MS: i64 = 1_762_636_800_000;

    fn golden_now() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp_millis(GOLDEN_NOW_MS).unwrap()
    }

    fn golden_round(secs: i64) -> TradableMarket {
        let now = golden_now();
        TradableMarket {
            start_price: Some(100.0),
            ends_at: Some(now + chrono::Duration::seconds(secs)),
            feed_symbol: Some("BTCUSDT".to_string()),
            ..market(1)
        }
    }

    /// The core and the live seam agree with the original golden values.
    #[test]
    fn shared_core_matches_original_tick_inputs() {
        let now = golden_now();
        let stale = book(&[(0.48, 50.0)], &[(0.52, 50.0)]);
        let mut core = DecisionCore::new(cfg());
        // Twenty strictly increasing prices warm the volatility up.
        for index in 0..21 {
            core.observe_spot(
                "BTCUSDT",
                100.0 + 0.0001 * index as f64,
                index as f64 * 300.0,
            );
        }
        assert_eq!(core.vol_samples("BTCUSDT"), Some(20));

        let market = golden_round(300);
        let entry = live_entry(
            &core,
            &market,
            &stale,
            SpotQuote {
                price: 100.1,
                source: SpotSource::Binance,
            },
            now,
            false,
        )
        .expect("the golden case produces an entry");
        assert_eq!(entry.token, "yes-1");
        assert_eq!(entry.price, 0.52);
        assert_eq!(entry.fair, 0.8756864185323019);
        assert_eq!(entry.edge, 0.34608641853230188);
    }

    /// The original golden values, unchanged, for the cases the original
    /// tests cover. The core must reach the same decisions.
    #[test]
    fn shared_core_reproduces_the_original_decisions() {
        let now = golden_now();
        let stale = book(&[(0.48, 50.0)], &[(0.52, 50.0)]);
        let mut core = DecisionCore::new(cfg());
        for index in 0..21 {
            core.observe_spot(
                "BTCUSDT",
                100.0 + 0.0001 * index as f64,
                index as f64 * 300.0,
            );
        }
        let market = golden_round(300);

        // The mirrored book buys Down when spot has fallen.
        let down = live_entry(
            &core,
            &market,
            &stale,
            SpotQuote {
                price: 99.9,
                source: SpotSource::Binance,
            },
            now,
            false,
        )
        .expect("the mirrored case produces an entry");
        assert_eq!(down.token, "no-1");
        assert_eq!(down.price, 0.52);
        assert_eq!(down.fair, 0.8761003108381178);
        assert_eq!(down.edge, 0.34650031083811778);

        // A book in line with spot is left alone.
        assert!(live_entry(
            &core,
            &market,
            &stale,
            SpotQuote {
                price: 100.0,
                source: SpotSource::Binance,
            },
            now,
            false,
        )
        .is_none());

        // Fees count against the edge: the fee is subtracted from it exactly.
        let fee_book = book(&[(0.3, 50.0)], &[(0.5, 50.0)]);
        let spot = 100.0 + 0.000_01 * 100.0 * 100.0;
        let free = TradableMarket {
            fee_rate_bps: 0,
            ..golden_round(300)
        };
        let dear = TradableMarket {
            fee_rate_bps: 2000,
            ..golden_round(300)
        };
        let free_entry = live_entry(
            &core,
            &free,
            &fee_book,
            SpotQuote {
                price: spot,
                source: SpotSource::Binance,
            },
            now,
            false,
        )
        .expect("the fee-free case produces an entry");
        let dear_entry = live_entry(
            &core,
            &dear,
            &fee_book,
            SpotQuote {
                price: spot,
                source: SpotSource::Binance,
            },
            now,
            false,
        )
        .expect("the fee-bearing case still clears the threshold here");
        assert_eq!(free_entry.fair, 0.8756864185323019);
        assert_eq!(free_entry.edge, 0.37568641853230189);
        assert_eq!(dear_entry.fair, 0.8756864185323019);
        assert_eq!(dear_entry.edge, 0.27568641853230191);
        assert!((free_entry.edge - dear_entry.edge - 0.1).abs() < 1e-12);

        // Near the end of the round, no entry.
        assert!(live_entry(
            &core,
            &golden_round(10),
            &stale,
            SpotQuote {
                price: 100.1,
                source: SpotSource::Binance,
            },
            now,
            false,
        )
        .is_none());

        // Exactly the minimum time left is allowed; one second less is not.
        let at_30 = live_entry(
            &core,
            &golden_round(30),
            &stale,
            SpotQuote {
                price: 100.1,
                source: SpotSource::Binance,
            },
            now,
            false,
        )
        .expect("30 seconds left is still tradable");
        assert_eq!(at_30.fair, 0.9998686020385528);
        assert!(live_entry(
            &core,
            &golden_round(29),
            &stale,
            SpotQuote {
                price: 100.1,
                source: SpotSource::Binance,
            },
            now,
            false,
        )
        .is_none());
    }

    /// The live seam refuses a substitute feed on a live account only.
    #[test]
    fn the_live_seam_keeps_the_source_guard() {
        let now = golden_now();
        let stale = book(&[(0.48, 50.0)], &[(0.52, 50.0)]);
        let mut core = DecisionCore::new(cfg());
        for index in 0..21 {
            core.observe_spot(
                "BTCUSDT",
                100.0 + 0.0001 * index as f64,
                index as f64 * 300.0,
            );
        }
        let market = golden_round(300);
        let coinbase = SpotQuote {
            price: 100.1,
            source: SpotSource::Coinbase,
        };
        // Live on a substitute feed: refused.
        assert!(live_entry(&core, &market, &stale, coinbase, now, true).is_none());
        // Paper on a substitute feed: allowed.
        assert!(live_entry(&core, &market, &stale, coinbase, now, false).is_some());
        // Live on the settlement feed: allowed.
        let binance = SpotQuote {
            price: 100.1,
            source: SpotSource::Binance,
        };
        assert!(live_entry(&core, &market, &stale, binance, now, true).is_some());
    }

    /// The original parameters are unchanged.
    #[test]
    fn the_original_parameters_are_unchanged() {
        let default = BtcArbConfig::default();
        assert_eq!(default.symbols, vec!["BTCUSDT".to_string()]);
        assert_eq!(default.min_edge, 0.03);
        assert_eq!(default.order_usd, 5.0);
        assert_eq!(default.min_secs_left, 30);
        assert_eq!(default.vol_floor, 0.00005);
        assert_eq!(VOL_HALF_LIFE_SECS, 300.0);
        assert_eq!(MIN_VOL_SAMPLES, 20);
    }
}
