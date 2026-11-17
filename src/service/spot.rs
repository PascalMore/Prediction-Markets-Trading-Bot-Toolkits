//! Underlying prices for up/down rounds, and the model that values them.
//!
//! Predict's rounds settle on the mid of Binance's top of book, so Binance is the
//! only feed that measures the right thing. Coinbase is a fallback for places
//! Binance refuses: its USD pairs sit a few basis points away because of the
//! USDT/USD basis, so a value computed from one is good enough to watch but not
//! to trade on.
//!
//! Neither feed carries the time of the event it prices, so a quote only ever
//! proves when this process received it. The phase1 path keeps that receipt
//! time next to the price; the ordinary path keeps its old, simpler shape.

use crate::service::observation::{
    CheckTime, Clock, FreshnessBasis, ObservationClock, ObservedSpot, SourceTimeStatus,
};
use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tracing::warn;

const BINANCE: &str = "https://api.binance.com/api/v3/ticker/bookTicker";
const COINBASE: &str = "https://api.coinbase.com/v2/prices";
/// Once Binance has refused us, don't ask again for a while.
const BINANCE_RETRY_AFTER: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum SpotSource {
    Binance,
    Coinbase,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpotQuote {
    pub price: f64,
    pub source: SpotSource,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceTicker {
    bid_price: String,
    ask_price: String,
}

#[derive(Debug, Deserialize)]
struct CoinbaseEnvelope {
    data: CoinbasePrice,
}

#[derive(Debug, Deserialize)]
struct CoinbasePrice {
    amount: String,
}

pub struct SpotFeed {
    http: Client,
    binance_refused_at: Option<Instant>,
    #[cfg(test)]
    fixture_quotes: Option<std::sync::Mutex<std::collections::VecDeque<(SpotSource, Option<f64>)>>>,
}

impl SpotFeed {
    pub fn new() -> Result<Self> {
        Ok(Self {
            http: Client::builder()
                .user_agent("polymarket-toolkits/0.1")
                .timeout(Duration::from_secs(5))
                .build()?,
            binance_refused_at: None,
            #[cfg(test)]
            fixture_quotes: None,
        })
    }

    #[cfg(test)]
    fn fixture_price(&self, expected: SpotSource) -> Option<Result<f64>> {
        self.fixture_quotes.as_ref().map(|quotes| {
            let (source, price) = quotes
                .lock()
                .unwrap()
                .pop_front()
                .expect("spot fixture exhausted");
            assert_eq!(source, expected);
            price.ok_or_else(|| anyhow!("SYNTHETIC_SPOT_FAILURE"))
        })
    }

    /// `symbol` as Predict's feeds name it, e.g. `BTCUSDT`.
    pub async fn quote(&mut self, symbol: &str) -> Result<SpotQuote> {
        match self.binance_with_clock(symbol, None).await {
            Ok((price, _)) => {
                self.binance_refused_at = None;
                return Ok(SpotQuote {
                    price,
                    source: SpotSource::Binance,
                });
            }
            Err(e) => {
                warn!(error = ?e, "Binance unavailable; falling back to Coinbase, which can't be traded on");
                self.binance_refused_at = Some(Instant::now());
            }
        }
        Ok(SpotQuote {
            price: self.coinbase_with_clock(symbol, None).await?.0,
            source: SpotSource::Coinbase,
        })
    }

    /// The same quote, with the clock that received it.
    ///
    /// The clock belongs to the source that answered: a Coinbase fallback's
    /// request starts after Binance gave up, so the wait for the failed source
    /// is not part of this quote's age.
    pub async fn quote_observed(
        &mut self,
        symbol: &str,
        clock: &dyn Clock,
    ) -> Result<ObservedSpot> {
        if self
            .binance_refused_at
            .is_none_or(|t| t.elapsed() >= BINANCE_RETRY_AFTER)
        {
            match self.binance_with_clock(symbol, Some(clock)).await {
                Ok((price, times)) if price.is_finite() && price > 0.0 => {
                    self.binance_refused_at = None;
                    return Ok(ObservedSpot {
                        price,
                        source: SpotSource::Binance,
                        actual_pair: symbol.to_string(),
                        clock: observation_clock(times.expect("a clocked quote has its readings")),
                    });
                }
                Ok((price, _)) => {
                    warn!(price, "Binance returned a value that is not a price; falling back to Coinbase");
                    self.binance_refused_at = Some(Instant::now());
                }
                Err(e) => {
                    warn!(error = ?e, "Binance unavailable; falling back to Coinbase, which can't be traded on");
                    self.binance_refused_at = Some(Instant::now());
                }
            }
        }
        let (price, times) = self.coinbase_with_clock(symbol, Some(clock)).await?;
        if !price.is_finite() || price <= 0.0 {
            bail!("DATA_INCOMPLETE: the spot feed returned a value that is not a price");
        }
        Ok(ObservedSpot {
            price,
            source: SpotSource::Coinbase,
            actual_pair: coinbase_pair(symbol)?,
            clock: observation_clock(times.expect("a clocked quote has its readings")),
        })
    }

    async fn binance_with_clock(
        &self,
        symbol: &str,
        clock: Option<&dyn Clock>,
    ) -> Result<(f64, Option<(CheckTime, CheckTime)>)> {
        let request_at = sample(clock)?;
        #[cfg(test)]
        if let Some(price) = self.fixture_price(SpotSource::Binance) {
            return Ok((price?, times(request_at, sample(clock)?)));
        }
        let resp = self
            .http
            .get(format!("{BINANCE}?symbol={symbol}"))
            .send()
            .await?;
        if !resp.status().is_success() {
            bail!("HTTP {}", resp.status().as_u16());
        }
        let ticker: BinanceTicker = resp.json().await?;
        let bid: f64 = ticker.bid_price.parse()?;
        let ask: f64 = ticker.ask_price.parse()?;
        let received_at = sample(clock)?;
        Ok(((bid + ask) / 2.0, times(request_at, received_at)))
    }

    async fn coinbase_with_clock(
        &self,
        symbol: &str,
        clock: Option<&dyn Clock>,
    ) -> Result<(f64, Option<(CheckTime, CheckTime)>)> {
        let pair = coinbase_pair(symbol)?;
        let request_at = sample(clock)?;
        #[cfg(test)]
        if let Some(price) = self.fixture_price(SpotSource::Coinbase) {
            return Ok((price?, times(request_at, sample(clock)?)));
        }
        let price: CoinbaseEnvelope = self
            .http
            .get(format!("{COINBASE}/{pair}/spot"))
            .send()
            .await
            .context("Coinbase price")?
            .error_for_status()?
            .json()
            .await?;
        let received_at = sample(clock)?;
        Ok((price.data.amount.parse()?, times(request_at, received_at)))
    }
}

/// The readings of one request, when the caller wanted them.
fn times(request_at: Option<CheckTime>, received_at: Option<CheckTime>) -> Option<(CheckTime, CheckTime)> {
    Some((request_at?, received_at?))
}

/// One reading of the caller's clock, or nothing when there is no clock.
fn sample(clock: Option<&dyn Clock>) -> Result<Option<CheckTime>> {
    match clock {
        Some(clock) => Ok(Some(clock.now()?)),
        None => Ok(None),
    }
}

/// The clock of an acquired quote.
///
/// The upstream feeds carry no event time, so the freshness can only be proven
/// from the local receipt: `source_at` stays `None` and is never guessed.
fn observation_clock((request_at, received_at): (CheckTime, CheckTime)) -> ObservationClock {
    ObservationClock {
        source_at: None,
        source_time_status: SourceTimeStatus::Missing,
        freshness_basis: FreshnessBasis::ReceiptBound,
        request_at: request_at.utc,
        received_at: received_at.utc,
        request_elapsed_ms: request_at.elapsed_ms,
        received_elapsed_ms: received_at.elapsed_ms,
    }
}

fn coinbase_pair(symbol: &str) -> Result<String> {
    let base = symbol
        .strip_suffix("USDT")
        .or_else(|| symbol.strip_suffix("USD"))
        .ok_or_else(|| anyhow!("can't map {symbol} to a Coinbase pair"))?;
    Ok(format!("{}-USD", base.trim_end_matches('_')))
}

/// Exponentially weighted volatility of log returns, per root second.
#[derive(Debug, Clone)]
pub struct EwmaVol {
    half_life_secs: f64,
    variance_per_sec: Option<f64>,
    last: Option<(f64, f64)>,
    samples: u32,
}

impl EwmaVol {
    pub fn new(half_life_secs: f64) -> Self {
        Self {
            half_life_secs,
            variance_per_sec: None,
            last: None,
            samples: 0,
        }
    }

    pub fn update(&mut self, price: f64, at_secs: f64) {
        if price <= 0.0 {
            return;
        }
        if let Some((last_price, last_at)) = self.last {
            let dt = at_secs - last_at;
            if dt <= 0.0 {
                return;
            }
            let ret = (price / last_price).ln();
            let sample = ret * ret / dt;
            let weight = 1.0 - 0.5f64.powf(dt / self.half_life_secs);
            self.variance_per_sec = Some(match self.variance_per_sec {
                Some(v) => v + weight * (sample - v),
                None => sample,
            });
            self.samples += 1;
        }
        self.last = Some((price, at_secs));
    }

    pub fn samples(&self) -> u32 {
        self.samples
    }

    pub fn vol(&self, floor: f64) -> f64 {
        self.variance_per_sec.map_or(floor, |v| v.sqrt().max(floor))
    }
}

/// Standard normal CDF (Abramowitz & Stegun 26.2.17).
pub fn normal_cdf(x: f64) -> f64 {
    if x < 0.0 {
        return 1.0 - normal_cdf(-x);
    }
    let t = 1.0 / (1.0 + 0.2316419 * x);
    let poly = t
        * (0.319381530
            + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
    let pdf = (-0.5 * x * x).exp() / (2.0 * std::f64::consts::PI).sqrt();
    1.0 - pdf * poly
}

/// Chance the price ends above `strike`, under a driftless lognormal model.
pub fn digital_up(spot: f64, strike: f64, vol_per_root_sec: f64, secs_left: f64) -> f64 {
    if secs_left <= 0.0 || vol_per_root_sec <= 0.0 {
        return if spot > strike {
            1.0
        } else if spot < strike {
            0.0
        } else {
            0.5
        };
    }
    let total = vol_per_root_sec * secs_left.sqrt();
    normal_cdf((spot / strike).ln() / total - total / 2.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn observed_fallback_and_retry_keep_source_clocks_and_epochs() {
        use crate::bot::arbitrage::{phase1::*, DecisionCore};
        use crate::bot::runtime::{paper, Broker};
        use crate::config::{BtcArbConfig, ControlledMode, ControlledRun, StrategyLimits};
        use crate::models::{BookLevel, BookSnapshot};
        use crate::service::observation::FixtureClock;
        use crate::service::venue::{RoundFacts, TradableMarket};
        use chrono::{DateTime, Utc};
        use std::collections::VecDeque;
        use std::sync::{Arc, Mutex};
        struct Times(Mutex<VecDeque<CheckTime>>);
        impl Clock for Times {
            fn now(&self) -> Result<CheckTime> {
                Ok(self
                    .0
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("clock fixture exhausted"))
            }
        }
        let origin = DateTime::<Utc>::from_timestamp_millis(1_762_636_800_000).unwrap();
        let clock = Times(Mutex::new(
            [0_u64, 5000, 5100, 5200, 5300, 306000, 306100]
                .into_iter()
                .map(|elapsed_ms| CheckTime {
                    utc: origin + chrono::Duration::milliseconds(elapsed_ms as i64),
                    elapsed_ms,
                })
                .collect(),
        ));
        let mut feed = SpotFeed::new().unwrap();
        feed.fixture_quotes = Some(Mutex::new(VecDeque::from([
            (SpotSource::Binance, None),
            (SpotSource::Coinbase, Some(100.0)),
            (SpotSource::Coinbase, Some(100.1)),
            (SpotSource::Binance, Some(100.2)),
        ])));
        let mut core = DecisionCore::phase1(BtcArbConfig {
            order_usd: 2.0,
            ..Default::default()
        });
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
            ControlledRun::new(
                ControlledMode::UnsignedPaper,
                "source-fixture".into(),
                Utc::now(),
            )
            .unwrap(),
            100.0,
        )
        .unwrap()
        .with_phase1(
            Arc::new(FixtureClock::new(origin)),
            StrategyContract::Phase1NetShares,
            0.03,
            30.0,
        )
        .unwrap();
        let book = BookSnapshot {
            bids: vec![BookLevel {
                price: 0.4,
                size: 100.0,
            }],
            asks: vec![BookLevel {
                price: 0.45,
                size: 100.0,
            }],
        };
        paper::clear_recorded_events();
        for index in 0..3 {
            if index == 2 {
                feed.binance_refused_at = Some(Instant::now() - BINANCE_RETRY_AFTER);
            }
            let quote = feed.quote_observed("BTCUSDT", &clock).await.unwrap();
            let now = CheckTime {
                utc: quote.clock.received_at,
                elapsed_ms: quote.clock.received_elapsed_ms,
            };
            core.observe_phase1(
                "BTCUSDT",
                quote.price,
                quote.source,
                &quote.actual_pair,
                &quote.clock,
                now,
            )
            .unwrap();
            let market = TradableMarket {
                id: index + 1,
                open: true,
                contested: false,
                tick: 0.01,
                fee_rate_bps: 200,
                feed_symbol: Some("BTCUSDT".into()),
                yes_token: "1001".into(),
                no_token: "1002".into(),
                start_price: Some(100.0),
                starts_at: Some(now.utc - chrono::Duration::seconds(1)),
                ends_at: Some(now.utc + chrono::Duration::seconds(299)),
                round_facts: Some(RoundFacts {
                    price_feed_provider: Some("CHAINLINK".into()),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let evaluation = core.evaluate_phase1(&EvaluationInputs {
                market: &market,
                book: &book,
                spot_price: quote.price,
                spot_source: Some(quote.source),
                actual_pair: Some(quote.actual_pair.clone()),
                spot_clock: Some(quote.clock.clone()),
                book_clock: Some(quote.clock.clone()),
                now,
                order_usd: 2.0,
                min_edge: 0.03,
                min_secs_left: 30.0,
                entered: false,
                identity_available_at: market.starts_at,
                strike_available_at: market.starts_at,
                settlement_provider: SettlementProvider::Chainlink,
            });
            assert!(evaluation.candidate.is_none());
            assert_eq!(evaluation.diagnostic.reason, Reason::Warmup);
            assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, None));
            let event = paper::recorded_events().pop().unwrap();
            let decision = &event["source"]["decision"];
            assert_eq!(decision["settlement_provider"], "CHAINLINK");
            assert_eq!(
                decision["prediction_source"],
                if index == 2 { "Binance" } else { "Coinbase" }
            );
            assert_eq!(
                decision["actual_pair"],
                if index == 2 { "BTCUSDT" } else { "BTC-USD" }
            );
            assert_eq!(
                decision["source_state"],
                if index == 2 { "PRIMARY" } else { "DEGRADED" }
            );
            assert_eq!(decision["source_epoch"], if index == 2 { 2 } else { 1 });
            assert_eq!(decision["samples"], if index == 1 { 1 } else { 0 });
            assert_eq!(
                decision["reset_reason"],
                if index == 2 {
                    "SOURCE_SWITCH"
                } else {
                    "FIRST_SAMPLE"
                }
            );
            assert!(decision["spot_clock"]["source_at"].is_null());
            assert_eq!(decision["spot_clock"]["freshness_basis"], "RECEIPT_BOUND");
            if index == 0 {
                assert_eq!(quote.clock.request_elapsed_ms, 5000);
                assert_eq!(quote.clock.received_elapsed_ms, 5100);
            }
            assert_eq!(event["accounting"]["cash"], 100.0);
            assert_eq!(event["accounting"]["reserved"], 0.0);
        }
        assert!(feed
            .fixture_quotes
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .is_empty());
        assert!(clock.0.lock().unwrap().is_empty());
        assert!(broker.holdings().is_empty());
        paper::clear_recorded_events();
    }

    #[test]
    fn normal_cdf_matches_known_values() {
        assert!((normal_cdf(0.0) - 0.5).abs() < 1e-6);
        assert!((normal_cdf(1.96) - 0.975).abs() < 1e-3);
        assert!((normal_cdf(-1.96) - 0.025).abs() < 1e-3);
    }

    #[test]
    fn at_the_money_is_a_coin_flip_and_moves_with_spot() {
        let flat = digital_up(100.0, 100.0, 0.0001, 300.0);
        assert!((flat - 0.5).abs() < 0.001);
        assert!(digital_up(100.1, 100.0, 0.0001, 300.0) > 0.7);
        assert!(digital_up(99.9, 100.0, 0.0001, 300.0) < 0.3);
    }

    #[test]
    fn value_converges_as_the_round_ends() {
        let far = digital_up(100.05, 100.0, 0.0001, 600.0);
        let near = digital_up(100.05, 100.0, 0.0001, 10.0);
        assert!(near > far);
        assert_eq!(digital_up(100.05, 100.0, 0.0001, 0.0), 1.0);
        assert_eq!(digital_up(100.0, 100.0, 0.0001, 0.0), 0.5);
    }

    #[test]
    fn ewma_tracks_a_known_volatility_and_respects_the_floor() {
        let mut vol = EwmaVol::new(60.0);
        assert_eq!(vol.vol(0.00005), 0.00005);
        // A steady 0.1% move every 10 seconds is 0.001 / sqrt(10) per root second.
        let mut price = 100.0;
        for i in 0..200 {
            price *= if i % 2 == 0 { 1.001 } else { 1.0 / 1.001 };
            vol.update(price, f64::from(i) * 10.0);
        }
        let expected = 0.001 / 10f64.sqrt();
        assert!((vol.vol(0.0) - expected).abs() < expected * 0.02, "{}", vol.vol(0.0));
        assert_eq!(vol.vol(0.01), 0.01);
    }

    #[test]
    fn coinbase_pairs_map_from_feed_symbols() {
        assert_eq!(coinbase_pair("BTCUSDT").unwrap(), "BTC-USD");
        assert_eq!(coinbase_pair("BTC_USD").unwrap(), "BTC-USD");
        assert!(coinbase_pair("nonsense").is_err());
    }
}
