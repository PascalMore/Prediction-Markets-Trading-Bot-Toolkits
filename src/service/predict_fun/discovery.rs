//! Recurring-series routing only; round facts remain the trading authority.

use super::{tradable, ApiEvent, ApiMarket, ApiSeries, PredictFunClient};
use crate::service::venue::{MarketData, MarketQuery, TradableMarket};
use anyhow::{bail, Result};
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tracing::debug;

const DIRECTORY_PATH: &str = "/series?marketVariant=CRYPTO_UP_DOWN";
const DIRECTORY_TTL: Duration = Duration::from_secs(300);
const DIRECTORY_MAX_PAGES: usize = 5;
const DIRECTORY_MAX_SERIES: usize = 1024;

/// One complete directory, scoped to the owning client and fixed network.
/// Empty directories are cached too; errors never refresh or serve stale data.
pub(super) struct SeriesDirectory {
    fetched_at: Instant,
    slugs: Vec<String>,
}

/// Slugs locate routes, not tradable identities. Only the documented up/down
/// spelling is mapped; returned rounds still pass all existing fact checks.
fn matches_scope(slug: &str, symbols: &[String], interval_secs: Option<u32>) -> bool {
    if symbols.is_empty() && interval_secs.is_none() {
        return true;
    }
    let Some((asset, cadence)) = slug.split_once("-updown-") else {
        return false;
    };
    if asset.is_empty()
        || !asset
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        return false;
    }
    let symbol = format!("{}USDT", asset.to_ascii_uppercase());
    if !symbols.is_empty() && !symbols.contains(&symbol) {
        return false;
    }
    let Some((unit, count)) = cadence
        .chars()
        .last()
        .map(|unit| (unit, &cadence[..cadence.len() - unit.len_utf8()]))
    else {
        return false;
    };
    let multiplier = match unit {
        'm' => 60,
        'h' => 3600,
        'd' => 86400,
        _ => return false,
    };
    let seconds = count
        .parse::<u32>()
        .ok()
        .and_then(|count| count.checked_mul(multiplier));
    seconds
        .is_some_and(|seconds| seconds > 0 && interval_secs.is_none_or(|wanted| wanted == seconds))
}

impl PredictFunClient {
    async fn series_directory(&self) -> Result<Vec<String>> {
        // Serialize only directory refreshes, never rounds or other market data.
        let mut cache = self.series_directory.lock().await;
        if let Some(hit) = cache
            .as_ref()
            .filter(|hit| hit.fetched_at.elapsed() < DIRECTORY_TTL)
        {
            return Ok(hit.slugs.clone());
        }
        let mut path = DIRECTORY_PATH.to_string();
        let mut cursors = HashSet::new();
        let mut seen = HashSet::new();
        let mut slugs = Vec::new();
        for page in 0..DIRECTORY_MAX_PAGES {
            let response = self.get::<Vec<ApiSeries>>(&path).await?;
            for series in response.data {
                if seen.insert(series.slug.clone()) {
                    slugs.push(series.slug);
                }
                if slugs.len() > DIRECTORY_MAX_SERIES {
                    bail!("series directory exceeds bounded capacity");
                }
            }
            let Some(cursor) = response.cursor.filter(|cursor| !cursor.is_empty()) else {
                *cache = Some(SeriesDirectory {
                    fetched_at: Instant::now(),
                    slugs: slugs.clone(),
                });
                return Ok(slugs);
            };
            if page + 1 == DIRECTORY_MAX_PAGES || !cursors.insert(cursor.clone()) {
                bail!("incomplete series directory pagination");
            }
            let mut url = reqwest::Url::parse(
                "https://directory.invalid/series?marketVariant=CRYPTO_UP_DOWN",
            )?;
            url.query_pairs_mut().append_pair("after", &cursor);
            path = format!("{}?{}", url.path(), url.query().unwrap());
        }
        unreachable!("bounded directory loop returns or errors")
    }

    /// Up/down rounds for configured symbols and optional cadence. Empty symbols
    /// mean unrestricted routing; no scope match returns empty, never a full scan.
    /// Only the directory is cached: every call re-reads target round bodies.
    pub async fn live_rounds_for(
        &self,
        symbols: &[String],
        interval_secs: Option<u32>,
    ) -> Result<Vec<TradableMarket>> {
        let series = self.series_directory().await?;
        if series.is_empty() {
            // Preserve the existing no-series category fallback (not a routing
            // failure fallback); the strategy's original post-filter still runs.
            return self
                .markets(&MarketQuery {
                    variants: vec!["CRYPTO_UP_DOWN".into()],
                    max_pages: 5,
                })
                .await;
        }
        let now = chrono::Utc::now();
        let mut out = Vec::new();
        for slug in series
            .into_iter()
            .filter(|slug| matches_scope(slug, symbols, interval_secs))
        {
            let rounds = self
                .get::<Vec<ApiEvent>>(&format!("/series/{slug}/rounds?first=2"))
                .await?
                .data;
            for round in &rounds {
                for raw in &round.markets {
                    match serde_json::from_value::<ApiMarket>(raw.clone()) {
                        Ok(market) => {
                            self.remember_market(&market);
                            out.push(tradable(&market, Some(round), now));
                        }
                        Err(error) => debug!(%error, "skipping unreadable market"),
                    }
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::account::SyntheticTransport;
    use super::super::*;
    use parking_lot::Mutex;

    pub(crate) const DIRECTORY: &str = "/series?marketVariant=CRYPTO_UP_DOWN";
    pub(crate) const BTC: &str = "/series/btc-updown-5m/rounds?first=2";

    pub(crate) struct DirectoryVenue {
        pub(crate) calls: Mutex<Vec<String>>,
        series: Mutex<Value>,
        fault: Mutex<Option<&'static str>>,
        pub(crate) responses: Mutex<HashMap<String, Value>>,
    }

    impl SyntheticTransport for DirectoryVenue {
        fn rest(
            &self,
            method: &Method,
            path: &str,
            bearer: Option<&str>,
            body: Option<&Value>,
        ) -> Result<String> {
            assert_eq!(*method, Method::GET);
            assert!(bearer.is_none() && body.is_none());
            self.calls.lock().push(path.into());
            if *self.fault.lock() == Some(path) {
                bail!("DIRECTORY_FIXTURE_FAILURE");
            }
            if let Some(response) = self.responses.lock().get(path) {
                return Ok(response.to_string());
            }
            if path == DIRECTORY {
                return Ok(json!({"data": self.series.lock().clone()}).to_string());
            }
            if path == "/categories?first=100&status=OPEN&marketVariants=CRYPTO_UP_DOWN" {
                return Ok(json!({"data": []}).to_string());
            }
            assert!(
                path.starts_with("/series/") && path.ends_with("/rounds?first=2"),
                "undeclared route: {path}"
            );
            Ok(json!({"data": []}).to_string())
        }
    }

    pub(crate) fn fixture() -> (Arc<PredictFunClient>, Arc<DirectoryVenue>) {
        let venue = Arc::new(DirectoryVenue {
            calls: Mutex::new(vec![]),
            series: Mutex::new(json!([
                {"slug":"eth-updown-5m"}, {"slug":"btc-updown-15m"},
                {"slug":"btc-updown-5m"}, {"slug":"bnb-updown-1h"},
                {"slug":"not-btc-updown-5m"}
            ])),
            fault: Mutex::new(None),
            responses: Mutex::new(HashMap::new()),
        });
        let mut cfg: AppConfig = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tools/predictfun/btc5m-controlled-paper.json"
        )))
        .unwrap();
        cfg.credentials.predict_api_key = Some("synthetic-market-only".into());
        let mut client = PredictFunClient::new(&cfg).unwrap();
        Arc::get_mut(&mut client).unwrap().synthetic = Some(venue.clone());
        (client, venue)
    }

    /// Wire DTO shared with the original RoundTaker consumer regression.
    pub(crate) fn round_response(symbol: &str, seconds: i64, id: u64) -> Value {
        let start = Utc::now() - chrono::Duration::seconds(1);
        json!({"data": [{
            "startsAt": start, "endsAt": start + chrono::Duration::seconds(seconds),
            "variantData": {"startPrice": 100.0, "priceFeedSymbol": symbol, "priceFeedProvider": "CHAINLINK"},
            "markets": [{"id": id, "tradingStatus": "OPEN", "status":"OPEN",
                "marketVariant":"CRYPTO_UP_DOWN", "isNegRisk":false, "isYieldBearing":false,
                "feeRateBps":0, "decimalPrecision":2,
                "outcomes":[{"onChainId":format!("{}1", id),"name":"Up"}, {"onChainId":format!("{}2", id),"name":"Down"}]}]
        }]})
    }

    #[tokio::test]
    async fn discovery_expiry_refresh_errors_and_recovery_never_serve_stale() {
        let (client, venue) = fixture();
        client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .unwrap();
        client
            .series_directory
            .lock()
            .await
            .as_mut()
            .unwrap()
            .fetched_at = std::time::Instant::now() - super::DIRECTORY_TTL;
        *venue.fault.lock() = Some(DIRECTORY);
        assert!(client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .is_err());
        assert_eq!(*venue.calls.lock(), vec![DIRECTORY, BTC, DIRECTORY]);
        *venue.fault.lock() = None;
        *venue.series.lock() = json!([{"slug":"eth-updown-5m"}]);
        assert!(client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            *venue.calls.lock(),
            vec![DIRECTORY, BTC, DIRECTORY, DIRECTORY]
        );
        client
            .live_rounds_for(&["ETHUSDT".into()], Some(300))
            .await
            .unwrap();
        assert_eq!(
            venue.calls.lock().last().unwrap(),
            "/series/eth-updown-5m/rounds?first=2"
        );
    }

    #[tokio::test]
    async fn discovery_cold_error_schema_error_and_round_error_are_not_full_scans() {
        let (client, venue) = fixture();
        *venue.fault.lock() = Some(DIRECTORY);
        assert!(client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .is_err());
        assert!(client.series_directory.lock().await.is_none());
        *venue.fault.lock() = None;
        venue
            .responses
            .lock()
            .insert(DIRECTORY.into(), json!({"data":[{"notSlug":"broken"}]}));
        assert!(client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .is_err());
        assert!(client.series_directory.lock().await.is_none());
        venue.responses.lock().clear();
        *venue.fault.lock() = Some(BTC);
        assert!(client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .is_err());
        assert_eq!(
            *venue.calls.lock(),
            vec![DIRECTORY, DIRECTORY, DIRECTORY, BTC]
        );
        *venue.fault.lock() = None;
        client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .unwrap();
        assert_eq!(venue.calls.lock().len(), 5);
    }

    #[tokio::test]
    async fn discovery_empty_directory_preserves_fresh_category_fallback() {
        let (client, venue) = fixture();
        *venue.series.lock() = json!([]);
        let categories = "/categories?first=100&status=OPEN&marketVariants=CRYPTO_UP_DOWN";
        venue
            .responses
            .lock()
            .insert(categories.into(), round_response("BTCUSDT", 300, 1));
        assert_eq!(
            client
                .live_rounds_for(&["BTCUSDT".into()], Some(300))
                .await
                .unwrap()[0]
                .id,
            1
        );
        venue
            .responses
            .lock()
            .insert(categories.into(), round_response("BTCUSDT", 300, 2));
        assert_eq!(client.live_rounds().await.unwrap()[0].id, 2);
        assert_eq!(*venue.calls.lock(), vec![DIRECTORY, categories, categories]);
    }

    #[tokio::test]
    async fn discovery_complete_pagination_deduplicates_and_encodes_cursor() {
        let (client, venue) = fixture();
        let page_two = "/series?marketVariant=CRYPTO_UP_DOWN&after=next%2F%2B%26";
        venue.responses.lock().insert(
            DIRECTORY.into(),
            json!({"data":[{"slug":"eth-updown-5m"}],"cursor":"next/+&"}),
        );
        venue.responses.lock().insert(
            page_two.into(),
            json!({"data":[{"slug":"btc-updown-5m"},{"slug":"btc-updown-5m"}]}),
        );
        client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .unwrap();
        client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .unwrap();
        assert_eq!(*venue.calls.lock(), vec![DIRECTORY, page_two, BTC, BTC]);
        println!(
            "DISCOVERY_PAGINATION pages=2 unique_series=2 targets=1 cold=3 hot=1 routes={:?}",
            venue.calls.lock()
        );
    }

    #[tokio::test]
    async fn discovery_incomplete_pagination_is_not_cached() {
        let (client, venue) = fixture();
        venue
            .responses
            .lock()
            .insert(DIRECTORY.into(), json!({"data":[],"cursor":"repeat"}));
        venue.responses.lock().insert(
            "/series?marketVariant=CRYPTO_UP_DOWN&after=repeat".into(),
            json!({"data":[],"cursor":"repeat"}),
        );
        assert!(client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .is_err());
        assert!(client.series_directory.lock().await.is_none());
        assert_eq!(venue.calls.lock().len(), 2);
    }

    #[tokio::test]
    async fn discovery_directory_is_client_scoped_and_rounds_are_always_fresh() {
        let (client, venue) = fixture();
        venue
            .responses
            .lock()
            .insert(BTC.into(), round_response("BTCUSDT", 300, 1));
        let first = client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .unwrap();
        venue
            .responses
            .lock()
            .insert(BTC.into(), round_response("ETHUSDT", 900, 2));
        let second = client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .unwrap();
        assert_eq!(first[0].id, 1);
        assert_eq!(second[0].id, 2);
        assert_eq!(second[0].feed_symbol.as_deref(), Some("ETHUSDT"));
        assert_eq!(*venue.calls.lock(), vec![DIRECTORY, BTC, BTC]);
        let (other_client, other_venue) = fixture();
        other_client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .unwrap();
        assert_eq!(*other_venue.calls.lock(), vec![DIRECTORY, BTC]);
    }

    #[tokio::test]
    async fn discovery_concurrent_scans_share_directory_not_rounds() {
        let (client, venue) = fixture();
        let symbols = vec!["BTCUSDT".into()];
        let (first, second) = tokio::join!(
            client.live_rounds_for(&symbols, Some(300)),
            client.live_rounds_for(&symbols, Some(300))
        );
        first.unwrap();
        second.unwrap();
        assert_eq!(*venue.calls.lock(), vec![DIRECTORY, BTC, BTC]);
    }

    #[test]
    fn discovery_slug_routes_do_not_guess_other_spellings() {
        for slug in [
            "not-btc-updown-5m",
            "btc-updown-0m",
            "btc-updown-4294967295d",
            "btc-updown-5m/extra",
            "btc-updown-5m?other",
            "btc-updown-💣",
        ] {
            assert!(
                !super::matches_scope(slug, &["BTCUSDT".into()], Some(300)),
                "{slug}"
            );
        }
    }

    #[tokio::test]
    async fn discovery_target_routes_cold_and_hot_vs_original() {
        let (baseline, original) = fixture();
        baseline.live_rounds().await.unwrap();
        assert_eq!(original.calls.lock().len(), 6);
        let (client, venue) = fixture();
        client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .unwrap();
        assert_eq!(*venue.calls.lock(), vec![DIRECTORY, BTC]);
        client
            .live_rounds_for(&["BTCUSDT".into()], Some(300))
            .await
            .unwrap();
        assert_eq!(*venue.calls.lock(), vec![DIRECTORY, BTC, BTC]);
        println!(
            "DISCOVERY_ROUTES original={:?} candidate={:?}",
            original.calls.lock(),
            venue.calls.lock()
        );
    }

    #[tokio::test]
    async fn discovery_configuration_and_no_target_never_scan_unrelated() {
        let (client, venue) = fixture();
        client
            .live_rounds_for(&["ETHUSDT".into()], Some(300))
            .await
            .unwrap();
        assert_eq!(
            *venue.calls.lock(),
            vec![DIRECTORY, "/series/eth-updown-5m/rounds?first=2"]
        );
        venue.calls.lock().clear();
        client
            .live_rounds_for(&["BTCUSDT".into()], Some(900))
            .await
            .unwrap();
        assert_eq!(
            *venue.calls.lock(),
            vec!["/series/btc-updown-15m/rounds?first=2"]
        );
        venue.calls.lock().clear();
        client
            .live_rounds_for(&["BTCUSDT".into()], None)
            .await
            .unwrap();
        assert_eq!(
            *venue.calls.lock(),
            vec!["/series/btc-updown-15m/rounds?first=2", BTC]
        );
        venue.calls.lock().clear();
        assert!(client
            .live_rounds_for(&["SOLUSDT".into()], Some(300))
            .await
            .unwrap()
            .is_empty());
        assert!(venue.calls.lock().is_empty());
    }
}
