//! The batched order-book fetch, with the clock that received each batch.
//!
//! This module owns the loop that asks Predict.fun for order books in batches
//! and turns the answers into the model's own book type. It is an extraction of
//! the existing fetch, not a second one: the plain path and the observed path
//! share the same requests, the same mapping and the same batch size.

use super::{to_snapshot, ApiMarketBook, PredictFunClient, BOOK_BATCH_SIZE};
use crate::models::BookSnapshot;
use crate::service::observation::{CheckTime, Clock, FreshnessBasis, ObservationClock, ObservedBook, SourceTimeStatus};
use anyhow::Result;
use std::collections::HashMap;

// A test hook that runs between the mapping of a batch and its receipt.
//
// It lets a test move the clock while the answer is being converted, so the
// receipt can be shown to be taken after the conversion and not before.
#[cfg(test)]
thread_local! {
    static MAPPED_HOOK: std::cell::RefCell<Option<Box<dyn Fn() + Send + Sync>>> =
        const { std::cell::RefCell::new(None) };
}

/// Where a batch of books comes from.
///
/// The production source is the venue client. A test may answer the same
/// batches without a socket, but it goes through the same loop, the same
/// mapping and the same clock sampling: only the request itself is replaced.
#[async_trait::async_trait]
trait BatchSource: Send + Sync {
    async fn fetch(&self, batch: &[u64]) -> Result<Vec<ApiMarketBook>>;
}

/// The venue itself.
struct VenueSource<'a> {
    client: &'a PredictFunClient,
}

#[async_trait::async_trait]
impl BatchSource for VenueSource<'_> {
    async fn fetch(&self, batch: &[u64]) -> Result<Vec<ApiMarketBook>> {
        fetch_batch(self.client, batch).await
    }
}

/// One fetched batch, already mapped to the model's own book type.
struct FetchedBatch {
    books: Vec<(u64, BookSnapshot)>,
    /// The clock of this batch, when the caller asked for one.
    clock: Option<ObservationClock>,
}

/// Fetch the books for `ids`, without a clock.
///
/// This is the ordinary path: it keeps the old shape, and nothing downstream
/// can claim to know when the answer arrived.
pub(super) async fn plain(
    client: &PredictFunClient,
    ids: &[u64],
) -> Result<HashMap<u64, BookSnapshot>> {
    let source = VenueSource { client };
    let mut out = HashMap::new();
    for batch in collect(&source, ids, None).await? {
        for (market_id, book) in batch.books {
            out.insert(market_id, book);
        }
    }
    Ok(out)
}

/// Fetch the books for `ids`, with the clock that received each batch.
///
/// Every market of a batch carries that batch's own clock: the first batch is
/// not stamped with the time the last batch finished.
pub(super) async fn observed(
    client: &PredictFunClient,
    ids: &[u64],
    clock: &dyn Clock,
) -> Result<HashMap<u64, ObservedBook>> {
    let source = VenueSource { client };
    let mut out = HashMap::new();
    for batch in collect(&source, ids, Some(clock)).await? {
        let observation = batch.clock.expect("the observed path always has a clock");
        for (market_id, book) in batch.books {
            out.insert(
                market_id,
                ObservedBook {
                    book,
                    clock: observation.clone(),
                },
            );
        }
    }
    Ok(out)
}

/// Fetch every batch of `ids` in turn.
///
/// The clock of a batch is taken around its own request: the request time
/// before the request leaves, the receipt time after the answer was decoded
/// and mapped. A batch that fails ends the whole fetch: there is no such thing
/// as a partly fresh book.
async fn collect(
    source: &dyn BatchSource,
    ids: &[u64],
    clock: Option<&dyn Clock>,
) -> Result<Vec<FetchedBatch>> {
    let mut out = Vec::new();
    for batch in ids.chunks(BOOK_BATCH_SIZE) {
        let request_at = match clock {
            Some(clock) => Some(clock.now()?),
            None => None,
        };
        let raw = source.fetch(batch).await?;
        // The answer is mapped before the receipt is taken: the receipt says
        // when the response was decoded and checked, not when it arrived.
        let books: Vec<(u64, BookSnapshot)> = raw
            .into_iter()
            .map(|book| (book.market_id, to_snapshot(&book.asks, &book.bids)))
            .collect();
        // A test may move the clock while the answer was converted: the
        // receipt below is taken after that, never before.
        #[cfg(test)]
        MAPPED_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow().as_ref() {
                hook();
            }
        });
        let observation = match (clock, request_at) {
            (Some(clock), Some(request_at)) => Some(observation_clock(request_at, clock.now()?)),
            _ => None,
        };
        out.push(FetchedBatch {
            books,
            clock: observation,
        });
    }
    Ok(out)
}

/// One batch of order books, as the API returns them.
async fn fetch_batch(client: &PredictFunClient, batch: &[u64]) -> Result<Vec<ApiMarketBook>> {
    let list = batch
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    Ok(client
        .get::<Vec<ApiMarketBook>>(&format!("/markets/orderbooks?ids={list}"))
        .await?
        .data)
}

/// The clock of one received batch.
///
/// The books carry no source event time, so the freshness can only be proven
/// from the local receipt.
fn observation_clock(request_at: CheckTime, received_at: CheckTime) -> ObservationClock {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::observation::Clock;
    use crate::service::predict_fun::account::SyntheticTransport;
    use chrono::{DateTime, Utc};
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn start() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp_millis(1_762_636_800_000).unwrap()
    }

    /// A clock that answers with a fixed list of readings.
    ///
    /// The list is walked once per call; the last reading repeats.
    struct ScriptedClock {
        readings: Mutex<Vec<CheckTime>>,
        calls: AtomicUsize,
    }

    impl ScriptedClock {
        fn new(readings: Vec<CheckTime>) -> Self {
            Self {
                readings: Mutex::new(readings),
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl Clock for ScriptedClock {
        fn now(&self) -> Result<CheckTime> {
            let index = self.calls.fetch_add(1, Ordering::SeqCst);
            let readings = self.readings.lock();
            Ok(*readings.get(index).unwrap_or_else(|| readings.last().unwrap()))
        }
    }

    /// A clock that moves when the test says so.
    ///
    /// Both readings move together, so a test can show that the receipt of a
    /// batch was taken after its answer was converted.
    struct MovableClock {
        base: CheckTime,
        offset_ms: AtomicUsize,
    }

    impl MovableClock {
        fn new(base: CheckTime) -> Self {
            Self {
                base,
                offset_ms: AtomicUsize::new(0),
            }
        }

        fn advance(&self, millis: usize) {
            self.offset_ms.fetch_add(millis, Ordering::SeqCst);
        }
    }

    impl Clock for MovableClock {
        fn now(&self) -> Result<CheckTime> {
            let offset = self.offset_ms.load(Ordering::SeqCst) as i64;
            Ok(CheckTime {
                utc: self.base.utc + chrono::Duration::milliseconds(offset),
                elapsed_ms: self.base.elapsed_ms + offset as u64,
            })
        }
    }

    /// A clock that cannot be read.
    struct BrokenClock;

    impl Clock for BrokenClock {
        fn now(&self) -> Result<CheckTime> {
            anyhow::bail!("SYNTHETIC_CLOCK_FAILURE")
        }
    }

    /// The venue's own REST call, answered without a socket.
    ///
    /// It answers the real order-book path with the real wire shape, so the
    /// client decodes it exactly as it decodes the venue's answer.
    struct FakeVenue {
        paths: Mutex<Vec<String>>,
        calls: AtomicUsize,
        /// The batch call that fails, counted from one.
        fail_at: Option<usize>,
        /// The batch call whose answer cannot be decoded.
        broken_at: Option<usize>,
    }

    impl FakeVenue {
        fn new(fail_at: Option<usize>, broken_at: Option<usize>) -> Self {
            Self {
                paths: Mutex::new(Vec::new()),
                calls: AtomicUsize::new(0),
                fail_at,
                broken_at,
            }
        }
    }

    impl SyntheticTransport for FakeVenue {
        fn rest(
            &self,
            _method: &reqwest::Method,
            path: &str,
            _bearer: Option<&str>,
            _body: Option<&serde_json::Value>,
        ) -> anyhow::Result<String> {
            self.paths.lock().push(path.to_string());
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_at == Some(call) {
                anyhow::bail!("SYNTHETIC_REQUEST_FAILURE")
            }
            if self.broken_at == Some(call) {
                return Ok("{not json".to_string());
            }
            let ids = path
                .rsplit_once("ids=")
                .map(|(_, ids)| ids.to_string())
                .unwrap_or_default();
            let data: Vec<serde_json::Value> = ids
                .split(',')
                .filter(|id| !id.is_empty())
                .map(|id| {
                    let market_id: u64 = id.parse().expect("a numeric market id");
                    serde_json::json!({
                        "marketId": market_id,
                        "asks": [[0.45, 100.0]],
                        "bids": [[0.40, 100.0]],
                    })
                })
                .collect();
            Ok(serde_json::json!({"data": data}).to_string())
        }
    }

    /// The client the venue's own answers travel through.
    fn fake_client(venue: Arc<FakeVenue>) -> Arc<PredictFunClient> {
        let mut cfg: crate::config::AppConfig = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tools/predictfun/btc5m-controlled-paper.json"
        )))
        .unwrap();
        cfg.credentials.predict_api_key = Some("synthetic-market-only".into());
        let mut client = PredictFunClient::new(&cfg).unwrap();
        Arc::get_mut(&mut client)
            .unwrap()
            .synthetic = Some(venue as Arc<dyn SyntheticTransport>);
        client
    }

    /// A reading `millis` after the start.
    fn at(millis: u64) -> CheckTime {
        CheckTime {
            utc: start() + chrono::Duration::milliseconds(millis as i64),
            elapsed_ms: millis,
        }
    }

    /// Every batch carries its own clock, and the first batch is not stamped
    /// with the time the last batch finished.
    #[tokio::test]
    async fn every_batch_carries_its_own_clock() {
        let venue = Arc::new(FakeVenue::new(None, None));
        let client = fake_client(Arc::clone(&venue));
        let ids: Vec<u64> = (1..=BOOK_BATCH_SIZE as u64 + 1).collect();
        let clock = ScriptedClock::new(vec![at(0), at(100), at(1_000), at(1_100)]);
        let books = observed(&client, &ids, &clock).await.unwrap();
        assert_eq!(books.len(), ids.len());
        // Two requests, one per batch, in the original order.
        let paths = venue.paths.lock().clone();
        assert_eq!(paths.len(), 2);
        assert!(paths[0].starts_with("/markets/orderbooks?ids=1,"));
        assert!(paths[1].ends_with(&ids[BOOK_BATCH_SIZE].to_string()));
        // The first batch keeps the first batch's receipt.
        let first = &books[&ids[0]];
        let last = &books[&ids[ids.len() - 1]];
        assert_eq!(first.clock.request_at, start());
        assert_eq!(
            first.clock.received_at,
            start() + chrono::Duration::milliseconds(100)
        );
        assert_eq!(last.clock.request_at, start() + chrono::Duration::seconds(1));
        assert_eq!(
            last.clock.received_at,
            start() + chrono::Duration::milliseconds(1_100)
        );
        assert_ne!(first.clock.received_at, last.clock.received_at);
        // The venue carries no source event time, so none is invented.
        assert!(first.clock.source_at.is_none());
        assert_eq!(first.clock.source_time_status, SourceTimeStatus::Missing);
        assert_eq!(first.clock.freshness_basis, FreshnessBasis::ReceiptBound);
    }

    /// The receipt of a batch is taken after its answer was converted.
    ///
    /// The clock moves while the answer is being mapped, so a receipt that was
    /// taken before the conversion would show the earlier time.
    #[tokio::test]
    async fn the_receipt_is_taken_after_the_answer_was_converted() {
        let venue = Arc::new(FakeVenue::new(None, None));
        let client = fake_client(Arc::clone(&venue));
        let clock = Arc::new(MovableClock::new(at(0)));
        let mover = Arc::clone(&clock);
        MAPPED_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || mover.advance(750)));
        });
        let books = observed(&client, &[1], clock.as_ref()).await.unwrap();
        MAPPED_HOOK.with(|hook| {
            *hook.borrow_mut() = None;
        });
        let book = &books[&1];
        assert_eq!(book.clock.request_at, start());
        assert_eq!(
            book.clock.received_at,
            start() + chrono::Duration::milliseconds(750),
            "the receipt includes the conversion"
        );
        assert_eq!(book.clock.received_elapsed_ms, 750);
        assert_eq!(book.clock.request_span_ms(), 750);
    }

    /// A failed request returns no books at all: a partly fresh book is not a
    /// book.
    #[tokio::test]
    async fn a_failed_request_returns_no_books() {
        let venue = Arc::new(FakeVenue::new(Some(2), None));
        let client = fake_client(Arc::clone(&venue));
        let ids: Vec<u64> = (1..=BOOK_BATCH_SIZE as u64 + 1).collect();
        let clock = ScriptedClock::new(vec![at(0), at(100), at(1_000), at(1_100)]);
        let error = observed(&client, &ids, &clock).await.unwrap_err();
        assert!(error.to_string().contains("SYNTHETIC_REQUEST_FAILURE"));
        assert_eq!(venue.calls.load(Ordering::SeqCst), 2);
    }

    /// An answer that cannot be decoded is a failure, not an empty book.
    #[tokio::test]
    async fn an_undecodable_answer_returns_no_books() {
        let venue = Arc::new(FakeVenue::new(None, Some(1)));
        let client = fake_client(Arc::clone(&venue));
        let clock = ScriptedClock::new(vec![at(0), at(100)]);
        let error = observed(&client, &[1], &clock).await.unwrap_err();
        assert!(error.to_string().contains("parsing"));
    }

    /// A clock that cannot be read is a refusal, never a missing timestamp.
    #[tokio::test]
    async fn an_unreadable_clock_refuses_the_fetch() {
        let venue = Arc::new(FakeVenue::new(None, None));
        let client = fake_client(Arc::clone(&venue));
        let error = observed(&client, &[1], &BrokenClock).await.unwrap_err();
        assert!(error.to_string().contains("SYNTHETIC_CLOCK_FAILURE"));
        assert_eq!(venue.calls.load(Ordering::SeqCst), 0);
    }

    /// The plain path keeps its own shape: the same mapping, and no clock.
    #[tokio::test]
    async fn the_plain_path_keeps_its_own_mapping() {
        let venue = Arc::new(FakeVenue::new(None, None));
        let client = fake_client(Arc::clone(&venue));
        let plain_books = plain(&client, &[1, 2]).await.unwrap();
        let clock = ScriptedClock::new(vec![at(0), at(100)]);
        let observed_books = observed(&client, &[1, 2], &clock).await.unwrap();
        assert_eq!(plain_books.len(), 2);
        for (market_id, book) in &plain_books {
            let observed = &observed_books[market_id].book;
            assert_eq!(observed.asks.len(), book.asks.len());
            assert_eq!(observed.bids.len(), book.bids.len());
            for (level, other) in observed.asks.iter().zip(&book.asks) {
                assert_eq!((level.price, level.size), (other.price, other.size));
            }
            for (level, other) in observed.bids.iter().zip(&book.bids) {
                assert_eq!((level.price, level.size), (other.price, other.size));
            }
        }
        assert_eq!(plain_books[&1].asks[0].price, 0.45);
        assert_eq!(plain_books[&1].bids[0].price, 0.40);
    }
}
