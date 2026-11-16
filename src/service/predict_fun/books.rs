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

/// Fetch the books for `ids`, without a clock.
///
/// This is the ordinary path: it keeps the old shape, and nothing downstream
/// can claim to know when the answer arrived.
pub(super) async fn plain(
    client: &PredictFunClient,
    ids: &[u64],
) -> Result<HashMap<u64, BookSnapshot>> {
    let mut out = HashMap::new();
    for batch in ids.chunks(BOOK_BATCH_SIZE) {
        for book in fetch_batch(client, batch).await? {
            out.insert(book.market_id, to_snapshot(&book.asks, &book.bids));
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
    let mut out = HashMap::new();
    for batch in ids.chunks(BOOK_BATCH_SIZE) {
        let request_at = clock.now()?;
        let books = fetch_batch(client, batch).await?;
        let received_at = clock.now()?;
        let observation = observation_clock(request_at, received_at);
        for book in books {
            out.insert(
                book.market_id,
                ObservedBook {
                    book: to_snapshot(&book.asks, &book.bids),
                    clock: observation.clone(),
                },
            );
        }
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
