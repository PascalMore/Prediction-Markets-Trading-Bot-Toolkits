//! Take-profit / stop-loss monitor.
//!
//! Periodically polls the midprice of every open position. When the P&L
//! relative to the recorded entry crosses the take-profit or stop-loss
//! threshold, the monitor synthesises an exit `PlannedOrder` and posts it
//! through the existing executor — same signing path, same risk gates.

use crate::config::TpSlConfig;
use crate::models::{OrderType, PlannedOrder, Side};
use crate::service::midprice::MidpriceSource;
use crate::service::position_store::{OpenPosition, PositionStore};
use crate::service::venue::OrderVenue;
use crate::utils;
use anyhow::Result;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    TakeProfit,
    StopLoss,
}

pub fn check_exit(pos: &OpenPosition, midprice: f64) -> Option<ExitReason> {
    let pnl = pos.pnl_pct(midprice);
    if pnl >= pos.take_profit_pct {
        Some(ExitReason::TakeProfit)
    } else if pnl <= -pos.stop_loss_pct {
        Some(ExitReason::StopLoss)
    } else {
        None
    }
}

/// Spawns the monitor task. The handle lets the caller observe shutdown
/// independently of the main bot loop.
pub fn spawn(
    cfg: TpSlConfig,
    positions: Arc<PositionStore>,
    venue: Arc<dyn OrderVenue>,
    midprice: Arc<dyn MidpriceSource>,
    live_trading: bool,
    price_buffer: f64,
    order_expiration_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if !cfg.enabled {
            info!("TP/SL monitor disabled in config — exiting task");
            return;
        }
        let mut ticker = interval(Duration::from_secs(cfg.poll_interval_secs.max(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let snapshot = positions.snapshot();
            for pos in snapshot {
                if let Err(e) = monitor_once(
                    &pos,
                    &positions,
                    venue.as_ref(),
                    midprice.as_ref(),
                    live_trading,
                    price_buffer,
                    order_expiration_secs,
                )
                .await
                {
                    error!(
                        token = %pos.token_id,
                        error = ?e,
                        "tp/sl tick failed for position"
                    );
                }
            }
        }
    })
}

async fn monitor_once(
    pos: &OpenPosition,
    positions: &PositionStore,
    venue: &dyn OrderVenue,
    midprice: &dyn MidpriceSource,
    live_trading: bool,
    price_buffer: f64,
    order_expiration_secs: u64,
) -> Result<()> {
    if live_trading && venue.id() == crate::models::VenueId::PredictFun {
        let key =
            crate::bot::runtime::live::monitor_key(&pos.token_id, &pos.opened_at.to_rfc3339());
        if let Some(handle) = positions.exit_handle(&key) {
            let record = handle.lock().clone();
            if record.submission != crate::service::venue::Submission::NotSubmitted {
                if let Some(hash) = record.identity.signed_hash {
                    let snapshot = venue.execution_snapshot(&hash).await?;
                    positions
                        .apply_exit(&key, &snapshot)
                        .map_err(anyhow::Error::msg)?;
                }
            }
            return Ok(());
        }
    }
    let mp = midprice.midprice(&pos.token_id).await?;
    let pnl = pos.pnl_pct(mp);
    debug!(
        token = %pos.token_id,
        mid = mp,
        entry = pos.entry_price,
        pnl_pct = pnl,
        "tp/sl tick"
    );

    let Some(reason) = check_exit(pos, mp) else {
        return Ok(());
    };

    info!(
        token = %pos.token_id,
        slug = %pos.slug,
        ?reason,
        pnl_pct = pnl,
        midprice = mp,
        "TP/SL triggered — submitting exit"
    );

    let exit_side = pos.side.flip();
    let limit_price = match exit_side {
        // Selling out — accept a buffer below the mid to make sure we fill.
        Side::Sell => utils::clamp_price(mp - price_buffer),
        // Buying to cover — pay up by the buffer.
        Side::Buy => utils::clamp_price(mp + price_buffer),
    };

    let planned = PlannedOrder {
        venue: venue.id(),
        token_id: pos.token_id.clone(),
        side: exit_side,
        shares: pos.shares,
        limit_price,
        usd_notional: pos.shares * limit_price,
        order_type: OrderType::Fak,
        source_trade_hash: None,
    };

    if live_trading && venue.id() == crate::models::VenueId::PredictFun {
        let key =
            crate::bot::runtime::live::monitor_key(&pos.token_id, &pos.opened_at.to_rfc3339());
        let (fresh, handle) = positions.reserve_exit(&key, &planned);
        let snapshot = if fresh {
            // The store owns the handle before the first await. Errors and task
            // cancellation cannot cause a second POST on the next tick.
            match venue
                .place_tracked(&planned, order_expiration_secs, true, handle.clone())
                .await
            {
                Ok(ticket) => ticket.execution,
                Err(error) => {
                    // The adapter releases a provably unsent gate; the monitor
                    // intent itself remains a tombstone, unlike a copy entry.
                    return Err(error);
                }
            }
        } else {
            let hash = handle.lock().identity.signed_hash.clone();
            match hash {
                Some(hash) => Some(venue.execution_snapshot(&hash).await?),
                None => None,
            }
        };
        if let Some(snapshot) = snapshot {
            positions
                .apply_exit(&key, &snapshot)
                .map_err(anyhow::Error::msg)?;
        }
        return Ok(());
    }

    if live_trading {
        match venue.place(&planned, order_expiration_secs, true).await {
            // A live venue reports its own facts. They decide what happened.
            Ok(ticket) => {
                match ticket.filled_shares {
                    // Accepted but unfilled: the shares are still held, so keep watching.
                    Some(filled) if filled <= 0.0 && ticket.order_id.is_some() => {
                        warn!(
                            order_id = ?ticket.order_id,
                            token = %pos.token_id,
                            "exit order filled nothing; will retry next tick"
                        );
                    }
                    // Part of it sold: keep watching what's left.
                    Some(filled) if filled > 0.0 && filled < pos.shares - 1e-6 => {
                        warn!(
                            order_id = ?ticket.order_id,
                            token = %pos.token_id,
                            filled,
                            held = pos.shares,
                            "exit filled in part; will retry the rest next tick"
                        );
                        positions.reduce(&pos.token_id, filled);
                    }
                    Some(_) => {
                        info!(
                            order_id = ?ticket.order_id,
                            token = %pos.token_id,
                            "exit order submitted"
                        );
                        positions.close(&pos.token_id);
                    }
                    None => {
                        positions.close(&pos.token_id);
                    }
                }
            }
            Err(e) => {
                warn!(error = ?e, token = %pos.token_id, "exit POST failed; will retry next tick");
            }
        }
    } else {
        venue.place(&planned, order_expiration_secs, false).await?;
        info!(
            token = %pos.token_id,
            shares = planned.shares,
            limit = planned.limit_price,
            "dry-run exit signed (not submitted) — closing position locally"
        );
        positions.close(&pos.token_id);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::VenueId;
    use crate::service::venue::OrderTicket;
    use async_trait::async_trait;
    use chrono::Utc;

    struct FakeVenue {
        order_id: Option<&'static str>,
        filled: Option<f64>,
    }

    #[async_trait]
    impl OrderVenue for FakeVenue {
        fn id(&self) -> VenueId {
            VenueId::Polymarket
        }
        async fn place(&self, planned: &PlannedOrder, _: u64, _: bool) -> Result<OrderTicket> {
            assert_eq!(planned.venue, VenueId::Polymarket);
            assert_eq!(planned.side, Side::Sell);
            Ok(OrderTicket {
                token_id: planned.token_id.clone(),
                maker_amount: "0".into(),
                taker_amount: "0".into(),
                order_id: self.order_id.map(String::from),
                filled_shares: self.filled,
                signed_hash: None,
                local_attempt_id: None,
                execution: None,
            })
        }
    }

    struct UnknownPredict {
        posts: std::sync::atomic::AtomicUsize,
        reads: std::sync::atomic::AtomicUsize,
        pending: bool,
        entered: tokio::sync::Notify,
    }

    #[async_trait]
    impl OrderVenue for UnknownPredict {
        fn id(&self) -> VenueId {
            VenueId::PredictFun
        }
        async fn place(&self, _: &PlannedOrder, _: u64, _: bool) -> Result<OrderTicket> {
            panic!("untracked path must not be called")
        }
        async fn place_tracked(
            &self,
            _: &PlannedOrder,
            _: u64,
            _: bool,
            handle: crate::service::venue::AttemptHandle,
        ) -> Result<OrderTicket> {
            self.posts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            {
                let mut record = handle.lock();
                record.identity.signed_hash = Some("original-hash".into());
                record.phase = crate::service::venue::AttemptPhase::MayHaveSubmitted;
                record.submission = crate::service::venue::Submission::Unknown;
            }
            self.entered.notify_one();
            if self.pending {
                std::future::pending::<()>().await;
            }
            anyhow::bail!("FAKE_POST_TIMEOUT")
        }
        async fn execution_snapshot(
            &self,
            hash: &str,
        ) -> Result<crate::service::venue::ExecutionSnapshot> {
            assert_eq!(hash, "original-hash");
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            anyhow::bail!("FAKE_READ_UNAVAILABLE")
        }
    }

    #[tokio::test]
    async fn a007_monitor_unknown_sell_no_retry() {
        use std::sync::atomic::Ordering;
        for pending in [false, true] {
            let venue = Arc::new(UnknownPredict {
                posts: Default::default(),
                reads: Default::default(),
                pending,
                entered: tokio::sync::Notify::new(),
            });
            let store = PositionStore::new();
            let position = pos(0.5, 30.0, 20.0, Side::Buy);
            store.open(position.clone());
            if pending {
                let task_store = store.clone();
                let task_venue = venue.clone();
                let task_position = position.clone();
                let task = tokio::spawn(async move {
                    monitor_once(
                        &task_position,
                        &task_store,
                        task_venue.as_ref(),
                        &FixedMid(0.8),
                        true,
                        0.005,
                        60,
                    )
                    .await
                });
                venue.entered.notified().await;
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                assert!(monitor_once(
                    &position,
                    &store,
                    venue.as_ref(),
                    &FixedMid(0.8),
                    true,
                    0.005,
                    60
                )
                .await
                .is_err());
            }
            assert!(monitor_once(
                &position,
                &store,
                venue.as_ref(),
                &FixedMid(0.8),
                true,
                0.005,
                60
            )
            .await
            .is_err());
            assert_eq!(venue.posts.load(Ordering::SeqCst), 1);
            assert_eq!(venue.reads.load(Ordering::SeqCst), 1);
            assert_eq!(
                store.get(&position.token_id).unwrap().shares,
                position.shares
            );
            let key = crate::bot::runtime::live::monitor_key(
                &position.token_id,
                &position.opened_at.to_rfc3339(),
            );
            let order = PlannedOrder {
                venue: VenueId::PredictFun,
                token_id: position.token_id.clone(),
                side: Side::Sell,
                shares: position.shares,
                limit_price: 0.8,
                usd_notional: 80.0,
                order_type: OrderType::Fak,
                source_trade_hash: None,
            };
            let (fresh, handle) = store.reserve_exit(&key, &order);
            assert!(!fresh);
            assert_eq!(
                handle.lock().identity.signed_hash.as_deref(),
                Some("original-hash")
            );
        }
    }

    #[tokio::test]
    async fn a007_monitor_real_adapter_original_hash_recovers_net_once() {
        use crate::service::predict_fun::fills::tests::scenario;
        use std::sync::atomic::Ordering;
        let (trader, transport) = scenario("sell", "receipt_error");
        let store = PositionStore::new();
        let mut position = pos(0.30, 20.0, 20.0, Side::Buy);
        position.token_id = "77".into();
        position.shares = 10.0;
        position.usd_notional = 3.0;
        store.open(position.clone());
        monitor_once(&position, &store, &trader, &FixedMid(0.4), true, 0.005, 60)
            .await
            .unwrap();
        assert_eq!(store.get("77").unwrap().shares, 10.0);
        assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
        *transport.mode.lock() = "";
        monitor_once(&position, &store, &trader, &FixedMid(0.4), true, 0.005, 60)
            .await
            .unwrap();
        assert!(store.get("77").is_none());
        // Even a stale monitor tick reuses its retained exit handle.
        monitor_once(&position, &store, &trader, &FixedMid(0.4), true, 0.005, 60)
            .await
            .unwrap();
        assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
        assert_eq!(transport.base.signs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fix_r1_monitor_prepared_failure_retains_intent() {
        use crate::service::predict_fun::fills::tests::scenario;
        use std::sync::atomic::Ordering;
        let (trader, transport) = scenario("sell", "signature_error");
        let store = PositionStore::new();
        let mut position = pos(0.30, 20.0, 20.0, Side::Buy);
        position.token_id = "77".into();
        position.shares = 10.0;
        store.open(position.clone());
        assert!(
            monitor_once(&position, &store, &trader, &FixedMid(0.4), true, 0.005, 60)
                .await
                .is_err()
        );
        let signs = transport.base.signs.load(Ordering::SeqCst);
        *transport.mode.lock() = "";
        monitor_once(&position, &store, &trader, &FixedMid(0.4), true, 0.005, 60)
            .await
            .unwrap();
        assert_eq!(transport.base.signs.load(Ordering::SeqCst), signs);
        assert_eq!(transport.base.posts.load(Ordering::SeqCst), 0);
        assert_eq!(store.get("77").unwrap().shares, 10.0);
    }

    struct FailedMid;

    #[async_trait]
    impl MidpriceSource for FailedMid {
        async fn midprice(&self, _: &str) -> Result<f64> {
            anyhow::bail!("SYNTHETIC_PRICE_UNAVAILABLE")
        }
    }

    #[tokio::test]
    async fn fix_r1_monitor_queries_original_without_new_price_trigger() {
        use crate::service::predict_fun::fills::tests::scenario;
        use std::sync::atomic::Ordering;
        for mid in [&FixedMid(0.30) as &dyn MidpriceSource, &FailedMid] {
            let (trader, transport) = scenario("sell", "receipt_error");
            let store = PositionStore::new();
            let mut position = pos(0.30, 20.0, 20.0, Side::Buy);
            position.token_id = "77".into();
            position.shares = 10.0;
            position.usd_notional = 3.0;
            store.open(position.clone());
            monitor_once(&position, &store, &trader, &FixedMid(0.4), true, 0.005, 60)
                .await
                .unwrap();
            assert_eq!(store.get("77").unwrap().shares, 10.0);
            *transport.mode.lock() = "";
            monitor_once(&position, &store, &trader, mid, true, 0.005, 60)
                .await
                .unwrap();
            assert!(store.get("77").is_none());
            monitor_once(&position, &store, &trader, mid, true, 0.005, 60)
                .await
                .unwrap();
            assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
            assert_eq!(transport.base.signs.load(Ordering::SeqCst), 1);
        }
    }

    struct FixedMid(f64);

    #[async_trait]
    impl MidpriceSource for FixedMid {
        async fn midprice(&self, _: &str) -> Result<f64> {
            Ok(self.0)
        }
    }

    /// Trigger take-profit against `venue` and report if the position survived.
    async fn still_open_after_exit(venue: FakeVenue) -> bool {
        let store = PositionStore::new();
        let p = pos(0.50, 30.0, 20.0, Side::Buy);
        store.open(p.clone());
        monitor_once(&p, &store, &venue, &FixedMid(0.80), true, 0.005, 60)
            .await
            .unwrap();
        store.get(&p.token_id).is_some()
    }

    #[tokio::test]
    async fn filled_exit_closes_the_position() {
        let venue = FakeVenue {
            order_id: Some("1"),
            filled: Some(100.0),
        };
        assert!(!still_open_after_exit(venue).await);
    }

    #[tokio::test]
    async fn exit_with_unknown_fill_closes_the_position() {
        // A delayed Polymarket order that hasn't settled by the last poll.
        let venue = FakeVenue {
            order_id: Some("1"),
            filled: None,
        };
        assert!(!still_open_after_exit(venue).await);
    }

    #[tokio::test]
    async fn unfilled_exit_keeps_watching_the_position() {
        let venue = FakeVenue {
            order_id: Some("1"),
            filled: Some(0.0),
        };
        assert!(still_open_after_exit(venue).await);
    }

    #[tokio::test]
    async fn exit_with_nothing_held_closes_the_position() {
        let venue = FakeVenue {
            order_id: None,
            filled: Some(0.0),
        };
        assert!(!still_open_after_exit(venue).await);
    }

    #[tokio::test]
    async fn partial_exit_keeps_watching_what_is_left() {
        let store = PositionStore::new();
        let p = pos(0.50, 30.0, 20.0, Side::Buy);
        store.open(p.clone());
        let venue = FakeVenue {
            order_id: Some("1"),
            filled: Some(40.0),
        };
        monitor_once(&p, &store, &venue, &FixedMid(0.80), true, 0.005, 60)
            .await
            .unwrap();
        let left = store.get(&p.token_id).expect("60 shares are still held");
        assert_eq!(left.shares, 60.0);
        assert_eq!(left.usd_notional, 30.0);
    }

    fn pos(entry: f64, tp: f64, sl: f64, side: Side) -> OpenPosition {
        OpenPosition {
            token_id: "t".into(),
            slug: "s".into(),
            category: None,
            tags: vec![],
            side,
            entry_price: entry,
            shares: 100.0,
            usd_notional: 100.0 * entry,
            take_profit_pct: tp,
            stop_loss_pct: sl,
            opened_at: Utc::now(),
        }
    }

    #[test]
    fn take_profit_triggers() {
        let p = pos(0.50, 30.0, 20.0, Side::Buy);
        assert_eq!(check_exit(&p, 0.70), Some(ExitReason::TakeProfit));
    }

    #[test]
    fn stop_loss_triggers() {
        let p = pos(0.50, 30.0, 20.0, Side::Buy);
        assert_eq!(check_exit(&p, 0.39), Some(ExitReason::StopLoss));
    }

    #[test]
    fn flat_range_does_not_trigger() {
        let p = pos(0.50, 30.0, 20.0, Side::Buy);
        assert_eq!(check_exit(&p, 0.55), None);
    }

    #[test]
    fn sell_inverted_pnl_logic() {
        let p = pos(0.50, 30.0, 20.0, Side::Sell);
        // Price went DOWN — short position is in profit.
        assert_eq!(check_exit(&p, 0.30), Some(ExitReason::TakeProfit));
        // Price went UP — short position hit stop.
        assert_eq!(check_exit(&p, 0.65), Some(ExitReason::StopLoss));
    }
}
