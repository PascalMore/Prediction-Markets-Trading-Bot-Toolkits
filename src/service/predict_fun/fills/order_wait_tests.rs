//! Script only the external REST/block facts of the real native adapter.
use std::sync::{atomic::{AtomicUsize, Ordering}, Mutex};

#[derive(Default)]
pub(crate) struct ReadScript {
    pub status: Mutex<Option<String>>,
    pub statuses: Mutex<std::collections::HashMap<String, String>>,
    pub venue_id: Mutex<Option<String>>,
    pub id_after: AtomicUsize,
    pub fail_reads: std::sync::atomic::AtomicBool,
    pub block_time: Mutex<Option<u64>>,
    pub close_flags: Mutex<Option<(bool, bool)>>,
    pub order_reads: AtomicUsize,
}

impl ReadScript {
    pub fn rpc(&self, method: &str, params: &serde_json::Value) -> Option<serde_json::Value> {
        let (closed, nonce_valid) = (*self.close_flags.lock().unwrap())?;
        if method != "eth_call" { return None; }
        let selector = params[0]["data"].as_str()?.get(2..10)?;
        let status = alloy_primitives::keccak256("getOrderStatus(bytes32)");
        let nonce = alloy_primitives::keccak256("isValidNonce(address,uint256)");
        if selector == hex::encode(&status.as_slice()[..4]) {
            return Some(serde_json::json!(format!("0x{:064x}{:064x}", u8::from(closed), 0)));
        }
        (selector == hex::encode(&nonce.as_slice()[..4]))
            .then(|| serde_json::json!(format!("0x{:064x}", u8::from(nonce_valid))))
    }

    pub fn rest(&self, path: &str, raw: String) -> anyhow::Result<String> {
        if !path.starts_with("/orders/") || path.contains('?') { return Ok(raw); }
        if self.fail_reads.load(Ordering::SeqCst) { anyhow::bail!("OFFLINE_READ_UNAVAILABLE"); }
        let read = self.order_reads.fetch_add(1, Ordering::SeqCst) + 1;
        let mut value: serde_json::Value = serde_json::from_str(&raw)?;
        if let Some(status) = self.status.lock().unwrap().as_ref() {
            value["data"]["status"] = status.clone().into();
        }
        if let Some(status) = self.statuses.lock().unwrap().get(path.trim_start_matches("/orders/")) {
            value["data"]["status"] = status.clone().into();
        }
        if read > self.id_after.load(Ordering::SeqCst) {
            if let Some(id) = self.venue_id.lock().unwrap().as_ref() {
                value["data"]["id"] = id.clone().into();
            }
        }
        Ok(value.to_string())
    }
}

#[test]
fn expiry_getter_is_controlled_only_and_rejects_bad_or_foreign_bindings() {
    use super::*;
    let (trader, _) = tests::scenario("fills", "");
    let hash = format!("0x{}", "a".repeat(64));
    let handle = new_untracked_handle();
    {
        let mut record = handle.lock();
        record.identity = OrderIdentity { local_attempt_id: 1, signed_hash: Some(hash.clone()),
            venue_order_id: Some("accepted-id".into()), market_id: 1, token_id: "77".into(),
            side: Side::Buy, account: trader.identity() };
        record.phase = AttemptPhase::Submitted;
        record.submission = Submission::Accepted;
    }
    let identity = handle.lock().identity.clone();
    let expiry = chrono::Utc::now().timestamp() + 300;
    trader.bound_orders.write().insert(hash.clone(), BoundOrder {
        handle: handle.clone(), market: trader.client.market_for_token("77").unwrap(),
        requested: U256::from(1), maker_amount: U256::from(1), taker_amount: U256::from(1),
        expiration: expiry,
    });
    assert_eq!(trader.controlled_order_expiration(&identity).unwrap(), None);
    trader.controlled_metadata.write().record(&hash, ControlledOrderMetadata {
        removal_locked_until: None, execution_generation: 1, chain_settled: false,
        settlement_checked_at: None,
    });
    assert_eq!(trader.controlled_order_expiration(&identity).unwrap(), Some(expiry));
    for value in [0, -1, i64::MAX] {
        trader.bound_orders.write().get_mut(&hash).unwrap().expiration = value;
        assert_eq!(trader.controlled_order_expiration(&identity).unwrap(), None);
    }
    trader.bound_orders.write().get_mut(&hash).unwrap().expiration = expiry;
    let mut foreign = identity.clone(); foreign.venue_order_id = Some("foreign-id".into());
    assert!(trader.controlled_order_expiration(&foreign).is_err());
    handle.lock().submission = Submission::Unknown;
    assert_eq!(trader.controlled_order_expiration(&identity).unwrap(), None);
}
