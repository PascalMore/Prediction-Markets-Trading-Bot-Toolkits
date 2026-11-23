//! The order payload and its closed diagnostic.
//!
//! The order event carries what the original attempt actually did. This
//! module owns that shape and the one optional diagnostic it may carry, so
//! the parent wire module stays small and the diagnostic's rules live in one
//! place.
//!
//! The diagnostic is a closed object with a fixed key set. It carries
//! numbers and closed tokens only: no raw error, no HTTP body, no account
//! detail, no URL, no key and no signature byte.

use crate::service::venue::{SendCheckDetail, SendCheckStage};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

/// The exact key set of one diagnostic object.
pub const DIAGNOSTIC_KEYS: &[&str] = &[
    "stage",
    "detail",
    "checked_at",
    "checked_elapsed_ms",
    "tick_started_elapsed_ms",
    "book_receipt_age_ms",
    "spot_receipt_age_ms",
    "account_receipt_age_ms",
    "book_request_age_ms",
    "spot_request_age_ms",
];

/// The original prepared/signed order and its proven execution bound.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderPayload {
    pub candidate_id: String,
    pub attempt_id: String,
    pub signed_hash: Option<String>,
    pub venue_order_id: Option<String>,
    pub attempt_phase: String,
    pub submission: String,
    pub request_gross_atoms: String,
    pub max_cash_atoms: String,
    pub min_net_shares_atoms: Option<String>,
    #[serde(serialize_with = "optional_time")]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "optional_time")]
    pub removal_locked_until: Option<DateTime<Utc>>,
    pub reason: String,
    /// The closed diagnostic of the check that refused or completed the order.
    ///
    /// It is `None` when no check ran for this attempt, and the key is always
    /// present so a reader never has to guess whether it was absent.
    pub diagnostic: Option<OrderDiagnostic>,
}

/// One send check's projection, on the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderDiagnostic {
    /// Where the check ran.
    pub stage: String,
    /// What the check found.
    pub detail: String,
    /// The check's own wall clock reading.
    #[serde(serialize_with = "optional_time")]
    pub checked_at: Option<DateTime<Utc>>,
    /// The check's own elapsed reading.
    pub checked_elapsed_ms: Option<u64>,
    /// When this tick's first account read was issued.
    pub tick_started_elapsed_ms: Option<u64>,
    /// The age of the book receipt at the check.
    pub book_receipt_age_ms: Option<u64>,
    /// The age of the spot receipt at the check.
    pub spot_receipt_age_ms: Option<u64>,
    /// The age of the account receipt at the check.
    pub account_receipt_age_ms: Option<u64>,
    /// The age of the book's own request at the check.
    pub book_request_age_ms: Option<u64>,
    /// The age of the spot's own request at the check.
    pub spot_request_age_ms: Option<u64>,
}

impl OrderDiagnostic {
    /// The diagnostic of one measured check.
    pub fn from_observation(observation: &crate::service::venue::SendCheckObservation) -> Self {
        Self {
            stage: observation.stage.as_str().to_string(),
            detail: observation.detail.as_str().to_string(),
            checked_at: Some(observation.checked_at),
            checked_elapsed_ms: Some(observation.checked_elapsed_ms),
            tick_started_elapsed_ms: observation.tick_started_elapsed_ms,
            book_receipt_age_ms: observation.book_receipt_age_ms,
            spot_receipt_age_ms: observation.spot_receipt_age_ms,
            account_receipt_age_ms: observation.account_receipt_age_ms,
            book_request_age_ms: observation.book_request_age_ms,
            spot_request_age_ms: observation.spot_request_age_ms,
        }
    }
}

/// Whether a stage token is one the contract names.
pub fn is_stage(value: &str) -> bool {
    SendCheckStage::ALL.iter().any(|stage| stage.as_str() == value)
}

/// Whether a detail token is one the contract names.
pub fn is_detail(value: &str) -> bool {
    SendCheckDetail::ALL.iter().any(|detail| detail.as_str() == value)
}

/// Check one diagnostic object against the closed contract.
///
/// Every age is a non-negative integer or absent; a bool is not an integer;
/// a stage or detail outside the closed set is refused. The check happens
/// before the value is written, so a bad diagnostic never reaches the disk.
pub fn check_diagnostic(value: &Value) -> Result<(), String> {
    // An absent measurement is not an invalid measurement: the key is
    // always present, and `null` means no check ran for this attempt.
    if value.is_null() {
        return Ok(());
    }
    let Some(object) = value.as_object() else {
        return Err("DIAGNOSTIC_INVALID".into());
    };
    let mut seen: Vec<&str> = Vec::with_capacity(DIAGNOSTIC_KEYS.len());
    for key in object.keys() {
        if !DIAGNOSTIC_KEYS.contains(&key.as_str()) {
            return Err(format!("UNKNOWN_KEY:{key}"));
        }
        seen.push(key.as_str());
    }
    for key in DIAGNOSTIC_KEYS {
        if !seen.contains(&key) {
            return Err(format!("MISSING_KEY:{key}"));
        }
    }
    let stage = object["stage"].as_str().ok_or("STAGE_INVALID")?;
    if !is_stage(stage) {
        return Err("STAGE_INVALID".into());
    }
    let detail = object["detail"].as_str().ok_or("DETAIL_INVALID")?;
    if !is_detail(detail) {
        return Err("DETAIL_INVALID".into());
    }
    if let Some(time) = object["checked_at"].as_str() {
        match DateTime::parse_from_rfc3339(time) {
            Ok(time) if time.timestamp_subsec_nanos() % 1_000_000 == 0 => {}
            _ => return Err("CHECKED_AT_INVALID".into()),
        }
    } else if !object["checked_at"].is_null() {
        return Err("CHECKED_AT_INVALID".into());
    }
    for key in ["checked_elapsed_ms", "tick_started_elapsed_ms", "book_receipt_age_ms",
                "spot_receipt_age_ms", "account_receipt_age_ms", "book_request_age_ms",
                "spot_request_age_ms"] {
        let value = &object[key];
        if value.is_null() {
            continue;
        }
        if !value.as_u64().is_some() {
            return Err(format!("{key}_INVALID"));
        }
    }
    Ok(())
}

/// Normalize an old order payload that does not carry the diagnostic.
///
/// The missing key becomes `None` in memory only: an old stream stays
/// readable and is never rewritten.
pub fn normalize_old_order(payload: &mut Map<String, Value>) {
    payload.entry("diagnostic".to_string()).or_insert(Value::Null);
}

/// Borrow modern payloads; compatibility normalization never changes input bytes.
pub(super) fn normalized_old_order(payload: &Map<String, Value>) -> std::borrow::Cow<'_, Map<String, Value>> {
    if payload.contains_key("diagnostic") { return std::borrow::Cow::Borrowed(payload); }
    let mut normalized = payload.clone();
    normalize_old_order(&mut normalized);
    std::borrow::Cow::Owned(normalized)
}

/// The diagnostic of one order payload, if it carries one.
pub fn diagnostic_of(payload: &Map<String, Value>) -> Option<Value> {
    match payload.get("diagnostic") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value.clone()),
    }
}

/// The order payload as a plain object, for the parent's own validation.
pub fn to_value(payload: &OrderPayload) -> Value {
    json!(payload)
}

fn optional_time<S: serde::Serializer>(time: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error> {
    match time {
        Some(time) => crate::bot::runtime::live_events::types::wire_time(time, serializer),
        None => serializer.serialize_none(),
    }
}
