//! Pure wire definitions and syntax validation; no broker or venue state.
use crate::bot::runtime::live_accounting::SignedAtoms;
use alloy_primitives::U256;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Map, Value};
use crate::bot::arbitrage::phase1::DecisionDiagnostic;

/// The only wire version of this domain.
pub const LIVE_SCHEMA_VERSION: &str = "btc5m-controlled-live.v1";
/// The framing prefix of a live event line.
pub const LIVE_EVENT_PREFIX: &str = "BTC5M_EVENT ";
/// The largest a single event line may be.
pub const MAX_EVENT_LINE_BYTES: usize = 64 * 1024;
/// The largest number of items an array field may hold.
pub const MAX_EVENT_ARRAY_ITEMS: usize = 1000;

/// The stable reason tokens of the new protocol.
///
/// The set is closed: a reason that is not here does not go on the wire.
pub mod reason {
    pub const NONE: &str = "NONE";
    pub const CONTEXT_MISSING: &str = "CONTEXT_MISSING";
    pub const SOURCE_NOT_ALLOWED: &str = "SOURCE_NOT_ALLOWED";
    pub const INPUT_CHANGED: &str = "INPUT_CHANGED";
    pub const SIGNED_BOUND_UNPROVEN: &str = "SIGNED_BOUND_UNPROVEN";
    pub const ALREADY_RUNNING: &str = "ALREADY_RUNNING";
    pub const RECOVERY_GUARD_UNAVAILABLE: &str = "RECOVERY_GUARD_UNAVAILABLE";
    pub const RECONCILIATION_REQUIRED: &str = "RECONCILIATION_REQUIRED";
    pub const SUBMISSION_UNKNOWN: &str = "SUBMISSION_UNKNOWN";
    pub const FACT_INCOMPLETE: &str = "FACT_INCOMPLETE";
    pub const FACT_CONFLICT: &str = "FACT_CONFLICT";
    pub const ACCOUNT_MISMATCH: &str = "ACCOUNT_MISMATCH";
    pub const READ_UNAVAILABLE: &str = "READ_UNAVAILABLE";
    pub const DATA_UNAVAILABLE: &str = "DATA_UNAVAILABLE";
    pub const OUTPUT_INVALID: &str = "OUTPUT_INVALID";
    pub const AUDIT_UNAVAILABLE: &str = "AUDIT_UNAVAILABLE";
    pub const KEYRING_UNAVAILABLE: &str = "KEYRING_UNAVAILABLE";
    pub const LOSS_LATCH: &str = "LOSS_LATCH";
    pub const DEADLINE: &str = "DEADLINE";
    pub const PARENT_LOST: &str = "PARENT_LOST";
    pub const CONTROL_CHANNEL: &str = "CONTROL_CHANNEL";
    pub const MANUAL_STOP: &str = "MANUAL_STOP";
    pub const PROCESS_FAILED: &str = "PROCESS_FAILED";
    pub const CASH_PENDING: &str = "CASH_PENDING";
    pub const CASH_READBACK_UNAVAILABLE: &str = "CASH_READBACK_UNAVAILABLE";
    pub const GAS_UNAVAILABLE: &str = "GAS_UNAVAILABLE";
    pub const GAS_BUDGET: &str = "GAS_BUDGET";
    pub const NOT_APPLICABLE: &str = "NOT_APPLICABLE";
    pub const BROKER_BLOCKED: &str = "BROKER_BLOCKED";
    pub const RUN_STOPPED: &str = "RUN_STOPPED";
}

/// Every reason token, in the order the contract lists them.
///
/// The order is fixed so a reader that deduplicates reasons gets the same
/// list every time.
pub const REASONS: &[&str] = &[
    reason::NONE,
    reason::CONTEXT_MISSING,
    reason::SOURCE_NOT_ALLOWED,
    reason::INPUT_CHANGED,
    reason::SIGNED_BOUND_UNPROVEN,
    reason::ALREADY_RUNNING,
    reason::RECOVERY_GUARD_UNAVAILABLE,
    reason::RECONCILIATION_REQUIRED,
    reason::SUBMISSION_UNKNOWN,
    reason::FACT_INCOMPLETE,
    reason::FACT_CONFLICT,
    reason::ACCOUNT_MISMATCH,
    reason::READ_UNAVAILABLE,
    reason::DATA_UNAVAILABLE,
    reason::OUTPUT_INVALID,
    reason::AUDIT_UNAVAILABLE,
    reason::KEYRING_UNAVAILABLE,
    reason::LOSS_LATCH,
    reason::DEADLINE,
    reason::PARENT_LOST,
    reason::CONTROL_CHANNEL,
    reason::MANUAL_STOP,
    reason::PROCESS_FAILED,
    reason::CASH_PENDING,
    reason::CASH_READBACK_UNAVAILABLE,
    reason::GAS_UNAVAILABLE,
    reason::GAS_BUDGET,
    reason::NOT_APPLICABLE,
];

/// Whether a reason token is one of the closed set.
pub fn is_known_reason(value: &str) -> bool {
    REASONS.contains(&value)
}

/// The kind of a live event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Decision,
    Reservation,
    Order,
    Execution,
    Reconciliation,
    LossLatch,
    Stop,
    Recovery,
    Payout,
    Final,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::Decision => "decision",
            EventKind::Reservation => "reservation",
            EventKind::Order => "order",
            EventKind::Execution => "execution",
            EventKind::Reconciliation => "reconciliation",
            EventKind::LossLatch => "loss_latch",
            EventKind::Stop => "stop",
            EventKind::Recovery => "recovery",
            EventKind::Payout => "payout",
            EventKind::Final => "final",
        }
    }

    /// Every kind, in the order the contract lists them.
    pub fn all() -> &'static [EventKind] {
        &[
            EventKind::Decision,
            EventKind::Reservation,
            EventKind::Order,
            EventKind::Execution,
            EventKind::Reconciliation,
            EventKind::LossLatch,
            EventKind::Stop,
            EventKind::Recovery,
            EventKind::Payout,
            EventKind::Final,
        ]
    }
}

/// The accounting block every live event carries.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LiveAccounting {
    pub cash: Option<U256>,
    pub available: Option<U256>,
    pub reserved: Option<U256>,
    pub cost: Option<U256>,
    pub realized: Option<SignedAtoms>,
    pub unrealized: Option<SignedAtoms>,
    pub nav: Option<SignedAtoms>,
    pub net_pnl: Option<SignedAtoms>,
    pub loss_latched: bool,
    pub status: String,
    pub reason: String,
    pub mark_time: Option<DateTime<Utc>>,
    pub pending_claim_value: Option<U256>,
    pub gas_status: String,
    pub gas_paid_atoms: Option<U256>,
}

impl LiveAccounting {
    /// The accounting as the wire object.
    pub fn to_json(&self) -> Value {
        json!({
            "cash": self.cash.map(|value| value.to_string()),
            "available": self.available.map(|value| value.to_string()),
            "reserved": self.reserved.map(|value| value.to_string()),
            "cost": self.cost.map(|value| value.to_string()),
            "realized": self.realized.map(|value| value.to_text()),
            "unrealized": self.unrealized.map(|value| value.to_text()),
            "nav": self.nav.map(|value| value.to_text()),
            "net_pnl": self.net_pnl.map(|value| value.to_text()),
            "loss_latched": self.loss_latched,
            "status": self.status,
            "reason": self.reason,
            "mark_time": self.mark_time.map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
            "pending_claim_value": self.pending_claim_value.map(|value| value.to_string()),
            "gas_status": self.gas_status,
            "gas_paid_atoms": self.gas_paid_atoms.map(|value| value.to_string()),
        })
    }
}

/// The identity of the run an event belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveRunIdentity {
    pub run_id: String,
    pub risk_window_id: String,
}

/// Production payloads have one concrete shape and derive their event kind.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum LivePayload {
    Decision(DecisionPayload),
    Reservation(ReservationPayload),
    Order(OrderPayload),
    Execution(ExecutionPayload),
    Reconciliation(ReconciliationPayload),
    LossLatch(LossLatchPayload),
    Stop(StopPayload),
    Recovery(RecoveryPayload),
    Payout(PayoutPayload),
    Final(FinalPayload),
}

impl LivePayload {
    /// The only possible event kind for this payload.
    pub fn kind(&self) -> EventKind {
        match self {
            Self::Decision(_) => EventKind::Decision,
            Self::Reservation(_) => EventKind::Reservation,
            Self::Order(_) => EventKind::Order,
            Self::Execution(_) => EventKind::Execution,
            Self::Reconciliation(_) => EventKind::Reconciliation,
            Self::LossLatch(_) => EventKind::LossLatch,
            Self::Stop(_) => EventKind::Stop,
            Self::Recovery(_) => EventKind::Recovery,
            Self::Payout(_) => EventKind::Payout,
            Self::Final(_) => EventKind::Final,
        }
    }
}

/// The original core diagnostic, without a second economic representation.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionPayload {
    pub decision: DecisionDiagnostic,
}

/// A change to the original attempt's cash reservation.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservationPayload {
    pub candidate_id: String,
    pub attempt_id: String,
    pub action: String,
    pub worst_cash_atoms: String,
    pub remaining_atoms: String,
    pub reason: String,
}

/// The original prepared/signed order and its proven execution bound.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
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
}

/// One official fact, carrying its own original coverage.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveFact {
    pub status: String,
    pub atoms: Option<String>,
    pub coverage_gross_atoms: Option<String>,
    pub reason: String,
}

/// The six official execution facts; no balance or fee calculation lives here.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionFacts {
    pub gross_cash: LiveFact,
    pub collateral_fee: LiveFact,
    pub share_fee: LiveFact,
    pub net_cash: LiveFact,
    pub net_inventory: LiveFact,
    pub vwap: LiveFact,
}

/// The six actual increments applied by the original ledger in the same lock.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionDelta {
    pub gross_q_atoms: Option<String>,
    pub gross_cash_atoms: Option<String>,
    pub collateral_fee_atoms: Option<String>,
    pub share_fee_atoms: Option<String>,
    pub net_cash_atoms: Option<String>,
    pub net_inventory_atoms: Option<String>,
}

/// An official snapshot and the original ledger's corresponding application.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPayload {
    pub attempt_id: String,
    pub signed_hash: String,
    pub venue_order_id: Option<String>,
    pub generation: u64,
    #[serde(serialize_with = "wire_time")]
    pub observed_at: DateTime<Utc>,
    pub pages_complete: bool,
    pub quantity_class: String,
    pub gross_q_atoms: Option<String>,
    pub accounting_gross_q_atoms: Option<String>,
    pub closure: String,
    pub final_gross_q_atoms: Option<String>,
    pub facts: ExecutionFacts,
    pub delta: ExecutionDelta,
    pub tx_hash: Option<String>,
    pub settlement_status: String,
    #[serde(serialize_with = "optional_time")]
    pub settlement_checked_at: Option<DateTime<Utc>>,
    pub reason: String,
}

/// The minimal window-related position disclosure.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LivePosition {
    pub market_id: String,
    pub token: String,
    pub net_shares_atoms: String,
    pub cost_atoms: Option<String>,
}

/// The original account read-back, not a reconstructed account.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationPayload {
    pub scope: String,
    #[serde(serialize_with = "wire_time")]
    pub checked_at: DateTime<Utc>,
    pub complete: bool,
    pub collateral_atoms: Option<String>,
    pub gas_balance_atoms: Option<String>,
    pub positions: Vec<LivePosition>,
    pub open_hashes: Vec<String>,
    pub foreign_risk: bool,
    pub reason: String,
}

/// The irreversible loss latch's original triggering value.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LossLatchPayload {
    pub latched: bool,
    pub trigger_net_pnl_atoms: Option<String>,
    pub reason: String,
}

/// The original run's sticky stop and local handoff status.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopPayload {
    pub stop_new_risk: bool,
    pub reason: String,
    pub takeover_required: bool,
    pub alert_status: String,
}

/// The persistent cold-start refusal policy's actual lifecycle state.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPayload {
    pub policy: String,
    pub state: String,
    pub reason: String,
}

/// An official outcome/claim or separately confirmed cash receipt.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayoutPayload {
    pub status: String,
    pub payout_yes: Option<f64>,
    pub position_net_atoms: String,
    pub claim_value_atoms: Option<String>,
    pub cash_received_atoms: Option<String>,
    pub tx_hash: Option<String>,
    #[serde(serialize_with = "wire_time")]
    pub observed_at: DateTime<Utc>,
    pub reason: String,
}

/// A single unresolved original attempt, without a second order registry.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnresolvedAttempt {
    pub attempt_id: String,
    pub signed_hash: Option<String>,
    pub closure: String,
    pub reserved_atoms: String,
    pub reason: String,
}

/// Independent execution, cash and audit completion facts.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalPayload {
    pub run_status: String,
    pub stop_reason: String,
    pub execution_complete: bool,
    pub cash_complete: bool,
    pub audit_complete: bool,
    pub takeover_required: bool,
    pub unresolved: Vec<UnresolvedAttempt>,
    pub positions: Vec<LivePosition>,
}

/// One live event, ready to be written.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveEvent {
    pub seq: u64,
    pub event_time: DateTime<Utc>,
    pub market_id: Option<String>,
    pub token: Option<String>,
    pub accounting: LiveAccounting,
    pub payload: LivePayload,
}

/// The strict key set of the envelope.
pub const ENVELOPE_KEYS: &[&str] = &[
    "schema_version",
    "run_id",
    "risk_window_id",
    "event_seq",
    "event_kind",
    "event_time",
    "market_id",
    "token",
    "accounting",
    "payload",
];

/// The strict key set of the accounting block.
pub const ACCOUNTING_KEYS: &[&str] = &[
    "cash",
    "available",
    "reserved",
    "cost",
    "realized",
    "unrealized",
    "nav",
    "net_pnl",
    "loss_latched",
    "status",
    "reason",
    "mark_time",
    "pending_claim_value",
    "gas_status",
    "gas_paid_atoms",
];

/// The strict key set of each payload.
pub fn payload_keys(kind: EventKind) -> &'static [&'static str] {
    match kind {
        EventKind::Decision => &["decision"],
        EventKind::Reservation => &[
            "candidate_id",
            "attempt_id",
            "action",
            "worst_cash_atoms",
            "remaining_atoms",
            "reason",
        ],
        EventKind::Order => &[
            "candidate_id",
            "attempt_id",
            "signed_hash",
            "venue_order_id",
            "attempt_phase",
            "submission",
            "request_gross_atoms",
            "max_cash_atoms",
            "min_net_shares_atoms",
            "expires_at",
            "removal_locked_until",
            "reason",
        ],
        EventKind::Execution => &[
            "attempt_id",
            "signed_hash",
            "venue_order_id",
            "generation",
            "observed_at",
            "pages_complete",
            "quantity_class",
            "gross_q_atoms",
            "accounting_gross_q_atoms",
            "closure",
            "final_gross_q_atoms",
            "facts",
            "delta",
            "tx_hash",
            "settlement_status",
            "settlement_checked_at",
            "reason",
        ],
        EventKind::Reconciliation => &[
            "scope",
            "checked_at",
            "complete",
            "collateral_atoms",
            "gas_balance_atoms",
            "positions",
            "open_hashes",
            "foreign_risk",
            "reason",
        ],
        EventKind::LossLatch => &["latched", "trigger_net_pnl_atoms", "reason"],
        EventKind::Stop => &[
            "stop_new_risk",
            "reason",
            "takeover_required",
            "alert_status",
        ],
        EventKind::Recovery => &["policy", "state", "reason"],
        EventKind::Payout => &[
            "status",
            "payout_yes",
            "position_net_atoms",
            "claim_value_atoms",
            "cash_received_atoms",
            "tx_hash",
            "observed_at",
            "reason",
        ],
        EventKind::Final => &[
            "run_status",
            "stop_reason",
            "execution_complete",
            "cash_complete",
            "audit_complete",
            "takeover_required",
            "unresolved",
            "positions",
        ],
    }
}

/// The strict key set of the execution facts.
pub const FACT_KEYS: &[&str] = &[
    "gross_cash",
    "collateral_fee",
    "share_fee",
    "net_cash",
    "net_inventory",
    "vwap",
];

/// The strict key set of the execution delta.
pub const DELTA_KEYS: &[&str] = &[
    "gross_q_atoms",
    "gross_cash_atoms",
    "collateral_fee_atoms",
    "share_fee_atoms",
    "net_cash_atoms",
    "net_inventory_atoms",
];

/// Whether a value is a canonical unsigned atom amount.
pub fn is_atoms(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 78
        && value.chars().all(|c| c.is_ascii_digit())
        && (value == "0" || !value.starts_with('0'))
        && U256::from_str_radix(value, 10).is_ok()
}

/// Whether a value is a canonical signed atom amount.
///
/// A leading `-` is allowed once, and `-0` is not a number this contract has.
pub fn is_signed_atoms(value: &str) -> bool {
    match value.strip_prefix('-') {
        Some(rest) => is_atoms(rest) && rest != "0",
        None => is_atoms(value),
    }
}

/// Whether a value is a strict `0x` plus sixty-four hex characters.
pub fn is_hash(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value[2..].chars().all(|c| c.is_ascii_hexdigit())
}

/// Whether a value is a strict venue order reference.
pub fn is_order_ref(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Whether a value is a strict decimal identifier.
pub fn is_decimal_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 78
        && value.chars().all(|c| c.is_ascii_digit())
}

/// Whether a value is a legal JSON number for this wire.
///
/// A boolean is not a number, and neither is a non-finite float.
fn is_finite_number(value: &Value) -> bool {
    match value {
        Value::Number(number) => number.as_f64().is_some_and(f64::is_finite),
        _ => false,
    }
}

/// Check one JSON object against an exact key set.
///
/// Every key must be present and no other key may be: a missing field and an
/// unknown field are both refused.
pub fn check_exact_keys(object: &Map<String, Value>, keys: &[&str]) -> Result<(), String> {
    for key in keys {
        if !object.contains_key(*key) {
            return Err(format!("MISSING_KEY:{key}"));
        }
    }
    for key in object.keys() {
        if !keys.contains(&key.as_str()) {
            return Err(format!("UNKNOWN_KEY:{key}"));
        }
    }
    Ok(())
}

/// Check that a value is one of a closed set.
fn check_enum(value: &Value, allowed: &[&str], label: &str) -> Result<(), String> {
    let text = value.as_str().ok_or_else(|| format!("{label}_NOT_A_STRING"))?;
    if !allowed.contains(&text) {
        return Err(format!("{label}_INVALID"));
    }
    Ok(())
}

struct StrictValue(Value);

impl<'de> serde::Deserialize<'de> for StrictValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = StrictValue;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("strict JSON")
            }
            fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<StrictValue, E> {
                Ok(StrictValue(Value::Bool(value)))
            }
            fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<StrictValue, E> {
                Ok(StrictValue(json!(value)))
            }
            fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<StrictValue, E> {
                Ok(StrictValue(json!(value)))
            }
            fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<StrictValue, E> {
                serde_json::Number::from_f64(value).map(|number| StrictValue(Value::Number(number)))
                    .ok_or_else(|| E::custom("NONFINITE"))
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<StrictValue, E> {
                Ok(StrictValue(Value::String(value.into())))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<StrictValue, E> {
                Ok(StrictValue(Value::Null))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<StrictValue, A::Error> {
                let mut items = Vec::new();
                while let Some(StrictValue(value)) = seq.next_element()? {
                    if items.len() >= MAX_EVENT_ARRAY_ITEMS {
                        return Err(serde::de::Error::custom("ARRAY_TOO_LONG"));
                    }
                    items.push(value);
                }
                Ok(StrictValue(Value::Array(items)))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<StrictValue, A::Error> {
                let mut object = Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if object.contains_key(&key) {
                        return Err(serde::de::Error::custom("DUPLICATE_KEY"));
                    }
                    let StrictValue(value) = map.next_value()?;
                    object.insert(key, value);
                }
                Ok(StrictValue(Value::Object(object)))
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

pub(super) fn decode_envelope(bytes: &[u8]) -> Result<Value, String> {
    if bytes.len() + LIVE_EVENT_PREFIX.len() + 1 > MAX_EVENT_LINE_BYTES {
        return Err("OUTPUT_INVALID".into());
    }
    let StrictValue(value) = serde_json::from_slice(bytes).map_err(|_| "OUTPUT_INVALID")?;
    validate_envelope(&value)?;
    Ok(value)
}

/// Check one envelope against the strict schema.
pub(super) fn validate_envelope(envelope: &Value) -> Result<(), String> {
    let Value::Object(object) = envelope else {
        return Err("EVENT_NOT_AN_OBJECT".into());
    };
    check_exact_keys(object, ENVELOPE_KEYS)?;
    if object["schema_version"].as_str() != Some(LIVE_SCHEMA_VERSION) {
        return Err("SCHEMA_VERSION_INVALID".into());
    }
    let seq = object["event_seq"].as_u64().ok_or("EVENT_SEQ_INVALID")?;
    if seq == 0 {
        return Err("EVENT_SEQ_INVALID".into());
    }
    let kind_text = object["event_kind"].as_str().ok_or("EVENT_KIND_INVALID")?;
    let kind = EventKind::all()
        .iter()
        .copied()
        .find(|kind| kind.as_str() == kind_text)
        .ok_or("EVENT_KIND_INVALID")?;
    let run_id = object["run_id"].as_str().ok_or("RUN_ID_INVALID")?;
    let valid_id = |id: &str| !id.is_empty() && id.len() <= 64 && !id.starts_with('-')
        && id.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-'));
    if !valid_id(run_id) {
        return Err("RUN_ID_INVALID".into());
    }
    let window_id = object["risk_window_id"].as_str().ok_or("RISK_WINDOW_ID_INVALID")?;
    if !valid_id(window_id) {
        return Err("RISK_WINDOW_ID_INVALID".into());
    }
    check_time(&object["event_time"], false, "EVENT_TIME")?;
    for (key, label) in [("market_id", "MARKET_ID"), ("token", "TOKEN")] {
        if !object[key].is_null() && !object[key].as_str().is_some_and(is_decimal_id) {
            return Err(format!("{label}_INVALID"));
        }
    }
    if matches!(kind, EventKind::Decision | EventKind::Reservation | EventKind::Order
        | EventKind::Execution | EventKind::Payout) && object["market_id"].is_null()
    {
        return Err("MARKET_ID_INVALID".into());
    }
    if matches!(kind, EventKind::Reservation | EventKind::Order | EventKind::Execution | EventKind::Payout)
        && object["token"].is_null()
    {
        return Err("TOKEN_INVALID".into());
    }
    let Value::Object(accounting) = &object["accounting"] else {
        return Err("ACCOUNTING_INVALID".into());
    };
    check_exact_keys(accounting, ACCOUNTING_KEYS)?;
    for key in ["cash", "available", "reserved", "cost", "pending_claim_value"] {
        check_optional_atoms(accounting.get(key).unwrap_or(&Value::Null), key)?;
    }
    for key in ["realized", "unrealized", "nav", "net_pnl"] {
        check_optional_signed(accounting.get(key).unwrap_or(&Value::Null), key)?;
    }
    if !accounting["loss_latched"].is_boolean() {
        return Err("LOSS_LATCHED_INVALID".into());
    }
    check_enum(
        &accounting["status"],
        &["COMPLETE", "INCOMPLETE", "CONFLICT"],
        "STATUS",
    )?;
    let accounting_reason = accounting["reason"].as_str().ok_or("REASON_INVALID")?;
    if !is_known_reason(accounting_reason) {
        return Err("REASON_INVALID".into());
    }
    check_enum(
        &accounting["gas_status"],
        &["NOT_MEASURED", "KNOWN", "UNAVAILABLE"],
        "GAS_STATUS",
    )?;
    check_optional_atoms(
        accounting.get("gas_paid_atoms").unwrap_or(&Value::Null),
        "gas_paid_atoms",
    )?;
    let Value::Object(payload) = &object["payload"] else {
        return Err("PAYLOAD_INVALID".into());
    };
    check_time(&accounting["mark_time"], true, "MARK_TIME")?;
    check_exact_keys(payload, payload_keys(kind))?;
    check_payload(kind, payload)?;
    Ok(())
}

fn wire_time<S: serde::Serializer>(time: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

fn optional_time<S: serde::Serializer>(time: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error> {
    match time {
        Some(time) => wire_time(time, serializer),
        None => serializer.serialize_none(),
    }
}

fn check_time(value: &Value, nullable: bool, label: &str) -> Result<(), String> {
    if nullable && value.is_null() {
        return Ok(());
    }
    let valid = value.as_str().is_some_and(|text| {
        DateTime::parse_from_rfc3339(text).is_ok_and(|time| time.timestamp_subsec_nanos() % 1_000_000 == 0)
    });
    if valid { Ok(()) } else { Err(format!("{label}_INVALID")) }
}

/// Check an optional atom amount.
fn check_optional_atoms(value: &Value, label: &str) -> Result<(), String> {
    match value {
        Value::Null => Ok(()),
        Value::String(text) if is_atoms(text) => Ok(()),
        _ => Err(format!("{label}_INVALID")),
    }
}

/// Check an optional signed atom amount.
fn check_optional_signed(value: &Value, label: &str) -> Result<(), String> {
    match value {
        Value::Null => Ok(()),
        Value::String(text) if is_signed_atoms(text) => Ok(()),
        _ => Err(format!("{label}_INVALID")),
    }
}

fn decode_payload<T: serde::de::DeserializeOwned>(payload: &Map<String, Value>) -> Result<(), String> {
    serde_json::from_value::<T>(Value::Object(payload.clone()))
        .map(|_| ())
        .map_err(|_| "PAYLOAD_INVALID".into())
}

/// Validate required-nullable core fields recursively without changing the core DTO.
/// Tokens name existing diagnostic fields and their wire types, not model rules.
fn check_fields(value: &Value, schema: &str) -> Result<(), String> {
    let object = value.as_object().ok_or("DECISION_INVALID")?;
    let fields: Vec<_> = schema.split_whitespace().map(|field| field.split_once(':').unwrap()).collect();
    check_exact_keys(object, &fields.iter().map(|(key, _)| *key).collect::<Vec<_>>())?;
    for (key, kind) in fields {
        let value = &object[key];
        if kind.ends_with('?') && value.is_null() { continue; }
        let valid = match kind.trim_end_matches('?') {
            "number" => is_finite_number(value),
            "integer" => value.as_u64().is_some(),
            "u32" => value.as_u64().is_some_and(|value| value <= u32::MAX as u64),
            "bool" => value.is_boolean(),
            "text" => value.as_str().is_some_and(|text| !text.is_empty() && text.len() <= 128),
            "id" => value.as_str().is_some_and(is_decimal_id),
            "time" => check_time(value, false, key).is_ok(),
            "clock" => {
                check_fields(value, "source_at:time? source_time_status:MISSING|PRESENT freshness_basis:RECEIPT_BOUND|SOURCE_AND_RECEIPT request_at:time received_at:time request_elapsed_ms:integer received_elapsed_ms:integer")?;
                true
            }
            "side" => {
                check_fields(value, SIDE_FIELDS)?;
                true
            }
            "levels" => {
                let levels = value.as_array().ok_or("DEPTH_LEVELS_INVALID")?;
                if levels.len() > MAX_EVENT_ARRAY_ITEMS { return Err("DEPTH_LEVELS_INVALID".into()); }
                for level in levels { check_fields(level, "price:number size:number")?; }
                true
            }
            allowed => value.as_str().is_some_and(|text| allowed.split('|').any(|item| item == text)),
        };
        if !valid { return Err(format!("DECISION_FIELD_INVALID:{key}")); }
    }
    Ok(())
}

const SIDE_FIELDS: &str = concat!(
    "token:id fair:number? limit_price:number? gross_shares:number? ",
    "fee_per_gross_share:number? net_shares:number? cost:number? ev:number? ",
    "edge:number? roi:number? breakeven_fair:number? qualifies:bool? ",
    "depth_levels:levels? depth_complete:bool? walk_vwap:number? ",
    "execution_net_shares:number? execution_cost:number? execution_ev:number? ",
    "execution_edge:number?",
);
const DECISION_FIELDS: &str = concat!(
    "decision_id:id candidate_id:id? phase:EVALUATE|PRE_SUBMIT|FINAL_SEND ",
    "strategy_contract:phase1_net_shares reason:RUN_STOPPED|ALREADY_ENTERED|MARKET_INVALID|",
    "MISSING_INPUT|INPUT_INVALID|CLOCK_INVALID|REQUEST_TOO_SLOW|SPOT_STALE|BOOK_STALE|",
    "ROUND_TOO_LATE|INPUT_CHANGED|WARMUP|NET_SHARES_INVALID|EDGE_BELOW_MIN|DIAGNOSTIC_LIMIT|",
    "BROKER_BLOCKED|CONTEXT_MISSING|ELIGIBLE symbol:text prediction_source:Binance|Coinbase? ",
    "actual_pair:text? source_state:PRIMARY|DEGRADED|UNAVAILABLE source_epoch:integer? ",
    "reset_reason:NONE|FIRST_SAMPLE|SOURCE_SWITCH|LONG_GAP|CLOCK_INVALID ",
    "settlement_provider:CHAINLINK|UNKNOWN starts_at:time? ends_at:time? ",
    "identity_available_at:time? strike_available_at:time? strike:number? spot:number? ",
    "spot_clock:clock? book_clock:clock? decision_at:time decision_elapsed_ms:integer ",
    "checked_at:time checked_elapsed_ms:integer secs_left:number? samples:u32 ",
    "sigma:number? p_up:number? best_yes_bid:number? best_yes_ask:number? ",
    "fee_rate_bps:u32? selected_token:id? up:side? down:side?",
);

/// Check the payload of one kind.
fn check_payload(kind: EventKind, payload: &Map<String, Value>) -> Result<(), String> {
    for (key, value) in payload {
        match key.as_str() {
            "candidate_id" | "attempt_id" if !value.as_str().is_some_and(is_decimal_id) =>
                return Err(format!("{}_INVALID", key.to_ascii_uppercase())),
            "signed_hash" | "tx_hash" if !value.is_null() && !value.as_str().is_some_and(is_hash) =>
                return Err(format!("{}_INVALID", key.to_ascii_uppercase())),
            "venue_order_id" if !value.is_null() && !value.as_str().is_some_and(is_order_ref) =>
                return Err("VENUE_ORDER_ID_INVALID".into()),
            "reason" | "stop_reason" => check_reason(value)?,
            _ => {}
        }
        let nullable = !matches!(key.as_str(), "worst_cash_atoms" | "remaining_atoms" |
            "request_gross_atoms" | "max_cash_atoms" | "position_net_atoms");
        if key.ends_with("_atoms") && key != "trigger_net_pnl_atoms" {
            if !nullable && value.is_null() {
                return Err(format!("{key}_INVALID"));
            }
            check_optional_atoms(value, key)?;
        }
        if matches!(key.as_str(), "expires_at" | "removal_locked_until" | "settlement_checked_at") {
            check_time(value, true, key)?;
        }
        if matches!(key.as_str(), "observed_at" | "checked_at") {
            check_time(value, false, key)?;
        }
    }
    match kind {
        EventKind::Decision => {
            check_fields(&payload["decision"], DECISION_FIELDS)?;
        }
        EventKind::Reservation => {
            check_enum(&payload["action"], &["RESERVE", "RELEASE"], "ACTION")?;
            decode_payload::<ReservationPayload>(payload)?;
        }
        EventKind::Order => {
            check_enum(&payload["attempt_phase"],
                &["PREPARED", "MAY_HAVE_SUBMITTED", "SUBMITTED", "CLOSED"], "ATTEMPT_PHASE")?;
            check_enum(&payload["submission"],
                &["NOT_SUBMITTED", "REJECTED_FINAL", "ACCEPTED", "UNKNOWN"], "SUBMISSION")?;
            decode_payload::<OrderPayload>(payload)?;
            if matches!(payload["attempt_phase"].as_str(), Some("MAY_HAVE_SUBMITTED" | "SUBMITTED"))
                && (payload["signed_hash"].is_null()
                    || !payload["min_net_shares_atoms"].as_str().is_some_and(|value| is_atoms(value) && value != "0"))
            {
                return Err("SIGNED_BOUND_UNPROVEN".into());
            }
        }
        EventKind::Execution => {
            decode_payload::<ExecutionPayload>(payload)?;
            if !payload["generation"].as_u64().is_some_and(|value| value > 0) {
                return Err("GENERATION_INVALID".into());
            }
            check_enum(&payload["quantity_class"],
                &["UNKNOWN", "ZERO", "PARTIAL", "FULL"], "QUANTITY_CLASS")?;
            check_enum(&payload["closure"], &["OPEN", "REMOVING", "UNPROVEN", "FINAL"], "CLOSURE")?;
            if (payload["quantity_class"] == "UNKNOWN") != payload["gross_q_atoms"].is_null()
                || (payload["quantity_class"] == "ZERO" && payload["gross_q_atoms"] != "0")
                || (matches!(payload["quantity_class"].as_str(), Some("PARTIAL" | "FULL")) && payload["gross_q_atoms"] == "0")
            {
                return Err("QUANTITY_CLASS_INVALID".into());
            }
            if (payload["closure"] == "FINAL") == payload["final_gross_q_atoms"].is_null() {
                return Err("CLOSURE_INVALID".into());
            }
            let Value::Object(facts) = &payload["facts"] else {
                return Err("FACTS_INVALID".into());
            };
            check_exact_keys(facts, FACT_KEYS)?;
            for key in FACT_KEYS {
                check_fact(&facts[*key], key)?;
                if facts[*key]["status"] == "NOT_APPLICABLE"
                    && (*key != "vwap" || payload["gross_q_atoms"] != "0"
                        || facts[*key]["coverage_gross_atoms"] != "0")
                {
                    return Err("FACT_STATUS_INVALID".into());
                }
            }
            let Value::Object(delta) = &payload["delta"] else {
                return Err("DELTA_INVALID".into());
            };
            check_exact_keys(delta, DELTA_KEYS)?;
            for key in DELTA_KEYS {
                check_optional_atoms(&delta[*key], key)?;
            }
            check_enum(
                &payload["settlement_status"],
                &["UNVERIFIED", "MATCHED", "CHAIN_SETTLED", "CONFLICT"],
                "SETTLEMENT_STATUS",
            )?;
        }
        EventKind::Reconciliation => {
            decode_payload::<ReconciliationPayload>(payload)?;
            check_enum(&payload["scope"], &["STARTUP", "RUNTIME", "HANDOFF"], "SCOPE")?;
            check_positions(&payload["positions"])?;
            check_hash_array(&payload["open_hashes"])?;
        }
        EventKind::LossLatch => {
            decode_payload::<LossLatchPayload>(payload)?;
            if payload["latched"] != Value::Bool(true) {
                return Err("LATCHED_INVALID".into());
            }
            check_optional_signed(&payload["trigger_net_pnl_atoms"], "trigger_net_pnl_atoms")?;
        }
        EventKind::Stop => {
            if payload["stop_new_risk"] != Value::Bool(true) {
                return Err("STOP_NEW_RISK_INVALID".into());
            }
            if !payload["takeover_required"].is_boolean() {
                return Err("TAKEOVER_REQUIRED_INVALID".into());
            }
            check_enum(
                &payload["alert_status"],
                &["LOCAL_RECORDED", "DELIVERED", "UNAVAILABLE"],
                "ALERT_STATUS",
            )?;
        }
        EventKind::Recovery => {
            if payload["policy"].as_str() != Some("COLD_START_REFUSE") {
                return Err("POLICY_INVALID".into());
            }
            check_enum(
                &payload["state"],
                &["ARMED", "RECONCILIATION_REQUIRED", "HANDOFF"],
                "STATE",
            )?;
        }
        EventKind::Payout => {
            decode_payload::<PayoutPayload>(payload)?;
            check_enum(
                &payload["status"],
                &["UNRESOLVED", "CONFIRMED", "CONFLICT", "CASH_PENDING", "CASH_CONFIRMED"],
                "STATUS",
            )?;
            if !payload["payout_yes"].is_null()
                && !payload["payout_yes"].as_f64().is_some_and(|value| (0.0..=1.0).contains(&value))
            {
                return Err("PAYOUT_YES_INVALID".into());
            }
            if matches!(payload["status"].as_str(), Some("CONFIRMED" | "CASH_PENDING" | "CASH_CONFIRMED"))
                && payload["payout_yes"].is_null()
            {
                return Err("PAYOUT_YES_INVALID".into());
            }
            if payload["status"] == "CASH_CONFIRMED"
                && (payload["cash_received_atoms"].is_null() || payload["tx_hash"].is_null())
            {
                return Err("PAYOUT_CASH_INVALID".into());
            }
        }
        EventKind::Final => {
            decode_payload::<FinalPayload>(payload)?;
            check_enum(
                &payload["run_status"],
                &["COMPLETED", "STOPPED", "INCOMPLETE"],
                "RUN_STATUS",
            )?;
            check_unresolved(&payload["unresolved"])?;
            check_positions(&payload["positions"])?;
        }
    }
    Ok(())
}

/// Check a reason token.
fn check_reason(value: &Value) -> Result<(), String> {
    let text = value.as_str().ok_or("REASON_INVALID")?;
    if !is_known_reason(text) {
        return Err("REASON_INVALID".into());
    }
    Ok(())
}

/// Check one accounting fact object.
fn check_fact(value: &Value, label: &str) -> Result<(), String> {
    let Value::Object(fact) = value else {
        return Err(format!("{label}_INVALID"));
    };
    check_exact_keys(
        fact,
        &["status", "atoms", "coverage_gross_atoms", "reason"],
    )?;
    let status = fact["status"].as_str().ok_or("FACT_STATUS_INVALID")?;
    match status {
        "KNOWN" => {
            let atoms = fact["atoms"].as_str().ok_or("FACT_ATOMS_INVALID")?;
            if !is_atoms(atoms) {
                return Err("FACT_ATOMS_INVALID".into());
            }
            if fact["coverage_gross_atoms"].is_null() {
                return Err("FACT_COVERAGE_INVALID".into());
            }
        }
        "UNKNOWN" => {
            if !fact["atoms"].is_null() {
                return Err("FACT_ATOMS_INVALID".into());
            }
        }
        "NOT_APPLICABLE" => {
            if !fact["atoms"].is_null() {
                return Err("FACT_ATOMS_INVALID".into());
            }
        }
        _ => return Err("FACT_STATUS_INVALID".into()),
    }
    check_optional_atoms(&fact["coverage_gross_atoms"], "coverage_gross_atoms")?;
    check_reason(&fact["reason"])?;
    Ok(())
}

/// Check the positions array.
fn check_positions(value: &Value) -> Result<(), String> {
    let Value::Array(items) = value else {
        return Err("POSITIONS_INVALID".into());
    };
    if items.len() > MAX_EVENT_ARRAY_ITEMS {
        return Err("POSITIONS_TOO_LONG".into());
    }
    for item in items {
        let Value::Object(position) = item else {
            return Err("POSITION_INVALID".into());
        };
        check_exact_keys(
            position,
            &["market_id", "token", "net_shares_atoms", "cost_atoms"],
        )?;
        if !position["market_id"].as_str().is_some_and(is_decimal_id) {
            return Err("POSITION_INVALID".into());
        }
        if !is_decimal_id(position["token"].as_str().unwrap_or_default()) {
            return Err("POSITION_INVALID".into());
        }
        check_optional_atoms(&position["net_shares_atoms"], "net_shares_atoms")?;
        check_optional_atoms(&position["cost_atoms"], "cost_atoms")?;
    }
    Ok(())
}

/// Check the unresolved array.
fn check_unresolved(value: &Value) -> Result<(), String> {
    let Value::Array(items) = value else {
        return Err("UNRESOLVED_INVALID".into());
    };
    if items.len() > MAX_EVENT_ARRAY_ITEMS {
        return Err("UNRESOLVED_TOO_LONG".into());
    }
    for item in items {
        let Value::Object(unresolved) = item else {
            return Err("UNRESOLVED_ITEM_INVALID".into());
        };
        check_exact_keys(
            unresolved,
            &["attempt_id", "signed_hash", "closure", "reserved_atoms", "reason"],
        )?;
        if !is_decimal_id(unresolved["attempt_id"].as_str().unwrap_or_default()) {
            return Err("ATTEMPT_ID_INVALID".into());
        }
        match &unresolved["signed_hash"] {
            Value::Null => {}
            Value::String(text) if is_hash(text) => {}
            _ => return Err("SIGNED_HASH_INVALID".into()),
        }
        check_enum(
            &unresolved["closure"],
            &["OPEN", "REMOVING", "UNPROVEN", "FINAL"],
            "CLOSURE",
        )?;
        check_optional_atoms(&unresolved["reserved_atoms"], "reserved_atoms")?;
        check_reason(&unresolved["reason"])?;
    }
    Ok(())
}

/// Check an array of hashes.
fn check_hash_array(value: &Value) -> Result<(), String> {
    let Value::Array(items) = value else {
        return Err("OPEN_HASHES_INVALID".into());
    };
    if items.len() > MAX_EVENT_ARRAY_ITEMS {
        return Err("OPEN_HASHES_TOO_LONG".into());
    }
    for item in items {
        let text = item.as_str().ok_or("OPEN_HASHES_INVALID")?;
        if !is_hash(text) {
            return Err("OPEN_HASHES_INVALID".into());
        }
    }
    Ok(())
}
