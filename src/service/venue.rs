//! The seam between the shared pipeline and a venue.
//!
//! Sizing, eligibility, exposure, risk and TP/SL are venue-neutral. A venue
//! supplies market metadata ([`MarketSource`]), order placement
//! ([`OrderVenue`]) and quotes ([`crate::service::midprice::MidpriceSource`]).

use crate::models::{BookSnapshot, PlannedOrder, Side, VenueId};
use crate::service::market_cache::MarketInfo;
use alloy_primitives::U256;
use anyhow::{bail, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

#[async_trait]
pub trait MarketSource: Send + Sync {
    /// Resolve an outcome token id to its market metadata.
    async fn by_token_id(&self, token_id: &str) -> Result<MarketInfo>;
}

// --- Live execution facts ----------------------------------------------------
//
// A live venue reports what actually happened: how much was filled, at what
// price, what it cost, and whether the order can still fill. Those are separate
// facts, and each one is either known or explicitly unknown. Nothing here
// defaults to zero: a missing fee is not a zero fee, and an unconfirmed fill is
// not a confirmed zero.

/// How the account trades: a plain EOA, or a Predict Account (Kernel) wallet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountMode {
    Eoa,
    PredictAccount,
}

/// The account an order trades from, and who signs for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountIdentity {
    pub chain_id: u64,
    /// The address the venue holds positions and pays from.
    pub trading_address: String,
    pub mode: AccountMode,
    /// The key that signs. Equal to `trading_address` for an EOA.
    pub signer_address: String,
    pub venue: VenueId,
}

/// The stable identity of one order attempt. `local_attempt_id` is always
/// present; a hash and a venue id are added as they become known and never
/// replace each other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderIdentity {
    pub local_attempt_id: u64,
    pub signed_hash: Option<String>,
    pub venue_order_id: Option<String>,
    pub market_id: u64,
    pub token_id: String,
    pub side: Side,
    pub account: AccountIdentity,
}

/// Whether an authenticated request may be sent again after the venue refused
/// the token.
///
/// A read may be sent once more with a fresh token: a token isn't part of the
/// order. A write is never sent again, not even with the same hash: the venue
/// may already have taken it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayPolicy {
    /// A read may be sent once more with a fresh token.
    ReadOnceRefresh,
    /// A write is never sent again.
    NoReplay,
}

/// What the venue did with an order attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// The request provably never left.
    NotSubmitted,
    /// The venue took it; nothing about a fill follows from that.
    Accepted,
    /// The request may or may not have reached the venue.
    Unknown,
}

/// How far an attempt got. `MayHaveSubmitted` is the boundary a signed hash
/// crosses: past it, the order may have reached the venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptPhase {
    Prepared,
    MayHaveSubmitted,
    Submitted,
    Closed,
}

/// What the venue said when the order was sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Submission {
    /// The request never left, provably.
    NotSubmitted,
    /// The venue refused, and the order provably can't fill.
    RejectedFinal,
    /// The venue took the order. Nothing about a fill follows from that.
    Accepted,
    /// The request may or may not have reached the venue.
    Unknown,
}

/// How much of the order filled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quantity {
    Unknown,
    Known {
        gross_q_atoms: U256,
        class: QuantityClass,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantityClass {
    Zero,
    Partial,
    Full,
}

/// One accounting field: known with a value, or unknown with a reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Field<T> {
    Known(T),
    Unknown(String),
}

impl<T> Field<T> {
    pub fn is_known(&self) -> bool {
        matches!(self, Field::Known(_))
    }

    pub fn value(&self) -> Option<&T> {
        match self {
            Field::Known(v) => Some(v),
            Field::Unknown(_) => None,
        }
    }
}

/// What the fill actually cost, in collateral atoms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Accounting {
    /// Collateral paid for the filled shares, before fees.
    pub gross_cash: Field<U256>,
    /// Fee taken in collateral.
    pub collateral_fee: Field<U256>,
    /// Fee taken in shares.
    pub share_fee: Field<U256>,
    /// Cash after fees: `gross_cash + collateral_fee` for a buy,
    /// `gross_cash - collateral_fee` for a sell.
    pub net_cash: Field<U256>,
    /// Inventory after fees: `gross_q - share_fee` for a buy,
    /// `gross_q + share_fee` for a sell.
    pub net_inventory: Field<U256>,
    /// `gross_cash / gross_q`, or unknown when the quantity is zero.
    pub vwap: Field<U256>,
}

impl Accounting {
    /// Accounting is complete only when every field is known.
    pub fn is_complete(&self) -> bool {
        self.gross_cash.is_known()
            && self.collateral_fee.is_known()
            && self.share_fee.is_known()
            && self.net_cash.is_known()
            && self.net_inventory.is_known()
            && self.vwap.is_known()
    }
}

/// Whether the order can still fill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Closure {
    Open,
    /// Removal was asked for; the venue may still fill it.
    Removing,
    /// No proof yet that it can't fill.
    Unproven,
    /// Provably can't fill, with the evidence that says so.
    Final {
        evidence: String,
        final_gross_q: U256,
    },
}

/// One complete reading of an order's execution facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionSnapshot {
    pub identity: OrderIdentity,
    pub submission: Submission,
    pub quantity: Quantity,
    pub accounting: Accounting,
    /// Actual matched gross quantity covered by the known accounting fields.
    /// Independent of the newer authoritative order quantity during a scan race.
    pub accounting_gross_q: Option<U256>,
    pub closure: Closure,
    /// Monotonic per order hash. An older reading never overwrites a newer one.
    pub generation: u64,
    /// Every page of the venue's history was read, and the scan ended cleanly.
    pub pages_complete: bool,
    /// The venue's own cumulative filled amount, before and after the scan.
    pub gross_q_before: U256,
    pub gross_q_after: U256,
    /// Where the facts came from, for diagnostics.
    pub source: String,
    pub observed_at: DateTime<Utc>,
    /// The settlement transaction, when the venue named one.
    pub tx_hash: Option<String>,
}

impl ExecutionSnapshot {
    /// A verified zero has no VWAP, but still requires all cash/fee/net fields.
    pub fn accounting_complete(&self) -> bool {
        self.accounting.is_complete()
            || (self.gross_q() == Some(U256::ZERO)
                && self.accounting.gross_cash.value() == Some(&U256::ZERO)
                && self.accounting.collateral_fee.value() == Some(&U256::ZERO)
                && self.accounting.share_fee.value() == Some(&U256::ZERO)
                && self.accounting.net_cash.value() == Some(&U256::ZERO)
                && self.accounting.net_inventory.value() == Some(&U256::ZERO))
    }

    /// The gross quantity this reading adds, or unknown.
    pub fn gross_q(&self) -> Option<U256> {
        match self.quantity {
            Quantity::Known { gross_q_atoms, .. } => Some(gross_q_atoms),
            Quantity::Unknown => None,
        }
    }
}

/// A history-only position the venue's own official result pays zero.
///
/// Settlement cancels such a claim without a sell or a mint: the venue stops
/// listing the position while its last trade stays in the history. These are
/// the venue's own facts that say so; a pending or missing result is never here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettledZero {
    /// The settled outcome token.
    pub token: String,
    /// The market whose official result pays this token zero.
    pub market_id: u64,
    /// The oracle state the venue reported, e.g. `RESOLVED`.
    pub oracle_status: String,
    /// The matching state the venue reported, e.g. `CLOSED`.
    pub trading_status: String,
    /// The outcome's own status, e.g. `LOST`.
    pub outcome_status: String,
    /// This token's payout in millionths of a share: zero.
    pub payout_millionths: u32,
}

/// A history-only position the venue's own official result pays a positive
/// amount for, and whose cash the account already received.
///
/// A claimed winner leaves the same trace as a settled loser — the venue stops
/// listing the position while its last trade stays in the history — but its
/// payout lands in the collateral balance. These are the venue's own facts plus
/// the exact atoms that explain the cash increase; an official result that is
/// not confirmed, a token that doesn't belong to its market, or any amount that
/// is not atom-exact is never here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedWin {
    /// The claimed outcome token.
    pub token: String,
    /// The market whose official result pays this token.
    pub market_id: u64,
    /// The oracle state the venue reported, e.g. `RESOLVED`.
    pub oracle_status: String,
    /// The matching state the venue reported, e.g. `CLOSED`.
    pub trading_status: String,
    /// The outcome's own status, e.g. `WON`.
    pub outcome_status: String,
    /// This token's payout in millionths of a share: `1000000` for a win and
    /// `500000` for a tie.
    pub payout_millionths: u32,
    /// The inventory the history carries for this token, in atoms.
    pub inventory_atoms: U256,
    /// `inventory_atoms * payout`, floored exactly in atoms. This is the cash
    /// the claim must have delivered.
    pub claim_atoms: U256,
}

/// What the account looks like right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSnapshot {
    pub identity: AccountIdentity,
    /// Collateral balance, when it was read.
    pub collateral_atoms: Option<U256>,
    /// Native gas balance of the paying account, when it was read.
    pub gas_atoms: Option<U256>,
    /// Positions held, as token id → shares.
    pub positions: Vec<(String, U256)>,
    /// Closed, settled historical net inventory exactly explains startup positions.
    pub positions_explained: bool,
    /// History-only positions the venue's official result pays zero, with the
    /// venue facts that say so.
    pub settled_zero: Vec<SettledZero>,
    /// History-only positions the venue's official result pays a positive
    /// amount for, whose cash increase was verified atom-for-atom against the
    /// declared budget.
    pub claimed_win: Vec<ClaimedWin>,
    /// Orders the venue still lists as open.
    pub open_orders: Vec<String>,
    /// Every required read succeeded and every page was read to the end.
    pub complete: bool,
}

/// What one attempt is worth while it is open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptRecord {
    pub identity: OrderIdentity,
    /// The largest gross quantity this order could fill.
    pub request_gross_atoms: U256,
    /// The worst-case cash this order was reserved against.
    pub r0: U256,
    /// Shares of an existing position this attempt is selling.
    pub reserved_sell: U256,
    pub phase: AttemptPhase,
    pub submission: Submission,
    /// The last failure, as a stable reason token.
    pub last_reason: Option<String>,
}

/// The handle a caller holds across an attempt. The venue writes the hash and
/// the phase into it before the request goes out, so a failure after that point
/// can't lose the order's identity.
pub type AttemptHandle = Arc<Mutex<AttemptRecord>>;

/// The proven execution bound of a signed BUY in the controlled live domain.
///
/// Every field is a fact the venue's own order carries, or a bound derived
/// from it. There is no signature, no salt and no raw request here: this is
/// what a reviewer may check, not what the venue receives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedExecutionBound {
    pub account: AccountIdentity,
    pub market_id: u64,
    pub token_id: String,
    /// The exchange the order is routed to.
    pub route: String,
    /// The signed fee rate, in basis points.
    pub fee_rate_bps: u32,
    /// The gross shares the order asks for.
    pub request_gross_atoms: U256,
    /// The cash the order pays.
    pub gross_cash_atoms: U256,
    /// The most cash the order can take. For a proven share-fee BUY this is
    /// the signed cash itself: the fee comes out of the received shares.
    pub max_cash_atoms: U256,
    /// The fewest net shares the order can deliver.
    pub min_net_shares_atoms: U256,
    /// The order's expiry, in unix seconds.
    pub expiration: i64,
    /// The hash of the signed order.
    pub signed_hash: String,
}

/// A guarded order the venue refused because the frozen decision no longer
/// stands while the request provably never left: the fresh book moved past the
/// candidate's limit or cannot cover the frozen quantity, or the frozen inputs
/// (book/spot age, edge, remaining round time) aged out at the last check after
/// signing but before the send permission and the first transport poll.
///
/// It is a refusal of one candidate, not of the run: the cash the attempt
/// reserved is released, the refusal is recorded with the candidate's evidence,
/// and the risk window stays open for the next decision. Any doubt about
/// whether the order left — and any account, identity, quantity, bound, fee or
/// evidence mismatch — is never this error; those stay hard stops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CandidateRefused;

impl std::fmt::Display for CandidateRefused {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The closed public reason token: mapping this error never yields a
        // free-form string.
        formatter.write_str("INPUT_CHANGED")
    }
}

impl std::error::Error for CandidateRefused {}

/// The narrow seam a controlled live run installs around a real submission.
///
/// Every method refuses by default: a venue that ignores this hook must not
/// be able to place a guarded order, and a guarded order must not be placeable
/// through the ordinary path.
pub trait ControlledSendGuard: Send + Sync {
    /// Stop preparation immediately after any potentially suspending step.
    fn check_preparation(&self) -> Result<()> {
        Err(VenueError::new(VenueErrorCode::ExecutionNotEnabled,
            "controlled_send", "EXECUTION_NOT_ENABLED").into())
    }

    /// Check that the venue's refreshed book is usable, not that it is
    /// unchanged.
    ///
    /// The refreshed book is a new observation of a moving market: requiring
    /// it to repeat the evaluation's snapshot byte for byte refuses every
    /// honest five-minute round. What the frozen decision needs is a complete
    /// book; its price and depth protection is carried by the candidate's
    /// limit, the amount walk and the signed bound.
    fn check_book(&self, _book: &BookSnapshot) -> Result<()> {
        Err(VenueError::new(VenueErrorCode::ExecutionNotEnabled,
            "controlled_send", "EXECUTION_NOT_ENABLED").into())
    }

    /// The receipt-bound account snapshot the controlled send must prepare from.
    ///
    /// A controlled send reuses the read that preceded its decision's inputs:
    /// the gate proves that receipt's completeness, identity, generation,
    /// attempt binding and ten-second freshness at the final check, so a second
    /// full account read here would only age the book the decision is frozen
    /// on. A guard that cannot bind such a read refuses the send.
    fn account_snapshot(&self) -> Result<AccountSnapshot> {
        Err(VenueError::new(VenueErrorCode::ExecutionNotEnabled,
            "controlled_send", "EXECUTION_NOT_ENABLED").into())
    }

    /// Check the prepared facts against the frozen candidate.
    fn check_facts(&self, _bound: &SignedExecutionBound) -> Result<()> {
        Err(VenueError::new(
            VenueErrorCode::ExecutionNotEnabled,
            "controlled_send",
            "EXECUTION_NOT_ENABLED",
        )
        .into())
    }

    /// Wait until the critical order's evidence is durable.
    fn await_durable_ack(&self, _bound: &SignedExecutionBound) -> Result<()> {
        Err(VenueError::new(
            VenueErrorCode::ExecutionNotEnabled,
            "controlled_send",
            "EXECUTION_NOT_ENABLED",
        )
        .into())
    }

    /// Run the first nonblocking transport poll under the owning State lock.
    /// The callback acquires only the attempt handle and AccountGate, in that order.
    /// Default refusal prevents a facts-only guard from authorizing a POST.
    fn with_send_permit(&self, _handle: &AttemptHandle,
        _first_poll: &mut dyn FnMut(&mut AttemptRecord, &mut dyn FnMut() -> Result<()>) -> Result<()>) -> Result<()> {
        Err(VenueError::new(VenueErrorCode::ExecutionNotEnabled,
            "controlled_send", "EXECUTION_NOT_ENABLED").into())
    }

    /// The last synchronous permission to hand the order to the transport.
    fn permit_send(&self) -> Result<()> {
        Err(VenueError::new(
            VenueErrorCode::ExecutionNotEnabled,
            "controlled_send",
            "EXECUTION_NOT_ENABLED",
        )
        .into())
    }

    /// Report one send check's outcome without granting anything.
    ///
    /// The default does nothing: a guard that does not measure never invents
    /// a measurement. The notification carries no permission and changes no
    /// decision; it only records where a check ran and what it found. The
    /// guard completes the measurement itself, because only it owns the
    /// clock the check ran on.
    fn note_send_check(&self, _stage: SendCheckStage, _detail: SendCheckDetail) {}
}

/// The boundary at which a send check ran.
///
/// The stage is where the check happened, never a guess about the last
/// successful step: it names the original branch that produced the outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendCheckStage {
    Prepare,
    SignedBound,
    Facts,
    Ack,
    SendPermit,
    Post,
}

impl SendCheckStage {
    /// The wire token of this stage.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prepare => "PREPARE",
            Self::SignedBound => "SIGNED_BOUND",
            Self::Facts => "FACTS",
            Self::Ack => "ACK",
            Self::SendPermit => "SEND_PERMIT",
            Self::Post => "POST",
        }
    }

    /// Every stage, in the order the contract lists them.
    pub const ALL: &'static [SendCheckStage] = &[
        Self::Prepare,
        Self::SignedBound,
        Self::Facts,
        Self::Ack,
        Self::SendPermit,
        Self::Post,
    ];
}

/// The closed set of reasons a send check can name.
///
/// The tokens are diagnostics only: they never enter the stop reason set and
/// never change what the original guard decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendCheckDetail {
    None,
    Unknown,
    BookStale,
    SpotStale,
    AccountStale,
    FrozenInput,
    LimitPrice,
    Depth,
    RoundTooLate,
    EdgeBelowMin,
    EmptyBook,
    AccountIncomplete,
    IdentityMismatch,
    GenerationMismatch,
    AttemptMismatch,
    SignedBoundUnproven,
    RunStopped,
    AckUnavailable,
    ReadUnavailable,
    VenueRejected,
    SubmissionUnknown,
}

impl SendCheckDetail {
    /// The wire token of this detail.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Unknown => "UNKNOWN",
            Self::BookStale => "BOOK_STALE",
            Self::SpotStale => "SPOT_STALE",
            Self::AccountStale => "ACCOUNT_STALE",
            Self::FrozenInput => "FROZEN_INPUT",
            Self::LimitPrice => "LIMIT_PRICE",
            Self::Depth => "DEPTH",
            Self::RoundTooLate => "ROUND_TOO_LATE",
            Self::EdgeBelowMin => "EDGE_BELOW_MIN",
            Self::EmptyBook => "EMPTY_BOOK",
            Self::AccountIncomplete => "ACCOUNT_INCOMPLETE",
            Self::IdentityMismatch => "IDENTITY_MISMATCH",
            Self::GenerationMismatch => "GENERATION_MISMATCH",
            Self::AttemptMismatch => "ATTEMPT_MISMATCH",
            Self::SignedBoundUnproven => "SIGNED_BOUND_UNPROVEN",
            Self::RunStopped => "RUN_STOPPED",
            Self::AckUnavailable => "ACK_UNAVAILABLE",
            Self::ReadUnavailable => "READ_UNAVAILABLE",
            Self::VenueRejected => "VENUE_REJECTED",
            Self::SubmissionUnknown => "SUBMISSION_UNKNOWN",
        }
    }

    /// Every detail, in the order the contract lists them.
    pub const ALL: &'static [SendCheckDetail] = &[
        Self::None,
        Self::Unknown,
        Self::BookStale,
        Self::SpotStale,
        Self::AccountStale,
        Self::FrozenInput,
        Self::LimitPrice,
        Self::Depth,
        Self::RoundTooLate,
        Self::EdgeBelowMin,
        Self::EmptyBook,
        Self::AccountIncomplete,
        Self::IdentityMismatch,
        Self::GenerationMismatch,
        Self::AttemptMismatch,
        Self::SignedBoundUnproven,
        Self::RunStopped,
        Self::AckUnavailable,
        Self::ReadUnavailable,
        Self::VenueRejected,
        Self::SubmissionUnknown,
    ];
}

/// One measured send check, with the ages it was taken against.
///
/// Every field is a number or an enum: no raw error, no HTTP body, no
/// account detail. A missing measurement is `None`, never a zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendCheckObservation {
    /// Where the check ran.
    pub stage: SendCheckStage,
    /// What the check found.
    pub detail: SendCheckDetail,
    /// The check's own wall clock reading.
    pub checked_at: DateTime<Utc>,
    /// The check's own elapsed reading, on this run's origin.
    pub checked_elapsed_ms: u64,
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

impl SendCheckObservation {
    /// A check whose only known facts are where it ran and what it found.
    ///
    /// The clocks and the ages are unknown until a real reading supplies them;
    /// a diagnostic never invents a measurement.
    pub fn new(stage: SendCheckStage, detail: SendCheckDetail) -> Self {
        Self {
            stage,
            detail,
            checked_at: DateTime::<Utc>::UNIX_EPOCH,
            checked_elapsed_ms: 0,
            tick_started_elapsed_ms: None,
            book_receipt_age_ms: None,
            spot_receipt_age_ms: None,
            account_receipt_age_ms: None,
            book_request_age_ms: None,
            spot_request_age_ms: None,
        }
    }
}

/// The claim inventory of this run's closed attempts, as the broker knows it.
///
/// The broker builds this under its own state lock from attempts that are
/// closed, fully applied and no longer reserved. It carries no cash and no
/// official result: it names what may still be claimed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeClaimInventory {
    /// The attempt this inventory belongs to.
    pub identity: OrderIdentity,
    /// The execution generation the attempt's facts were read at.
    pub execution_generation: u64,
    /// The net inventory the attempt still holds.
    pub inventory_atoms: U256,
}

/// The evidence that one bound order's claim actually arrived.
///
/// It is a fact the account's own read proves, not a claim about cash: the
/// adapter may produce it, but only the broker's own reconciliation can turn
/// it into ledger cash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundClaimEvidence {
    /// The attempt this evidence belongs to.
    pub identity: OrderIdentity,
    /// The execution generation the read that produced it belongs to.
    pub execution_generation: u64,
    /// The outcome token the claim paid.
    pub yes_token: String,
    /// The complementary outcome token of the same market.
    pub no_token: String,
    /// The inventory the claim consumed, in atoms.
    pub inventory_atoms: U256,
    /// The cash the claim delivered, in atoms.
    pub claim_atoms: U256,
    /// The official payout of the claim, in millionths of a share.
    pub payout_millionths: u32,
}

/// One account read plus the claim evidence the same read proved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeAccountObservation {
    /// The account's own facts.
    pub snapshot: AccountSnapshot,
    /// The claim evidence the same read proved.
    pub bound_claims: Vec<BoundClaimEvidence>,
}

/// What the controlled domain recorded about a signed order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlledOrderMetadata {
    /// The venue's own removal lock, when it named one.
    pub removal_locked_until: Option<DateTime<Utc>>,
    /// The execution generation this metadata belongs to.
    pub execution_generation: u64,
    /// Whether the chain proof for this generation was obtained.
    pub chain_settled: bool,
    /// When the chain proof was checked.
    pub settlement_checked_at: Option<DateTime<Utc>>,
}

/// A structured failure. `code` is stable; `reason` is a stable token, never a
/// raw error, so nothing sensitive can leak through it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueError {
    pub code: VenueErrorCode,
    pub step: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueErrorCode {
    InvalidInput,
    ExecutionNotEnabled,
    AccountMismatch,
    ReadUnavailable,
    FactIncomplete,
    SubmissionUnknown,
    ReconciliationRequired,
    TxUnconfirmed,
    SdkIncompatible,
    BudgetUnconfirmed,
}

impl VenueError {
    pub fn new(code: VenueErrorCode, step: &str, reason: &str) -> Self {
        Self {
            code,
            step: step.to_string(),
            reason: reason.to_string(),
        }
    }
}

impl std::fmt::Display for VenueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} at {}: {}", self.code, self.step, self.reason)
    }
}

impl std::error::Error for VenueError {}

/// What a venue did with a [`PlannedOrder`].
#[derive(Debug, Clone)]
pub struct OrderTicket {
    pub token_id: String,
    pub maker_amount: String,
    pub taker_amount: String,
    /// `None` until the venue has accepted the order.
    pub order_id: Option<String>,
    /// Shares the venue confirmed filled. `None` when the venue doesn't report
    /// fills synchronously, in which case callers assume the planned size.
    pub filled_shares: Option<f64>,
    /// The hash of the signed order, once it exists.
    pub signed_hash: Option<String>,
    /// The attempt this ticket belongs to.
    pub local_attempt_id: Option<u64>,
    /// What the venue actually reports about the fill. A live venue always
    /// fills this in, even when the facts are unknown.
    pub execution: Option<ExecutionSnapshot>,
}

#[async_trait]
pub trait OrderVenue: Send + Sync {
    fn id(&self) -> VenueId;

    /// Worst-case collateral atoms required before reserving a Predict attempt.
    /// Other venues keep their existing sizing and do not call this method.
    fn reservation_atoms(&self, _planned: &PlannedOrder) -> Result<U256> {
        Err(VenueError::new(
            VenueErrorCode::ReadUnavailable,
            "reservation",
            "UNSUPPORTED_VENUE",
        )
        .into())
    }

    /// The worst-case cash a controlled live BUY must reserve.
    ///
    /// Only a venue that has proven the BUY's fee domain may answer this: the
    /// default refuses, so an ordinary reservation never borrows the live
    /// domain's tighter bound.
    fn controlled_reservation_atoms(&self, _planned: &PlannedOrder) -> Result<U256> {
        Err(VenueError::new(
            VenueErrorCode::ExecutionNotEnabled,
            "controlled_reservation",
            "EXECUTION_NOT_ENABLED",
        )
        .into())
    }

    /// Freeze the venue's actual signing route before any preparation await.
    fn controlled_route(&self, _token_id: &str) -> Result<String> {
        Err(VenueError::new(VenueErrorCode::ExecutionNotEnabled,
            "controlled_route", "SIGNED_BOUND_UNPROVEN").into())
    }

    /// What the controlled domain recorded about a signed order.
    fn controlled_order_metadata(&self, _signed_hash: &str) -> Option<ControlledOrderMetadata> {
        None
    }

    /// The signed expiry of one controlled order, in unix seconds.
    ///
    /// This is a read-only projection of the order's own signed bound: the
    /// default answers `None`, so an ordinary venue never gains a lifetime it
    /// cannot prove. A venue that answers `Some` has proven that the order
    /// named by `identity` carries exactly this expiry; it is not a fresh
    /// reading of the venue and never proves anything about a fill.
    ///
    /// The answer is not a permission and never causes a request: an order
    /// whose expiry cannot be proven keeps the runtime's existing budget.
    fn controlled_order_expiration(&self, _identity: &OrderIdentity) -> Result<Option<i64>> {
        Ok(None)
    }

    /// Read the account once and report the claim evidence the same read proved.
    ///
    /// The default delegates to the venue's own account read and reports no
    /// evidence at all: a venue that cannot prove a claim must never let the
    /// broker treat a balance change as one.
    async fn runtime_account_observation(
        &self,
        _closed: &[RuntimeClaimInventory],
    ) -> Result<RuntimeAccountObservation> {
        Ok(RuntimeAccountObservation {
            snapshot: self.account_snapshot().await?,
            bound_claims: Vec::new(),
        })
    }

    /// Build and sign `planned`. Transmits it only when `live` is true, so a
    /// dry run exercises everything up to the wire.
    async fn place(
        &self,
        planned: &PlannedOrder,
        expiration_secs: u64,
        live: bool,
    ) -> Result<OrderTicket>;

    /// Place `planned` under a controlled live guard.
    ///
    /// The guard is not optional here: a venue that implements the controlled
    /// domain routes the same preparation, signing and single POST through
    /// it, and a venue that doesn't refuses the call outright.
    async fn place_controlled_tracked(
        &self,
        _planned: &PlannedOrder,
        _expiration_secs: u64,
        _live: bool,
        _handle: AttemptHandle,
        _guard: &dyn ControlledSendGuard,
    ) -> Result<OrderTicket> {
        Err(VenueError::new(
            VenueErrorCode::ExecutionNotEnabled,
            "controlled_place",
            "EXECUTION_NOT_ENABLED",
        )
        .into())
    }

    /// Place `planned` while keeping the attempt's identity alive across the
    /// call. Venues that don't track attempts delegate to [`OrderVenue::place`].
    async fn place_tracked(
        &self,
        planned: &PlannedOrder,
        expiration_secs: u64,
        live: bool,
        handle: AttemptHandle,
    ) -> Result<OrderTicket> {
        let _ = handle;
        self.place(planned, expiration_secs, live).await
    }

    /// Stop all new account risk when a consumer detects conflicting ledger facts.
    fn record_account_reconciliation(&self, _reason: &str) {}

    /// Read the execution facts of an order the venue already has.
    async fn execution_snapshot(&self, _signed_hash: &str) -> Result<ExecutionSnapshot> {
        Err(VenueError::new(
            VenueErrorCode::ReadUnavailable,
            "execution_snapshot",
            "READ_UNAVAILABLE",
        )
        .into())
    }

    /// Read the account's balances, positions and open orders.
    async fn account_snapshot(&self) -> Result<AccountSnapshot> {
        Err(VenueError::new(
            VenueErrorCode::ReadUnavailable,
            "account_snapshot",
            "READ_UNAVAILABLE",
        )
        .into())
    }
}

/// The official facts about one round's outcome.
///
/// These are the venue's own words, carried through unchanged. A missing fact
/// stays missing: it is never inferred from a local price or a start price.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RoundFacts {
    /// The oracle's state, e.g. `RESOLVED`.
    pub oracle_status: Option<String>,
    /// The matching state, e.g. `OPEN` or `CLOSED`.
    pub trading_status: Option<String>,
    /// The first outcome's status, e.g. `WON` or `LOST`.
    pub yes_status: Option<String>,
    /// The second outcome's status.
    pub no_status: Option<String>,
    /// The number of price decimals the market accepts, when stated.
    pub decimal_precision: Option<u32>,
    /// The official price feed, e.g. `CHAINLINK`. Unknown stays unknown.
    pub price_feed_provider: Option<String>,
}

/// A market as the strategy bots see it.
#[derive(Debug, Clone, Default)]
pub struct TradableMarket {
    pub id: u64,
    pub question: String,
    pub category_slug: String,
    pub variant: String,
    pub yes_token: String,
    pub no_token: String,
    pub yes_name: String,
    pub no_name: String,
    /// Accepting orders now: open for matching and not past its end.
    pub open: bool,
    /// Under an oracle dispute or paused, whatever the book says.
    pub contested: bool,
    pub tick: f64,
    pub fee_rate_bps: u32,
    pub starts_at: Option<DateTime<Utc>>,
    pub ends_at: Option<DateTime<Utc>>,
    pub start_price: Option<f64>,
    pub end_price: Option<f64>,
    pub feed_symbol: Option<String>,
    pub polymarket_condition_ids: Vec<String>,
    /// What one Yes share pays, once that's known.
    pub payout_yes: Option<f64>,
    /// Category and tags, when they came along with the market.
    pub labels: Option<(Option<String>, Vec<String>)>,
    /// The venue's own round facts, when the market carried them.
    pub round_facts: Option<RoundFacts>,
}

impl TradableMarket {
    /// Taker fee per share at `price`. Makers pay none.
    pub fn taker_fee(&self, price: f64) -> f64 {
        f64::from(self.fee_rate_bps) / 10_000.0 * price.min(1.0 - price).max(0.0)
    }

    pub fn secs_left(&self, now: DateTime<Utc>) -> Option<i64> {
        self.ends_at.map(|end| (end - now).num_seconds())
    }

    pub fn is_yes(&self, token: &str) -> bool {
        self.yes_token == token
    }
}

#[derive(Debug, Clone, Default)]
pub struct MarketQuery {
    /// Those variants, with their schedules. Empty means the most traded markets
    /// of any variant, without schedules: see [`MarketData::with_schedule`].
    pub variants: Vec<String>,
    pub max_pages: u32,
}

/// Read side the strategy bots share. Books are quoted for the Yes outcome.
#[async_trait]
pub trait MarketData: Send + Sync {
    async fn markets(&self, query: &MarketQuery) -> Result<Vec<TradableMarket>>;
    async fn market(&self, id: u64) -> Result<TradableMarket>;
    /// Authoritative result read; adapters must preserve required auxiliary read errors.
    async fn settlement_market(&self, id: u64) -> Result<TradableMarket> {
        self.market(id).await
    }
    /// `market` with its end time, category and tags filled in. A scan leaves these
    /// out so that it doesn't need a request per market.
    async fn with_schedule(&self, market: &TradableMarket) -> Result<TradableMarket>;
    async fn books(&self, ids: &[u64]) -> Result<HashMap<u64, BookSnapshot>>;
    /// The same books, with the clock that received each batch.
    ///
    /// A venue that cannot say when it received a book does not implement this:
    /// the default refuses, so a cached book is never presented as a fresh one.
    async fn observed_books(
        &self,
        _ids: &[u64],
        _clock: &dyn crate::service::observation::Clock,
    ) -> Result<HashMap<u64, crate::service::observation::ObservedBook>> {
        bail!("DATA_INCOMPLETE: this venue cannot report when its books arrived")
    }
}

#[derive(Debug, Clone)]
pub struct LimitOrder {
    pub market_id: u64,
    pub token_id: String,
    pub side: Side,
    pub shares: f64,
    pub price: f64,
    pub ttl_secs: u64,
    pub post_only: bool,
}

#[derive(Debug, Clone)]
pub struct PostedOrder {
    pub order_hash: String,
    /// `None` on a dry run, where nothing reached the venue.
    pub order_id: Option<String>,
    /// The attempt this order belongs to.
    pub local_attempt_id: Option<u64>,
    /// What the venue reports about the fill, once it knows.
    pub execution: Option<ExecutionSnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoveResult {
    pub removed: Vec<String>,
    /// Already filled, removed or cancelled.
    pub noop: Vec<String>,
    /// Inside the market's removal lock; retry later.
    pub rejected: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RestingState {
    pub open: bool,
    pub filled_shares: f64,
    /// The venue's own execution facts, when it has any.
    pub execution: Option<ExecutionSnapshot>,
}

#[derive(Debug, Clone)]
pub struct HeldPosition {
    pub market: TradableMarket,
    pub token_id: String,
    pub shares: f64,
    pub avg_price: f64,
}

/// Resting orders, on top of the taker path in [`OrderVenue`].
#[async_trait]
pub trait LimitVenue: Send + Sync {
    /// Build and sign `order`. Transmits it only when `live` is true.
    async fn post_limit(&self, order: &LimitOrder, live: bool) -> Result<PostedOrder>;

    /// Post `order` while keeping the attempt's identity alive across the call.
    /// Venues that don't track attempts delegate to [`LimitVenue::post_limit`].
    async fn post_limit_tracked(
        &self,
        order: &LimitOrder,
        live: bool,
        handle: AttemptHandle,
    ) -> Result<PostedOrder> {
        let _ = handle;
        self.post_limit(order, live).await
    }

    /// Take orders off the book. The venue says this doesn't cancel them on-chain.
    async fn remove(&self, order_ids: &[String]) -> Result<RemoveResult>;
    async fn order_state(&self, order_hash: &str) -> Result<RestingState>;
    async fn positions(&self) -> Result<Vec<HeldPosition>>;
}
