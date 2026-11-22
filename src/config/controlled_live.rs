//! The explicit live contract of a controlled 07-007 run.
//!
//! A live run is never reached by accident. The launcher names the contract,
//! the operations mode, the risk window, the authorization reference and the
//! deadline; the run's public activation file names the account, the storage
//! binding and the gas policy. Both are checked before any secret is read,
//! and neither is a default: without the whole set the old entry points behave
//! exactly as they did before.

use crate::config::ControlledRun;
use alloy_primitives::U256;
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::HashMap;

/// The only strategy contract this domain trades.
pub const LIVE_CONTRACT: &str = "phase1_net_shares";
/// The recovery policy a controlled live run declares.
pub const RECOVERY_POLICY: &str = "COLD_START_REFUSE";
/// The cash recovery policy a controlled live run declares.
pub const CASH_RECOVERY_POLICY: &str = "MANUAL_PAUSE";
/// The activation file's schema version.
pub const ACTIVATION_SCHEMA: &str = "btc5m-live-activation/1";

/// How the run is staffed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationsMode {
    /// A human is watching and takes the decisions.
    Supervised,
    /// The run is on its own inside a bounded window.
    Unattended,
}

impl OperationsMode {
    /// The positive, inclusive authorization window for this staffing mode.
    pub fn allows_duration(self, duration: chrono::Duration) -> bool {
        let maximum = match self {
            Self::Supervised => 21_600,
            Self::Unattended => 86_400,
        };
        duration > chrono::Duration::zero() && duration <= chrono::Duration::seconds(maximum)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            OperationsMode::Supervised => "supervised",
            OperationsMode::Unattended => "unattended",
        }
    }
}

/// The gas policy of one approved deployment.
///
/// The payer is the account the chain read already knows; no other payer is
/// reachable, and no RPC path is added for one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GasPolicy {
    pub payer_address: String,
    pub minimum_balance_atoms: U256,
    pub maximum_consumption_atoms: U256,
}

/// The public, secret-free context of a controlled live run.
///
/// It comes from the command line and the activation file. It grants no
/// permission of its own: it names the window the run belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlledLiveOptions {
    /// The public account binding read back from the parent's manifest.
    pub account_identity: Option<ActivationAccountIdentity>,
    /// The strategy contract. Always [`LIVE_CONTRACT`] here.
    pub contract: String,
    pub operations_mode: OperationsMode,
    pub risk_window_id: String,
    pub authorization_ref: String,
    pub deadline_at: DateTime<Utc>,
    pub budget_secs: i64,
    /// The public money and duration frozen for this run. Legacy launches keep
    /// the historical values; a parameterized launch carries its own.
    pub params: super::RunParameters,
    pub gas_policy: GasPolicy,
}

/// Whether a short public identifier is well formed.
///
/// Same syntax as the controlled run id: lowercase letters, digits, dash and
/// underscore, and never a path.
fn valid_public_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
        && !value.starts_with('-')
}

/// Whether an address is a strict `0x` plus forty hex characters.
fn valid_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value[2..].chars().all(|c| c.is_ascii_hexdigit())
}

impl ControlledLiveOptions {
    /// Build the options from the launcher's public arguments.
    ///
    /// All five belong together: a half-specified live run is refused rather
    /// than half assembled, and the contract is fixed rather than chosen.
    pub fn from_arguments(
        contract: Option<&str>,
        operations_mode: Option<&str>,
        risk_window_id: Option<&str>,
        authorization_ref: Option<&str>,
        deadline_at: Option<&str>,
        started_at: &str,
    ) -> Result<Option<Self>> {
        let given = [
            contract.is_some(),
            operations_mode.is_some(),
            risk_window_id.is_some(),
            authorization_ref.is_some(),
            deadline_at.is_some(),
        ];
        if !given.iter().any(|present| *present) {
            return Ok(None);
        }
        if !given.iter().all(|present| *present) {
            bail!("CONTROLLED_MODE: the live arguments belong together");
        }
        let contract = contract.expect("the contract is present");
        if contract != LIVE_CONTRACT {
            bail!("CONTROLLED_MODE: the live contract must be {LIVE_CONTRACT}");
        }
        let operations_mode = match operations_mode.expect("the mode is present") {
            "supervised" => OperationsMode::Supervised,
            "unattended" => OperationsMode::Unattended,
            _ => bail!("CONTROLLED_MODE: the operations mode must be supervised or unattended"),
        };
        let risk_window_id = risk_window_id.expect("the window is present");
        if !valid_public_id(risk_window_id) {
            bail!("CONTROLLED_MODE: the risk window id must be a short public identifier");
        }
        let authorization_ref = authorization_ref.expect("the authorization is present");
        if !valid_public_id(authorization_ref) {
            bail!("CONTROLLED_MODE: the authorization reference must be a short public identifier");
        }
        let started_at = parse_utc(started_at)?;
        let deadline_at = parse_utc(deadline_at.expect("the deadline is present"))?;
        let budget = deadline_at - started_at;
        let budget_secs = budget.num_seconds();
        if !operations_mode.allows_duration(budget) {
            bail!("CONTROLLED_MODE: the live budget must be between zero and the maximum");
        }
        Ok(Some(Self {
            account_identity: None,
            contract: contract.to_string(),
            operations_mode,
            risk_window_id: risk_window_id.to_string(),
            authorization_ref: authorization_ref.to_string(),
            deadline_at,
            budget_secs,
            params: super::RunParameters::legacy(),
            gas_policy: GasPolicy {
                payer_address: String::new(),
                minimum_balance_atoms: U256::ZERO,
                maximum_consumption_atoms: U256::ZERO,
            },
        }))
    }

    /// Attach the gas policy of the approved deployment.
    pub fn with_gas_policy(mut self, gas_policy: GasPolicy) -> Self {
        self.gas_policy = gas_policy;
        self
    }

    /// Whether the run may still add risk.
    pub fn in_window(&self, now: DateTime<Utc>) -> bool {
        now < self.deadline_at
    }
}

/// Parse one UTC timestamp.
fn parse_utc(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| anyhow::anyhow!("CONTROLLED_MODE: the timestamp must be RFC 3339 UTC"))
}

/// The fixed account a controlled live run trades.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationAccountIdentity {
    pub venue: String,
    pub chain_id: u64,
    pub account_mode: String,
    pub trading_address: String,
    pub signer_address: String,
}

impl ActivationAccountIdentity {
    /// The one account this domain trades.
    pub fn check(&self) -> Result<()> {
        if self.venue != "PREDICT_FUN" || self.chain_id != 56 {
            bail!("CONTROLLED_MODE: the account must be the fixed Predict.fun chain 56 account");
        }
        if self.account_mode != "predict-account" {
            bail!("CONTROLLED_MODE: the account must be a predict-account");
        }
        if !valid_address(&self.trading_address) {
            bail!("CONTROLLED_MODE: the trading address must be 0x plus forty hex characters");
        }
        if !valid_address(&self.signer_address) {
            bail!("CONTROLLED_MODE: the signer address must be 0x plus forty hex characters");
        }
        Ok(())
    }
}

/// The one Mongo deployment a controlled live run may use.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageBinding {
    pub deployment_id: String,
    pub database: String,
    pub collection: String,
    pub credential_env: String,
    pub endpoint_id: EndpointId,
}

/// The shape of the storage endpoint, compared exactly against the URI.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointId {
    pub scheme: String,
    pub hosts: Vec<String>,
    pub replica_set: Option<String>,
    pub tls: bool,
}

/// The public activation file of an approved deployment.
///
/// It is created by a human approval and read as-is. It carries no secret and
/// grants no permission: the external execution and the risk confirmation are
/// what authorize a run.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveActivation {
    pub schema_version: String,
    pub authorization_ref: String,
    pub risk_window_id: String,
    pub operations_mode: String,
    pub account_identity: ActivationAccountIdentity,
    pub not_before: DateTime<Utc>,
    pub deadline_at: DateTime<Utc>,
    pub candidate_digest: String,
    pub storage_binding: StorageBinding,
    pub gas_policy: ActivationGasPolicy,
    pub takeover_ref: String,
}

/// The gas policy as the activation file states it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationGasPolicy {
    pub payer_address: String,
    pub minimum_balance_atoms: String,
    pub maximum_consumption_atoms: String,
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

impl LiveActivation {
    /// Read and check the activation file.
    ///
    /// Every field is checked before anything else happens: an unknown
    /// version, a changed identity, a broken digest or a gas policy that names
    /// a payer other than the signer account all refuse the run.
    pub fn parse(raw: &str) -> Result<Self> {
        let activation: LiveActivation = serde_json::from_str(raw)
            .map_err(|_| anyhow::anyhow!("CONTROLLED_MODE: the activation file is not readable"))?;
        if activation.schema_version != ACTIVATION_SCHEMA {
            bail!("CONTROLLED_MODE: the activation schema version is not supported");
        }
        if !valid_public_id(&activation.authorization_ref)
            || !valid_public_id(&activation.risk_window_id)
            || !valid_public_id(&activation.takeover_ref)
            || !valid_public_id(&activation.storage_binding.deployment_id)
        {
            bail!("CONTROLLED_MODE: the activation identifiers must be short public identifiers");
        }
        if !valid_digest(&activation.candidate_digest) {
            bail!("CONTROLLED_MODE: the candidate digest must be sixty-four lowercase hex characters");
        }
        activation.account_identity.check()?;
        let minimum = parse_atoms(&activation.gas_policy.minimum_balance_atoms)?;
        let maximum = parse_atoms(&activation.gas_policy.maximum_consumption_atoms)?;
        if minimum.is_zero() || maximum.is_zero() {
            bail!("CONTROLLED_MODE: the gas budgets must be positive");
        }
        if !valid_address(&activation.gas_policy.payer_address) {
            bail!("CONTROLLED_MODE: the gas payer must be 0x plus forty hex characters");
        }
        if activation.gas_policy.payer_address != activation.account_identity.signer_address {
            bail!("CONTROLLED_MODE: the gas payer must be the signer account");
        }
        if activation.storage_binding.endpoint_id.scheme != "mongodb" {
            bail!("CONTROLLED_MODE: the storage scheme must be mongodb");
        }
        if activation.storage_binding.endpoint_id.hosts.is_empty() {
            bail!("CONTROLLED_MODE: the storage endpoint must name its hosts");
        }
        if activation.deadline_at <= activation.not_before {
            bail!("CONTROLLED_MODE: the activation deadline must be after its start");
        }
        Ok(activation)
    }

    /// The gas policy, in atoms.
    pub fn gas_policy(&self) -> Result<GasPolicy> {
        Ok(GasPolicy {
            payer_address: self.gas_policy.payer_address.clone(),
            minimum_balance_atoms: parse_atoms(&self.gas_policy.minimum_balance_atoms)?,
            maximum_consumption_atoms: parse_atoms(&self.gas_policy.maximum_consumption_atoms)?,
        })
    }

    /// The operations mode, as the activation file states it.
    pub fn operations_mode(&self) -> Result<OperationsMode> {
        match self.operations_mode.as_str() {
            "supervised" => Ok(OperationsMode::Supervised),
            "unattended" => Ok(OperationsMode::Unattended),
            _ => bail!("CONTROLLED_MODE: the activation operations mode is not supported"),
        }
    }
}

/// Parse a decimal atom amount.
fn parse_atoms(raw: &str) -> Result<U256> {
    if raw.is_empty() || !raw.chars().all(|c| c.is_ascii_digit()) {
        bail!("CONTROLLED_MODE: an atom amount must be decimal digits");
    }
    U256::from_str_radix(raw, 10).map_err(|_| anyhow::anyhow!("CONTROLLED_MODE: the atom amount does not fit"))
}

/// The parent's secret-free, strictly typed startup readback.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveContext {
    pub schema_version: String,
    pub run_id: String,
    pub risk_window_id: String,
    pub authorization_ref: String,
    pub operations_mode: String,
    pub account_identity: ActivationAccountIdentity,
    pub started_at: DateTime<Utc>,
    pub deadline_at: DateTime<Utc>,
    pub candidate_digest: String,
    pub asset_units: AssetUnits,
    pub gas_policy: ActivationGasPolicy,
    pub recovery_policy: String,
    pub cash_recovery_policy: String,
}

/// Units of the original chain-56 adapter, never caller-selected scales.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetUnits {
    pub collateral_symbol: String,
    pub collateral_decimals: u8,
    pub share_decimals: u8,
    pub gas_symbol: String,
    pub gas_decimals: u8,
}

impl ControlledRun {
    /// Construct only the explicitly selected live domain; legacy budgets stay unchanged.
    pub fn from_live_arguments(
        mode: Option<&str>,
        run_id: Option<&str>,
        started_at: Option<&str>,
        deadline_at: Option<&str>,
        options: ControlledLiveOptions,
    ) -> Result<Self> {
        if mode != Some("signed-future-live") {
            bail!("CONTROLLED_MODE: LIVE_OPTION_REFUSED");
        }
        let run_id = run_id.filter(|value| valid_public_id(value))
            .ok_or_else(|| anyhow::anyhow!("CONTROLLED_MODE: RUN_ID"))?;
        let started_at = parse_utc(started_at.ok_or_else(|| anyhow::anyhow!("CONTROLLED_MODE: START"))?)?;
        let deadline_at = parse_utc(deadline_at.ok_or_else(|| anyhow::anyhow!("CONTROLLED_MODE: DEADLINE"))?)?;
        let budget = deadline_at.signed_duration_since(started_at);
        if !options.operations_mode.allows_duration(budget)
            || deadline_at != options.deadline_at
            || budget.num_seconds() != options.budget_secs
        {
            bail!("CONTROLLED_MODE: LIVE_BUDGET");
        }
        Ok(Self {
            live: Some(options), mode: crate::config::ControlledMode::SignedFutureLive,
            run_id: run_id.into(), started_at, deadline_at,
            started_instant: std::time::Instant::now(),
        })
    }
}

impl LiveContext {
    /// Validate the same public identity against argv and the fixed activation binding.
    pub fn parse(raw: &str, run: &ControlledRun, activation: &LiveActivation) -> Result<Self> {
        let strict: super::StrictJson = serde_json::from_str(raw)
            .map_err(|_| anyhow::anyhow!("CONTROLLED_MODE: LIVE_CONTEXT"))?;
        let context: Self = serde_json::from_value(strict.0)
            .map_err(|_| anyhow::anyhow!("CONTROLLED_MODE: LIVE_CONTEXT"))?;
        let options = run.live.as_ref().ok_or_else(|| anyhow::anyhow!("CONTROLLED_MODE: LIVE_CONTEXT"))?;
        context.account_identity.check()?;
        activation.operations_mode()?;
        if context.schema_version != "btc5m-live-context/1"
            || context.run_id != run.run_id
            || context.risk_window_id != options.risk_window_id
            || context.authorization_ref != options.authorization_ref
            || context.operations_mode != options.operations_mode.as_str()
            || context.started_at != run.started_at || context.deadline_at != run.deadline_at
            || context.recovery_policy != RECOVERY_POLICY
            || context.cash_recovery_policy != CASH_RECOVERY_POLICY
            || !valid_digest(&context.candidate_digest)
            || context.candidate_digest != activation.candidate_digest
            || context.account_identity != activation.account_identity
            || context.account_identity.trading_address.to_ascii_lowercase()
                != "0xfc9eb66e6f22f96608058c0a711ed048eed8c2e2"
            || context.risk_window_id != activation.risk_window_id
            || context.authorization_ref != activation.authorization_ref
            || context.operations_mode != activation.operations_mode
            || context.started_at < activation.not_before
            || context.deadline_at != activation.deadline_at
            || context.gas_policy != activation.gas_policy
        {
            bail!("CONTROLLED_MODE: LIVE_CONTEXT_MISMATCH");
        }
        let units = &context.asset_units;
        if units.collateral_symbol != "USDT" || units.collateral_decimals != 18
            || units.share_decimals != 18 || units.gas_symbol != "BNB" || units.gas_decimals != 18
        {
            bail!("CONTROLLED_MODE: ASSET_UNITS");
        }
        Ok(context)
    }
}

/// Keep the original launcher's date/run evidence root; no arbitrary manifest input.
pub fn live_manifest_path(run: &ControlledRun) -> std::path::PathBuf {
    let bot = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = bot.ancestors().nth(5).expect("the fixed bot repository layout");
    let date = run.started_at.with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
        .format("%Y-%m-%d");
    root.join(format!("data/reports/{date}-btc5m-controlled-paper/evidence"))
        .join(&run.run_id).join("live-context.json")
}

/// Check that the existing parent supervision channel is a pipe, before secrets.
pub fn require_control_pipe() -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;
        let metadata = std::fs::metadata("/proc/self/fd/0")
            .map_err(|_| anyhow::anyhow!("CONTROLLED_MODE: CONTROL_CHANNEL"))?;
        if metadata.file_type().is_fifo() { return Ok(()); }
    }
    bail!("CONTROLLED_MODE: CONTROL_CHANNEL")
}

fn read_public_file(path: &std::path::Path) -> Result<String> {
    for component in path.ancestors() {
        if std::fs::symlink_metadata(component)
            .map_err(|_| anyhow::anyhow!("CONTROLLED_MODE: PUBLIC_CONTEXT_UNAVAILABLE"))?
            .file_type().is_symlink()
        {
            bail!("CONTROLLED_MODE: PUBLIC_CONTEXT_PATH");
        }
    }
    let metadata = std::fs::metadata(path)
        .map_err(|_| anyhow::anyhow!("CONTROLLED_MODE: PUBLIC_CONTEXT_UNAVAILABLE"))?;
    if !metadata.is_file() || metadata.len() > 65_536 {
        bail!("CONTROLLED_MODE: PUBLIC_CONTEXT_PATH");
    }
    std::fs::read_to_string(path)
        .map_err(|_| anyhow::anyhow!("CONTROLLED_MODE: PUBLIC_CONTEXT_UNAVAILABLE"))
}

/// Read only the fixed public startup files, never credentials or arbitrary paths.
pub fn read_live_context(run: &mut ControlledRun, path: &std::path::Path) -> Result<()> {
    if run.live.is_none() || path != live_manifest_path(run) {
        bail!("CONTROLLED_MODE: LIVE_CONTEXT_PATH");
    }
    require_control_pipe()?;
    let activation_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tools/predictfun/btc5m-live-activation.json");
    let activation = LiveActivation::parse(&read_public_file(&activation_path)?)?;
    let context = LiveContext::parse(&read_public_file(path)?, run, &activation)?;
    let options = run.live.as_mut().expect("the explicit live domain");
    options.gas_policy = activation.gas_policy()?;
    options.account_identity = Some(context.account_identity);
    Ok(())
}

/// Recheck only public runtime fields before constructing the original trader.
pub(crate) fn check_runtime_config(cfg: &super::AppConfig, strategy: &str) -> Result<()> {
    let selected = cfg.controlled.as_ref().and_then(|run| run.live.as_ref());
    if selected.is_none() && cfg.controlled_live.is_none() { return Ok(()); }
    let run = cfg.controlled.as_ref().ok_or_else(|| anyhow::anyhow!("CONTROLLED_MODE: LIVE_CONTEXT"))?;
    let options = selected.ok_or_else(|| anyhow::anyhow!("CONTROLLED_MODE: LIVE_CONTEXT"))?;
    if cfg.controlled_live.as_ref() != Some(options)
        || run.mode != super::ControlledMode::SignedFutureLive
        || strategy != "BTC arbitrage" || !cfg.live_trading_allowed()
        || options.account_identity.is_none() || run.deadline_at != options.deadline_at
    {
        bail!("CONTROLLED_MODE: LIVE_CONTEXT");
    }
    let account = options.account_identity.as_ref()
        .ok_or_else(|| anyhow::anyhow!("CONTROLLED_MODE: ACCOUNT"))?;
    account.check()?;
    if account.trading_address.to_ascii_lowercase() != "0xfc9eb66e6f22f96608058c0a711ed048eed8c2e2"
        || options.contract != LIVE_CONTRACT
        || !options.operations_mode.allows_duration(run.deadline_at - run.started_at)
        || (run.deadline_at - run.started_at).num_seconds() != options.budget_secs
        || options.gas_policy.payer_address != account.signer_address
        || options.gas_policy.minimum_balance_atoms.is_zero()
        || options.gas_policy.maximum_consumption_atoms.is_zero()
    {
        bail!("CONTROLLED_MODE: LIVE_CONTEXT");
    }
    // The frozen public money must be exactly what the loaded configuration
    // states. A swapped configuration is a different live context, never a
    // reuse of the old activation.
    use super::run_parameters::{atoms_of_float, atoms_of_token};
    let limits = &cfg.strategies.limits;
    let params = options.params;
    if !params.ordered()
        || atoms_of_float(cfg.strategies.btc_arb.order_usd)? != params.target
        || atoms_of_float(limits.max_order_usd)? != params.order
        || atoms_of_float(limits.max_market_usd)? != params.market
        || atoms_of_float(limits.max_total_usd)? != params.total
        || atoms_of_float(limits.max_loss_usd)? != params.loss
    {
        bail!("CONTROLLED_MODE: LIVE_CONTEXT");
    }
    match cfg.controlled_run.as_ref() {
        Some(settings) => {
            if settings.duration_seconds != options.budget_secs as u64
                || settings.operations_mode != options.operations_mode.as_str()
                || settings.budget_atoms()? != params.budget
            {
                bail!("CONTROLLED_MODE: LIVE_CONTEXT");
            }
        }
        None => {
            if params != super::RunParameters::legacy() {
                bail!("CONTROLLED_MODE: LIVE_CONTEXT");
            }
        }
    }
    // Select public fields explicitly: never serialize the credentials container.
    super::StrictJson(serde_json::json!({
        "venue": cfg.venue, "predict_fun": cfg.predict_fun,
        "bot": {"enable_trading": cfg.bot.enable_trading, "mock_trading": cfg.bot.mock_trading},
        "controlled_paper": cfg.controlled_paper, "strategies": cfg.strategies,
    })).check_controlled(super::ControlledMode::SignedFutureLive)
}

/// The run identity a controlled live run carries.
pub fn live_run(options: &ControlledLiveOptions, run: &ControlledRun) -> Result<ControlledRun> {
    if run.mode != crate::config::ControlledMode::SignedFutureLive {
        bail!("CONTROLLED_MODE: a live run must be a signed live run");
    }
    if run.deadline_at != options.deadline_at {
        bail!("CONTROLLED_MODE: the live deadline must be the controlled deadline");
    }
    Ok(run.clone())
}

/// The environment variable names a binding may use. Nothing else is read.
pub fn credential_env_names(binding: &StorageBinding) -> Vec<String> {
    let mut names = vec![binding.credential_env.clone()];
    names.sort();
    names.dedup();
    names
}

/// The extra fields a binding may never carry.
pub fn forbidden_binding_fields() -> HashMap<&'static str, &'static str> {
    HashMap::from([
        ("uri", "the connection string is never part of the public binding"),
        ("password", "a secret is never part of the public binding"),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACCOUNT: &str = r#"{
        "venue": "PREDICT_FUN",
        "chain_id": 56,
        "account_mode": "predict-account",
        "trading_address": "0x1111111111111111111111111111111111111111",
        "signer_address": "0x2222222222222222222222222222222222222222"
    }"#;

    fn activation_json(extra: &str) -> String {
        format!(
            r#"{{
                "schema_version": "btc5m-live-activation/1",
                "authorization_ref": "auth-1",
                "risk_window_id": "window-1",
                "operations_mode": "supervised",
                "account_identity": {ACCOUNT},
                "not_before": "2026-11-19T04:00:00Z",
                "deadline_at": "2026-11-19T10:00:00Z",
                "candidate_digest": "{}",
                "storage_binding": {{
                    "deployment_id": "dep-1",
                    "database": "tradingagents",
                    "collection": "07_trading_btc5m_live_guard",
                    "credential_env": "YQUANT_BTC5M_GUARD_MONGO_URI",
                    "endpoint_id": {{
                        "scheme": "mongodb",
                        "hosts": ["127.0.0.1:27017"],
                        "replica_set": null,
                        "tls": false
                    }}
                }},
                "gas_policy": {{
                    "payer_address": "0x2222222222222222222222222222222222222222",
                    "minimum_balance_atoms": "10000000000000000",
                    "maximum_consumption_atoms": "20000000000000000"
                }},
                "takeover_ref": "takeover-1"{extra}
            }}"#,
            "a".repeat(64)
        )
    }

    #[test]
    fn a_complete_activation_is_read() {
        let activation = LiveActivation::parse(&activation_json("")).unwrap();
        assert_eq!(activation.operations_mode().unwrap(), OperationsMode::Supervised);
        assert_eq!(
            activation.gas_policy().unwrap().minimum_balance_atoms,
            U256::from(10_000_000_000_000_000u128)
        );
    }

    #[test]
    fn an_unknown_field_is_refused() {
        let error = LiveActivation::parse(&activation_json(r#", "extra": 1"#)).unwrap_err();
        assert!(error.to_string().contains("readable"));
    }

    #[test]
    fn an_unknown_schema_version_is_refused() {
        let raw = activation_json("").replace("btc5m-live-activation/1", "btc5m-live-activation/2");
        assert!(LiveActivation::parse(&raw).is_err());
    }

    #[test]
    fn only_the_signer_gas_payer_is_accepted() {
        let raw = activation_json("");
        let activation = LiveActivation::parse(&raw).unwrap();
        assert_ne!(activation.account_identity.trading_address, activation.account_identity.signer_address);
        assert_eq!(activation.gas_policy.payer_address, activation.account_identity.signer_address);
        for payer in [activation.account_identity.trading_address.as_str(),
            "0x3333333333333333333333333333333333333333", "bad", ""] {
            let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
            value["gas_policy"]["payer_address"] = payer.into();
            assert!(LiveActivation::parse(&value.to_string()).is_err(), "{payer}");
        }
        let mut missing: serde_json::Value = serde_json::from_str(&raw).unwrap();
        missing["gas_policy"].as_object_mut().unwrap().remove("payer_address");
        assert!(LiveActivation::parse(&missing.to_string()).is_err());
        let (run, activation, context) = live_fixture();
        let mut mismatch: serde_json::Value = serde_json::from_str(&context).unwrap();
        mismatch["gas_policy"]["payer_address"] = activation.account_identity.trading_address.clone().into();
        assert!(LiveContext::parse(&mismatch.to_string(), &run, &activation).is_err());
    }

    #[test]
    fn a_half_specified_live_run_is_refused() {
        let error = ControlledLiveOptions::from_arguments(
            Some(LIVE_CONTRACT),
            None,
            Some("window-1"),
            Some("auth-1"),
            Some("2026-11-19T10:00:00Z"),
            "2026-11-19T04:00:00Z",
        )
        .unwrap_err();
        assert!(error.to_string().contains("belong together"));
    }

    #[test]
    fn no_live_arguments_leave_the_old_entry_points_alone() {
        let options =
            ControlledLiveOptions::from_arguments(None, None, None, None, None, "2026-11-19T04:00:00Z")
                .unwrap();
        assert!(options.is_none());
    }

    #[test]
    fn native_mode_windows_bind_options_run_context_and_runtime_validator() {
        let start_text = "2026-11-19T04:00:00Z";
        let start = parse_utc(start_text).unwrap();
        for (mode, cap) in [(OperationsMode::Unattended, 86_400), (OperationsMode::Supervised, 21_600)] {
            for millis in [-1000, 0, 1000, 21_600_000, 21_600_001, 21_601_000, 86_400_000, 86_400_001, 86_401_000] {
                let duration = chrono::Duration::milliseconds(millis);
                let accepted = millis > 0 && millis <= cap * 1000;
                let deadline = start + duration;
                let deadline_text = deadline.to_rfc3339();
                let options = ControlledLiveOptions::from_arguments(Some(LIVE_CONTRACT), Some(mode.as_str()),
                    Some("window-1"), Some("auth-1"), Some(&deadline_text), start_text);
                assert_eq!(options.is_ok(), accepted, "options {mode:?} {millis}");
                // Independently exercise native run validation with manually supplied options.
                let (mut run, mut activation, context_raw) = live_fixture();
                let mut options = run.live.clone().unwrap();
                options.operations_mode = mode;
                options.deadline_at = deadline;
                options.budget_secs = duration.num_seconds();
                options.account_identity = Some(activation.account_identity.clone());
                options.gas_policy = activation.gas_policy().unwrap();
                let constructed = ControlledRun::from_live_arguments(Some("signed-future-live"), Some("run-1"),
                    Some(start_text), Some(&deadline_text), options.clone());
                assert_eq!(constructed.is_ok(), accepted, "run {mode:?} {millis}");
                // Even a forged configuration cannot bypass the last public validator.
                run.started_at = start;
                run.deadline_at = deadline;
                run.live = Some(options.clone());
                let mut public: serde_json::Value = serde_json::from_str(include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"), "/tools/predictfun/btc5m-controlled-paper.json"))).unwrap();
                public["bot"]["enable_trading"] = true.into();
                public["bot"]["mock_trading"] = false.into();
                public.as_object_mut().unwrap().remove("controlled_paper");
                let mut cfg: super::super::AppConfig = serde_json::from_value(public).unwrap();
                cfg.controlled = Some(run.clone());
                cfg.controlled_live = Some(options);
                assert_eq!(check_runtime_config(&cfg, "BTC arbitrage").is_ok(), accepted,
                    "runtime {mode:?} {millis}");
                if accepted {
                    let mut raw: serde_json::Value = serde_json::from_str(&activation_json("")).unwrap();
                    raw["account_identity"]["trading_address"] = activation.account_identity.trading_address.clone().into();
                    raw["operations_mode"] = mode.as_str().into();
                    raw["deadline_at"] = deadline_text.clone().into();
                    activation = LiveActivation::parse(&raw.to_string()).unwrap();
                    let mut context: serde_json::Value = serde_json::from_str(&context_raw).unwrap();
                    context["operations_mode"] = mode.as_str().into();
                    context["deadline_at"] = deadline_text.into();
                    let parsed = LiveContext::parse(&context.to_string(), &constructed.unwrap(), &activation).unwrap();
                    assert_eq!(parsed.deadline_at, deadline);
                }
            }
        }
        assert!(ControlledLiveOptions::from_arguments(Some(LIVE_CONTRACT), Some("unknown"),
            Some("window-1"), Some("auth-1"), Some("2026-11-19T10:00:00Z"), start_text).is_err());
        let raw = activation_json("").replace("\"supervised\"", "\"unknown\"");
        assert!(LiveActivation::parse(&raw).unwrap().operations_mode().is_err());
    }

    #[test]
    fn a_complete_live_run_is_read() {
        let options = ControlledLiveOptions::from_arguments(
            Some(LIVE_CONTRACT),
            Some("supervised"),
            Some("window-1"),
            Some("auth-1"),
            Some("2026-11-19T10:00:00Z"),
            "2026-11-19T04:00:00Z",
        )
        .unwrap()
        .unwrap();
        assert_eq!(options.budget_secs, 21_600);
        assert_eq!(options.operations_mode, OperationsMode::Supervised);
        assert!(options.in_window(DateTime::parse_from_rfc3339("2026-11-19T05:00:00Z").unwrap().with_timezone(&Utc)));
    }

    fn live_fixture() -> (ControlledRun, LiveActivation, String) {
        let activation_raw = activation_json("").replace(
            "0x1111111111111111111111111111111111111111",
            "0xfc9eb66e6f22f96608058c0a711ed048eed8c2e2");
        let activation = LiveActivation::parse(&activation_raw).unwrap();
        let options = ControlledLiveOptions::from_arguments(Some(LIVE_CONTRACT), Some("supervised"),
            Some("window-1"), Some("auth-1"), Some("2026-11-19T10:00:00Z"), "2026-11-19T04:00:00Z")
            .unwrap().unwrap();
        let run = ControlledRun::from_live_arguments(Some("signed-future-live"), Some("run-1"),
            Some("2026-11-19T04:00:00Z"), Some("2026-11-19T10:00:00Z"), options).unwrap();
        let mut value: serde_json::Value = serde_json::from_str(&activation_raw).unwrap();
        let fields = value.as_object_mut().unwrap();
        fields.remove("storage_binding"); fields.remove("takeover_ref"); fields.remove("not_before");
        fields.insert("schema_version".into(), "btc5m-live-context/1".into());
        fields.insert("run_id".into(), "run-1".into());
        fields.insert("started_at".into(), "2026-11-19T04:00:00Z".into());
        fields.insert("recovery_policy".into(), RECOVERY_POLICY.into());
        fields.insert("cash_recovery_policy".into(), CASH_RECOVERY_POLICY.into());
        fields.insert("asset_units".into(), serde_json::json!({
            "collateral_symbol": "USDT", "collateral_decimals": 18, "share_decimals": 18,
            "gas_symbol": "BNB", "gas_decimals": 18,
        }));
        (run, activation, value.to_string())
    }

    #[test]
    fn public_context_binds_all_arguments_activation_units_and_policies() {
        let (run, activation, raw) = live_fixture();
        let context = LiveContext::parse(&raw, &run, &activation).unwrap();
        assert_eq!(context.run_id, run.run_id);
        assert_eq!(context.account_identity, activation.account_identity);
        for (field, bad) in [
            ("run_id", "another"), ("risk_window_id", "another"), ("authorization_ref", "another"),
            ("operations_mode", "unattended"), ("candidate_digest", &"b".repeat(64)),
            ("started_at", "2026-11-19T04:00:01Z"), ("deadline_at", "2026-11-19T09:00:00Z"),
            ("recovery_policy", "AUTO"), ("cash_recovery_policy", "AUTO"),
            ("schema_version", "btc5m-live-context/2"),
        ] {
            let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
            value[field] = bad.into();
            assert!(LiveContext::parse(&value.to_string(), &run, &activation).is_err(), "{field}");
        }
        for field in ["collateral_decimals", "share_decimals", "gas_decimals"] {
            let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
            value["asset_units"][field] = 6.into();
            assert!(LiveContext::parse(&value.to_string(), &run, &activation).is_err(), "{field}");
        }
    }

    #[test]
    fn public_context_refuses_unknown_missing_duplicate_and_recursive_fields() {
        let (run, activation, raw) = live_fixture();
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        for field in value.as_object().unwrap().keys() {
            let mut missing = value.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(LiveContext::parse(&missing.to_string(), &run, &activation).is_err(), "{field}");
        }
        for parent in ["account_identity", "gas_policy", "asset_units"] {
            let mut extra = value.clone();
            extra[parent]["unexpected"] = true.into();
            assert!(LiveContext::parse(&extra.to_string(), &run, &activation).is_err(), "{parent}");
        }
        let duplicate = raw.replacen('{', "{\"run_id\":\"run-1\",", 1);
        assert!(LiveContext::parse(&duplicate, &run, &activation).is_err());
        let mut extra = value;
        extra["unexpected"] = true.into();
        assert!(LiveContext::parse(&extra.to_string(), &run, &activation).is_err());
    }

    #[test]
    fn live_context_path_is_fixed_and_old_runs_do_not_read_it() {
        let (mut run, _, _) = live_fixture();
        assert!(live_manifest_path(&run).ends_with(
            "data/reports/2026-11-19-btc5m-controlled-paper/evidence/run-1/live-context.json"));
        assert!(read_live_context(&mut run, std::path::Path::new("/not-read/live-context.json")).is_err());
        run.live = None;
        let path = live_manifest_path(&run);
        assert!(read_live_context(&mut run, &path).is_err());
    }

    #[test]
    fn short_live_budget_does_not_relax_the_legacy_signed_budget() {
        let options = ControlledLiveOptions::from_arguments(Some(LIVE_CONTRACT), Some("supervised"),
            Some("window-1"), Some("auth-1"), Some("2026-11-19T05:00:00Z"), "2026-11-19T04:00:00Z")
            .unwrap().unwrap();
        let run = ControlledRun::from_live_arguments(Some("signed-future-live"), Some("run-1"),
            Some("2026-11-19T04:00:00Z"), Some("2026-11-19T05:00:00Z"), options.clone()).unwrap();
        assert_eq!(run.live.unwrap().budget_secs, 3600);
        assert!(ControlledRun::from_arguments(Some("signed-future-live"), Some("run-1"),
            Some("2026-11-19T04:00:00Z"), Some("2026-11-19T05:00:00Z")).is_err());
        assert!(ControlledRun::from_live_arguments(Some("unsigned-paper"), Some("run-1"),
            Some("2026-11-19T04:00:00Z"), Some("2026-11-19T05:00:00Z"), options).is_err());
    }

    struct PublicScratch(std::path::PathBuf);

    impl PublicScratch {
        fn new() -> Self {
            let scratch = std::env::temp_dir();
            assert!(scratch.to_string_lossy().contains("/.hermes/profiles/")
                && scratch.ends_with("cache/scratch"), "fixtures must stay in the current profile scratch");
            let serial = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
            let path = scratch.join(format!("btc5m-public-context-{}-{serial}", std::process::id()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for PublicScratch {
        fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); }
    }

    #[test]
    fn real_public_file_reads_validate_scratch_manifests_and_reject_path_and_digest_faults() {
        let scratch = PublicScratch::new();
        let (mut run, _, raw) = live_fixture();
        let activation_raw = activation_json("").replace(
            "0x1111111111111111111111111111111111111111",
            "0xfc9eb66e6f22f96608058c0a711ed048eed8c2e2");
        let activation_path = scratch.0.join("activation.json");
        let context_path = scratch.0.join("live-context.json");
        std::fs::write(&activation_path, &activation_raw).unwrap();
        std::fs::write(&context_path, &raw).unwrap();
        let activation = LiveActivation::parse(&read_public_file(&activation_path).unwrap()).unwrap();
        let context = LiveContext::parse(&read_public_file(&context_path).unwrap(), &run, &activation).unwrap();
        assert_eq!(context.account_identity, activation.account_identity);
        // Scratch is not an alternate production root or an activation bypass.
        assert!(read_live_context(&mut run, &context_path).unwrap_err().to_string().contains("LIVE_CONTEXT_PATH"));
        for (field, bad) in [("unexpected", "extra"), ("candidate_digest", &"b".repeat(64))] {
            let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
            value[field] = bad.into();
            std::fs::write(&context_path, value.to_string()).unwrap();
            assert!(LiveContext::parse(&read_public_file(&context_path).unwrap(), &run, &activation).is_err());
        }
        std::fs::write(&context_path, "x".repeat(65_537)).unwrap();
        assert!(read_public_file(&context_path).is_err());
        std::fs::write(&context_path, [0xff, 0xfe]).unwrap();
        assert!(read_public_file(&context_path).is_err());
        assert!(read_public_file(&scratch.0).is_err());
        assert!(read_public_file(&scratch.0.join("missing.json")).is_err());
        #[cfg(unix)]
        {
            let link = scratch.0.join("linked.json");
            std::os::unix::fs::symlink(&activation_path, &link).unwrap();
            assert!(read_public_file(&link).unwrap_err().to_string().contains("PUBLIC_CONTEXT_PATH"));
            let directory_link = scratch.0.join("linked-directory");
            std::os::unix::fs::symlink(&scratch.0, &directory_link).unwrap();
            assert!(read_public_file(&directory_link.join("activation.json")).is_err());
        }
    }

    #[test]
    fn real_control_pipe_check_uses_the_child_stdin_descriptor() {
        let Ok(expected) = std::env::var("BTC5M_TEST_EXPECT_PUBLIC_PIPE") else { return; };
        assert_eq!(require_control_pipe().is_ok(), expected == "pipe");
    }

    #[cfg(unix)]
    #[test]
    fn original_pipe_guard_accepts_a_real_pipe_and_refuses_null_and_regular_file() {
        use std::process::{Command, Stdio};
        let scratch = PublicScratch::new();
        let regular = scratch.0.join("public-stdin.txt");
        std::fs::write(&regular, "public test input").unwrap();
        for (expected, stdin) in [("pipe", Stdio::piped()), ("null", Stdio::null()),
            ("file", Stdio::from(std::fs::File::open(&regular).unwrap()))] {
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "config::controlled_live::tests::real_control_pipe_check_uses_the_child_stdin_descriptor"])
                .env("BTC5M_TEST_EXPECT_PUBLIC_PIPE", expected).stdin(stdin)
                .stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
            drop(child.stdin.take());
            let output = child.wait_with_output().unwrap();
            assert!(output.status.success(), "{expected}: {} {}", String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr));
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"), "child must run the actual guard test");
        }
    }

    #[test]
    fn an_overlong_live_budget_is_refused() {
        let error = ControlledLiveOptions::from_arguments(
            Some(LIVE_CONTRACT),
            Some("supervised"),
            Some("window-1"),
            Some("auth-1"),
            Some("2026-11-19T12:00:00Z"),
            "2026-11-19T04:00:00Z",
        )
        .unwrap_err();
        assert!(error.to_string().contains("budget"));
    }
}
