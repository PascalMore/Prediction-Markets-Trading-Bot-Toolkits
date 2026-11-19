use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use polymarket_toolkits::{
    bot::{self, BotKind},
    config::AppConfig,
    models::VenueId,
    service::{clob, predict_fun},
    ui,
};
use std::path::PathBuf;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "polymarket-toolkits")]
#[command(about = "Multi-venue prediction-market trading toolkit.", long_about = None)]
struct Cli {
    /// Path to public config (JSON).
    #[arg(long, default_value = "config.json")]
    config: PathBuf,

    /// Path to credentials file (YAML).
    #[arg(long, default_value = "config.yaml")]
    credentials: PathBuf,

    /// Venue to trade. Overrides `venue` in the config.
    #[arg(long, value_enum)]
    venue: Option<VenueArg>,

    /// Confirm this run's budget for one Predict.fun account:
    /// `chain:address:mode`, with mode `eoa` or `predict-account`.
    ///
    /// It is a declaration that a human looked at the account's risk and
    /// accepts the budget already configured. It overrides no health check and
    /// carries no amount. Without it a live Predict.fun run stays blocked.
    #[arg(long)]
    predict_budget_confirmation: Option<String>,

    /// An order hash the operator says they explained. Repeatable.
    #[arg(long)]
    predict_reconciled_order: Vec<String>,

    /// Explicit read-only RPC endpoint. Never logged or read from configuration.
    #[arg(long)]
    predict_rpc_url: Option<String>,

    /// Run in the controlled unsigned-paper mode. Only the fixed `run btc-arb`
    /// command accepts it, and only with the run's identity below.
    #[arg(long)]
    controlled_mode: Option<String>,

    /// The controlled run's public id.
    #[arg(long)]
    controlled_run_id: Option<String>,

    /// The controlled run's start, in UTC.
    #[arg(long)]
    controlled_started_at: Option<String>,

    /// The controlled run's deadline, in UTC.
    #[arg(long)]
    controlled_deadline_at: Option<String>,

    /// Explicit 07-007 strategy contract (all five live options belong together).
    #[arg(long)]
    controlled_live_contract: Option<String>,
    #[arg(long)]
    controlled_operations_mode: Option<String>,
    #[arg(long)]
    controlled_risk_window_id: Option<String>,
    #[arg(long)]
    controlled_authorization_ref: Option<String>,
    /// Fixed run evidence path, never an arbitrary context file.
    #[arg(long)]
    controlled_live_manifest: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Launch the interactive TUI to select a bot. (Default if no subcommand.)
    Tui,
    /// Run a specific bot headlessly (no TUI).
    Run {
        /// Which bot to run.
        #[arg(value_enum)]
        bot: BotKindArg,
    },
    /// Check a venue's connection and signing without placing an order.
    CheckVenue {
        /// Also sign in, proving the wallet and API key work. On Predict.fun this
        /// accepts its Terms of Service; on Polymarket it creates or derives API
        /// credentials and reads the balance. Either way it's opt-in.
        #[arg(long)]
        sign_in: bool,
    },
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum VenueArg {
    Polymarket,
    PredictFun,
}

impl From<VenueArg> for VenueId {
    fn from(v: VenueArg) -> Self {
        match v {
            VenueArg::Polymarket => VenueId::Polymarket,
            VenueArg::PredictFun => VenueId::PredictFun,
        }
    }
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum BotKindArg {
    CopyTrading,
    BtcArb,
    CrossArb,
    DirectionalArb,
    SpreadFarming,
    Sports,
    ResolutionSniper,
    OrderbookImbalance,
    MarketMaking,
    WhaleSignal,
}

impl From<BotKindArg> for BotKind {
    fn from(b: BotKindArg) -> Self {
        match b {
            BotKindArg::CopyTrading => BotKind::CopyTrading,
            BotKindArg::BtcArb => BotKind::BtcArb,
            BotKindArg::CrossArb => BotKind::CrossArb,
            BotKindArg::DirectionalArb => BotKind::DirectionalArb,
            BotKindArg::SpreadFarming => BotKind::SpreadFarming,
            BotKindArg::Sports => BotKind::Sports,
            BotKindArg::ResolutionSniper => BotKind::ResolutionSniper,
            BotKindArg::OrderbookImbalance => BotKind::OrderbookImbalance,
            BotKindArg::MarketMaking => BotKind::MarketMaking,
            BotKindArg::WhaleSignal => BotKind::WhaleSignal,
        }
    }
}

impl Cli {
    /// Resolve only public arguments; reject wrong/partial entry points before any file or secret.
    fn controlled_run(&self) -> Result<Option<polymarket_toolkits::config::ControlledRun>> {
        use polymarket_toolkits::config::{controlled_live::ControlledLiveOptions, ControlledRun};
        let live_fields = [
            self.controlled_live_contract.is_some(), self.controlled_operations_mode.is_some(),
            self.controlled_risk_window_id.is_some(), self.controlled_authorization_ref.is_some(),
            self.controlled_live_manifest.is_some(),
        ];
        let explicit_live = live_fields.iter().any(|present| *present);
        let run = if explicit_live {
            if !live_fields.iter().all(|present| *present)
                || self.controlled_mode.as_deref() != Some("signed-future-live")
                || self.venue.is_some()
                || !matches!(self.command, Some(Command::Run { bot: BotKindArg::BtcArb }))
            {
                anyhow::bail!("CONTROLLED_MODE: LIVE_ARGUMENTS");
            }
            let options = ControlledLiveOptions::from_arguments(
                self.controlled_live_contract.as_deref(), self.controlled_operations_mode.as_deref(),
                self.controlled_risk_window_id.as_deref(), self.controlled_authorization_ref.as_deref(),
                self.controlled_deadline_at.as_deref(), self.controlled_started_at.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("CONTROLLED_MODE: START"))?,
            )?.expect("all live arguments are present");
            Some(ControlledRun::from_live_arguments(
                self.controlled_mode.as_deref(), self.controlled_run_id.as_deref(),
                self.controlled_started_at.as_deref(), self.controlled_deadline_at.as_deref(), options,
            )?)
        } else {
            ControlledRun::from_arguments(
                self.controlled_mode.as_deref(), self.controlled_run_id.as_deref(),
                self.controlled_started_at.as_deref(), self.controlled_deadline_at.as_deref(),
            )?
        };
        Ok(run)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("polymarket_toolkits=info,info"))
                .add_directive("reqwest=off".parse()?)
                .add_directive("hyper=off".parse()?)
                .add_directive("hyper_util=off".parse()?)
                .add_directive("tower_http=off".parse()?),
        )
        .with_target(false)
        .init();

    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) =>
        {
            error.exit()
        }
        Err(_) => anyhow::bail!("INVALID_INPUT/CLI_ARGUMENTS"),
    };

    let mut controlled = cli.controlled_run()?;
    if let (Some(run), Some(path)) = (controlled.as_mut(), cli.controlled_live_manifest.as_deref()) {
        polymarket_toolkits::config::controlled_live::read_live_context(run, path)?;
    }

    // The confirmation is parsed before any credential is read, and it holds
    // no secret: it is a declaration about one account.
    let confirmation = match cli.predict_budget_confirmation.as_deref() {
        Some(raw) => {
            let mut confirmation =
                predict_fun::account::parse_confirmation(raw).map_err(|e| anyhow::anyhow!(e))?;
            confirmation.reconciled_orders = cli.predict_reconciled_order.clone();
            Some(confirmation)
        }
        None => None,
    };

    let rpc_url = cli
        .predict_rpc_url
        .as_deref()
        .map(predict_fun::account::parse_rpc_url)
        .transpose()?;
    for hash in &cli.predict_reconciled_order {
        predict_fun::account::validate_order_hash(hash)?;
    }
    let run_context = predict_fun::account::RunContext {
        confirmation,
        reconciled_orders: cli.predict_reconciled_order,
        rpc_url,
    };

    let mut cfg = match &controlled {
        Some(run) => {
            // Only the fixed strategy may run in a controlled mode, and only
            // with the controlled configuration.
            if !matches!(
                cli.command,
                Some(Command::Run {
                    bot: BotKindArg::BtcArb
                })
            ) {
                anyhow::bail!("CONTROLLED_MODE: only 'run btc-arb' may run in a controlled mode");
            }
            if cli.venue.is_some() {
                anyhow::bail!("CONTROLLED_MODE: the venue is fixed and can't be overridden");
            }
            if run.mode == polymarket_toolkits::config::ControlledMode::SignedFutureLive {
                let expected = format!(
                    "56:{}:predict-account",
                    "0xfc9eb66e6f22f96608058c0a711ed048eed8c2e2"
                );
                if cli
                    .predict_budget_confirmation
                    .as_deref()
                    .map(str::to_ascii_lowercase)
                    .as_deref()
                    != Some(expected.as_str())
                    || cli.predict_rpc_url.is_none()
                {
                    anyhow::bail!("CONTROLLED_MODE: LIVE_CONFIRMATION");
                }
                let url = url::Url::parse(cli.predict_rpc_url.as_deref().unwrap())
                    .map_err(|_| anyhow::anyhow!("CONTROLLED_MODE"))?;
                if !url.username().is_empty()
                    || url.password().is_some()
                    || url.query().is_some()
                    || url.fragment().is_some()
                {
                    anyhow::bail!("CONTROLLED_MODE: RPC_INPUT");
                }
            } else if cli.predict_budget_confirmation.is_some() || cli.predict_rpc_url.is_some() {
                anyhow::bail!("CONTROLLED_MODE: LIVE_OPTION_REFUSED");
            }
            let mut cfg = AppConfig::load_controlled(&cli.config, run.mode)?;
            cfg.controlled_live = run.live.clone();
            cfg.controlled = Some(run.clone());
            cfg
        }
        None => AppConfig::load(&cli.config, &cli.credentials).context("loading configuration")?,
    };
    if let Some(venue) = cli.venue {
        cfg.venue = venue.into();
    }

    info!(
        venue = ?cfg.venue,
        wallets = cfg.bot.wallets_to_track.len(),
        enable_trading = cfg.bot.enable_trading,
        mock_trading = cfg.bot.mock_trading,
        "configuration loaded"
    );

    if cfg.live_trading_allowed() {
        warn!(
            venue = ?cfg.venue,
            "LIVE TRADING IS ON: orders use real funds. See docs/LIVE-TRADING.md"
        );
    }

    if cfg.live_trading_allowed()
        && cfg.venue == VenueId::PredictFun
        && !matches!(cli.command, Some(Command::CheckVenue { .. }))
    {
        predict_fun::account::run_confirmation_slot().install_context(run_context);
    }
    match cli.command {
        Some(Command::Run { bot: kind }) => bot::run(kind.into(), cfg).await,
        Some(Command::CheckVenue { sign_in }) => match cfg.venue {
            VenueId::Polymarket => clob::check(&cfg, sign_in).await,
            _ => predict_fun::check(&cfg, sign_in).await,
        },
        Some(Command::Tui) | None => ui::run(cfg).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live_args() -> Vec<&'static str> {
        vec!["bot", "--controlled-mode", "signed-future-live",
            "--controlled-run-id", "live-test", "--controlled-started-at", "2026-11-19T04:00:00Z",
            "--controlled-deadline-at", "2026-11-19T05:00:00Z",
            "--controlled-live-contract", "phase1_net_shares", "--controlled-operations-mode", "supervised",
            "--controlled-risk-window-id", "window-1", "--controlled-authorization-ref", "auth-1",
            "--controlled-live-manifest", "/not-read/live-context.json", "run", "btc-arb"]
    }

    #[test]
    fn live_cli_resolves_short_budget_without_reading_credentials_or_manifest() {
        let run = Cli::try_parse_from(live_args()).unwrap().controlled_run().unwrap().unwrap();
        assert_eq!(run.live.unwrap().budget_secs, 3600);
    }

    #[test]
    fn every_partial_live_cli_group_is_refused_before_file_reads() {
        for option in ["--controlled-live-contract", "--controlled-operations-mode",
            "--controlled-risk-window-id", "--controlled-authorization-ref", "--controlled-live-manifest"] {
            let mut args = live_args();
            let position = args.iter().position(|value| *value == option).unwrap();
            args.drain(position..position + 2);
            assert!(Cli::try_parse_from(args).unwrap().controlled_run().is_err(), "{option}");
        }
    }

    #[test]
    fn live_cli_rejects_unsigned_wrong_strategy_tui_and_check_before_file_reads() {
        for (old, new) in [("signed-future-live", "unsigned-paper"), ("btc-arb", "sports")] {
            let mut args = live_args();
            let position = args.iter().position(|value| *value == old).unwrap();
            args[position] = new;
            assert!(Cli::try_parse_from(args).unwrap().controlled_run().is_err());
        }
        for command in ["tui", "check-venue"] {
            let mut args = live_args();
            args.truncate(args.len() - 2);
            args.push(command);
            assert!(Cli::try_parse_from(args).unwrap().controlled_run().is_err());
        }
    }

    #[test]
    fn legacy_cli_keeps_ordinary_and_fixed_unsigned_and_signed_budgets() {
        assert!(Cli::try_parse_from(["bot", "run", "btc-arb"]).unwrap().controlled_run().unwrap().is_none());
        for (mode, deadline) in [("unsigned-paper", "2026-11-22T04:00:00Z"),
            ("signed-future-live", "2026-11-19T10:00:00Z")] {
            let run = Cli::try_parse_from(["bot", "--controlled-mode", mode,
                "--controlled-run-id", "legacy", "--controlled-started-at", "2026-11-19T04:00:00Z",
                "--controlled-deadline-at", deadline, "run", "btc-arb"])
                .unwrap().controlled_run().unwrap().unwrap();
            assert!(run.live.is_none());
        }
    }
}
