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

    let mut cfg =
        AppConfig::load(&cli.config, &cli.credentials).context("loading configuration")?;
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
