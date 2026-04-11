use anyhow::Result;
use clap::Parser;
use tracing_subscriber::{EnvFilter, fmt};

use crate::{
    cli::{Cli, Commands},
    commands,
    config::Config,
    paths::AppPaths,
};

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let paths = AppPaths::new(cli.config_path, cli.data_dir);
    let log_filter = Config::load_log_filter(&paths.config_path);
    init_tracing(&log_filter);

    match cli.command {
        Commands::Start(args) => commands::start::run(paths, args).await,
        Commands::Auth(args) => commands::auth::run(paths, args).await,
    }
}

fn init_tracing(config_log_filter: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        let directive = if config_log_filter.trim().is_empty() {
            "codex_proxy=info,info"
        } else {
            config_log_filter
        };
        EnvFilter::new(directive)
    });

    let _ = fmt().with_env_filter(filter).with_target(false).try_init();
}
