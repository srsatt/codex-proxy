use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "codex-proxy",
    version,
    about = "A local Codex-compatible proxy backed by ChatGPT OAuth"
)]
pub struct Cli {
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        default_value = "./config.toml",
        help = "Path to the runtime configuration file"
    )]
    pub config_path: PathBuf,

    #[arg(
        long,
        global = true,
        value_name = "DIR",
        default_value = "./data",
        help = "Directory used for state.json and tokens.json"
    )]
    pub data_dir: PathBuf,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Start(StartArgs),
    Auth(AuthArgs),
}

#[derive(Debug, Clone, Args)]
pub struct StartArgs {
    #[arg(long, value_name = "HOST", help = "Override the listen host")]
    pub bind: Option<String>,

    #[arg(long, value_name = "PORT", help = "Override the listen port")]
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Args)]
pub struct AuthArgs {
    #[arg(long, help = "Do not attempt to open a browser automatically")]
    pub no_browser: bool,

    #[arg(
        long,
        help = "Force a new browser login even when stored tokens are usable"
    )]
    pub force: bool,

    #[arg(long, value_name = "HOST", help = "Override the OAuth callback host")]
    pub callback_host: Option<String>,

    #[arg(long, value_name = "PORT", help = "Override the OAuth callback port")]
    pub callback_port: Option<u16>,
}
