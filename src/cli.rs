//! Command-line interface definition.
//!
//! Two subcommands are exposed:
//! - `start` — launch the proxy daemon.
//! - `auth`  — run the interactive ChatGPT OAuth flow and persist tokens.
//!
//! Both share the global `--config-path` and `--data-dir` flags so that
//! the runtime file layout can be overridden without modifying `config.toml`.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// Top-level CLI struct parsed by Clap.
#[derive(Debug, Parser)]
#[command(
    name = "codex-proxy",
    version,
    about = "A local Codex-compatible proxy backed by ChatGPT OAuth"
)]
pub struct Cli {
    /// Path to the runtime TOML configuration file.
    /// Created with defaults on first run if it does not exist.
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        default_value = "./config.toml",
        help = "Path to the runtime configuration file"
    )]
    pub config_path: PathBuf,

    /// Directory used to store `state.json` (fake API key, pending auth) and
    /// `tokens.json` (OAuth tokens).
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

/// Available subcommands.
#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Start the local proxy daemon.
    Start(StartArgs),
    /// Authenticate with ChatGPT via OAuth and persist the resulting tokens.
    Auth(AuthArgs),
}

/// Arguments for the `start` subcommand.
#[derive(Debug, Clone, Args)]
pub struct StartArgs {
    /// Override the TCP host the proxy binds on (default: value in config.toml).
    #[arg(long, value_name = "HOST", help = "Override the listen host")]
    pub bind: Option<String>,

    /// Override the TCP port the proxy listens on (default: value in config.toml).
    #[arg(long, value_name = "PORT", help = "Override the listen port")]
    pub port: Option<u16>,
}

/// Arguments for the `auth` subcommand.
#[derive(Debug, Clone, Args)]
pub struct AuthArgs {
    /// Skip automatic browser launch; print the authorization URL instead.
    #[arg(long, help = "Do not attempt to open a browser automatically")]
    pub no_browser: bool,

    /// Start a fresh browser login even when stored tokens are still valid or
    /// can be refreshed.
    #[arg(
        long,
        help = "Force a new browser login even when stored tokens are usable"
    )]
    pub force: bool,

    /// Complete a pending OAuth flow by pasting the final browser callback URL.
    ///
    /// Useful when the browser reaches `http://localhost:1455/auth/callback?...`
    /// but cannot connect to the waiting local callback server, for example in
    /// some Docker or remote-environment setups.
    #[arg(
        long,
        value_name = "URL",
        help = "Complete auth using a pasted callback URL"
    )]
    pub callback_url: Option<String>,

    /// Override the host embedded in the OAuth redirect URI.
    #[arg(long, value_name = "HOST", help = "Override the OAuth redirect host")]
    pub callback_host: Option<String>,

    /// Override the host the local OAuth callback server binds on.
    #[arg(
        long,
        value_name = "HOST",
        help = "Override the OAuth callback bind host"
    )]
    pub callback_bind_host: Option<String>,

    /// Override the port the local OAuth callback server listens on.
    #[arg(long, value_name = "PORT", help = "Override the OAuth callback port")]
    pub callback_port: Option<u16>,
}
