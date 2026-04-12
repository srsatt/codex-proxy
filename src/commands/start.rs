//! `codex-proxy start` command handler.
//!
//! Loads configuration, ensures the fake API key exists, optionally refreshes
//! expiring tokens, and launches the Axum proxy server.

use anyhow::Result;
use reqwest::Client;
use tracing::{info, warn};

use crate::{
    auth::refresh_access_token,
    cli::StartArgs,
    config::Config,
    paths::AppPaths,
    proxy::{ProxyAppState, serve},
    state::{StoredAuthStatus, ensure_fake_api_key, load_tokens, save_tokens, stored_auth_status},
};

/// Entry point for `codex-proxy start`.
///
/// 1. Loads (or creates) `config.toml`.
/// 2. Ensures the fake API key is generated and stored.
/// 3. Attempts a token refresh if stored tokens are expiring or expired.
/// 4. Starts the Axum HTTP server and blocks until it exits.
pub async fn run(paths: AppPaths, args: StartArgs) -> Result<()> {
    paths.ensure_runtime_layout()?;

    let config = Config::load_or_create(&paths)?;
    let fake_api_key = ensure_fake_api_key(&paths)?;
    let mut tokens = load_tokens(&paths)?;

    let bind = args.bind.unwrap_or_else(|| config.server.bind.clone());
    let port = args.port.unwrap_or(config.server.port);

    info!(
        config_path = %paths.config_path.display(),
        data_dir = %paths.data_dir.display(),
        state_path = %paths.state_path.display(),
        tokens_path = %paths.tokens_path.display(),
        "Loaded runtime paths"
    );

    match stored_auth_status(tokens.as_ref()) {
        StoredAuthStatus::Missing => {
            warn!(
                "No OAuth tokens found yet; the skeleton server will still start, but upstream forwarding is not implemented."
            );
        }
        StoredAuthStatus::Valid => {
            info!("Stored OAuth tokens are valid.");
        }
        StoredAuthStatus::Expiring | StoredAuthStatus::Expired => {
            let client = Client::new();
            if let Some(current_tokens) = tokens.as_ref() {
                match refresh_access_token(&client, &config.upstream, &current_tokens.refresh_token)
                    .await
                {
                    Ok(refreshed_tokens) => {
                        save_tokens(&paths, &refreshed_tokens)?;
                        tokens = Some(refreshed_tokens);
                        info!("Stored OAuth tokens were refreshed on startup.");
                    }
                    Err(error) => {
                        warn!("Stored OAuth refresh failed on startup: {error:#}");
                    }
                }
            }
        }
    }

    info!(
        bind,
        port,
        fake_api_key_present = !fake_api_key.is_empty(),
        "Starting codex-proxy skeleton server"
    );

    serve(
        bind,
        port,
        ProxyAppState::new(config, paths, fake_api_key, tokens),
    )
    .await
}
