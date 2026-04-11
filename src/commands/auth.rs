use std::time::Duration;

use anyhow::Result;
use reqwest::Client;
use tracing::{info, warn};

use crate::{
    auth::{
        build_authorization_flow, callback_url, exchange_authorization_code, refresh_access_token,
        wait_for_authorization_code,
    },
    browser::open_browser_url,
    cli::AuthArgs,
    config::Config,
    paths::AppPaths,
    state::{
        PendingAuthFlow, ProxyState, StoredAuthStatus, TokenStore, ensure_fake_api_key,
        load_proxy_state, load_tokens, save_proxy_state, save_tokens, stored_auth_status,
    },
    time::unix_timestamp_secs,
};

pub async fn run(paths: AppPaths, args: AuthArgs) -> Result<()> {
    paths.ensure_runtime_layout()?;

    let config = Config::load_or_create(&paths)?;
    let callback_host = args
        .callback_host
        .clone()
        .unwrap_or_else(|| config.auth.callback_host.clone());
    let callback_port = args.callback_port.unwrap_or(config.auth.callback_port);
    let redirect_uri = callback_url(&callback_host, callback_port);
    let fake_api_key = ensure_fake_api_key(&paths)?;
    let existing_tokens = load_tokens(&paths)?;
    let client = Client::new();

    if !args.force {
        match stored_auth_status(existing_tokens.as_ref()) {
            StoredAuthStatus::Valid => {
                print_reusable_auth_message(&fake_api_key, "Stored OAuth tokens are still valid.");
                return Ok(());
            }
            StoredAuthStatus::Expiring | StoredAuthStatus::Expired => {
                if let Some(tokens) = existing_tokens.as_ref() {
                    match try_refresh_tokens(&paths, &config, &client, tokens).await {
                        Ok(Some(refreshed_tokens)) => {
                            let status = refreshed_tokens.auth_status();
                            print_reusable_auth_message(
                                &fake_api_key,
                                &format!(
                                    "Stored OAuth tokens were refreshed successfully. Current status: {}.",
                                    status.as_str()
                                ),
                            );
                            return Ok(());
                        }
                        Ok(None) => {}
                        Err(error) => {
                            warn!(
                                "Stored OAuth refresh failed: {error:#}. Falling back to browser login."
                            );
                        }
                    }
                }
            }
            StoredAuthStatus::Missing => {}
        }
    }

    let auth_flow = build_authorization_flow(&config.upstream, &redirect_uri)?;

    let mut proxy_state = load_proxy_state(&paths)?;
    proxy_state.last_auth_started_at = Some(unix_timestamp_secs());
    proxy_state.pending_auth = Some(PendingAuthFlow::new(
        auth_flow.state.clone(),
        auth_flow.pkce_verifier.clone(),
        redirect_uri.clone(),
        auth_flow.authorize_url.clone(),
    ));
    save_proxy_state(&paths, &proxy_state)?;

    println!(
        "Open this URL to authenticate codex-proxy:\n\n{}\n",
        auth_flow.authorize_url
    );

    if config.auth.open_browser && !args.no_browser && !open_browser_url(&auth_flow.authorize_url) {
        warn!("Failed to launch a browser automatically; use the URL printed above.");
    }

    info!(
        callback_host,
        callback_port, "Waiting for the ChatGPT OAuth callback"
    );

    let auth_result = complete_oauth_flow(
        &config,
        &callback_host,
        callback_port,
        &auth_flow.state,
        &auth_flow.pkce_verifier,
        &redirect_uri,
    )
    .await;

    match auth_result {
        Ok(tokens) => {
            save_tokens(&paths, &tokens)?;

            let mut updated_state = load_proxy_state(&paths)?;
            updated_state.pending_auth = None;
            updated_state.last_auth_completed_at = Some(unix_timestamp_secs());
            save_proxy_state(&paths, &updated_state)?;

            println!("Authentication complete.");
            println!(
                "Use this fake API key with local clients:\n\n{}\n",
                fake_api_key
            );

            if let Some(account_id) = tokens.account_id.as_deref() {
                info!(%account_id, "Stored OAuth tokens for ChatGPT account");
            } else {
                info!("Stored OAuth tokens; account id was not present in the access token");
            }
        }
        Err(error) => {
            clear_pending_auth(&paths)?;
            return Err(error);
        }
    }

    Ok(())
}

async fn complete_oauth_flow(
    config: &Config,
    callback_host: &str,
    callback_port: u16,
    expected_state: &str,
    pkce_verifier: &str,
    redirect_uri: &str,
) -> Result<crate::state::TokenStore> {
    let code = wait_for_authorization_code(
        callback_host,
        callback_port,
        expected_state,
        Duration::from_secs(config.auth.timeout_seconds),
    )
    .await?;
    let client = Client::new();

    exchange_authorization_code(
        &client,
        &config.upstream,
        &code,
        pkce_verifier,
        redirect_uri,
    )
    .await
}

fn clear_pending_auth(paths: &AppPaths) -> Result<()> {
    let mut state = load_proxy_state(paths)?;
    state.pending_auth = None;
    save_proxy_state(paths, &state)
}

async fn try_refresh_tokens(
    paths: &AppPaths,
    config: &Config,
    client: &Client,
    tokens: &TokenStore,
) -> Result<Option<TokenStore>> {
    if matches!(tokens.auth_status(), StoredAuthStatus::Valid) {
        return Ok(None);
    }

    let refreshed_tokens =
        refresh_access_token(client, &config.upstream, &tokens.refresh_token).await?;
    save_tokens(paths, &refreshed_tokens)?;
    clear_pending_auth(paths)?;

    if let Some(account_id) = refreshed_tokens.account_id.as_deref() {
        info!(%account_id, "Refreshed stored OAuth tokens");
    } else {
        info!("Refreshed stored OAuth tokens");
    }

    Ok(Some(refreshed_tokens))
}

fn print_reusable_auth_message(fake_api_key: &str, message: &str) {
    println!("{message}");
    println!("Use this fake API key with local clients:\n\n{fake_api_key}\n");
    println!("Use --force to start a new browser login.");
}

#[allow(dead_code)]
fn _empty_state() -> ProxyState {
    ProxyState::default()
}
