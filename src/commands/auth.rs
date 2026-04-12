//! `codex-proxy auth` command handler.
//!
//! Runs the interactive ChatGPT OAuth PKCE flow:
//! 1. Check whether stored tokens are still valid (skip browser login if so).
//! 2. Attempt a token refresh for expiring/expired tokens (skip browser if ok).
//! 3. Build an authorization URL and open the system browser.
//! 4. Start a local callback server and wait for the authorization code.
//! 5. Exchange the code for tokens and persist them to `tokens.json`.
//!
//! Use `--force` to restart the browser flow even when stored tokens are valid.

use std::{io, io::IsTerminal, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::Client;
use tokio::io::AsyncBufReadExt;
use tracing::{info, warn};
use url::Url;

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
    let fake_api_key = ensure_fake_api_key(&paths)?;

    if let Some(callback_url) = args.callback_url.as_deref() {
        return complete_pasted_callback_flow(&paths, &config, &fake_api_key, callback_url).await;
    }

    let redirect_host = args
        .callback_host
        .clone()
        .unwrap_or_else(|| config.auth.redirect_host.clone());
    let callback_bind_host = args
        .callback_bind_host
        .clone()
        .unwrap_or_else(|| config.auth.callback_bind_host.clone());
    let callback_port = args.callback_port.unwrap_or(config.auth.callback_port);
    let redirect_uri = callback_url(&redirect_host, callback_port);
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
    if io::stdin().is_terminal() {
        println!(
            "If the browser later shows a localhost callback URL but cannot connect,\n\
paste the full URL from the address bar into this terminal and press Enter.\n\
You can also use a second terminal:\n\n  \
codex-proxy auth --callback-url '<full-callback-url>'\n"
        );
    }

    if config.auth.open_browser && !args.no_browser && !open_browser_url(&auth_flow.authorize_url) {
        warn!("Failed to launch a browser automatically; use the URL printed above.");
    }

    info!(
        callback_bind_host,
        redirect_host,
        callback_port,
        %redirect_uri,
        "Waiting for the ChatGPT OAuth callback"
    );

    let auth_result = complete_oauth_flow(
        &config,
        &callback_bind_host,
        callback_port,
        &auth_flow.state,
        &auth_flow.pkce_verifier,
        &redirect_uri,
        io::stdin().is_terminal(),
    )
    .await;

    match auth_result {
        Ok(tokens) => {
            finalize_successful_auth(&paths, &fake_api_key, &tokens)?;
        }
        Err(error) => {
            warn!(
                "OAuth flow did not complete: {error:#}. The pending auth state was preserved so you can retry or use `codex-proxy auth --callback-url ...`."
            );
            return Err(error);
        }
    }

    Ok(())
}

async fn complete_oauth_flow(
    config: &Config,
    callback_bind_host: &str,
    callback_port: u16,
    expected_state: &str,
    pkce_verifier: &str,
    redirect_uri: &str,
    allow_paste_fallback: bool,
) -> Result<crate::state::TokenStore> {
    let code = if allow_paste_fallback {
        tokio::select! {
            result = wait_for_authorization_code(
                callback_bind_host,
                callback_port,
                expected_state,
                Duration::from_secs(config.auth.timeout_seconds),
            ) => result?,
            result = wait_for_pasted_callback_code(expected_state) => result?,
        }
    } else {
        wait_for_authorization_code(
            callback_bind_host,
            callback_port,
            expected_state,
            Duration::from_secs(config.auth.timeout_seconds),
        )
        .await?
    };
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

async fn complete_pasted_callback_flow(
    paths: &AppPaths,
    config: &Config,
    fake_api_key: &str,
    callback_url: &str,
) -> Result<()> {
    let pending = load_proxy_state(paths)?
        .pending_auth
        .ok_or_else(|| anyhow!("no pending OAuth flow found; run `codex-proxy auth` first"))?;
    let parsed = parse_callback_url(callback_url)?;

    if let Some(error_code) = parsed.error.as_deref() {
        let description = parsed
            .error_description
            .as_deref()
            .unwrap_or("no description");
        bail!("OAuth provider returned error `{error_code}`: {description}");
    }

    let returned_state = parsed
        .state
        .as_deref()
        .ok_or_else(|| anyhow!("callback URL did not contain a `state` parameter"))?;
    if returned_state != pending.oauth_state {
        bail!("callback URL state did not match the pending OAuth flow");
    }

    let code = parsed
        .code
        .as_deref()
        .ok_or_else(|| anyhow!("callback URL did not contain an authorization `code` parameter"))?;

    let client = Client::new();
    let tokens = exchange_authorization_code(
        &client,
        &config.upstream,
        code,
        &pending.pkce_verifier,
        &pending.redirect_uri,
    )
    .await
    .context("failed to exchange the pasted callback URL for OAuth tokens")?;

    finalize_successful_auth(paths, fake_api_key, &tokens)?;
    Ok(())
}

fn finalize_successful_auth(
    paths: &AppPaths,
    fake_api_key: &str,
    tokens: &TokenStore,
) -> Result<()> {
    save_tokens(paths, tokens)?;

    let mut updated_state = load_proxy_state(paths)?;
    updated_state.pending_auth = None;
    updated_state.last_auth_completed_at = Some(unix_timestamp_secs());
    save_proxy_state(paths, &updated_state)?;

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

    Ok(())
}

async fn wait_for_pasted_callback_code(expected_state: &str) -> Result<String> {
    let stdin = tokio::io::stdin();
    let mut lines = tokio::io::BufReader::new(stdin).lines();

    loop {
        let Some(line) = lines
            .next_line()
            .await
            .context("failed while reading a pasted callback URL from stdin")?
        else {
            bail!("stdin closed before a pasted callback URL was received");
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parsed = parse_callback_url(line)?;
        if let Some(error_code) = parsed.error.as_deref() {
            let description = parsed
                .error_description
                .as_deref()
                .unwrap_or("no description");
            bail!("OAuth provider returned error `{error_code}`: {description}");
        }

        let returned_state = parsed
            .state
            .as_deref()
            .ok_or_else(|| anyhow!("callback URL did not contain a `state` parameter"))?;
        if returned_state != expected_state {
            bail!("callback URL state did not match the pending OAuth flow");
        }

        let code = parsed.code.ok_or_else(|| {
            anyhow!("callback URL did not contain an authorization `code` parameter")
        })?;

        info!("received pasted OAuth callback URL from stdin");
        return Ok(code);
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedCallbackUrl {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

fn parse_callback_url(raw: &str) -> Result<ParsedCallbackUrl> {
    let raw = raw.trim();
    let candidate = if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.to_owned()
    } else if raw.starts_with('/') {
        format!("http://localhost{raw}")
    } else if raw.starts_with('?') {
        format!("http://localhost/auth/callback{raw}")
    } else {
        bail!("callback URL must be a full URL or callback path/query copied from the browser");
    };

    let url = Url::parse(&candidate).context("failed to parse callback URL")?;
    let query_pairs = url.query_pairs();

    let mut parsed = ParsedCallbackUrl {
        code: None,
        state: None,
        error: None,
        error_description: None,
    };
    for (key, value) in query_pairs {
        match key.as_ref() {
            "code" => parsed.code = Some(value.into_owned()),
            "state" => parsed.state = Some(value.into_owned()),
            "error" => parsed.error = Some(value.into_owned()),
            "error_description" => parsed.error_description = Some(value.into_owned()),
            _ => {}
        }
    }

    Ok(parsed)
}

#[allow(dead_code)]
fn _empty_state() -> ProxyState {
    ProxyState::default()
}

#[cfg(test)]
mod tests {
    use super::{ParsedCallbackUrl, parse_callback_url};

    #[test]
    fn parse_callback_url_reads_code_and_state_from_full_url() {
        let parsed =
            parse_callback_url("http://localhost:1455/auth/callback?code=abc123&state=state-456")
                .expect("parsed");

        assert_eq!(
            parsed,
            ParsedCallbackUrl {
                code: Some("abc123".to_owned()),
                state: Some("state-456".to_owned()),
                error: None,
                error_description: None,
            }
        );
    }

    #[test]
    fn parse_callback_url_supports_query_only_input() {
        let parsed = parse_callback_url("?code=abc123&state=state-456").expect("parsed");

        assert_eq!(parsed.code.as_deref(), Some("abc123"));
        assert_eq!(parsed.state.as_deref(), Some("state-456"));
    }

    #[test]
    fn parse_callback_url_reads_oauth_errors() {
        let parsed = parse_callback_url(
            "http://localhost:1455/auth/callback?error=access_denied&error_description=denied",
        )
        .expect("parsed");

        assert_eq!(parsed.error.as_deref(), Some("access_denied"));
        assert_eq!(parsed.error_description.as_deref(), Some("denied"));
    }
}
