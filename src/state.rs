//! Persistent daemon state and OAuth token management.
//!
//! Two JSON files are managed here:
//! - **`state.json`** ([`ProxyState`]) — stores the fake API key that clients
//!   must present to use the proxy, plus metadata about ongoing or completed
//!   OAuth flows.
//! - **`tokens.json`** ([`TokenStore`]) — stores the OAuth access and refresh
//!   tokens obtained from ChatGPT.
//!
//! The fake API key is generated once via `openssl rand -hex 24` and re-used
//! across daemon restarts. Clients configure their OpenAI-compatible tools to
//! use this key as if it were a real API key.

use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::{
    constants::{FAKE_API_KEY_PREFIX, TOKEN_REFRESH_WINDOW_SECONDS},
    paths::AppPaths,
    storage::{read_json, write_json},
    time::unix_timestamp_secs,
};

/// Classification of the stored OAuth token lifecycle state.
///
/// The proxy uses this to decide whether to proceed with a request, attempt a
/// background refresh, or require the user to re-authenticate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredAuthStatus {
    /// No token file exists. The user must run `codex-proxy auth`.
    Missing,
    /// The token is valid and not close to expiry.
    Valid,
    /// The token will expire within [`TOKEN_REFRESH_WINDOW_SECONDS`] seconds.
    /// A proactive refresh should be attempted.
    Expiring,
    /// The token has already expired. A refresh is required before forwarding.
    Expired,
}

impl StoredAuthStatus {
    /// Return a short lowercase string suitable for logs and JSON payloads.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Valid => "valid",
            Self::Expiring => "expiring",
            Self::Expired => "expired",
        }
    }
}

/// Contents of `data/state.json`. Persisted across daemon restarts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProxyState {
    /// The fake API key presented to local clients. Generated once and reused.
    pub fake_api_key: Option<String>,
    /// In-flight OAuth flow data written before opening the browser and cleared
    /// after the code is exchanged for tokens.
    pub pending_auth: Option<PendingAuthFlow>,
    /// Unix timestamp of the most recent `codex-proxy auth` invocation.
    pub last_auth_started_at: Option<i64>,
    /// Unix timestamp of the last successful token exchange.
    pub last_auth_completed_at: Option<i64>,
}

/// Ephemeral OAuth flow state saved to `state.json` while the browser-based
/// login is in progress.
///
/// Saved before the browser is launched and removed once the authorization
/// code has been exchanged for tokens (or on error). Allows a crashed or
/// interrupted auth flow to be detected on the next startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingAuthFlow {
    /// Random UUID used as the OAuth `state` parameter for CSRF protection.
    pub oauth_state: String,
    /// PKCE verifier that must be sent alongside the authorization code.
    pub pkce_verifier: String,
    /// OAuth `redirect_uri` registered with the callback server.
    pub redirect_uri: String,
    /// The full authorization URL that was opened in the browser.
    pub authorize_url: String,
    /// Unix timestamp when this flow was started.
    pub created_at: i64,
}

impl PendingAuthFlow {
    pub fn new(
        oauth_state: String,
        pkce_verifier: String,
        redirect_uri: String,
        authorize_url: String,
    ) -> Self {
        Self {
            oauth_state,
            pkce_verifier,
            redirect_uri,
            authorize_url,
            created_at: unix_timestamp_secs(),
        }
    }
}

/// Contents of `data/tokens.json`. Persisted OAuth credentials.
///
/// Written after every successful token exchange or refresh and read on daemon
/// startup to restore forwarding capability without requiring a new login.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenStore {
    /// Short-lived JWT used as the `Authorization: Bearer` header for upstream
    /// requests.
    pub access_token: String,
    /// Long-lived token used to obtain a new `access_token` when it expires.
    pub refresh_token: String,
    /// Unix timestamp when `access_token` expires.
    pub expires_at: i64,
    /// Token type from the OAuth response (typically `"Bearer"`).
    #[serde(default = "default_token_type")]
    pub token_type: String,
    /// Scopes granted by the OAuth server.
    #[serde(default)]
    pub scope: String,
    /// ChatGPT account identifier decoded from the access token JWT payload.
    /// Required by the upstream API in the `chatgpt-account-id` request header.
    pub account_id: Option<String>,
    /// Unix timestamp when these tokens were obtained.
    pub obtained_at: i64,
}

impl TokenStore {
    /// Returns `true` if `expires_at` is within `window_seconds` of now.
    pub fn expires_soon(&self, window_seconds: i64) -> bool {
        self.expires_at <= unix_timestamp_secs() + window_seconds
    }

    /// Classify the token using the standard [`TOKEN_REFRESH_WINDOW_SECONDS`].
    pub fn auth_status(&self) -> StoredAuthStatus {
        self.auth_status_with_window(TOKEN_REFRESH_WINDOW_SECONDS)
    }

    /// Classify the token using a caller-supplied refresh window. Useful in
    /// tests where the window must be varied independently of the constant.
    pub fn auth_status_with_window(&self, refresh_window_seconds: i64) -> StoredAuthStatus {
        self.auth_status_at(unix_timestamp_secs(), refresh_window_seconds)
    }

    /// Internal implementation that accepts an explicit `now` timestamp so
    /// tests can exercise all state transitions deterministically.
    fn auth_status_at(&self, now: i64, refresh_window_seconds: i64) -> StoredAuthStatus {
        if self.expires_at <= now {
            StoredAuthStatus::Expired
        } else if self.expires_at <= now + refresh_window_seconds {
            StoredAuthStatus::Expiring
        } else {
            StoredAuthStatus::Valid
        }
    }
}

/// Convenience wrapper: return the [`StoredAuthStatus`] for an optional token.
///
/// Returns [`StoredAuthStatus::Missing`] when `tokens` is `None`, avoiding
/// repetitive `Option::map` boilerplate at call sites.
pub fn stored_auth_status(tokens: Option<&TokenStore>) -> StoredAuthStatus {
    tokens
        .map(TokenStore::auth_status)
        .unwrap_or(StoredAuthStatus::Missing)
}

/// Load [`ProxyState`] from `data/state.json`, returning a default value if
/// the file does not exist yet.
pub fn load_proxy_state(paths: &AppPaths) -> Result<ProxyState> {
    Ok(read_json(&paths.state_path)?.unwrap_or_default())
}

/// Persist [`ProxyState`] to `data/state.json`.
pub fn save_proxy_state(paths: &AppPaths, state: &ProxyState) -> Result<()> {
    write_json(&paths.state_path, state)
}

/// Load [`TokenStore`] from `data/tokens.json`.
///
/// Returns `Ok(None)` when the file does not exist (i.e. the user has not
/// authenticated yet).
pub fn load_tokens(paths: &AppPaths) -> Result<Option<TokenStore>> {
    read_json(&paths.tokens_path)
}

/// Persist [`TokenStore`] to `data/tokens.json`.
pub fn save_tokens(paths: &AppPaths, tokens: &TokenStore) -> Result<()> {
    write_json(&paths.tokens_path, tokens)
}

/// Return the fake API key, generating and persisting it if necessary.
///
/// The key is stored in `state.json` so it survives daemon restarts. Clients
/// must present this key to use any proxy endpoint. The key is generated once
/// via `openssl rand -hex 24` and prefixed with [`FAKE_API_KEY_PREFIX`].
pub fn ensure_fake_api_key(paths: &AppPaths) -> Result<String> {
    paths.ensure_runtime_layout()?;

    let mut state = load_proxy_state(paths)?;
    if let Some(fake_api_key) = state.fake_api_key.clone() {
        return Ok(fake_api_key);
    }

    let fake_api_key = generate_fake_api_key()?;
    state.fake_api_key = Some(fake_api_key.clone());
    save_proxy_state(paths, &state)?;
    Ok(fake_api_key)
}

fn generate_fake_api_key() -> Result<String> {
    let output = Command::new("openssl")
        .args(["rand", "-hex", "24"])
        .output()
        .context("failed to execute `openssl rand -hex 24`")?;

    if !output.status.success() {
        bail!(
            "`openssl rand -hex 24` exited with status {}",
            output.status
        );
    }

    let suffix = String::from_utf8(output.stdout)
        .context("openssl returned non-UTF-8 bytes")?
        .trim()
        .to_owned();

    if suffix.is_empty() {
        bail!("openssl returned an empty fake API key payload");
    }

    Ok(format!("{FAKE_API_KEY_PREFIX}{suffix}"))
}

fn default_token_type() -> String {
    "Bearer".to_owned()
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn token_store_reports_valid_expiring_and_expired_states() {
        let token = TokenStore {
            access_token: "access".to_owned(),
            refresh_token: "refresh".to_owned(),
            expires_at: 1_000,
            token_type: "Bearer".to_owned(),
            scope: "openid".to_owned(),
            account_id: Some("account-123".to_owned()),
            obtained_at: 100,
        };

        assert_eq!(token.auth_status_at(100, 300), StoredAuthStatus::Valid);
        assert_eq!(token.auth_status_at(750, 300), StoredAuthStatus::Expiring);
        assert_eq!(token.auth_status_at(1_000, 300), StoredAuthStatus::Expired);
        assert_eq!(token.auth_status_at(1_100, 300), StoredAuthStatus::Expired);
    }

    #[test]
    fn stored_auth_status_reports_missing_without_tokens() {
        assert_eq!(stored_auth_status(None), StoredAuthStatus::Missing);
    }

    #[test]
    fn save_and_load_tokens_round_trip() {
        let temp: tempfile::TempDir = tempdir().expect("tempdir");
        let paths = AppPaths::new(temp.path().join("config.toml"), temp.path().join("data"));
        let tokens = TokenStore {
            access_token: "access-token".to_owned(),
            refresh_token: "refresh-token".to_owned(),
            expires_at: 1_234_567,
            token_type: "Bearer".to_owned(),
            scope: "openid profile".to_owned(),
            account_id: Some("account-123".to_owned()),
            obtained_at: 1_111_111,
        };

        save_tokens(&paths, &tokens).expect("save tokens");
        let loaded = load_tokens(&paths)
            .expect("load tokens")
            .expect("tokens should exist");

        assert_eq!(loaded.access_token, tokens.access_token);
        assert_eq!(loaded.refresh_token, tokens.refresh_token);
        assert_eq!(loaded.account_id, tokens.account_id);
        assert_eq!(loaded.scope, tokens.scope);
    }
}
