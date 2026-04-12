use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::{
    constants::{FAKE_API_KEY_PREFIX, TOKEN_REFRESH_WINDOW_SECONDS},
    paths::AppPaths,
    storage::{read_json, write_json},
    time::unix_timestamp_secs,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredAuthStatus {
    Missing,
    Valid,
    Expiring,
    Expired,
}

impl StoredAuthStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Valid => "valid",
            Self::Expiring => "expiring",
            Self::Expired => "expired",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProxyState {
    pub fake_api_key: Option<String>,
    pub pending_auth: Option<PendingAuthFlow>,
    pub last_auth_started_at: Option<i64>,
    pub last_auth_completed_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingAuthFlow {
    pub oauth_state: String,
    pub pkce_verifier: String,
    pub redirect_uri: String,
    pub authorize_url: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenStore {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
    #[serde(default = "default_token_type")]
    pub token_type: String,
    #[serde(default)]
    pub scope: String,
    pub account_id: Option<String>,
    pub obtained_at: i64,
}

impl TokenStore {
    pub fn expires_soon(&self, window_seconds: i64) -> bool {
        self.expires_at <= unix_timestamp_secs() + window_seconds
    }

    pub fn auth_status(&self) -> StoredAuthStatus {
        self.auth_status_with_window(TOKEN_REFRESH_WINDOW_SECONDS)
    }

    pub fn auth_status_with_window(&self, refresh_window_seconds: i64) -> StoredAuthStatus {
        self.auth_status_at(unix_timestamp_secs(), refresh_window_seconds)
    }

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

pub fn stored_auth_status(tokens: Option<&TokenStore>) -> StoredAuthStatus {
    tokens
        .map(TokenStore::auth_status)
        .unwrap_or(StoredAuthStatus::Missing)
}

pub fn load_proxy_state(paths: &AppPaths) -> Result<ProxyState> {
    Ok(read_json(&paths.state_path)?.unwrap_or_default())
}

pub fn save_proxy_state(paths: &AppPaths, state: &ProxyState) -> Result<()> {
    write_json(&paths.state_path, state)
}

pub fn load_tokens(paths: &AppPaths) -> Result<Option<TokenStore>> {
    read_json(&paths.tokens_path)
}

pub fn save_tokens(paths: &AppPaths, tokens: &TokenStore) -> Result<()> {
    write_json(&paths.tokens_path, tokens)
}

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
