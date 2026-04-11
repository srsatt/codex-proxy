mod callback;

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use url::Url;
use uuid::Uuid;

use crate::{
    config::UpstreamConfig, constants::CALLBACK_PATH, state::TokenStore, time::unix_timestamp_secs,
};

pub use callback::wait_for_authorization_code;

#[derive(Debug, Clone)]
pub struct AuthorizationFlow {
    pub state: String,
    pub pkce_verifier: String,
    pub code_challenge: String,
    pub authorize_url: String,
}

#[derive(Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    refresh_token: String,
    expires_in: i64,
    #[serde(default = "default_token_type")]
    token_type: String,
    #[serde(default)]
    scope: String,
}

pub fn build_authorization_flow(
    upstream: &UpstreamConfig,
    redirect_uri: &str,
) -> Result<AuthorizationFlow> {
    let state = Uuid::new_v4().simple().to_string();
    let pkce_verifier = create_pkce_verifier();
    let code_challenge = create_code_challenge(&pkce_verifier);

    let mut authorize_url =
        Url::parse(&upstream.authorize_url).context("invalid OAuth authorize URL")?;
    authorize_url
        .query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &upstream.client_id)
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("scope", &upstream.scope)
        .append_pair("code_challenge", &code_challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", &state)
        .append_pair("id_token_add_organizations", "true")
        .append_pair("codex_cli_simplified_flow", "true")
        .append_pair("originator", &upstream.originator);

    Ok(AuthorizationFlow {
        state,
        pkce_verifier,
        code_challenge,
        authorize_url: authorize_url.to_string(),
    })
}

pub async fn exchange_authorization_code(
    client: &Client,
    upstream: &UpstreamConfig,
    code: &str,
    pkce_verifier: &str,
    redirect_uri: &str,
) -> Result<TokenStore> {
    request_tokens(
        client,
        upstream,
        vec![
            ("grant_type", "authorization_code".to_owned()),
            ("client_id", upstream.client_id.clone()),
            ("code", code.to_owned()),
            ("code_verifier", pkce_verifier.to_owned()),
            ("redirect_uri", redirect_uri.to_owned()),
        ],
    )
    .await
}

pub async fn refresh_access_token(
    client: &Client,
    upstream: &UpstreamConfig,
    refresh_token: &str,
) -> Result<TokenStore> {
    request_tokens(
        client,
        upstream,
        vec![
            ("grant_type", "refresh_token".to_owned()),
            ("client_id", upstream.client_id.clone()),
            ("refresh_token", refresh_token.to_owned()),
        ],
    )
    .await
}

async fn request_tokens(
    client: &Client,
    upstream: &UpstreamConfig,
    form_fields: Vec<(&str, String)>,
) -> Result<TokenStore> {
    let response = client
        .post(&upstream.token_url)
        .form(&form_fields)
        .send()
        .await
        .context("failed to reach the OAuth token endpoint")?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let body = body.trim();
        let detail = if body.is_empty() {
            format!("OAuth token request failed with status {status}")
        } else {
            format!("OAuth token request failed with status {status}: {body}")
        };
        anyhow::bail!(detail);
    }

    let token_response: OAuthTokenResponse = response
        .json()
        .await
        .context("failed to parse the OAuth token response")?;
    let obtained_at = unix_timestamp_secs();

    Ok(TokenStore {
        account_id: decode_jwt_account_id(&token_response.access_token)?,
        access_token: token_response.access_token,
        refresh_token: token_response.refresh_token,
        expires_at: obtained_at + token_response.expires_in,
        token_type: token_response.token_type,
        scope: token_response.scope,
        obtained_at,
    })
}

pub fn decode_jwt_account_id(token: &str) -> Result<Option<String>> {
    let mut parts = token.split('.');
    let _header = parts.next();
    let payload = match parts.next() {
        Some(value) => value,
        None => return Ok(None),
    };

    let decoded = URL_SAFE_NO_PAD
        .decode(payload.as_bytes())
        .context("failed to decode JWT payload")?;
    let payload: serde_json::Value =
        serde_json::from_slice(&decoded).context("failed to parse JWT payload")?;

    Ok(payload
        .get("https://api.openai.com/auth")
        .and_then(|value| value.get("chatgpt_account_id"))
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned))
}

pub fn callback_url(host: &str, port: u16) -> String {
    format!("http://{host}:{port}{CALLBACK_PATH}")
}

fn create_pkce_verifier() -> String {
    let mut raw = Vec::with_capacity(32);
    raw.extend_from_slice(Uuid::new_v4().as_bytes());
    raw.extend_from_slice(Uuid::new_v4().as_bytes());
    URL_SAFE_NO_PAD.encode(raw)
}

fn create_code_challenge(pkce_verifier: &str) -> String {
    let digest = Sha256::digest(pkce_verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(digest)
}

fn default_token_type() -> String {
    "Bearer".to_owned()
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use axum::{Json, Router, routing::post};
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn build_authorization_flow_uses_expected_parameters() {
        let upstream = UpstreamConfig::default();
        let redirect_uri = "http://127.0.0.1:1455/auth/callback";

        let flow = build_authorization_flow(&upstream, redirect_uri).expect("flow");
        let url = Url::parse(&flow.authorize_url).expect("url");

        assert_eq!(url.path(), "/oauth/authorize");
        assert_eq!(
            url.query_pairs()
                .find(|(key, _)| key == "response_type")
                .map(|(_, value)| value.to_string())
                .as_deref(),
            Some("code")
        );
        assert_eq!(
            url.query_pairs()
                .find(|(key, _)| key == "client_id")
                .map(|(_, value)| value.to_string())
                .as_deref(),
            Some(upstream.client_id.as_str())
        );
        assert_eq!(
            url.query_pairs()
                .find(|(key, _)| key == "redirect_uri")
                .map(|(_, value)| value.to_string())
                .as_deref(),
            Some(redirect_uri)
        );
        assert_eq!(
            url.query_pairs()
                .find(|(key, _)| key == "originator")
                .map(|(_, value)| value.to_string())
                .as_deref(),
            Some(upstream.originator.as_str())
        );
        assert!(!flow.pkce_verifier.is_empty());
        assert!(!flow.code_challenge.is_empty());
    }

    #[test]
    fn decode_jwt_account_id_reads_chatgpt_account() {
        let payload = json!({
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "account-123"
            }
        });
        let token = format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).expect("payload"))
        );

        let account_id = decode_jwt_account_id(&token).expect("account id");

        assert_eq!(account_id.as_deref(), Some("account-123"));
    }

    #[tokio::test]
    async fn refresh_access_token_reads_and_normalizes_token_response() {
        let account_payload = json!({
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "account-456"
            }
        });
        let access_token = format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&account_payload).expect("payload"))
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let addr: SocketAddr = listener.local_addr().expect("addr");
        async fn token_handler(access_token: String) -> Json<serde_json::Value> {
            Json(json!({
                "access_token": access_token,
                "refresh_token": "new-refresh-token",
                "expires_in": 3600,
                "token_type": "Bearer",
                "scope": "openid profile email offline_access"
            }))
        }
        let app = Router::new().route(
            "/oauth/token",
            post(move || token_handler(access_token.clone())),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });

        let upstream = UpstreamConfig {
            token_url: format!("http://{addr}/oauth/token"),
            ..UpstreamConfig::default()
        };

        let tokens = refresh_access_token(&Client::new(), &upstream, "old-refresh-token")
            .await
            .expect("refresh tokens");

        assert_eq!(tokens.refresh_token, "new-refresh-token");
        assert_eq!(tokens.account_id.as_deref(), Some("account-456"));
        assert_eq!(tokens.scope, "openid profile email offline_access");
        assert!(tokens.expires_at > tokens.obtained_at);

        server.abort();
    }
}
