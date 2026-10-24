//! Runtime configuration loaded from `config.toml`.
//!
//! [`Config`] is the root container. If `config.toml` does not exist when the
//! daemon starts, it is written with the compiled-in defaults so users have a
//! concrete starting point for customisation.
//!
//! ## Sections
//! | TOML key            | Rust type                   | Purpose                          |
//! |---------------------|-----------------------------|----------------------------------|
//! | `[server]`          | [`ServerConfig`]            | Bind address and log level        |
//! | `[auth]`            | [`AuthConfig`]              | OAuth callback settings           |
//! | `[upstream]`        | [`UpstreamConfig`]          | ChatGPT backend URLs & credentials |
//! | `[request_defaults]`| [`RequestDefaultsConfig`]   | Default request fields injected upstream |
//! | `[models]`          | [`ModelsConfig`]            | Advertised model list and aliases |

use std::{collections::BTreeMap, fs, path::Path};

use anyhow::Result;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{constants::CALLBACK_PATH, paths::AppPaths, storage::write_string};

/// Root configuration container, deserialized from `config.toml`.
///
/// All sub-sections implement `Default` so missing TOML sections fall back to
/// sensible values without requiring a full config file on first run.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub upstream: UpstreamConfig,
    pub request_defaults: RequestDefaultsConfig,
    pub models: ModelsConfig,
}

impl Config {
    /// Load configuration from `paths.config_path`.
    ///
    /// If the file does not exist, a default [`Config`] is written to disk and
    /// returned. This ensures the user always has an editable `config.toml`
    /// after the first run.
    pub fn load_or_create(paths: &AppPaths) -> Result<Self> {
        paths.ensure_runtime_layout()?;

        if paths.config_path.exists() {
            let content = fs::read_to_string(&paths.config_path)?;
            let config = toml::from_str(&content)?;
            return Ok(config);
        }

        let config = Self::default();
        let rendered = toml::to_string_pretty(&config)?;
        write_string(&paths.config_path, &rendered)?;
        Ok(config)
    }

    /// Read only the `server.log_filter` value from `config_path`.
    ///
    /// This is used during startup *before* the rest of the config is parsed so
    /// that structured logging can be initialised at the correct level without
    /// having to fully validate the entire config file first.
    /// Returns the compiled-in default when the file is missing or unparseable.
    pub fn load_log_filter(config_path: &Path) -> String {
        if !config_path.exists() {
            return Self::default().server.log_filter;
        }

        fs::read_to_string(config_path)
            .ok()
            .and_then(|content| toml::from_str::<Self>(&content).ok())
            .map(|config| config.server.log_filter)
            .unwrap_or_else(|| Self::default().server.log_filter)
    }
}

/// HTTP server settings for the local proxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Host the proxy TCP listener binds on. Default: `127.0.0.1`.
    pub bind: String,
    /// Port the proxy TCP listener binds on. Default: `8317`.
    pub port: u16,
    /// `tracing` filter directive (same syntax as `RUST_LOG`). Default: `info`.
    pub log_filter: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_owned(),
            port: 8317,
            log_filter: "info".to_owned(),
        }
    }
}

/// OAuth callback listener settings used during `codex-proxy auth`.
#[derive(Debug, Clone, Serialize)]
pub struct AuthConfig {
    /// Host the temporary OAuth callback server binds on. Default: `127.0.0.1`.
    ///
    /// When authenticating from inside Docker, this typically needs to be
    /// `0.0.0.0` so the published callback port can reach the container.
    pub callback_bind_host: String,
    /// Host embedded in the OAuth `redirect_uri`. Default: `localhost`.
    ///
    /// The current first-party Codex OAuth client uses `localhost` in the
    /// registered redirect URI; using `127.0.0.1` can cause the upstream auth
    /// page to fail before the callback is reached.
    pub redirect_host: String,
    /// Port the temporary OAuth callback server listens on. Default: `1455`.
    pub callback_port: u16,
    /// Whether to attempt to open the authorization URL in the system browser
    /// automatically. Default: `true`.
    pub open_browser: bool,
    /// Seconds to wait for the browser to complete the OAuth flow before the
    /// callback listener times out. Default: `300`.
    pub timeout_seconds: u64,
}

impl AuthConfig {
    /// Build the full OAuth `redirect_uri` from the callback host and port.
    pub fn redirect_uri(&self) -> String {
        format!(
            "http://{}:{}{}",
            self.redirect_host, self.callback_port, CALLBACK_PATH
        )
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            callback_bind_host: "127.0.0.1".to_owned(),
            redirect_host: "localhost".to_owned(),
            callback_port: 1455,
            open_browser: true,
            timeout_seconds: 300,
        }
    }
}

impl<'de> Deserialize<'de> for AuthConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Default, Deserialize)]
        #[serde(default)]
        struct RawAuthConfig {
            callback_host: Option<String>,
            callback_bind_host: Option<String>,
            redirect_host: Option<String>,
            callback_port: Option<u16>,
            open_browser: Option<bool>,
            timeout_seconds: Option<u64>,
        }

        let raw = RawAuthConfig::deserialize(deserializer)?;
        let defaults = AuthConfig::default();
        let legacy_callback_host = raw.callback_host.as_deref().map(str::trim);

        let callback_bind_host = raw
            .callback_bind_host
            .or_else(|| raw.callback_host.clone())
            .unwrap_or(defaults.callback_bind_host);

        let redirect_host = raw
            .redirect_host
            .unwrap_or_else(|| match legacy_callback_host {
                Some("127.0.0.1") | Some("0.0.0.0") => "localhost".to_owned(),
                Some(host) if !host.is_empty() => host.to_owned(),
                _ => defaults.redirect_host,
            });

        Ok(Self {
            callback_bind_host,
            redirect_host,
            callback_port: raw.callback_port.unwrap_or(defaults.callback_port),
            open_browser: raw.open_browser.unwrap_or(defaults.open_browser),
            timeout_seconds: raw.timeout_seconds.unwrap_or(defaults.timeout_seconds),
        })
    }
}

/// URLs, credentials, and headers for the upstream ChatGPT / OpenAI endpoints.
///
/// These defaults are derived from the TypeScript reference implementation and
/// should not need to change under normal circumstances.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UpstreamConfig {
    /// Base URL of the ChatGPT backend API. Default: `https://chatgpt.com/backend-api`.
    pub responses_base_url: String,
    /// Path appended to `responses_base_url` for the Codex responses endpoint.
    /// Default: `/codex/responses`.
    pub codex_responses_path: String,
    /// Path appended to `responses_base_url` for the Codex compact endpoint.
    /// Default: `/codex/responses/compact`.
    pub codex_compact_path: String,
    /// OAuth authorization endpoint. Default: `https://auth.openai.com/oauth/authorize`.
    pub authorize_url: String,
    /// OAuth token endpoint. Default: `https://auth.openai.com/oauth/token`.
    pub token_url: String,
    /// OAuth `client_id` for the Codex CLI application.
    pub client_id: String,
    /// OAuth scopes requested during authorization.
    pub scope: String,
    /// Value sent in the `originator` header and as a query parameter during
    /// authorization. Identifies this client to the backend.
    pub originator: String,
    /// Value sent in the `OpenAI-Beta` header to opt in to the experimental
    /// responses API. Default: `responses=experimental`.
    pub beta_header: String,
    /// Interval in seconds between SSE keep-alive comment heartbeats sent to
    /// clients during streaming responses. Set to `0` to disable. Default: `15`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_alive_interval_secs: Option<u64>,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            responses_base_url: "https://chatgpt.com/backend-api".to_owned(),
            codex_responses_path: "/codex/responses".to_owned(),
            codex_compact_path: "/codex/responses/compact".to_owned(),
            authorize_url: "https://auth.openai.com/oauth/authorize".to_owned(),
            token_url: "https://auth.openai.com/oauth/token".to_owned(),
            client_id: "app_EMoamEEZ73f0CkXaXp7hrann".to_owned(),
            scope: "openid profile email offline_access".to_owned(),
            originator: "codex_cli_rs".to_owned(),
            beta_header: "responses=experimental".to_owned(),
            keep_alive_interval_secs: Some(15),
        }
    }
}

/// Default fields injected into every outgoing request by the proxy.
///
/// These are generic request-shaping defaults for the client-facing
/// OpenAI-compatible surface exposed by `codex-proxy`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RequestDefaultsConfig {
    /// Items to append to the `include` array on every request.
    /// Defaults to `["reasoning.encrypted_content"]` so reasoning tokens are
    /// always returned.
    pub include: Vec<String>,
    /// Optional default for the `reasoning.effort` field (`"low"`, `"medium"`,
    /// `"high"`). Omitted when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Optional default for the `reasoning.summary` field. Omitted when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_summary: Option<String>,
    /// Optional default for the `text.verbosity` field. Omitted when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_verbosity: Option<String>,
}

impl Default for RequestDefaultsConfig {
    fn default() -> Self {
        Self {
            include: vec!["reasoning.encrypted_content".to_owned()],
            reasoning_effort: None,
            reasoning_summary: None,
            text_verbosity: None,
        }
    }
}

/// Model advertisement and normalisation configuration.
///
/// Clients send model names that may differ from what the upstream API expects.
/// `ModelsConfig` provides a two-level lookup:
/// 1. **Advertised list** — exact-match (case-insensitive) against known names.
/// 2. **Aliases map** — maps alternative names (e.g. `openai/gpt-5.4`) to
///    canonical upstream names.
///
/// If neither lookup matches, the request is rejected; models are never silently substituted.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelsConfig {
    /// Model IDs returned by `GET /v1/models` and accepted by the proxy.
    pub advertised: Vec<String>,
    /// Map of alternative / prefixed names to their canonical upstream names.
    /// Keys are case-insensitive during lookup.
    pub aliases: BTreeMap<String, String>,
    /// Legacy configuration field retained for compatibility; unknown models are rejected.
    pub fallback: String,
}

impl ModelsConfig {
    /// Resolve a client-supplied `model` string to the canonical upstream name.
    ///
    /// Lookup order:
    /// 1. Exact (case-insensitive) match in `advertised`.
    /// 2. Strip any provider prefix (`openai/` → `gpt-5.4-mini`) and retry
    ///    step 1.
    /// 3. Exact (case-insensitive) match in `aliases`.
    /// 4. Strip provider prefix and retry step 3.
    /// 5. Reject an unknown model.
    pub fn canonicalize(&self, model: &str) -> Result<String> {
        let trimmed = model.trim();
        let stripped = trimmed.rsplit('/').next().unwrap_or(trimmed).trim();

        if let Some(advertised) = self.find_advertised(trimmed) {
            return Ok(advertised.to_owned());
        }
        if let Some(advertised) = self.find_advertised(stripped) {
            return Ok(advertised.to_owned());
        }
        if let Some(alias) = self.find_alias(trimmed) {
            return Ok(alias.to_owned());
        }
        if let Some(alias) = self.find_alias(stripped) {
            return Ok(alias.to_owned());
        }

        anyhow::bail!("model `{model}` is not configured for codex-proxy")
    }

    /// Returns `true` if `model` is in the `advertised` list (case-insensitive).
    ///
    /// Used by `GET /v1/models/{model_id}` to determine whether to return 200
    /// or 404.
    pub fn supports(&self, model: &str) -> bool {
        self.find_advertised(model).is_some()
    }

    fn find_advertised(&self, candidate: &str) -> Option<&str> {
        self.advertised
            .iter()
            .find(|value| value.eq_ignore_ascii_case(candidate))
            .map(String::as_str)
    }

    fn find_alias(&self, candidate: &str) -> Option<&str> {
        self.aliases
            .iter()
            .find(|(alias, _)| alias.eq_ignore_ascii_case(candidate))
            .map(|(_, target)| target.as_str())
    }
}

impl Default for ModelsConfig {
    fn default() -> Self {
        let mut aliases = BTreeMap::new();
        aliases.insert("openai/gpt-5.4".to_owned(), "gpt-5.4".to_owned());
        aliases.insert("openai/gpt-5.4-mini".to_owned(), "gpt-5.4-mini".to_owned());
        aliases.insert(
            "openai/gpt-5.3-codex".to_owned(),
            "gpt-5.3-codex".to_owned(),
        );
        aliases.insert("openai/gpt-5-codex".to_owned(), "gpt-5.3-codex".to_owned());
        aliases.insert("gpt-5-codex".to_owned(), "gpt-5.3-codex".to_owned());

        Self {
            advertised: vec![
                "gpt-5.4".to_owned(),
                "gpt-5.4-mini".to_owned(),
                "gpt-5.3-codex".to_owned(),
            ],
            aliases,
            fallback: "gpt-5.4".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::paths::AppPaths;

    #[test]
    fn load_or_create_writes_default_config_when_missing() {
        let temp = tempdir().expect("tempdir");
        let paths = AppPaths::new(temp.path().join("config.toml"), temp.path().join("data"));

        let config = Config::load_or_create(&paths).expect("config");

        assert!(paths.config_path.exists());
        assert_eq!(config.server.port, 8317);
        assert_eq!(config.auth.callback_bind_host, "127.0.0.1");
        assert_eq!(
            config.auth.redirect_uri(),
            "http://localhost:1455/auth/callback"
        );
        assert_eq!(
            config.request_defaults.include,
            vec!["reasoning.encrypted_content".to_owned()]
        );
        assert_eq!(
            config.models.advertised,
            vec![
                "gpt-5.4".to_owned(),
                "gpt-5.4-mini".to_owned(),
                "gpt-5.3-codex".to_owned()
            ]
        );
        assert_eq!(config.models.fallback, "gpt-5.4");
    }

    #[test]
    fn auth_config_deserialize_migrates_legacy_callback_host_to_localhost_redirect() {
        let config: Config = toml::from_str(
            r#"
[auth]
callback_host = "127.0.0.1"
callback_port = 1455
"#,
        )
        .expect("config");

        assert_eq!(config.auth.callback_bind_host, "127.0.0.1");
        assert_eq!(config.auth.redirect_host, "localhost");
        assert_eq!(
            config.auth.redirect_uri(),
            "http://localhost:1455/auth/callback"
        );
    }

    #[test]
    fn auth_config_deserialize_supports_explicit_bind_and_redirect_hosts() {
        let config: Config = toml::from_str(
            r#"
[auth]
callback_bind_host = "0.0.0.0"
redirect_host = "localhost"
callback_port = 1455
"#,
        )
        .expect("config");

        assert_eq!(config.auth.callback_bind_host, "0.0.0.0");
        assert_eq!(config.auth.redirect_host, "localhost");
    }

    #[test]
    fn models_config_canonicalize_uses_advertised_and_alias_entries() {
        let config = ModelsConfig::default();

        assert_eq!(config.canonicalize("gpt-5.4").unwrap(), "gpt-5.4");
        assert_eq!(
            config.canonicalize("openai/gpt-5.4-mini").unwrap(),
            "gpt-5.4-mini"
        );
        assert_eq!(config.canonicalize("gpt-5-codex").unwrap(), "gpt-5.3-codex");
        assert!(config.canonicalize("unknown-model").is_err());
    }
}
