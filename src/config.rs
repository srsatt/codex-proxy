use std::{collections::BTreeMap, fs, path::Path};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{constants::CALLBACK_PATH, paths::AppPaths, storage::write_string};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind: String,
    pub port: u16,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    pub callback_host: String,
    pub callback_port: u16,
    pub open_browser: bool,
    pub timeout_seconds: u64,
}

impl AuthConfig {
    pub fn redirect_uri(&self) -> String {
        format!(
            "http://{}:{}{}",
            self.callback_host, self.callback_port, CALLBACK_PATH
        )
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            callback_host: "127.0.0.1".to_owned(),
            callback_port: 1455,
            open_browser: true,
            timeout_seconds: 300,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UpstreamConfig {
    pub responses_base_url: String,
    pub codex_responses_path: String,
    pub authorize_url: String,
    pub token_url: String,
    pub client_id: String,
    pub scope: String,
    pub originator: String,
    pub beta_header: String,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            responses_base_url: "https://chatgpt.com/backend-api".to_owned(),
            codex_responses_path: "/codex/responses".to_owned(),
            authorize_url: "https://auth.openai.com/oauth/authorize".to_owned(),
            token_url: "https://auth.openai.com/oauth/token".to_owned(),
            client_id: "app_EMoamEEZ73f0CkXaXp7hrann".to_owned(),
            scope: "openid profile email offline_access".to_owned(),
            originator: "codex_cli_rs".to_owned(),
            beta_header: "responses=experimental".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RequestDefaultsConfig {
    pub include: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_verbosity: Option<String>,
    pub preserve_orphaned_function_call_outputs: bool,
}

impl Default for RequestDefaultsConfig {
    fn default() -> Self {
        Self {
            include: vec!["reasoning.encrypted_content".to_owned()],
            reasoning_effort: None,
            reasoning_summary: None,
            text_verbosity: None,
            preserve_orphaned_function_call_outputs: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelsConfig {
    pub advertised: Vec<String>,
    pub aliases: BTreeMap<String, String>,
    pub fallback: String,
}

impl ModelsConfig {
    pub fn canonicalize(&self, model: &str) -> String {
        let trimmed = model.trim();
        let stripped = trimmed.rsplit('/').next().unwrap_or(trimmed).trim();

        if let Some(advertised) = self.find_advertised(trimmed) {
            return advertised.to_owned();
        }
        if let Some(advertised) = self.find_advertised(stripped) {
            return advertised.to_owned();
        }
        if let Some(alias) = self.find_alias(trimmed) {
            return alias.to_owned();
        }
        if let Some(alias) = self.find_alias(stripped) {
            return alias.to_owned();
        }

        self.fallback.clone()
    }

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
        assert_eq!(
            config.auth.redirect_uri(),
            "http://127.0.0.1:1455/auth/callback"
        );
        assert_eq!(
            config.request_defaults.include,
            vec!["reasoning.encrypted_content".to_owned()]
        );
        assert!(config.request_defaults.preserve_orphaned_function_call_outputs);
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
    fn models_config_canonicalize_uses_advertised_and_alias_entries() {
        let config = ModelsConfig::default();

        assert_eq!(config.canonicalize("gpt-5.4"), "gpt-5.4");
        assert_eq!(config.canonicalize("openai/gpt-5.4-mini"), "gpt-5.4-mini");
        assert_eq!(config.canonicalize("gpt-5-codex"), "gpt-5.3-codex");
        assert_eq!(config.canonicalize("unknown-model"), "gpt-5.4");
    }
}
