use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::sync::RwLock;
use url::Url;

use crate::{
    auth::refresh_access_token,
    config::{Config, ModelsConfig},
    paths::AppPaths,
    state::{StoredAuthStatus, TokenStore, save_tokens, stored_auth_status},
};

#[derive(Clone)]
pub struct ForwardingState {
    pub config: Config,
    pub paths: AppPaths,
    pub client: Client,
    pub tokens: Arc<RwLock<Option<TokenStore>>>,
}

#[derive(Debug, Clone)]
pub struct PreparedRequest {
    pub body: Value,
    pub original_stream: bool,
    pub transformation: TransformationSummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformationSummary {
    pub original_model: Option<String>,
    pub normalized_model: Option<String>,
    pub input_items_before: usize,
    pub input_items_after: usize,
    pub filtered_item_references: usize,
    pub stripped_item_ids: usize,
    pub include_reasoning_added: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct InputNormalizationStats {
    pub input_items_before: usize,
    pub input_items_after: usize,
    pub filtered_item_references: usize,
    pub stripped_item_ids: usize,
}

pub async fn forward_responses(
    state: &ForwardingState,
    request_headers: &HeaderMap,
    request_body: Bytes,
) -> Response {
    let prepared_request = match prepare_request_body(&request_body, &state.config.models) {
        Ok(prepared_request) => prepared_request,
        Err(error) => return error_response(StatusCode::BAD_REQUEST, error.to_string()),
    };

    let upstream_response =
        match send_prepared_request(state, request_headers, &prepared_request, "responses").await {
            Ok(response) => response,
            Err(error) => return error_response(StatusCode::BAD_GATEWAY, error.to_string()),
        };

    if !upstream_response.status().is_success() {
        return map_upstream_error(upstream_response).await;
    }

    match handle_success_response(upstream_response, prepared_request.original_stream).await {
        Ok(response) => response,
        Err(error) => error_response(StatusCode::BAD_GATEWAY, error.to_string()),
    }
}

pub async fn send_prepared_request(
    state: &ForwardingState,
    request_headers: &HeaderMap,
    prepared_request: &PreparedRequest,
    route_kind: &'static str,
) -> Result<reqwest::Response> {
    let request_id = uuid::Uuid::new_v4();
    let tokens = match get_or_refresh_tokens(state).await {
        Ok(tokens) => tokens,
        Err(error) => {
            tracing::warn!(%request_id, route_kind, error = %error, "proxy auth state prevents forwarding");
            bail!(error);
        }
    };
    let token_status = tokens.auth_status().as_str();

    let upstream_url = upstream_responses_url(&state.config)?;
    let upstream_headers = build_upstream_headers(
        request_headers,
        &tokens,
        &state.config,
        &prepared_request.body,
    )?;
    let transformation = &prepared_request.transformation;
    let model = transformation
        .normalized_model
        .as_deref()
        .unwrap_or("unknown");
    let original_model = transformation
        .original_model
        .as_deref()
        .unwrap_or("unknown");

    tracing::info!(
        %request_id,
        route_kind,
        token_status,
        model,
        original_model,
        model_changed = transformation.original_model != transformation.normalized_model,
        original_stream = prepared_request.original_stream,
        input_items_before = transformation.input_items_before,
        input_items_after = transformation.input_items_after,
        filtered_item_references = transformation.filtered_item_references,
        stripped_item_ids = transformation.stripped_item_ids,
        include_reasoning_added = transformation.include_reasoning_added,
        "forwarding request to upstream"
    );

    let response = state
        .client
        .post(upstream_url)
        .headers(upstream_headers)
        .body(serde_json::to_vec(&prepared_request.body)?)
        .send()
        .await
        .context("failed to reach the ChatGPT Codex backend")?;
    tracing::info!(
        %request_id,
        route_kind,
        status = %response.status(),
        "upstream request completed"
    );

    Ok(response)
}

pub fn prepare_request_body(body: &[u8], models: &ModelsConfig) -> Result<PreparedRequest> {
    let mut body: Value =
        serde_json::from_slice(body).context("request body must be valid JSON")?;
    let original_stream = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
    let original_model = body
        .get("model")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    let object = body
        .as_object_mut()
        .ok_or_else(|| anyhow!("request body must be a JSON object"))?;

    if let Some(normalized_model) = object
        .get("model")
        .and_then(Value::as_str)
        .map(|model| models.canonicalize(model))
    {
        object.insert("model".to_owned(), Value::String(normalized_model));
    }

    object.insert("store".to_owned(), Value::Bool(false));
    object.insert("stream".to_owned(), Value::Bool(true));

    let input_stats = object
        .get_mut("input")
        .map(normalize_input)
        .unwrap_or_default();

    let include_reasoning_added = normalize_include(object);
    let normalized_model = object
        .get("model")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    Ok(PreparedRequest {
        body: Value::Object(object.clone()),
        original_stream,
        transformation: TransformationSummary {
            original_model,
            normalized_model,
            input_items_before: input_stats.input_items_before,
            input_items_after: input_stats.input_items_after,
            filtered_item_references: input_stats.filtered_item_references,
            stripped_item_ids: input_stats.stripped_item_ids,
            include_reasoning_added,
        },
    })
}

pub fn normalize_input(input: &mut Value) -> InputNormalizationStats {
    let Some(items) = input.as_array_mut() else {
        return InputNormalizationStats::default();
    };

    let input_items_before = items.len();
    let mut normalized_items = Vec::with_capacity(items.len());
    let mut filtered_item_references = 0;
    let mut stripped_item_ids = 0;

    for item in items.iter() {
        let Some(mut object) = item.as_object().cloned() else {
            normalized_items.push(item.clone());
            continue;
        };

        if object
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|item_type| item_type == "item_reference")
        {
            filtered_item_references += 1;
            continue;
        }

        if object.remove("id").is_some() {
            stripped_item_ids += 1;
        }
        normalized_items.push(Value::Object(object));
    }

    *items = normalized_items;

    InputNormalizationStats {
        input_items_before,
        input_items_after: items.len(),
        filtered_item_references,
        stripped_item_ids,
    }
}

fn normalize_include(body: &mut serde_json::Map<String, Value>) -> bool {
    let mut include = body
        .get("include")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let had_reasoning_encrypted = include
        .iter()
        .any(|value| value.as_str() == Some("reasoning.encrypted_content"));

    if !had_reasoning_encrypted {
        include.push(Value::String("reasoning.encrypted_content".to_owned()));
    }

    if !include.is_empty() {
        body.insert("include".to_owned(), Value::Array(include));
    }

    !had_reasoning_encrypted
}

pub fn build_upstream_headers(
    request_headers: &HeaderMap,
    tokens: &TokenStore,
    config: &Config,
    request_body: &Value,
) -> Result<reqwest::header::HeaderMap> {
    let account_id = tokens.account_id.as_deref().ok_or_else(|| {
        anyhow!(
            "stored OAuth tokens are missing chatgpt_account_id; rerun `codex-proxy auth --force`"
        )
    })?;

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", tokens.access_token))
            .context("invalid OAuth access token for Authorization header")?,
    );
    headers.insert(
        "chatgpt-account-id",
        HeaderValue::from_str(account_id).context("invalid ChatGPT account id header")?,
    );
    headers.insert(
        "OpenAI-Beta",
        HeaderValue::from_str(&config.upstream.beta_header)
            .context("invalid OpenAI-Beta header")?,
    );
    headers.insert(
        "originator",
        HeaderValue::from_str(&config.upstream.originator).context("invalid originator header")?,
    );
    headers.insert(
        header::ACCEPT,
        HeaderValue::from_static("text/event-stream"),
    );

    let content_type = request_headers
        .get(header::CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_static("application/json"));
    headers.insert(header::CONTENT_TYPE, content_type);

    if let Some(prompt_cache_key) = request_body
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        let value =
            HeaderValue::from_str(prompt_cache_key).context("invalid prompt cache key header")?;
        headers.insert("conversation_id", value.clone());
        headers.insert("session_id", value);
    }

    Ok(headers)
}

async fn get_or_refresh_tokens(state: &ForwardingState) -> Result<TokenStore> {
    let current = state.tokens.read().await.clone();
    let status = stored_auth_status(current.as_ref());

    match status {
        StoredAuthStatus::Missing => {
            bail!("no stored OAuth tokens are available; run `codex-proxy auth`");
        }
        StoredAuthStatus::Valid => {
            return current.ok_or_else(|| anyhow!("stored OAuth tokens disappeared"));
        }
        StoredAuthStatus::Expiring | StoredAuthStatus::Expired => {}
    }

    let current = current.ok_or_else(|| anyhow!("stored OAuth tokens disappeared"))?;
    let refreshed_tokens = refresh_access_token(
        &state.client,
        &state.config.upstream,
        &current.refresh_token,
    )
    .await
    .context("failed to refresh stored OAuth tokens")?;

    save_tokens(&state.paths, &refreshed_tokens).context("failed to persist refreshed tokens")?;

    let mut guard = state.tokens.write().await;
    *guard = Some(refreshed_tokens.clone());
    Ok(refreshed_tokens)
}

fn upstream_responses_url(config: &Config) -> Result<Url> {
    let mut base_url = config.upstream.responses_base_url.clone();
    if !base_url.ends_with('/') {
        base_url.push('/');
    }

    let base = Url::parse(&base_url).context("invalid ChatGPT backend base URL")?;
    base.join(config.upstream.codex_responses_path.trim_start_matches('/'))
        .context("invalid Codex responses path")
}

async fn handle_success_response(
    response: reqwest::Response,
    original_stream: bool,
) -> Result<Response> {
    if original_stream {
        let status = response.status();
        let headers = response_headers(response.headers());
        let stream = response.bytes_stream();

        return Response::builder()
            .status(status)
            .body(Body::from_stream(stream))
            .map(|mut response| {
                *response.headers_mut() = headers;
                response
            })
            .context("failed to build streaming proxy response");
    }

    let status = response.status();
    let text = response
        .text()
        .await
        .context("failed to read upstream SSE response")?;
    let json_response = parse_sse_final_response(&text).unwrap_or(Value::String(text));

    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
        .body(Body::from(serde_json::to_vec(&json_response)?))
        .context("failed to build JSON proxy response")
}

pub async fn map_upstream_error(response: reqwest::Response) -> Response {
    let mut status = response.status();
    let headers = response_headers(response.headers());
    let body = response.bytes().await.unwrap_or_default();

    let usage_limit_response = if status == StatusCode::NOT_FOUND {
        std::str::from_utf8(&body)
            .ok()
            .map(|text| {
                let lower = text.to_lowercase();
                lower.contains("usage_limit_reached")
                    || lower.contains("usage_not_included")
                    || lower.contains("rate_limit_exceeded")
                    || lower.contains("usage limit")
            })
            .unwrap_or(false)
    } else {
        false
    };

    if usage_limit_response {
        status = StatusCode::TOO_MANY_REQUESTS;
    }

    Response::builder()
        .status(status)
        .body(Body::from(body))
        .map(|mut response| {
            *response.headers_mut() = headers;
            response
        })
        .unwrap_or_else(|_| {
            error_response(
                StatusCode::BAD_GATEWAY,
                "failed to build upstream error response",
            )
        })
}

fn response_headers(source: &reqwest::header::HeaderMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in source {
        headers.append(name, value.clone());
    }

    if !headers.contains_key(header::CONTENT_TYPE) {
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream; charset=utf-8"),
        );
    }

    headers
}

pub fn parse_sse_final_response(sse_text: &str) -> Option<Value> {
    for line in sse_text.lines() {
        let Some(data) = line.strip_prefix("data: ") else {
            continue;
        };
        let Ok(event) = serde_json::from_str::<Value>(data) else {
            continue;
        };
        let Some(event_type) = event.get("type").and_then(Value::as_str) else {
            continue;
        };
        if matches!(event_type, "response.done" | "response.completed") {
            return event.get("response").cloned();
        }
    }

    None
}

pub fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    let payload = json!({
        "error": {
            "message": message.into(),
            "type": "proxy_error",
        }
    });

    (status, axum::Json(payload)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_request_body_normalizes_stateless_responses_request() {
        let input = include_str!("../../tests/fixtures/responses_request_input.json");
        let expected = include_str!("../../tests/fixtures/responses_request_expected.json");
        let config = Config::default();

        let prepared =
            prepare_request_body(input.as_bytes(), &config.models).expect("prepared request");
        let expected_json: Value = serde_json::from_str(expected).expect("expected json");

        assert!(!prepared.original_stream);
        assert_eq!(prepared.body, expected_json);
        assert_eq!(
            prepared.transformation,
            TransformationSummary {
                original_model: Some("openai/gpt-5-codex".to_owned()),
                normalized_model: Some("gpt-5.3-codex".to_owned()),
                input_items_before: 3,
                input_items_after: 2,
                filtered_item_references: 1,
                stripped_item_ids: 2,
                include_reasoning_added: true,
            }
        );
    }

    #[test]
    fn build_upstream_headers_sets_required_oauth_headers() {
        let config = Config::default();
        let tokens = TokenStore {
            access_token: "access-token".to_owned(),
            refresh_token: "refresh-token".to_owned(),
            expires_at: 999_999,
            token_type: "Bearer".to_owned(),
            scope: "openid".to_owned(),
            account_id: Some("account-123".to_owned()),
            obtained_at: 111_111,
        };
        let request_headers = HeaderMap::new();
        let request_body = json!({
            "prompt_cache_key": "session-123"
        });

        let headers = build_upstream_headers(&request_headers, &tokens, &config, &request_body)
            .expect("headers");

        assert_eq!(
            headers
                .get(reqwest::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok()),
            Some("Bearer access-token")
        );
        assert_eq!(
            headers
                .get("chatgpt-account-id")
                .and_then(|v| v.to_str().ok()),
            Some("account-123")
        );
        assert_eq!(
            headers.get("OpenAI-Beta").and_then(|v| v.to_str().ok()),
            Some("responses=experimental")
        );
        assert_eq!(
            headers.get("conversation_id").and_then(|v| v.to_str().ok()),
            Some("session-123")
        );
        assert_eq!(
            headers.get("session_id").and_then(|v| v.to_str().ok()),
            Some("session-123")
        );
    }

    #[test]
    fn parse_sse_final_response_extracts_response_done_payload() {
        let sse = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\
data: {\"type\":\"response.done\",\"response\":{\"id\":\"resp_123\",\"object\":\"response\"}}\n";

        let parsed = parse_sse_final_response(sse).expect("parsed response");

        assert_eq!(parsed["id"], "resp_123");
        assert_eq!(parsed["object"], "response");
    }
}
