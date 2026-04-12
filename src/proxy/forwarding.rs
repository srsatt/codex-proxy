//! Core request forwarding logic for `POST /v1/responses`.
//!
//! ## Request pipeline
//!
//! 1. **Parse** the raw request body as JSON.
//! 2. **Normalize** the body via [`prepare_request_body`]:
//!    - Canonicalize the model name via [`crate::config::ModelsConfig`].
//!    - Force `store: false` and `stream: true` for upstream compatibility.
//!    - Filter `item_reference` items from the `input` array.
//!    - Strip `id` fields from remaining `input` items.
//!    - Drop `function_call_output` items orphaned by reference filtering.
//!    - Apply configurable request defaults (`reasoning`, `text`, `include`).
//! 3. **Forward** the prepared body to the ChatGPT Codex backend via
//!    [`send_prepared_request`], attaching OAuth headers.
//! 4. **Handle** the upstream response:
//!    - Streaming: pipe bytes directly back to the client.
//!    - Non-streaming: parse the SSE stream, extract the final response
//!      object, and return it as JSON.

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
    config::{Config, ModelsConfig, RequestDefaultsConfig},
    paths::AppPaths,
    state::{StoredAuthStatus, TokenStore, save_tokens, stored_auth_status},
};

/// State shared between the proxy router and the forwarding layer.
///
/// Cloned cheaply from [`super::ProxyAppState`] on each request via
/// [`super::ProxyAppState::forwarding_state`].
#[derive(Clone)]
pub struct ForwardingState {
    pub config: Config,
    pub paths: AppPaths,
    pub client: Client,
    /// Shared, mutable OAuth token store. Updated in place when a refresh
    /// succeeds during a live request.
    pub tokens: Arc<RwLock<Option<TokenStore>>>,
}

/// A request body that has been parsed, normalized, and is ready to send
/// upstream.
#[derive(Debug, Clone)]
pub struct PreparedRequest {
    /// Normalized JSON body to be serialized and sent to the upstream API.
    pub body: Value,
    /// Whether the *original* client request set `stream: true`.
    /// Used to decide whether to pipe bytes or parse SSE in the response.
    pub original_stream: bool,
    /// Audit trail of every transformation applied during normalization.
    pub transformation: TransformationSummary,
}

/// Records every transformation applied during [`prepare_request_body`].
///
/// Logged with each upstream request so operators can trace model normalization
/// and input filtering without re-parsing the request body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformationSummary {
    /// Model name as supplied by the client (before normalization).
    pub original_model: Option<String>,
    /// Model name after [`crate::config::ModelsConfig::canonicalize`].
    pub normalized_model: Option<String>,
    pub input_items_before: usize,
    pub input_items_after: usize,
    /// Number of `item_reference` items removed from the `input` array.
    pub filtered_item_references: usize,
    /// Number of `id` fields stripped from surviving input items.
    pub stripped_item_ids: usize,
    /// Number of `function_call_output` items dropped because their paired
    /// `function_call` was removed by reference filtering.
    pub dropped_orphaned_tool_outputs: usize,
    /// Number of entries appended to `include` from `request_defaults.include`.
    pub include_defaults_added: usize,
}

/// Per-request statistics produced by [`normalize_input`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct InputNormalizationStats {
    pub input_items_before: usize,
    pub input_items_after: usize,
    pub filtered_item_references: usize,
    pub stripped_item_ids: usize,
    pub dropped_orphaned_tool_outputs: usize,
}

/// Handler for `POST /v1/responses`.
///
/// Normalizes the request body, forwards it to the upstream Codex backend,
/// and streams or collapses the SSE response according to the original
/// client's `stream` flag.
pub async fn forward_responses(
    state: &ForwardingState,
    request_headers: &HeaderMap,
    request_body: Bytes,
) -> Response {
    let prepared_request = match prepare_request_body(
        &request_body,
        &state.config.request_defaults,
        &state.config.models,
    ) {
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

/// Send a [`PreparedRequest`] to the upstream ChatGPT Codex backend.
///
/// - Resolves/refreshes the current OAuth tokens.
/// - Constructs upstream headers (Authorization, chatgpt-account-id, etc.).
/// - Logs detailed request metadata including transformation stats.
/// - Returns the raw upstream [`reqwest::Response`] for the caller to handle.
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
        dropped_orphaned_tool_outputs = transformation.dropped_orphaned_tool_outputs,
        include_defaults_added = transformation.include_defaults_added,
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

/// Parse and normalize a raw request body into a [`PreparedRequest`].
///
/// All stateless transformations are applied here so `send_prepared_request`
/// only has to deal with network I/O. The transformations are:
/// - Model canonicalization via [`ModelsConfig::canonicalize`].
/// - `store: false` — prevents the backend from caching the conversation.
/// - `stream: true` — the upstream always streams; the proxy un-streams when
///   the client did not request streaming.
/// - [`normalize_input`] — filters references and strips item IDs.
/// - [`apply_request_defaults`] — applies configurable `reasoning`, `text`,
///   and `include` defaults without overwriting explicit client values.
pub fn prepare_request_body(
    body: &[u8],
    request_defaults: &RequestDefaultsConfig,
    models: &ModelsConfig,
) -> Result<PreparedRequest> {
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

    let include_defaults_added = apply_request_defaults(object, request_defaults);
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
            dropped_orphaned_tool_outputs: input_stats.dropped_orphaned_tool_outputs,
            include_defaults_added,
        },
    })
}

/// Normalize the `input` array of a responses request in place.
///
/// Transformations applied (in order):
/// 1. Remove items where `type == "item_reference"`.
/// 2. Strip the `id` field from every surviving item.
/// 3. Drop `function_call_output` items whose `call_id` no longer refers to
///    a live `function_call` item (i.e. its pair was removed in step 1).
///
/// Returns statistics used for logging and test assertions.
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

    // Build the set of call_ids for function_call items that survived
    // filtering. Used below to detect orphaned function_call_output items.
    let live_call_ids: std::collections::HashSet<String> = normalized_items
        .iter()
        .filter_map(|item| {
            let obj = item.as_object()?;
            if obj.get("type").and_then(Value::as_str)? == "function_call" {
                obj.get("call_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            } else {
                None
            }
        })
        .collect();

    // Drop function_call_output items whose matching function_call was removed
    // during item_reference filtering. Leaving them in causes an upstream API
    // error because the backend cannot match them to a call.
    let mut dropped_orphaned_tool_outputs = 0;
    normalized_items.retain(|item| {
        let Some(obj) = item.as_object() else {
            return true;
        };
        if obj.get("type").and_then(Value::as_str) != Some("function_call_output") {
            return true;
        }
        let call_id = obj.get("call_id").and_then(Value::as_str).unwrap_or("");
        if live_call_ids.contains(call_id as &str) {
            return true;
        }
        dropped_orphaned_tool_outputs += 1;
        false
    });

    *items = normalized_items;

    InputNormalizationStats {
        input_items_before,
        input_items_after: items.len(),
        filtered_item_references,
        stripped_item_ids,
        dropped_orphaned_tool_outputs,
    }
}

fn apply_request_defaults(
    body: &mut serde_json::Map<String, Value>,
    request_defaults: &RequestDefaultsConfig,
) -> usize {
    let has_reasoning_defaults = request_defaults
        .reasoning_effort
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        || request_defaults
            .reasoning_summary
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());

    if has_reasoning_defaults {
        if let Some(reasoning) = ensure_object_field(body, "reasoning") {
            insert_default_string_field(
                reasoning,
                "effort",
                request_defaults.reasoning_effort.as_deref(),
            );
            insert_default_string_field(
                reasoning,
                "summary",
                request_defaults.reasoning_summary.as_deref(),
            );
        }
    }

    let has_text_defaults = request_defaults
        .text_verbosity
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());

    if has_text_defaults {
        if let Some(text) = ensure_object_field(body, "text") {
            insert_default_string_field(
                text,
                "verbosity",
                request_defaults.text_verbosity.as_deref(),
            );
        }
    }

    normalize_include(body, &request_defaults.include)
}

fn ensure_object_field<'a>(
    body: &'a mut serde_json::Map<String, Value>,
    key: &str,
) -> Option<&'a mut serde_json::Map<String, Value>> {
    if !body.contains_key(key) {
        body.insert(key.to_owned(), Value::Object(serde_json::Map::new()));
    }

    body.get_mut(key)?.as_object_mut()
}

fn insert_default_string_field(
    object: &mut serde_json::Map<String, Value>,
    key: &str,
    default: Option<&str>,
) {
    let Some(default) = default.filter(|value| !value.trim().is_empty()) else {
        return;
    };

    if !object.contains_key(key) {
        object.insert(key.to_owned(), Value::String(default.to_owned()));
    }
}

/// Ensure every configured default include entry is present in the `include`
/// array.
///
/// Returns the number of entries that had to be appended. The return value is
/// recorded in [`TransformationSummary`] for logging.
fn normalize_include(
    body: &mut serde_json::Map<String, Value>,
    default_include: &[String],
) -> usize {
    let mut include = body
        .get("include")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut added = 0;

    for value in default_include
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        let present = include.iter().any(|entry| entry.as_str() == Some(value));
        if !present {
            include.push(Value::String(value.to_owned()));
            added += 1;
        }
    }

    if !include.is_empty() {
        body.insert("include".to_owned(), Value::Array(include));
    }

    added
}

/// Build the header map for the upstream ChatGPT Codex request.
///
/// Required headers:
/// - `Authorization: Bearer <access_token>`
/// - `chatgpt-account-id: <account_id>` (decoded from the JWT)
/// - `OpenAI-Beta: <config.upstream.beta_header>`
/// - `originator: <config.upstream.originator>`
/// - `Accept: text/event-stream` (upstream always streams)
/// - `Content-Type` (copied from the incoming client request)
///
/// Optional headers (added when `prompt_cache_key` is present in the body):
/// - `conversation_id`
/// - `session_id`
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

/// Return valid OAuth tokens, attempting a refresh if they are expiring or
/// expired.
///
/// On a successful refresh the new tokens are persisted to `tokens.json` and
/// the in-memory `Arc<RwLock<...>>` is updated so subsequent requests in the
/// same daemon session do not trigger unnecessary refreshes.
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

/// Construct the full upstream URL by joining `responses_base_url` and
/// `codex_responses_path` from config.
fn upstream_responses_url(config: &Config) -> Result<Url> {
    let mut base_url = config.upstream.responses_base_url.clone();
    if !base_url.ends_with('/') {
        base_url.push('/');
    }

    let base = Url::parse(&base_url).context("invalid ChatGPT backend base URL")?;
    base.join(config.upstream.codex_responses_path.trim_start_matches('/'))
        .context("invalid Codex responses path")
}

/// Convert a successful upstream response into an Axum [`Response`].
///
/// - **Streaming** (`original_stream = true`): byte-streams the upstream
///   SSE body directly to the client.
/// - **Non-streaming**: reads the full SSE body, finds the `response.done`
///   or `response.completed` event, and returns its `response` field as JSON.
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
    let json_response = parse_success_text_to_json(&text).unwrap_or(Value::String(text));

    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
        .body(Body::from(serde_json::to_vec(&json_response)?))
        .context("failed to build JSON proxy response")
}

/// Pass an upstream error response through to the client.
///
/// Special case: a `404` whose body contains usage-limit language is remapped
/// to `429 Too Many Requests` so clients that understand rate-limit errors
/// can back off automatically.
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

/// Copy headers from a `reqwest` response into an Axum `HeaderMap`.
///
/// Inserts a default `Content-Type: text/event-stream` when the upstream
/// response did not include one.
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

/// Scan an SSE text body for the final response object.
///
/// Reads each `data: <json>` line and returns the `response` field of the
/// first event whose `type` is `"response.done"` or `"response.completed"`.
/// Returns `None` when no such event is found (e.g. if the stream was cut
/// short).
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

fn parse_success_text_to_json(body: &str) -> Option<Value> {
    parse_sse_final_response(body).or_else(|| serde_json::from_str(body).ok())
}

/// Read a successful upstream response body and extract the final JSON payload.
///
/// The ChatGPT Codex backend typically responds as SSE even when the original
/// client requested non-streaming output. This helper accepts either a plain
/// JSON body or an SSE stream containing a final `response.done` /
/// `response.completed` event.
pub async fn extract_success_response_json(
    response: reqwest::Response,
) -> Result<(StatusCode, Value)> {
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();

    if content_type.contains("application/json") {
        let json = response
            .json()
            .await
            .context("failed to parse upstream JSON response")?;
        return Ok((status, json));
    }

    let body = response
        .text()
        .await
        .context("failed to read upstream success response body")?;
    let json = parse_success_text_to_json(&body)
        .ok_or_else(|| anyhow!("upstream success response did not contain a final JSON payload"))?;

    Ok((status, json))
}

/// Build a JSON error response with a `proxy_error` type envelope.
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
            prepare_request_body(input.as_bytes(), &config.request_defaults, &config.models)
                .expect("prepared request");
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
                dropped_orphaned_tool_outputs: 0,
                include_defaults_added: 1,
            }
        );
    }

    #[test]
    fn prepare_request_body_applies_request_defaults_without_overwriting_explicit_fields() {
        let mut config = Config::default();
        config.request_defaults.reasoning_effort = Some("high".to_owned());
        config.request_defaults.reasoning_summary = Some("auto".to_owned());
        config.request_defaults.text_verbosity = Some("low".to_owned());
        config
            .request_defaults
            .include
            .push("message.output_text.logprobs".to_owned());

        let request = json!({
            "model": "gpt-5.4",
            "reasoning": {
                "effort": "medium"
            },
            "text": {
                "verbosity": "high"
            },
            "include": ["reasoning.encrypted_content"],
            "input": [
                { "type": "message", "role": "user", "content": "hello" }
            ]
        });

        let prepared = prepare_request_body(
            serde_json::to_vec(&request).expect("request").as_slice(),
            &config.request_defaults,
            &config.models,
        )
        .expect("prepared request");

        assert_eq!(prepared.body["reasoning"]["effort"], "medium");
        assert_eq!(prepared.body["reasoning"]["summary"], "auto");
        assert_eq!(prepared.body["text"]["verbosity"], "high");
        assert_eq!(
            prepared.body["include"],
            json!([
                "reasoning.encrypted_content",
                "message.output_text.logprobs"
            ])
        );
        assert_eq!(prepared.transformation.include_defaults_added, 1);
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

    #[test]
    fn parse_sse_final_response_extracts_response_completed_payload() {
        let sse = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_456\"}}\n";

        let parsed = parse_sse_final_response(sse).expect("parsed response");

        assert_eq!(parsed["id"], "resp_456");
    }

    #[test]
    fn normalize_input_drops_orphaned_function_call_outputs() {
        // Simulate what happens after item_reference filtering removes the
        // function_call that a function_call_output was paired with: the
        // output has no live sibling and must be dropped to avoid an upstream
        // API error.
        let mut input = json!([
            {
                "type": "function_call",
                "call_id": "call_alive",
                "name": "read_file",
                "arguments": "{}"
            },
            {
                "type": "function_call_output",
                "call_id": "call_alive",
                "output": "file contents"
            },
            {
                "type": "function_call_output",
                "call_id": "call_orphan",
                "output": "stale output"
            }
        ]);

        let stats = normalize_input(&mut input);

        let items = input.as_array().expect("array");
        assert_eq!(items.len(), 2);
        assert!(items.iter().all(|i| i["call_id"] != "call_orphan"));
        assert_eq!(stats.dropped_orphaned_tool_outputs, 1);
        assert_eq!(stats.input_items_before, 3);
        assert_eq!(stats.input_items_after, 2);
    }
}
