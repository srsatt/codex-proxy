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

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use async_stream::stream;
use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
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

const UPSTREAM_CLIENT_REQUEST_ID_HEADER: &str = "x-client-request-id";
const PROXY_REQUEST_ID_HEADER: &str = "x-codex-proxy-request-id";
const PROXY_UPSTREAM_REQUEST_ID_HEADER: &str = "x-codex-proxy-upstream-request-id";
// TODO: update once the real header is confirmed from debug logs — the ChatGPT backend
// may not return `x-request-id` at all, in which case "missing" in logs is expected.
const UPSTREAM_REQUEST_ID_HEADER: &str = "x-request-id";

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

/// Correlated upstream exchange for a single proxied request.
pub struct UpstreamExchange {
    pub request_id: String,
    pub upstream_request_id: Option<String>,
    pub status: StatusCode,
    pub upstream_headers: reqwest::header::HeaderMap,
    pub body: UpstreamExchangeBody,
}

/// The upstream response body — either a live stream or already-buffered bytes.
pub enum UpstreamExchangeBody {
    /// Body has not been read yet.
    Live(reqwest::Response),
    /// Body was consumed during unsupported-parameter retry detection.
    Buffered(Bytes),
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
    let request_id = uuid::Uuid::new_v4().to_string();
    let prepared_request = match prepare_request_body(
        &request_body,
        &state.config.request_defaults,
        &state.config.models,
    ) {
        Ok(prepared_request) => prepared_request,
        Err(error) => {
            tracing::warn!(%request_id, route_kind = "responses", error = %error, "failed to normalize client request");
            return error_response_with_request_id(
                StatusCode::BAD_REQUEST,
                error.to_string(),
                &request_id,
            );
        }
    };

    let upstream_url = match upstream_responses_url(&state.config) {
        Ok(url) => url,
        Err(error) => {
            return error_response_with_request_id(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.to_string(),
                &request_id,
            );
        }
    };

    let upstream = match send_prepared_request(
        state,
        request_headers,
        &prepared_request,
        upstream_url,
        "responses",
        &request_id,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => {
            return error_response_with_request_id(
                StatusCode::BAD_GATEWAY,
                error.to_string(),
                &request_id,
            );
        }
    };

    if !upstream.status.is_success() {
        return map_upstream_error(upstream).await;
    }

    let keep_alive_secs = state.config.upstream.keep_alive_interval_secs.unwrap_or(15);
    match handle_success_response(upstream, prepared_request.original_stream, keep_alive_secs).await
    {
        Ok(response) => response,
        Err(error) => {
            error_response_with_request_id(StatusCode::BAD_GATEWAY, error.to_string(), &request_id)
        }
    }
}

/// Handler for `POST /v1/responses/compact`.
///
/// Forwards a compaction request to the upstream `/codex/responses/compact`
/// endpoint. Does not inject `store` or `stream` since the compact endpoint
/// does not accept those parameters. Streaming is not supported — requests
/// with `stream: true` are rejected with 400.
pub async fn forward_responses_compact(
    state: &ForwardingState,
    request_headers: &HeaderMap,
    request_body: Bytes,
) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();

    // Compact does not support streaming — reject early before parsing.
    if let Ok(body_json) = serde_json::from_slice::<Value>(&request_body) {
        if body_json
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            tracing::warn!(%request_id, route_kind = "compact", "client sent stream: true to compact endpoint");
            return error_response_with_request_id(
                StatusCode::BAD_REQUEST,
                "streaming is not supported for compact responses",
                &request_id,
            );
        }
    }

    let prepared_request = match prepare_compact_request_body(&request_body, &state.config.models) {
        Ok(prepared_request) => prepared_request,
        Err(error) => {
            tracing::warn!(%request_id, route_kind = "compact", error = %error, "failed to normalize compact request");
            return error_response_with_request_id(
                StatusCode::BAD_REQUEST,
                error.to_string(),
                &request_id,
            );
        }
    };

    let upstream_url = match upstream_compact_url(&state.config) {
        Ok(url) => url,
        Err(error) => {
            return error_response_with_request_id(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.to_string(),
                &request_id,
            );
        }
    };

    let upstream = match send_prepared_request(
        state,
        request_headers,
        &prepared_request,
        upstream_url,
        "compact",
        &request_id,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => {
            return error_response_with_request_id(
                StatusCode::BAD_GATEWAY,
                error.to_string(),
                &request_id,
            );
        }
    };

    if !upstream.status.is_success() {
        return map_upstream_error(upstream).await;
    }

    let keep_alive_secs = state.config.upstream.keep_alive_interval_secs.unwrap_or(15);
    match handle_success_response(upstream, false, keep_alive_secs).await {
        Ok(response) => response,
        Err(error) => {
            error_response_with_request_id(StatusCode::BAD_GATEWAY, error.to_string(), &request_id)
        }
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
    upstream_url: Url,
    route_kind: &'static str,
    request_id: &str,
) -> Result<UpstreamExchange> {
    let tokens = match get_or_refresh_tokens(state).await {
        Ok(tokens) => tokens,
        Err(error) => {
            tracing::warn!(%request_id, route_kind, error = %error, "proxy auth state prevents forwarding");
            bail!(error);
        }
    };
    let token_status = tokens.auth_status().as_str();
    let transformation = &prepared_request.transformation;
    let model = transformation
        .normalized_model
        .as_deref()
        .unwrap_or("unknown");
    let model_changed = transformation.original_model != transformation.normalized_model;

    if model_changed {
        tracing::info!(
            %request_id,
            route_kind,
            token_status,
            model,
            original_model = transformation.original_model.as_deref().unwrap_or("unknown"),
            model_changed = true,
            original_stream = prepared_request.original_stream,
            "forwarding request to upstream"
        );
    } else {
        tracing::info!(
            %request_id,
            route_kind,
            token_status,
            model,
            original_stream = prepared_request.original_stream,
            "forwarding request to upstream"
        );
    }

    let has_transformation = transformation.filtered_item_references > 0
        || transformation.stripped_item_ids > 0
        || transformation.dropped_orphaned_tool_outputs > 0
        || transformation.include_defaults_added > 0;

    if has_transformation {
        tracing::info!(
            %request_id,
            route_kind,
            input_items_before = transformation.input_items_before,
            input_items_after = transformation.input_items_after,
            filtered_item_references = transformation.filtered_item_references,
            stripped_item_ids = transformation.stripped_item_ids,
            dropped_orphaned_tool_outputs = transformation.dropped_orphaned_tool_outputs,
            include_defaults_added = transformation.include_defaults_added,
            "request transformation stats"
        );
    } else {
        tracing::debug!(
            %request_id,
            route_kind,
            input_items_before = transformation.input_items_before,
            input_items_after = transformation.input_items_after,
            filtered_item_references = transformation.filtered_item_references,
            stripped_item_ids = transformation.stripped_item_ids,
            dropped_orphaned_tool_outputs = transformation.dropped_orphaned_tool_outputs,
            include_defaults_added = transformation.include_defaults_added,
            "request transformation stats"
        );
    }

    let request_start = std::time::Instant::now();

    // Mutable body so we can strip unsupported parameters and retry.
    let mut body = prepared_request.body.clone();
    const MAX_STRIP_RETRIES: usize = 10;

    for attempt in 0..=MAX_STRIP_RETRIES {
        let upstream_headers =
            build_upstream_headers(request_headers, &tokens, &state.config, &body, request_id)?;

        let response = state
            .client
            .post(upstream_url.clone())
            .headers(upstream_headers)
            .body(serde_json::to_vec(&body)?)
            .send()
            .await;

        let response = match response {
            Ok(response) => response,
            Err(error) => {
                tracing::error!(%request_id, route_kind, error = %error, "failed to reach the ChatGPT Codex backend");
                return Err(error).context("failed to reach the ChatGPT Codex backend");
            }
        };

        let status = response.status();
        let upstream_request_id = upstream_request_id_from_headers(response.headers());

        let upstream_headers = response.headers().clone();
        let latency_ms = request_start.elapsed().as_millis();

        // On 400, check whether the upstream is rejecting a specific field.
        // If so, strip it and retry rather than surfacing the error to the client.
        if status == StatusCode::BAD_REQUEST && attempt < MAX_STRIP_RETRIES {
            let body_bytes = response.bytes().await.unwrap_or_default();
            if let Some(param) = extract_unsupported_param(&body_bytes) {
                tracing::warn!(
                    %request_id,
                    route_kind,
                    param,
                    attempt,
                    "upstream rejected unsupported parameter — stripping and retrying"
                );
                if let Some(obj) = body.as_object_mut() {
                    obj.remove(&param);
                }
                continue;
            }
            // 400 but not an unsupported-parameter error — body already consumed.
            tracing::warn!(
                %request_id,
                route_kind,
                status = %status,
                model,
                latency_ms,
                upstream_request_id = upstream_request_id.as_deref().unwrap_or("missing"),
                "upstream request completed with non-success status"
            );
            return Ok(UpstreamExchange {
                request_id: request_id.to_owned(),
                upstream_request_id,
                status,
                upstream_headers,
                body: UpstreamExchangeBody::Buffered(body_bytes),
            });
        }

        if status.is_success() {
            tracing::info!(
                %request_id,
                route_kind,
                status = %status,
                model,
                latency_ms,
                upstream_request_id = upstream_request_id.as_deref().unwrap_or("missing"),
                "upstream request completed"
            );
        } else {
            tracing::warn!(
                %request_id,
                route_kind,
                status = %status,
                model,
                latency_ms,
                upstream_request_id = upstream_request_id.as_deref().unwrap_or("missing"),
                "upstream request completed with non-success status"
            );
        }

        return Ok(UpstreamExchange {
            request_id: request_id.to_owned(),
            upstream_request_id,
            status,
            upstream_headers,
            body: UpstreamExchangeBody::Live(response),
        });
    }

    bail!("upstream rejected too many parameters; gave up after {MAX_STRIP_RETRIES} retries")
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
        .transpose()?
    {
        object.insert("model".to_owned(), Value::String(normalized_model));
    }

    if let Some(required) = original_model
        .as_deref()
        .and_then(|model| models.required_reasoning_effort(model))
    {
        let reasoning = object.entry("reasoning").or_insert_with(|| json!({}));
        let reasoning = reasoning
            .as_object_mut()
            .ok_or_else(|| anyhow!("profiled model requires a reasoning object"))?;
        if reasoning
            .get("effort")
            .is_some_and(|effort| effort.as_str() != Some(required))
        {
            bail!("requested reasoning effort conflicts with the model profile");
        }
        reasoning.insert("effort".to_owned(), json!(required));
    }

    object.insert("store".to_owned(), Value::Bool(false));
    object.insert("stream".to_owned(), Value::Bool(true));
    // Fields sent by some clients that the upstream rejects.
    object.remove("prompt_cache_retention");
    object.remove("safety_identifier");

    // The OpenAI Responses API allows `input` to be a plain string (shorthand
    // for a single user message), but the ChatGPT upstream requires a list.
    // Coerce string inputs before normalization so downstream always sees an array.
    if let Some(Value::String(s)) = object.get("input").cloned() {
        object.insert(
            "input".to_owned(),
            json!([{"type": "message", "role": "user", "content": s}]),
        );
    }

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

/// Parse and normalize a raw compact request body into a [`PreparedRequest`].
///
/// Unlike [`prepare_request_body`], this does **not** inject `store` or `stream`
/// because the compact endpoint does not support those parameters. It does:
/// - Canonicalize the model name via [`ModelsConfig::canonicalize`].
/// - Strip `stream` (compact never streams), `store`, `prompt_cache_retention`,
///   and `safety_identifier`.
pub fn prepare_compact_request_body(body: &[u8], models: &ModelsConfig) -> Result<PreparedRequest> {
    let mut body: Value =
        serde_json::from_slice(body).context("compact request body must be valid JSON")?;
    let original_model = body
        .get("model")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    if original_model
        .as_deref()
        .and_then(|model| models.required_reasoning_effort(model))
        .is_some()
    {
        bail!("reasoning-profiled models are not supported by compaction");
    }

    let object = body
        .as_object_mut()
        .ok_or_else(|| anyhow!("compact request body must be a JSON object"))?;

    if let Some(normalized_model) = object
        .get("model")
        .and_then(Value::as_str)
        .map(|model| models.canonicalize(model))
        .transpose()?
    {
        object.insert("model".to_owned(), Value::String(normalized_model));
    }

    // Fields the compact endpoint does not accept.
    object.remove("stream");
    object.remove("store");
    object.remove("prompt_cache_retention");
    object.remove("safety_identifier");

    let normalized_model = object
        .get("model")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    Ok(PreparedRequest {
        body: Value::Object(object.clone()),
        original_stream: false,
        transformation: TransformationSummary {
            original_model,
            normalized_model,
            input_items_before: 0,
            input_items_after: 0,
            filtered_item_references: 0,
            stripped_item_ids: 0,
            dropped_orphaned_tool_outputs: 0,
            include_defaults_added: 0,
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

    if has_reasoning_defaults && let Some(reasoning) = ensure_object_field(body, "reasoning") {
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

    let has_text_defaults = request_defaults
        .text_verbosity
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());

    if has_text_defaults && let Some(text) = ensure_object_field(body, "text") {
        insert_default_string_field(
            text,
            "verbosity",
            request_defaults.text_verbosity.as_deref(),
        );
    }

    normalize_include(body, &request_defaults.include)
}

fn ensure_object_field<'a>(
    body: &'a mut serde_json::Map<String, Value>,
    key: &str,
) -> Option<&'a mut serde_json::Map<String, Value>> {
    if !body.contains_key(key) {
        body.insert(key.to_owned(), Value::Object(serde_json::Map::new()));
    } else if !body[key].is_object() {
        // The client sent a non-object value (e.g. a plain string). Replace it
        // with an empty object so defaults can be injected; without this the
        // bad value would propagate to the upstream and cause an opaque 400.
        tracing::warn!(
            key,
            actual_type = body[key].as_str().map(|_| "string").unwrap_or("non-string"),
            "replacing non-object field with empty object to apply request defaults"
        );
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
    request_id: &str,
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
    headers.insert(
        UPSTREAM_CLIENT_REQUEST_ID_HEADER,
        HeaderValue::from_str(request_id).context("invalid proxy request id header")?,
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
    // Hold the write lock through refresh and persistence to avoid refresh-token reuse.
    let mut guard = state.tokens.write().await;
    if guard.is_none() {
        *guard =
            crate::state::load_tokens(&state.paths).context("failed to read tokens from disk")?;
    }
    let current = guard.clone();
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
    tracing::info!(
        token_status = status.as_str(),
        "refreshing stored OAuth tokens before forwarding request"
    );
    let refreshed_tokens = refresh_access_token(
        &state.client,
        &state.config.upstream,
        &current.refresh_token,
    )
    .await
    .context("failed to refresh stored OAuth tokens")?;

    *guard = Some(refreshed_tokens.clone());
    save_tokens(&state.paths, &refreshed_tokens).context("failed to persist refreshed tokens")?;
    tracing::info!(
        account_id = refreshed_tokens.account_id.as_deref().unwrap_or("missing"),
        "refreshed stored OAuth tokens during request forwarding"
    );
    Ok(refreshed_tokens)
}

/// Construct the full upstream URL by joining `responses_base_url` and
/// `codex_responses_path` from config.
pub(crate) fn upstream_responses_url(config: &Config) -> Result<Url> {
    let mut base_url = config.upstream.responses_base_url.clone();
    if !base_url.ends_with('/') {
        base_url.push('/');
    }

    let base = Url::parse(&base_url).context("invalid ChatGPT backend base URL")?;
    base.join(config.upstream.codex_responses_path.trim_start_matches('/'))
        .context("invalid Codex responses path")
}

/// Construct the full upstream URL by joining `responses_base_url` and
/// `codex_compact_path` from config.
fn upstream_compact_url(config: &Config) -> Result<Url> {
    let mut base_url = config.upstream.responses_base_url.clone();
    if !base_url.ends_with('/') {
        base_url.push('/');
    }

    let base = Url::parse(&base_url).context("invalid ChatGPT backend base URL")?;
    base.join(config.upstream.codex_compact_path.trim_start_matches('/'))
        .context("invalid Codex compact path")
}

/// Convert a successful upstream response into an Axum [`Response`].
///
/// - **Streaming** (`original_stream = true`): byte-streams the upstream SSE
///   body to the client with periodic `: keep-alive` heartbeat comments
///   (every `keep_alive_secs` seconds, disabled when `0`) to prevent idle
///   connection timeouts through load balancers. If the upstream stream errors
///   mid-flight an SSE `event: error` frame is injected before closing.
/// - **Non-streaming**: reads the full SSE body (sending periodic `\n`
///   keep-alive whitespace so the client connection stays open), finds the
///   `response.done` / `response.completed` event, and returns its `response`
///   field as JSON.
async fn handle_success_response(
    upstream: UpstreamExchange,
    original_stream: bool,
    keep_alive_secs: u64,
) -> Result<Response> {
    let request_id = upstream.request_id;
    let upstream_request_id = upstream.upstream_request_id;
    let response = match upstream.body {
        UpstreamExchangeBody::Live(r) => r,
        UpstreamExchangeBody::Buffered(_) => {
            anyhow::bail!("unexpected buffered body on success response")
        }
    };

    if original_stream {
        let status = response.status();
        let headers = response_headers(response.headers());
        let upstream_stream = response.bytes_stream();

        let body = if keep_alive_secs == 0 {
            // Keep-alive disabled — forward bytes passively.
            Body::from_stream(upstream_stream)
        } else {
            let interval_duration = Duration::from_secs(keep_alive_secs);
            let body_stream = stream! {
                let mut upstream = Box::pin(upstream_stream);
                let mut interval = tokio::time::interval(interval_duration);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                // Consume the first immediate tick so the first heartbeat only
                // fires after a full interval has elapsed.
                interval.tick().await;

                loop {
                    tokio::select! {
                        chunk = upstream.next() => {
                            match chunk {
                                Some(Ok(bytes)) => yield Ok::<_, anyhow::Error>(bytes),
                                Some(Err(e)) => {
                                    // Inject an SSE error event so clients get an
                                    // explanation instead of a silent EOF.
                                    let error_json = json!({
                                        "type": "error",
                                        "code": "upstream_error",
                                        "message": e.to_string(),
                                        "sequence_number": 0
                                    });
                                    let frame = format!(
                                        "event: error\ndata: {}\n\n",
                                        error_json
                                    );
                                    yield Ok(Bytes::from(frame));
                                    break;
                                }
                                None => break,
                            }
                        }
                        _ = interval.tick() => {
                            yield Ok(Bytes::from_static(b": keep-alive\n\n"));
                        }
                    }
                }
            };
            Body::from_stream(body_stream)
        };

        return Response::builder()
            .status(status)
            .body(body)
            .map(|mut response| {
                *response.headers_mut() = headers;
                attach_diagnostic_headers(
                    response.headers_mut(),
                    &request_id,
                    upstream_request_id.as_deref(),
                );
                response
            })
            .context("failed to build streaming proxy response");
    }

    // Non-streaming path: the client expects a single JSON object.
    // We spawn the upstream body read on a task and send periodic `\n`
    // keep-alive whitespace (valid JSON leading whitespace per RFC 8259) so
    // the connection doesn't go idle while waiting for a long upstream call.
    let status = response.status();
    let (tx, rx) = tokio::sync::oneshot::channel::<reqwest::Result<String>>();
    tokio::spawn(async move {
        let _ = tx.send(response.text().await);
    });

    let request_id_clone = request_id.clone();
    let upstream_request_id_clone = upstream_request_id.clone();

    let body_stream = stream! {
        let mut rx = rx;

        if keep_alive_secs > 0 {
            let interval_duration = Duration::from_secs(keep_alive_secs);
            let mut interval = tokio::time::interval(interval_duration);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await; // consume first immediate tick

            loop {
                tokio::select! {
                    result = &mut rx => {
                        let text = match result {
                            Ok(Ok(text)) => text,
                            Ok(Err(error)) => {
                                yield Err::<Bytes, anyhow::Error>(
                                    anyhow!("failed to read upstream SSE response: {error}")
                                );
                                break;
                            }
                            Err(_) => {
                                yield Err::<Bytes, anyhow::Error>(
                                    anyhow!("upstream reader task dropped unexpectedly")
                                );
                                break;
                            }
                        };
                        let json_response = match parse_success_text_to_json(&text) {
                            Some(json) => json,
                            None => {
                                tracing::warn!(
                                    request_id = %request_id_clone,
                                    upstream_request_id = upstream_request_id_clone
                                        .as_deref()
                                        .unwrap_or("missing"),
                                    "successful upstream response did not contain a parseable \
                                     final JSON payload; returning raw text envelope"
                                );
                                Value::String(text)
                            }
                        };
                        match serde_json::to_vec(&json_response) {
                            Ok(json_bytes) => yield Ok::<_, anyhow::Error>(Bytes::from(json_bytes)),
                            Err(error) => yield Err::<Bytes, anyhow::Error>(
                                error.into()
                            ),
                        }
                        break;
                    }
                    _ = interval.tick() => {
                        // Leading JSON whitespace — ignored by all conforming parsers.
                        yield Ok(Bytes::from_static(b"\n"));
                    }
                }
            }
        } else {
            // Keep-alive disabled — await the body directly.
            let result = rx.await;
            let text = match result {
                Ok(Ok(text)) => text,
                Ok(Err(error)) => {
                    yield Err::<Bytes, anyhow::Error>(
                        anyhow!("failed to read upstream SSE response: {error}")
                    );
                    return;
                }
                Err(_) => {
                    yield Err::<Bytes, anyhow::Error>(
                        anyhow!("upstream reader task dropped unexpectedly")
                    );
                    return;
                }
            };
            let json_response = match parse_success_text_to_json(&text) {
                Some(json) => json,
                None => {
                    tracing::warn!(
                        request_id = %request_id_clone,
                        upstream_request_id = upstream_request_id_clone
                            .as_deref()
                            .unwrap_or("missing"),
                        "successful upstream response did not contain a parseable \
                         final JSON payload; returning raw text envelope"
                    );
                    Value::String(text)
                }
            };
            match serde_json::to_vec(&json_response) {
                Ok(json_bytes) => yield Ok::<_, anyhow::Error>(Bytes::from(json_bytes)),
                Err(error) => yield Err::<Bytes, anyhow::Error>(error.into()),
            }
        }
    };

    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
        .body(Body::from_stream(body_stream))
        .map(|mut response| {
            attach_diagnostic_headers(
                response.headers_mut(),
                &request_id,
                upstream_request_id.as_deref(),
            );
            response
        })
        .context("failed to build JSON proxy response")
}

/// Pass an upstream error response through to the client.
///
/// Special case: a `404` whose body contains usage-limit language is remapped
/// to `429 Too Many Requests` so clients that understand rate-limit errors
/// can back off automatically.
pub async fn map_upstream_error(upstream: UpstreamExchange) -> Response {
    let request_id = upstream.request_id;
    let original_status = upstream.status;
    let mut status = original_status;
    let upstream_request_id = upstream.upstream_request_id;
    let headers = response_headers(&upstream.upstream_headers);
    let body = match upstream.body {
        UpstreamExchangeBody::Buffered(bytes) => bytes,
        UpstreamExchangeBody::Live(response) => response.bytes().await.unwrap_or_default(),
    };

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

    tracing::warn!(
        %request_id,
        upstream_request_id = upstream_request_id.as_deref().unwrap_or("missing"),
        status = %original_status,
        mapped_status = %status,
        "upstream returned error response"
    );

    Response::builder()
        .status(status)
        .body(Body::from(body))
        .map(|mut response| {
            *response.headers_mut() = headers;
            attach_diagnostic_headers(
                response.headers_mut(),
                &request_id,
                upstream_request_id.as_deref(),
            );
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

fn attach_diagnostic_headers(
    headers: &mut HeaderMap,
    request_id: &str,
    upstream_request_id: Option<&str>,
) {
    if let Ok(value) = HeaderValue::from_str(request_id) {
        headers.insert(PROXY_REQUEST_ID_HEADER, value);
    }

    if let Some(upstream_request_id) = upstream_request_id
        && let Ok(value) = HeaderValue::from_str(upstream_request_id)
    {
        headers.insert(PROXY_UPSTREAM_REQUEST_ID_HEADER, value);
    }
}

fn upstream_request_id_from_headers(headers: &reqwest::header::HeaderMap) -> Option<String> {
    headers
        .get(UPSTREAM_REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

/// If the upstream returned an unsupported-parameter error, return the
/// parameter name. Returns `None` for any other error shape.
///
/// Recognised shapes:
/// - `{"detail": "Unsupported parameter: <name>"}` (ChatGPT Codex backend)
/// - `{"error": {"message": "Unsupported parameter: <name>", ...}}` (OpenAI standard)
fn extract_unsupported_param(body: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;

    let try_extract = |s: &str| -> Option<String> {
        let param = s.strip_prefix("Unsupported parameter: ")?;
        if param.is_empty() {
            None
        } else {
            Some(param.to_owned())
        }
    };

    // Shape 1: {"detail": "Unsupported parameter: X"}
    if let Some(detail) = value.get("detail").and_then(Value::as_str) {
        if let Some(param) = try_extract(detail) {
            return Some(param);
        }
    }

    // Shape 2: {"error": {"message": "Unsupported parameter: X", ...}}
    if let Some(message) = value
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(Value::as_str)
    {
        if let Some(param) = try_extract(message) {
            return Some(param);
        }
    }

    None
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
) -> Result<(StatusCode, Value, Option<String>)> {
    let status = response.status();
    let upstream_request_id = upstream_request_id_from_headers(response.headers());
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
        return Ok((status, json, upstream_request_id));
    }

    let body = response
        .text()
        .await
        .context("failed to read upstream success response body")?;
    let json = parse_success_text_to_json(&body)
        .ok_or_else(|| anyhow!("upstream success response did not contain a final JSON payload"))?;

    Ok((status, json, upstream_request_id))
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

pub fn error_response_with_request_id(
    status: StatusCode,
    message: impl Into<String>,
    request_id: &str,
) -> Response {
    let mut response = error_response(status, message);
    attach_diagnostic_headers(response.headers_mut(), request_id, None);
    response
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
    fn alias_reasoning_profile_is_enforced_without_changing_base_model() {
        let mut config = Config::default();
        config
            .models
            .aliases
            .insert("gpt-6-luna-xhigh".into(), "gpt-6-luna".into());
        config.models.advertised = vec!["gpt-6-luna".into()];
        config
            .models
            .reasoning_efforts
            .insert("gpt-6-luna-xhigh".into(), "xhigh".into());
        assert!(
            prepare_compact_request_body(
                br#"{"model":"gpt-6-luna-xhigh","input":[]}"#,
                &config.models
            )
            .is_err()
        );
        for alias in ["gpt-6-luna-xhigh", "codex-proxy/GPT-6-LUNA-XHIGH"] {
            let request = json!({"model":alias,"input":[]});
            let prepared = prepare_request_body(
                &serde_json::to_vec(&request).unwrap(),
                &config.request_defaults,
                &config.models,
            )
            .unwrap();
            assert_eq!(prepared.body["model"], "gpt-6-luna");
            assert_eq!(prepared.body["reasoning"]["effort"], "xhigh");
        }
        let request = json!({"model":"gpt-6-luna","input":[]});
        let prepared = prepare_request_body(
            &serde_json::to_vec(&request).unwrap(),
            &config.request_defaults,
            &config.models,
        )
        .unwrap();
        assert!(
            prepared
                .body
                .get("reasoning")
                .and_then(|r| r.get("effort"))
                .is_none()
        );
        for reasoning in [
            json!({"effort":"low"}),
            json!({"effort":null}),
            json!("xhigh"),
        ] {
            let request = json!({"model":"gpt-6-luna-xhigh","input":[],"reasoning":reasoning});
            assert!(
                prepare_request_body(
                    &serde_json::to_vec(&request).unwrap(),
                    &config.request_defaults,
                    &config.models
                )
                .is_err()
            );
        }
        let request = json!({"model":"gpt-6-luna-xhigh","input":[],"reasoning":{"effort":"xhigh","summary":"auto"}});
        let prepared = prepare_request_body(
            &serde_json::to_vec(&request).unwrap(),
            &config.request_defaults,
            &config.models,
        )
        .unwrap();
        assert_eq!(prepared.body["reasoning"]["summary"], "auto");
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

        let headers = build_upstream_headers(
            &request_headers,
            &tokens,
            &config,
            &request_body,
            "proxy-request-123",
        )
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
        assert_eq!(
            headers
                .get("x-client-request-id")
                .and_then(|v| v.to_str().ok()),
            Some("proxy-request-123")
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

    #[test]
    fn extract_unsupported_param_handles_detail_shape() {
        let body = br#"{"detail":"Unsupported parameter: temperature"}"#;
        assert_eq!(
            extract_unsupported_param(body),
            Some("temperature".to_owned())
        );
    }

    #[test]
    fn extract_unsupported_param_handles_openai_error_message_shape() {
        let body = br#"{"error":{"message":"Unsupported parameter: top_p","type":"invalid_request_error"}}"#;
        assert_eq!(extract_unsupported_param(body), Some("top_p".to_owned()));
    }

    #[test]
    fn extract_unsupported_param_returns_none_for_unrelated_errors() {
        let body = br#"{"error":{"message":"Invalid model","type":"invalid_request_error"}}"#;
        assert_eq!(extract_unsupported_param(body), None);
    }

    #[test]
    fn prepare_request_body_replaces_non_object_reasoning_with_defaults() {
        let mut config = Config::default();
        config.request_defaults.reasoning_effort = Some("high".to_owned());

        // Client sends `reasoning` as a string (wrong type). The proxy should
        // replace it with an object and inject the configured default effort.
        let request = json!({
            "model": "gpt-5.4",
            "reasoning": "high",
            "input": [{ "type": "message", "role": "user", "content": "hello" }]
        });

        let prepared = prepare_request_body(
            serde_json::to_vec(&request).expect("request").as_slice(),
            &config.request_defaults,
            &config.models,
        )
        .expect("prepared request");

        assert_eq!(prepared.body["reasoning"]["effort"], "high");
    }
}
