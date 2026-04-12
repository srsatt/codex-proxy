//! WebSocket compatibility transport for `GET /v1/responses`.
//!
//! The Codex CLI can speak a WebSocket protocol where the client sends
//! `response.create` / `response.append` control messages and expects response
//! events back as WebSocket text frames. The upstream ChatGPT backend still
//! speaks SSE, so this module translates between the two transports:
//!
//! - WebSocket client message -> normalize via `prepare_request_body`
//! - Forward to upstream `/codex/responses`
//! - SSE `data:` lines -> WebSocket text frames containing the JSON payload

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderValue, header},
    response::Response,
};
use futures_util::StreamExt;
use serde_json::{Map, Value, json};

use super::{
    ProxyAppState, authorize_request,
    forwarding::{
        ForwardingState, UpstreamExchange, UpstreamExchangeBody, prepare_request_body,
        send_prepared_request, upstream_responses_url,
    },
    unauthorized_response,
};

#[derive(Debug, Default)]
struct WebSocketSession {
    last_request: Option<Value>,
    last_response: Option<Value>,
}

pub(super) async fn forward_v1_responses_websocket(
    State(state): State<ProxyAppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if !authorize_request("responses_websocket", &headers, &state.fake_api_key) {
        return unauthorized_response();
    }

    ws.on_upgrade(move |socket| async move {
        let request_headers = websocket_request_headers();
        handle_responses_websocket(socket, state.forwarding_state(), request_headers).await;
    })
}

async fn handle_responses_websocket(
    mut socket: WebSocket,
    state: ForwardingState,
    request_headers: HeaderMap,
) {
    let connection_id = uuid::Uuid::new_v4().to_string();
    let mut session = WebSocketSession::default();

    tracing::info!(
        connection_id = %connection_id,
        route_kind = "responses_websocket",
        "websocket connection opened"
    );

    while let Some(message_result) = socket.recv().await {
        let message = match message_result {
            Ok(message) => message,
            Err(error) => {
                tracing::warn!(
                    connection_id = %connection_id,
                    route_kind = "responses_websocket",
                    error = %error,
                    "websocket receive failed"
                );
                break;
            }
        };

        match message {
            Message::Text(text) => {
                if let Err(error) = handle_client_message(
                    &mut socket,
                    &state,
                    &request_headers,
                    &mut session,
                    text.as_str(),
                )
                .await
                {
                    tracing::warn!(
                        connection_id = %connection_id,
                        route_kind = "responses_websocket",
                        error = %error,
                        "websocket request failed"
                    );
                    if send_error_frame(
                        &mut socket,
                        "invalid_request_error",
                        error.to_string(),
                        None,
                        None,
                    )
                    .await
                    .is_err()
                    {
                        break;
                    }
                }
            }
            Message::Binary(_) => {
                if send_error_frame(
                    &mut socket,
                    "invalid_request_error",
                    "websocket messages must be UTF-8 JSON text frames",
                    None,
                    None,
                )
                .await
                .is_err()
                {
                    break;
                }
            }
            Message::Ping(payload) => {
                if socket.send(Message::Pong(payload)).await.is_err() {
                    break;
                }
            }
            Message::Pong(_) => {}
            Message::Close(_) => break,
        }
    }

    tracing::info!(
        connection_id = %connection_id,
        route_kind = "responses_websocket",
        "websocket connection closed"
    );
}

async fn handle_client_message(
    socket: &mut WebSocket,
    state: &ForwardingState,
    request_headers: &HeaderMap,
    session: &mut WebSocketSession,
    text: &str,
) -> Result<()> {
    let request = parse_client_request(text, session)?;
    let request_id = uuid::Uuid::new_v4().to_string();
    let prepared_request = prepare_request_body(
        serde_json::to_vec(&request)?.as_slice(),
        &state.config.request_defaults,
        &state.config.models,
    )
    .context("failed to normalize websocket responses request")?;
    let upstream_url = upstream_responses_url(&state.config)?;

    let upstream = send_prepared_request(
        state,
        request_headers,
        &prepared_request,
        upstream_url,
        "responses_websocket",
        &request_id,
    )
    .await
    .with_context(|| format!("failed to forward websocket request {request_id}"))?;

    session.last_request = Some(request);

    if !upstream.status.is_success() {
        return emit_upstream_error_frame(socket, upstream, &request_id).await;
    }

    stream_upstream_events(socket, upstream, session, &request_id).await
}

fn parse_client_request(text: &str, session: &WebSocketSession) -> Result<Value> {
    let message: Value =
        serde_json::from_str(text).context("websocket message must be valid JSON")?;
    let object = message
        .as_object()
        .ok_or_else(|| anyhow!("websocket message must be a JSON object"))?;
    let message_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("websocket message is missing `type`"))?;

    match message_type {
        "response.create" => extract_response_payload(object),
        "response.append" => {
            let previous_request = session.last_request.as_ref().ok_or_else(|| {
                anyhow!(
                    "`response.append` requires a prior `response.create` on the same WebSocket"
                )
            })?;
            let patch = extract_response_payload(object)?;
            merge_append_request(previous_request, &patch)
        }
        other => bail!("unsupported websocket message type `{other}`"),
    }
}

fn extract_response_payload(message: &Map<String, Value>) -> Result<Value> {
    if let Some(response) = message.get("response") {
        if !response.is_object() {
            bail!("websocket `response` field must be a JSON object");
        }
        return Ok(response.clone());
    }

    let mut response = message.clone();
    response.remove("type");
    if response.is_empty() {
        bail!("websocket message is missing `response`");
    }

    Ok(Value::Object(response))
}

fn merge_append_request(previous: &Value, patch: &Value) -> Result<Value> {
    let mut merged = previous
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("cached websocket request is not a JSON object"))?;
    let patch = patch
        .as_object()
        .ok_or_else(|| anyhow!("websocket append payload must be a JSON object"))?;

    merge_objects(&mut merged, patch);
    Ok(Value::Object(merged))
}

fn merge_objects(target: &mut Map<String, Value>, patch: &Map<String, Value>) {
    for (key, value) in patch {
        match (target.get_mut(key), value) {
            (Some(Value::Array(existing)), Value::Array(new_items)) if key == "input" => {
                existing.extend(new_items.iter().cloned());
            }
            (Some(Value::Object(existing)), Value::Object(new_object)) => {
                merge_objects(existing, new_object);
            }
            _ => {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}

async fn emit_upstream_error_frame(
    socket: &mut WebSocket,
    upstream: UpstreamExchange,
    request_id: &str,
) -> Result<()> {
    let status = upstream.status;
    let upstream_request_id = upstream.upstream_request_id.clone();
    let body = match upstream.body {
        UpstreamExchangeBody::Buffered(bytes) => bytes,
        UpstreamExchangeBody::Live(response) => response
            .bytes()
            .await
            .context("failed to read upstream websocket error body")?,
    };

    let message = extract_upstream_error_message(&body)
        .unwrap_or_else(|| format!("upstream returned HTTP {}", status.as_u16()));

    send_error_frame(
        socket,
        "upstream_error",
        message,
        Some(request_id),
        upstream_request_id.as_deref(),
    )
    .await
}

async fn stream_upstream_events(
    socket: &mut WebSocket,
    upstream: UpstreamExchange,
    session: &mut WebSocketSession,
    request_id: &str,
) -> Result<()> {
    let upstream_request_id = upstream.upstream_request_id.clone();
    let response = match upstream.body {
        UpstreamExchangeBody::Live(response) => response,
        UpstreamExchangeBody::Buffered(_) => {
            bail!("unexpected buffered body on successful websocket response")
        }
    };

    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("failed to read upstream SSE stream")?;
        buffer.extend_from_slice(&chunk);

        while let Some(line) = take_next_line(&mut buffer)? {
            process_sse_line(socket, session, &line).await?;
        }
    }

    if !buffer.is_empty() {
        let line = String::from_utf8(buffer).context("upstream SSE line was not valid UTF-8")?;
        process_sse_line(socket, session, line.trim_end_matches('\r')).await?;
    }

    if session.last_response.is_none() {
        tracing::warn!(
            request_id = %request_id,
            upstream_request_id = upstream_request_id.as_deref().unwrap_or("missing"),
            route_kind = "responses_websocket",
            "websocket upstream stream ended without a final response event"
        );
    }

    Ok(())
}

fn take_next_line(buffer: &mut Vec<u8>) -> Result<Option<String>> {
    let Some(newline_index) = buffer.iter().position(|byte| *byte == b'\n') else {
        return Ok(None);
    };

    let remainder = buffer.split_off(newline_index + 1);
    let mut line = std::mem::replace(buffer, remainder);
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }

    Ok(Some(
        String::from_utf8(line).context("upstream SSE line was not valid UTF-8")?,
    ))
}

async fn process_sse_line(
    socket: &mut WebSocket,
    session: &mut WebSocketSession,
    line: &str,
) -> Result<()> {
    if line.is_empty() || line.starts_with(':') || line.starts_with("event:") {
        return Ok(());
    }

    let Some(data) = line.strip_prefix("data:").map(str::trim_start) else {
        return Ok(());
    };

    let event: Value = serde_json::from_str(data).context("upstream SSE data line was not JSON")?;
    if matches!(
        event.get("type").and_then(Value::as_str),
        Some("response.done" | "response.completed")
    ) {
        session.last_response = event.get("response").cloned();
    }

    socket
        .send(Message::Text(data.to_owned().into()))
        .await
        .context("failed to send websocket text frame")
}

async fn send_error_frame(
    socket: &mut WebSocket,
    code: &str,
    message: impl Into<String>,
    request_id: Option<&str>,
    upstream_request_id: Option<&str>,
) -> Result<()> {
    let mut payload = json!({
        "type": "error",
        "code": code,
        "message": message.into(),
        "sequence_number": 0
    });

    if let Some(request_id) = request_id {
        payload["request_id"] = Value::String(request_id.to_owned());
    }
    if let Some(upstream_request_id) = upstream_request_id {
        payload["upstream_request_id"] = Value::String(upstream_request_id.to_owned());
    }

    socket
        .send(Message::Text(payload.to_string().into()))
        .await
        .context("failed to send websocket error frame")
}

fn websocket_request_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers
}

fn extract_upstream_error_message(body: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(body);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(parsed) = serde_json::from_slice::<Value>(body) {
        if let Some(message) = parsed
            .get("error")
            .and_then(|value| value.get("message"))
            .and_then(Value::as_str)
        {
            return Some(message.to_owned());
        }
        if let Some(detail) = parsed.get("detail").and_then(Value::as_str) {
            return Some(detail.to_owned());
        }
    }

    Some(trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_append_request_appends_input_and_merges_nested_objects() {
        let previous = json!({
            "model": "gpt-5.4",
            "input": [{ "type": "message", "role": "user", "content": "hello" }],
            "reasoning": { "effort": "low" },
            "metadata": { "a": 1 }
        });
        let patch = json!({
            "input": [{ "type": "message", "role": "user", "content": "again" }],
            "reasoning": { "summary": "auto" },
            "metadata": { "b": 2 }
        });

        let merged = merge_append_request(&previous, &patch).expect("merged append request");

        assert_eq!(merged["input"].as_array().expect("input").len(), 2);
        assert_eq!(merged["reasoning"]["effort"], "low");
        assert_eq!(merged["reasoning"]["summary"], "auto");
        assert_eq!(merged["metadata"]["a"], 1);
        assert_eq!(merged["metadata"]["b"], 2);
    }

    #[test]
    fn parse_client_request_requires_prior_create_for_append() {
        let session = WebSocketSession::default();
        let error = parse_client_request(
            r#"{"type":"response.append","response":{"input":[]}}"#,
            &session,
        )
        .expect_err("append without create should fail");

        assert!(
            error
                .to_string()
                .contains("requires a prior `response.create`")
        );
    }
}
