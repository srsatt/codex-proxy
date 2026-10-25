//! `POST /v1/chat/completions` compatibility adapter.
//!
//! Translates OpenAI chat-completions requests into the ChatGPT Codex
//! responses format, forwards them via [`super::forwarding`], and converts
//! the responses-format reply back into an OpenAI-compatible
//! `chat.completion` object.
//!
//! Streaming replies are translated incrementally into Chat Completion chunks.

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    body::Bytes,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use serde_json::{Map, Value, json};

use crate::config::{ModelsConfig, RequestDefaultsConfig};

use super::forwarding::{
    ForwardingState, PreparedRequest, UpstreamExchange, error_response_with_request_id,
    extract_success_response_json, map_upstream_error, prepare_request_body, send_prepared_request,
    upstream_responses_url,
};

/// Handler for `POST /v1/chat/completions`.
///
/// 1. Translates the chat-completions request body to the responses format.
/// 2. Preserves the requested response mode.
/// 3. Forwards via [`send_prepared_request`].
/// 4. Converts the upstream response to a `chat.completion` object.
pub async fn forward_chat_completions(
    state: &ForwardingState,
    request_headers: &HeaderMap,
    request_body: Bytes,
) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    let prepared_request = match translate_chat_completions_request(
        &request_body,
        &state.config.request_defaults,
        &state.config.models,
    ) {
        Ok(prepared_request) => prepared_request,
        Err(error) => {
            tracing::warn!(%request_id, route_kind = "chat_completions", error = %error, "failed to translate chat completions request");
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
        "chat_completions",
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

    if prepared_request.original_stream {
        let include_usage = serde_json::from_slice::<Value>(&request_body)
            .ok()
            .and_then(|v| {
                v.pointer("/stream_options/include_usage")
                    .and_then(Value::as_bool)
            })
            .unwrap_or(false);
        return super::chat_stream::response(
            upstream,
            include_usage,
            state.config.upstream.keep_alive_interval_secs.unwrap_or(15),
        );
    }

    match adapt_chat_completions_response(upstream).await {
        Ok(response) => response,
        Err(error) => {
            error_response_with_request_id(StatusCode::BAD_GATEWAY, error.to_string(), &request_id)
        }
    }
}

/// Translate a raw `POST /v1/chat/completions` body into a [`PreparedRequest`]
/// targeting the responses API.
///
/// Field mappings:
/// - `messages` → `input` (via [`convert_chat_messages`])
/// - `max_completion_tokens` or `max_tokens` → `max_output_tokens`
/// - Function `tools` and forced `tool_choice` are flattened for Responses.
/// - `reasoning`, `include`, `prompt_cache_key`, `text` are copied verbatim.
/// - `stream` selects the incremental or buffered response adapter.
pub fn translate_chat_completions_request(
    body: &[u8],
    request_defaults: &RequestDefaultsConfig,
    models: &ModelsConfig,
) -> Result<PreparedRequest> {
    let request: Value =
        serde_json::from_slice(body).context("chat completions request body must be valid JSON")?;
    let object = request
        .as_object()
        .ok_or_else(|| anyhow!("chat completions request must be a JSON object"))?;

    let model = object
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("chat completions request is missing `model`"))?;
    let messages = match object.get("messages").and_then(Value::as_array) {
        Some(messages) => messages,
        None => {
            if object.contains_key("input") || object.contains_key("instructions") {
                bail!(
                    "request appears to be in Responses API format (has `input`/`instructions` but no `messages`); send it to `POST /v1/responses` instead"
                );
            }
            bail!("chat completions request is missing `messages`");
        }
    };
    if messages.is_empty() {
        bail!("chat completions request must include at least one message");
    }

    let mut responses_body = Map::new();
    responses_body.insert("model".to_owned(), Value::String(model.to_owned()));
    responses_body.insert(
        "stream".to_owned(),
        Value::Bool(
            object
                .get("stream")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        ),
    );
    responses_body.insert(
        "input".to_owned(),
        Value::Array(convert_chat_messages(messages)?),
    );

    if let Some(tools) = object.get("tools").filter(|v| !v.is_null()) {
        let tools = tools
            .as_array()
            .ok_or_else(|| anyhow!("`tools` must be an array"))?;
        let tools = tools
            .iter()
            .map(|tool| {
                let function = chat_function(tool)?;
                let mut flat = function.clone();
                flat.insert("type".to_owned(), json!("function"));
                // Chat defaults to non-strict schemas; Responses defaults to strict.
                flat.entry("strict").or_insert(json!(false));
                Ok(Value::Object(flat))
            })
            .collect::<Result<Vec<_>>>()?;
        responses_body.insert("tools".to_owned(), Value::Array(tools));
    }
    if let Some(choice) = object.get("tool_choice").filter(|v| !v.is_null()) {
        let choice = match choice.as_str() {
            Some("auto" | "none" | "required") => choice.clone(),
            Some(_) => bail!("unsupported `tool_choice`"),
            None => json!({"type": "function", "name": chat_function(choice)?["name"]}),
        };
        responses_body.insert("tool_choice".to_owned(), choice);
    }
    copy_field(object, &mut responses_body, "reasoning");
    copy_field(object, &mut responses_body, "include");
    copy_field(object, &mut responses_body, "prompt_cache_key");
    copy_field(object, &mut responses_body, "text");

    // Standard OpenAI o-series clients send `reasoning_effort` as a top-level
    // string (e.g. `"reasoning_effort": "high"`). The Responses API expects it
    // nested as `reasoning.effort`. Translate only when no `reasoning` object
    // was already copied above.
    if !responses_body.contains_key("reasoning")
        && let Some(effort) = object.get("reasoning_effort").and_then(Value::as_str)
    {
        responses_body.insert("reasoning".to_owned(), json!({"effort": effort}));
    }

    if let Some(v) = object
        .get("max_output_tokens")
        .or_else(|| object.get("max_completion_tokens"))
        .or_else(|| object.get("max_tokens"))
    {
        responses_body.insert("max_output_tokens".to_owned(), v.clone());
    }

    prepare_request_body(
        serde_json::to_vec(&Value::Object(responses_body))?.as_slice(),
        request_defaults,
        models,
    )
}

/// Read the upstream success response and convert it to `chat.completion`
/// format.
async fn adapt_chat_completions_response(upstream: UpstreamExchange) -> Result<Response> {
    let request_id = upstream.request_id;
    let (status, response_json, upstream_request_id) =
        extract_success_response_json(match upstream.body {
            crate::proxy::forwarding::UpstreamExchangeBody::Live(r) => r,
            crate::proxy::forwarding::UpstreamExchangeBody::Buffered(_) => {
                return Err(anyhow::anyhow!(
                    "unexpected buffered body on success response"
                ));
            }
        })
        .await?;
    let chat_completion = responses_to_chat_completion(&response_json)?;

    axum::http::Response::builder()
        .status(status)
        .header("content-type", "application/json; charset=utf-8")
        .header("x-codex-proxy-request-id", &request_id)
        .body(axum::body::Body::from(serde_json::to_vec(
            &chat_completion,
        )?))
        .map(|mut response| {
            if let Some(upstream_request_id) = upstream_request_id
                && let Ok(value) = axum::http::HeaderValue::from_str(&upstream_request_id)
            {
                response
                    .headers_mut()
                    .insert("x-codex-proxy-upstream-request-id", value);
            }
            response
        })
        .context("failed to build chat completions compatibility response")
}

/// Convert a responses-API response object into an OpenAI `chat.completion`.
///
/// Output items are mapped as follows:
/// - `type: "message"` → assistant message content (text parts concatenated).
/// - `type: "function_call"` → tool call entry in `choices[0].message.tool_calls`.
///
/// `finish_reason` is `"tool_calls"` when any function call items are present,
/// otherwise `"stop"`.
pub fn responses_to_chat_completion(response: &Value) -> Result<Value> {
    let output = response
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("responses payload is missing `output`"))?;

    let mut content_parts = Vec::new();
    let mut tool_calls = Vec::new();

    for item in output {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
        match item_type {
            "message" => {
                if let Some(content) = item.get("content").and_then(Value::as_array) {
                    for part in content {
                        let part_type =
                            part.get("type").and_then(Value::as_str).unwrap_or_default();
                        if !matches!(part_type, "output_text" | "text") {
                            continue;
                        }
                        if let Some(text) = part.get("text").and_then(Value::as_str) {
                            content_parts.push(text.to_owned());
                        }
                    }
                } else if let Some(content) = item.get("content").and_then(Value::as_str) {
                    content_parts.push(content.to_owned());
                }
            }
            "function_call" => {
                let call_id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let name = item.get("name").and_then(Value::as_str).unwrap_or_default();
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or("{}");
                if !call_id.is_empty() && !name.is_empty() {
                    tool_calls.push(json!({
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": arguments
                        }
                    }));
                }
            }
            _ => {}
        }
    }

    let message_content = if content_parts.is_empty() {
        Value::Null
    } else {
        Value::String(content_parts.join(""))
    };

    // Map the upstream `status` / `incomplete_details.reason` fields to the
    // OpenAI `finish_reason` values clients expect.
    let finish_reason = match response.get("status").and_then(Value::as_str) {
        Some("incomplete") => {
            match response
                .get("incomplete_details")
                .and_then(|d| d.get("reason"))
                .and_then(Value::as_str)
            {
                Some("max_output_tokens") => "length",
                Some("content_filter") => "content_filter",
                _ => "stop",
            }
        }
        _ => {
            if tool_calls.is_empty() {
                "stop"
            } else {
                "tool_calls"
            }
        }
    };

    let mut message = Map::new();
    message.insert("role".to_owned(), Value::String("assistant".to_owned()));
    message.insert("content".to_owned(), message_content);
    if !tool_calls.is_empty() {
        message.insert("tool_calls".to_owned(), Value::Array(tool_calls));
    }

    let created = response
        .get("created_at")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let model = response
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("unknown");

    let usage = response
        .get("usage")
        .and_then(Value::as_object)
        .map(|usage| {
            json!({
                "prompt_tokens": usage.get("input_tokens").and_then(Value::as_u64).unwrap_or_default(),
                "completion_tokens": usage.get("output_tokens").and_then(Value::as_u64).unwrap_or_default(),
                "total_tokens": usage.get("total_tokens").and_then(Value::as_u64).unwrap_or_default()
            })
        })
        .unwrap_or_else(|| json!({
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0
        }));

    Ok(json!({
        "id": response.get("id").and_then(Value::as_str).unwrap_or("chatcmpl-local"),
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": Value::Object(message),
                "finish_reason": finish_reason
            }
        ],
        "usage": usage
    }))
}

/// Convert a chat-completions `messages` array to a responses `input` array.
///
/// Role mappings:
/// - `system` / `developer` → `type: "message"`, role becomes `"developer"`.
/// - `user` → `type: "message"`.
/// - `assistant` → `type: "message"` for text content, plus one
///   `type: "function_call"` item per tool call.
/// - `tool` → `type: "function_call_output"`, `call_id` from `tool_call_id`.
fn convert_chat_messages(messages: &[Value]) -> Result<Vec<Value>> {
    let mut converted = Vec::with_capacity(messages.len());

    for message in messages {
        let object = message
            .as_object()
            .ok_or_else(|| anyhow!("chat completion message must be an object"))?;
        let role = object
            .get("role")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("chat completion message is missing `role`"))?;

        match role {
            "system" | "developer" | "user" => {
                let content = extract_message_content(object.get("content"))?;
                converted.push(json!({
                    "type": "message",
                    "role": if role == "system" { "developer" } else { role },
                    "content": content
                }));
            }
            "assistant" => {
                if let Some(content) = extract_optional_message_content(object.get("content"))? {
                    converted.push(json!({
                        "type": "message",
                        "role": "assistant",
                        "content": content
                    }));
                }

                if let Some(tool_calls) = object.get("tool_calls").and_then(Value::as_array) {
                    for tool_call in tool_calls {
                        let call_object = tool_call
                            .as_object()
                            .ok_or_else(|| anyhow!("assistant tool call must be an object"))?;
                        let function = call_object
                            .get("function")
                            .and_then(Value::as_object)
                            .ok_or_else(|| anyhow!("assistant tool call is missing `function`"))?;
                        let call_id = call_object
                            .get("id")
                            .and_then(Value::as_str)
                            .ok_or_else(|| anyhow!("assistant tool call is missing `id`"))?;
                        let name =
                            function
                                .get("name")
                                .and_then(Value::as_str)
                                .ok_or_else(|| {
                                    anyhow!("assistant tool call is missing `function.name`")
                                })?;
                        let arguments = function
                            .get("arguments")
                            .and_then(Value::as_str)
                            .unwrap_or("{}");

                        converted.push(json!({
                            "type": "function_call",
                            "call_id": call_id,
                            "name": name,
                            "arguments": arguments
                        }));
                    }
                }
            }
            "tool" => {
                let tool_call_id = object
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("tool message is missing `tool_call_id`"))?;
                let output = extract_message_content(object.get("content"))?;
                converted.push(json!({
                    "type": "function_call_output",
                    "call_id": tool_call_id,
                    "output": output
                }));
            }
            _ => bail!("unsupported chat completion message role `{role}`"),
        }
    }

    Ok(converted)
}

/// Extract required string content from a message's `content` field.
/// Returns an error if the content is null or empty.
fn extract_message_content(content: Option<&Value>) -> Result<String> {
    extract_optional_message_content(content)?
        .ok_or_else(|| anyhow!("chat completion message content must not be empty"))
}

/// Extract optional string content, returning `Ok(None)` for null/missing
/// and `Ok(Some(...))` for non-empty string or array of text parts.
fn extract_optional_message_content(content: Option<&Value>) -> Result<Option<String>> {
    let Some(content) = content else {
        return Ok(None);
    };

    match content {
        Value::Null => Ok(None),
        Value::String(text) => Ok(Some(text.clone())),
        Value::Array(parts) => {
            let mut text = String::new();
            for part in parts {
                let object = part
                    .as_object()
                    .ok_or_else(|| anyhow!("chat completion content part must be an object"))?;
                let part_type = object
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("chat completion content part is missing `type`"))?;
                match part_type {
                    "text" | "input_text" => {
                        let value =
                            object.get("text").and_then(Value::as_str).ok_or_else(|| {
                                anyhow!("chat completion text part is missing `text`")
                            })?;
                        text.push_str(value);
                    }
                    _ => bail!("unsupported chat completion content part type `{part_type}`"),
                }
            }

            if text.is_empty() {
                Ok(None)
            } else {
                Ok(Some(text))
            }
        }
        _ => bail!("chat completion content must be string, null, or an array of text parts"),
    }
}

/// Validate the nested Chat Completions function shape before flattening it.
fn chat_function(value: &Value) -> Result<&Map<String, Value>> {
    if value.get("type").and_then(Value::as_str) != Some("function") {
        bail!("only Chat Completions function tools are supported");
    }
    let function = value
        .get("function")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("function tool is missing `function`"))?;
    if function
        .get("name")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
        || function.get("description").is_some_and(|v| !v.is_string())
        || function.get("parameters").is_some_and(|v| !v.is_object())
        || function.get("strict").is_some_and(|v| !v.is_boolean())
    {
        bail!("invalid Chat Completions function definition");
    }
    Ok(function)
}

/// Copy a field from `source` to `target` if it exists.
fn copy_field(source: &Map<String, Value>, target: &mut Map<String, Value>, key: &str) {
    if let Some(value) = source.get(key) {
        target.insert(key.to_owned(), value.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn translate_chat_completions_request_converts_messages_to_responses_input() {
        let models = ModelsConfig::default();
        let request = json!({
            "model": "gpt-5.3-codex",
            "stream": false,
            "messages": [
                { "role": "system", "content": "You are helpful." },
                { "role": "user", "content": "Write a file." },
                {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {
                                "name": "write_file",
                                "arguments": "{\"path\":\"a.txt\"}"
                            }
                        }
                    ]
                },
                { "role": "tool", "tool_call_id": "call_1", "content": "done" }
            ],
            "max_tokens": 256
        });

        let prepared = translate_chat_completions_request(
            serde_json::to_vec(&request).expect("request").as_slice(),
            &Config::default().request_defaults,
            &models,
        )
        .expect("prepared request");

        assert!(!prepared.original_stream);
        assert_eq!(prepared.body["model"], "gpt-5.3-codex");
        assert_eq!(prepared.body["stream"], true);
        assert_eq!(prepared.body["store"], false);
        assert_eq!(prepared.body["max_output_tokens"], 256);
        assert_eq!(prepared.body["input"][0]["role"], "developer");
        assert_eq!(prepared.body["input"][2]["type"], "function_call");
        assert_eq!(prepared.body["input"][3]["type"], "function_call_output");
    }

    #[test]
    fn translate_home_assistant_tools_and_forced_choice() {
        let mut request = json!({
            "model": "gpt-5.4",
            "messages": [{"role": "user", "content": "Turn on a light."}],
            "tools": [{"type": "function", "function": {
                "name": "execute_services", "description": "Execute Home Assistant services.",
                "parameters": {"type": "object", "properties": {
                    "list": {"type": "array", "items": {"type": "object", "properties": {
                        "domain": {"type": "string"}, "service": {"type": "string"},
                        "service_data": {"type": "object"}
                    }, "required": ["domain", "service", "service_data"]}}
                }, "required": ["list"]}
            }}],
            "tool_choice": {"type": "function", "function": {"name": "execute_services"}}
        });
        let translate = |request: &Value| {
            translate_chat_completions_request(
                &serde_json::to_vec(request).unwrap(),
                &RequestDefaultsConfig::default(),
                &ModelsConfig::default(),
            )
        };
        let prepared = translate(&request).unwrap();
        assert_eq!(prepared.body["tools"][0]["name"], "execute_services");
        assert_eq!(
            prepared.body["tools"][0]["parameters"],
            request["tools"][0]["function"]["parameters"]
        );
        assert_eq!(prepared.body["tools"][0]["strict"], false);
        assert!(prepared.body["tools"][0].get("function").is_none());
        assert_eq!(
            prepared.body["tool_choice"],
            json!({"type": "function", "name": "execute_services"})
        );
        for choice in ["auto", "none", "required"] {
            request["tool_choice"] = json!(choice);
            assert_eq!(translate(&request).unwrap().body["tool_choice"], choice);
        }
        request["tools"][0]["function"]["strict"] = json!(true);
        assert_eq!(
            translate(&request).unwrap().body["tools"][0]["strict"],
            true
        );
        for tool in [
            json!({"type":"web_search"}),
            json!({"type":"function"}),
            json!({"type":"function","function":{"name":""}}),
            json!({"type":"function","function":{"name":"x","parameters":[]}}),
        ] {
            request["tools"] = json!([tool]);
            assert!(translate(&request).is_err());
        }
        request["tools"] = json!([]);
        request["tool_choice"] = json!({"type":"function","function":{}});
        assert!(translate(&request).is_err());
    }

    #[test]
    fn translate_chat_completions_request_remaps_max_completion_tokens() {
        let models = ModelsConfig::default();
        let request = json!({
            "model": "gpt-5.3-codex",
            "messages": [{ "role": "user", "content": "hi" }],
            "max_completion_tokens": 512
        });

        let prepared = translate_chat_completions_request(
            serde_json::to_vec(&request).expect("request").as_slice(),
            &Config::default().request_defaults,
            &models,
        )
        .expect("prepared request");

        assert_eq!(prepared.body["max_output_tokens"], 512);
        assert!(prepared.body.get("max_completion_tokens").is_none());
    }

    #[test]
    fn translate_chat_completions_request_preserves_explicit_max_output_tokens() {
        let models = ModelsConfig::default();
        let request = json!({
            "model": "gpt-5.3-codex",
            "messages": [{ "role": "user", "content": "hi" }],
            "max_output_tokens": 1024
        });

        let prepared = translate_chat_completions_request(
            serde_json::to_vec(&request).expect("request").as_slice(),
            &Config::default().request_defaults,
            &models,
        )
        .expect("prepared request");

        assert_eq!(prepared.body["max_output_tokens"], 1024);
    }

    #[test]
    fn responses_to_chat_completion_adapts_text_and_tool_calls() {
        let response = json!({
            "id": "resp_123",
            "created_at": 12345,
            "model": "gpt-5.3-codex",
            "output": [
                {
                    "type": "message",
                    "content": [
                        { "type": "output_text", "text": "Finished." }
                    ]
                },
                {
                    "type": "function_call",
                    "call_id": "call_9",
                    "name": "write_file",
                    "arguments": "{\"path\":\"a.txt\"}"
                }
            ],
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "total_tokens": 15
            }
        });

        let adapted = responses_to_chat_completion(&response).expect("adapted response");

        assert_eq!(adapted["object"], "chat.completion");
        assert_eq!(adapted["choices"][0]["message"]["role"], "assistant");
        assert_eq!(adapted["choices"][0]["message"]["content"], "Finished.");
        assert_eq!(adapted["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(
            adapted["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            "write_file"
        );
        assert_eq!(adapted["usage"]["total_tokens"], 15);
    }

    #[test]
    fn translate_chat_completions_request_translates_reasoning_effort_to_nested_reasoning() {
        let models = ModelsConfig::default();
        let request = json!({
            "model": "gpt-5.4",
            "messages": [{ "role": "user", "content": "hi" }],
            "reasoning_effort": "high"
        });

        let prepared = translate_chat_completions_request(
            serde_json::to_vec(&request).expect("request").as_slice(),
            &Config::default().request_defaults,
            &models,
        )
        .expect("prepared request");

        assert_eq!(prepared.body["reasoning"]["effort"], "high");
        assert!(prepared.body.get("reasoning_effort").is_none());
    }

    #[test]
    fn translate_chat_completions_request_explicit_reasoning_object_takes_priority_over_reasoning_effort()
     {
        let models = ModelsConfig::default();
        // When both `reasoning` and `reasoning_effort` are present, the explicit
        // `reasoning` object wins and `reasoning_effort` is ignored.
        let request = json!({
            "model": "gpt-5.4",
            "messages": [{ "role": "user", "content": "hi" }],
            "reasoning": { "effort": "low" },
            "reasoning_effort": "high"
        });

        let prepared = translate_chat_completions_request(
            serde_json::to_vec(&request).expect("request").as_slice(),
            &Config::default().request_defaults,
            &models,
        )
        .expect("prepared request");

        assert_eq!(prepared.body["reasoning"]["effort"], "low");
    }

    #[test]
    fn translate_chat_completions_request_rejects_responses_format_payloads() {
        let models = ModelsConfig::default();
        let request = json!({
            "model": "gpt-5.4",
            "input": [{ "type": "message", "role": "user", "content": "hi" }]
        });

        let error = translate_chat_completions_request(
            serde_json::to_vec(&request).expect("request").as_slice(),
            &Config::default().request_defaults,
            &models,
        )
        .expect_err("responses-format payload should be rejected");

        assert!(
            error
                .to_string()
                .contains("send it to `POST /v1/responses` instead")
        );
    }

    #[test]
    fn responses_to_chat_completion_returns_length_finish_reason_when_truncated() {
        let response = json!({
            "id": "resp_trunc",
            "created_at": 0,
            "model": "gpt-5.4",
            "status": "incomplete",
            "incomplete_details": { "reason": "max_output_tokens" },
            "output": [
                { "type": "message", "content": [{ "type": "output_text", "text": "truncated" }] }
            ],
            "usage": { "input_tokens": 5, "output_tokens": 10, "total_tokens": 15 }
        });

        let adapted = responses_to_chat_completion(&response).expect("adapted response");
        assert_eq!(adapted["choices"][0]["finish_reason"], "length");
    }

    #[test]
    fn responses_to_chat_completion_returns_content_filter_finish_reason() {
        let response = json!({
            "id": "resp_filter",
            "created_at": 0,
            "model": "gpt-5.4",
            "status": "incomplete",
            "incomplete_details": { "reason": "content_filter" },
            "output": [],
            "usage": { "input_tokens": 5, "output_tokens": 0, "total_tokens": 5 }
        });

        let adapted = responses_to_chat_completion(&response).expect("adapted response");
        assert_eq!(adapted["choices"][0]["finish_reason"], "content_filter");
    }

    #[test]
    fn responses_to_chat_completion_returns_stop_for_completed_status() {
        let response = json!({
            "id": "resp_done",
            "created_at": 0,
            "model": "gpt-5.4",
            "status": "completed",
            "output": [
                { "type": "message", "content": [{ "type": "output_text", "text": "done" }] }
            ],
            "usage": { "input_tokens": 5, "output_tokens": 3, "total_tokens": 8 }
        });

        let adapted = responses_to_chat_completion(&response).expect("adapted response");
        assert_eq!(adapted["choices"][0]["finish_reason"], "stop");
    }
}
