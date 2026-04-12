use anyhow::{Context, Result, anyhow, bail};
use axum::{
    body::Bytes,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use serde_json::{Map, Value, json};

use crate::config::ModelsConfig;

use super::forwarding::{
    ForwardingState, PreparedRequest, error_response, map_upstream_error, prepare_request_body,
    send_prepared_request,
};

pub async fn forward_chat_completions(
    state: &ForwardingState,
    request_headers: &HeaderMap,
    request_body: Bytes,
) -> Response {
    let prepared_request =
        match translate_chat_completions_request(&request_body, &state.config.models) {
            Ok(prepared_request) => prepared_request,
            Err(error) => return error_response(StatusCode::BAD_REQUEST, error.to_string()),
        };

    if prepared_request.original_stream {
        return error_response(
            StatusCode::NOT_IMPLEMENTED,
            "streaming `/v1/chat/completions` compatibility is not implemented yet; use `/v1/responses` for streaming requests",
        );
    }

    let upstream_response = match send_prepared_request(
        state,
        request_headers,
        &prepared_request,
        "chat_completions",
    )
    .await
    {
        Ok(response) => response,
        Err(error) => return error_response(StatusCode::BAD_GATEWAY, error.to_string()),
    };

    if !upstream_response.status().is_success() {
        return map_upstream_error(upstream_response).await;
    }

    match adapt_chat_completions_response(upstream_response).await {
        Ok(response) => response,
        Err(error) => error_response(StatusCode::BAD_GATEWAY, error.to_string()),
    }
}

pub fn translate_chat_completions_request(
    body: &[u8],
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
    let messages = object
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("chat completions request is missing `messages`"))?;
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

    copy_field(object, &mut responses_body, "tools");
    copy_field(object, &mut responses_body, "tool_choice");
    copy_field(object, &mut responses_body, "reasoning");
    copy_field(object, &mut responses_body, "include");
    copy_field(object, &mut responses_body, "prompt_cache_key");
    copy_field(object, &mut responses_body, "text");

    if let Some(v) = object
        .get("max_completion_tokens")
        .or_else(|| object.get("max_tokens"))
    {
        responses_body.insert("max_output_tokens".to_owned(), v.clone());
    }

    prepare_request_body(
        serde_json::to_vec(&Value::Object(responses_body))?.as_slice(),
        models,
    )
}

async fn adapt_chat_completions_response(response: reqwest::Response) -> Result<Response> {
    let status = response.status();
    let response_json: Value = response
        .json()
        .await
        .context("failed to parse responses API JSON for chat completions compatibility")?;
    let chat_completion = responses_to_chat_completion(&response_json)?;

    axum::http::Response::builder()
        .status(status)
        .header("content-type", "application/json; charset=utf-8")
        .body(axum::body::Body::from(serde_json::to_vec(
            &chat_completion,
        )?))
        .context("failed to build chat completions compatibility response")
}

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
    let finish_reason = if tool_calls.is_empty() {
        "stop"
    } else {
        "tool_calls"
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

fn extract_message_content(content: Option<&Value>) -> Result<String> {
    extract_optional_message_content(content)?
        .ok_or_else(|| anyhow!("chat completion message content must not be empty"))
}

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

fn copy_field(source: &Map<String, Value>, target: &mut Map<String, Value>, key: &str) {
    if let Some(value) = source.get(key) {
        target.insert(key.to_owned(), value.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn translate_chat_completions_request_remaps_max_completion_tokens() {
        let models = ModelsConfig::default();
        let request = json!({
            "model": "gpt-5.3-codex",
            "messages": [{ "role": "user", "content": "hi" }],
            "max_completion_tokens": 512
        });

        let prepared = translate_chat_completions_request(
            serde_json::to_vec(&request).expect("request").as_slice(),
            &models,
        )
        .expect("prepared request");

        assert_eq!(prepared.body["max_output_tokens"], 512);
        assert!(prepared.body.get("max_completion_tokens").is_none());
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
}
