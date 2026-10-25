//! Incremental Responses SSE to Chat Completions SSE adapter.
use super::{
    chat_completions::responses_to_chat_completion,
    forwarding::{UpstreamExchange, UpstreamExchangeBody},
};
use axum::{
    body::{Body, Bytes},
    response::Response,
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    time::Duration,
};

const MAX_LINE: usize = 4 * 1024 * 1024;

struct Adapter {
    metadata: Value,
    started: bool,
    text: BTreeSet<(u64, u64)>,
    tools: BTreeMap<u64, usize>,
    arguments: BTreeSet<u64>,
    usage: bool,
    done: bool,
}
impl Adapter {
    fn new(usage: bool) -> Self {
        Self {
            metadata: json!({"id":format!("chatcmpl-{}",uuid::Uuid::new_v4()),"created":crate::time::unix_timestamp_secs(),"model":"unknown"}),
            started: false,
            text: BTreeSet::new(),
            tools: BTreeMap::new(),
            arguments: BTreeSet::new(),
            usage,
            done: false,
        }
    }
    fn chunk(&self, delta: Value, finish: Value) -> Value {
        let mut value = self.metadata.clone();
        value["object"] = json!("chat.completion.chunk");
        value["choices"] = json!([{"index":0,"delta":delta,"finish_reason":finish}]);
        if self.usage {
            value["usage"] = Value::Null;
        }
        value
    }
    fn event(&mut self, event: Value) -> Result<Vec<Value>, ()> {
        let kind = event["type"].as_str().ok_or(())?;
        if self.text.len() + self.tools.len() > 1024 {
            return Err(());
        }
        if matches!(kind, "error" | "response.failed") {
            return Err(());
        }
        let mut out = Vec::new();
        if !self.started {
            if let Some(response) = event.get("response") {
                for (target, source) in
                    [("id", "id"), ("created", "created_at"), ("model", "model")]
                {
                    if let Some(value) = response.get(source) {
                        self.metadata[target] = value.clone();
                    }
                }
            }
            self.started = true;
            out.push(self.chunk(json!({"role":"assistant","content":""}), Value::Null));
        }
        let index = event["output_index"].as_u64().unwrap_or(0);
        match kind {
            "response.output_text.delta" => {
                let delta = event["delta"].as_str().ok_or(())?;
                self.text
                    .insert((index, event["content_index"].as_u64().unwrap_or(0)));
                out.push(self.chunk(json!({"content":delta}), Value::Null));
            }
            "response.output_item.added" if event["item"]["type"] == "function_call" => {
                let item = &event["item"];
                let tool_index = self.tools.len();
                if self.tools.insert(index, tool_index).is_some() {
                    return Err(());
                }
                let arguments = item["arguments"].as_str().unwrap_or("");
                if !arguments.is_empty() {
                    self.arguments.insert(index);
                }
                out.push(self.chunk(json!({"tool_calls":[{"index":tool_index,"id":item["call_id"],"type":"function","function":{"name":item["name"],"arguments":arguments}}]}),Value::Null));
            }
            "response.function_call_arguments.delta" => {
                let tool_index = *self.tools.get(&index).ok_or(())?;
                let delta = event["delta"].as_str().ok_or(())?;
                self.arguments.insert(index);
                out.push(self.chunk(
                    json!({"tool_calls":[{"index":tool_index,"function":{"arguments":delta}}]}),
                    Value::Null,
                ));
            }
            "response.done" | "response.completed" | "response.incomplete" => {
                let response = &event["response"];
                if matches!(response["status"].as_str(), Some("failed" | "cancelled")) {
                    return Err(());
                }
                let completion = responses_to_chat_completion(response).map_err(|_| ())?;
                // Some backends provide output only in the terminal event.
                for (i, item) in response["output"].as_array().ok_or(())?.iter().enumerate() {
                    let i = i as u64;
                    if item["type"] == "message" {
                        if let Some(parts) = item["content"].as_array() {
                            for (part_index, part) in parts.iter().enumerate() {
                                if matches!(part["type"].as_str(), Some("output_text" | "text"))
                                    && !self.text.contains(&(i, part_index as u64))
                                    && let Some(text) = part["text"].as_str()
                                {
                                    out.push(self.chunk(json!({"content":text}), Value::Null));
                                }
                            }
                        }
                    } else if item["type"] == "function_call" {
                        if !self.tools.contains_key(&i) {
                            let tool_index = self.tools.len();
                            self.tools.insert(i, tool_index);
                            out.push(self.chunk(json!({"tool_calls":[{"index":tool_index,"id":item["call_id"],"type":"function","function":{"name":item["name"],"arguments":item["arguments"].as_str().unwrap_or("")}}]}),Value::Null));
                        } else if !self.arguments.contains(&i) {
                            out.push(self.chunk(json!({"tool_calls":[{"index":self.tools[&i],"function":{"arguments":item["arguments"].as_str().unwrap_or("")}}]}),Value::Null));
                        }
                    }
                }
                out.push(self.chunk(json!({}), completion["choices"][0]["finish_reason"].clone()));
                if self.usage {
                    let mut usage = self.chunk(json!({}), Value::Null);
                    usage["choices"] = json!([]);
                    usage["usage"] = completion["usage"].clone();
                    out.push(usage);
                }
                self.done = true;
            }
            _ => {}
        }
        Ok(out)
    }
}
fn frame(value: &Value) -> Bytes {
    Bytes::from(format!("data: {value}\n\n"))
}
fn failure() -> Bytes {
    frame(
        &json!({"error":{"message":"Upstream chat stream failed or ended before completion","type":"proxy_error","code":"upstream_stream_error"}}),
    )
}

pub(super) fn response(upstream: UpstreamExchange, usage: bool, heartbeat: u64) -> Response {
    let UpstreamExchangeBody::Live(response) = upstream.body else {
        return super::forwarding::error_response_with_request_id(
            axum::http::StatusCode::BAD_GATEWAY,
            "Unexpected buffered upstream response",
            &upstream.request_id,
        );
    };
    let body = async_stream::stream! {
        let mut source=response.bytes_stream();
        let mut adapter=Adapter::new(usage);
        let mut line=Vec::new();
        let mut data=String::new();
        let mut ticker=tokio::time::interval(Duration::from_secs(heartbeat.max(1)));
        ticker.tick().await;
        'read: loop {
            let next=tokio::select! {
                value=source.next()=>value,
                _=ticker.tick(), if heartbeat>0 => { yield Ok::<Bytes,Infallible>(Bytes::from_static(b": keep-alive\n\n")); continue; }
            };
            let Some(Ok(bytes))=next else { yield Ok(failure()); break; };
            for byte in bytes {
                if byte!=b'\n' {
                    line.push(byte);
                    if line.len()+data.len()>MAX_LINE { yield Ok(failure()); break 'read; }
                    continue;
                }
                if line.last()==Some(&b'\r') {line.pop();}
                let Ok(text)=std::str::from_utf8(&line) else {yield Ok(failure()); break 'read;};
                if text.is_empty() && !data.is_empty() {
                    let payload=std::mem::take(&mut data);
                    let event=serde_json::from_str::<Value>(payload.trim_end_matches('\n'));
                    let Ok(chunks)=event.map_err(|_|()).and_then(|e|adapter.event(e)) else {yield Ok(failure()); break 'read;};
                    for chunk in chunks {yield Ok(frame(&chunk));}
                    if adapter.done {yield Ok(Bytes::from_static(b"data: [DONE]\n\n")); break 'read;}
                } else if let Some(value)=text.strip_prefix("data:") {
                    data.push_str(value.strip_prefix(' ').unwrap_or(value));data.push('\n');
                }
                line.clear();
            }
        }
    };
    let mut builder = Response::builder()
        .status(upstream.status)
        .header("content-type", "text/event-stream; charset=utf-8")
        .header("cache-control", "no-cache")
        .header("x-accel-buffering", "no")
        .header("x-codex-proxy-request-id", upstream.request_id);
    if let Some(id) = upstream.upstream_request_id {
        builder = builder.header("x-codex-proxy-upstream-request-id", id);
    }
    builder
        .body(Body::from_stream(body))
        .expect("valid streaming response headers")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn text_tools_usage_and_terminal_output_do_not_duplicate() {
        let mut a = Adapter::new(true);
        let created=a.event(json!({"type":"response.created","response":{"id":"resp_test","created_at":1,"model":"gpt-6-luna"}})).unwrap();
        assert_eq!(created[0]["choices"][0]["delta"]["role"], "assistant");
        assert!(
            a.event(json!({"type":"response.reasoning_text.delta","delta":"private"}))
                .unwrap()
                .is_empty()
        );
        let text=a.event(json!({"type":"response.output_text.delta","output_index":1,"content_index":0,"delta":"Привет"})).unwrap();
        assert_eq!(text[0]["choices"][0]["delta"]["content"], "Привет");
        let tool=a.event(json!({"type":"response.output_item.added","output_index":2,"item":{"type":"function_call","call_id":"call1","name":"light","arguments":""}})).unwrap();
        assert_eq!(tool[0]["choices"][0]["delta"]["tool_calls"][0]["index"], 0);
        a.event(
            json!({"type":"response.function_call_arguments.delta","output_index":2,"delta":"{}"}),
        )
        .unwrap();
        let chunks=a.event(json!({"type":"response.completed","response":{"id":"resp_test","status":"completed","output":[{"type":"reasoning"},{"type":"message","content":[{"type":"output_text","text":"Привет"}]},{"type":"function_call","call_id":"call1","name":"light","arguments":"{}"}],"usage":{"input_tokens":3,"output_tokens":4,"total_tokens":7}}})).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0]["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(chunks[1]["choices"], json!([]));
        assert_eq!(chunks[1]["usage"]["total_tokens"], 7);
        assert!(a.done);
    }
    #[test]
    fn final_only_incomplete_and_errors() {
        let mut a = Adapter::new(false);
        let chunks=a.event(json!({"type":"response.done","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"message","content":[{"type":"output_text","text":"Hello"}]}]}})).unwrap();
        assert_eq!(chunks[1]["choices"][0]["delta"]["content"], "Hello");
        assert_eq!(chunks[2]["choices"][0]["finish_reason"], "length");
        assert!(chunks[0].get("usage").is_none());
        assert!(
            Adapter::new(false)
                .event(json!({"type":"response.failed","response":{"error":{"message":"secret"}}}))
                .is_err()
        );
        assert!(!std::str::from_utf8(&failure()).unwrap().contains("secret"));
    }
}
