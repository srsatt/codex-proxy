use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::State,
    http::{HeaderMap, Request, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use codex_proxy::{
    config::Config,
    paths::AppPaths,
    proxy::{ProxyAppState, router},
    state::{TokenStore, load_tokens},
    time::unix_timestamp_secs,
};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tower::util::ServiceExt;

#[derive(Clone)]
struct MockUpstreamState {
    responses_requests: Arc<Mutex<Vec<RecordedRequest>>>,
    token_requests: Arc<Mutex<Vec<String>>>,
    responses_status: StatusCode,
    responses_content_type: String,
    responses_body: String,
    token_response: Option<Value>,
}

const MOCK_UPSTREAM_REQUEST_ID: &str = "upstream-test-request-id";

#[derive(Debug, Clone)]
struct RecordedRequest {
    headers: BTreeMap<String, String>,
    body: Value,
}

impl RecordedRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }
}

struct MockUpstream {
    addr: SocketAddr,
    responses_requests: Arc<Mutex<Vec<RecordedRequest>>>,
    token_requests: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl Drop for MockUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl MockUpstream {
    async fn spawn(
        responses_status: StatusCode,
        responses_content_type: &str,
        responses_body: impl Into<String>,
        token_response: Option<Value>,
    ) -> Self {
        let state = MockUpstreamState {
            responses_requests: Arc::new(Mutex::new(Vec::new())),
            token_requests: Arc::new(Mutex::new(Vec::new())),
            responses_status,
            responses_content_type: responses_content_type.to_owned(),
            responses_body: responses_body.into(),
            token_response,
        };

        let responses_requests = Arc::clone(&state.responses_requests);
        let token_requests = Arc::clone(&state.token_requests);

        let app = Router::new()
            .route("/backend/codex/responses", post(mock_responses_handler))
            .route("/oauth/token", post(mock_token_handler))
            .with_state(state);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let addr = listener.local_addr().expect("addr");
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock upstream server");
        });

        Self {
            addr,
            responses_requests,
            token_requests,
            task,
        }
    }
}

async fn mock_responses_handler(
    State(state): State<MockUpstreamState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body: Value = serde_json::from_slice(&body).expect("responses request json");
    let headers = headers
        .iter()
        .filter_map(|(name, value)| {
            Some((
                name.as_str().to_ascii_lowercase(),
                value.to_str().ok()?.to_owned(),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    state
        .responses_requests
        .lock()
        .await
        .push(RecordedRequest { headers, body });

    Response::builder()
        .status(state.responses_status)
        .header(header::CONTENT_TYPE, state.responses_content_type.as_str())
        .header("x-request-id", MOCK_UPSTREAM_REQUEST_ID)
        .body(Body::from(state.responses_body.clone()))
        .expect("responses response")
}

async fn mock_token_handler(State(state): State<MockUpstreamState>, body: Bytes) -> Response {
    let form_body = String::from_utf8(body.to_vec()).expect("token form body");
    state.token_requests.lock().await.push(form_body);

    let (status, body) = match state.token_response.as_ref() {
        Some(token_response) => (StatusCode::OK, token_response.clone()),
        None => (
            StatusCode::NOT_FOUND,
            json!({
                "error": "token endpoint not configured for this test"
            }),
        ),
    };

    (status, axum::Json(body)).into_response()
}

fn test_paths(temp: &TempDir) -> AppPaths {
    AppPaths::new(temp.path().join("config.toml"), temp.path().join("data"))
}

fn make_config(mock_upstream: &MockUpstream) -> Config {
    let mut config = Config::default();
    config.upstream.responses_base_url = format!("http://{}/backend", mock_upstream.addr);
    config.upstream.token_url = format!("http://{}/oauth/token", mock_upstream.addr);
    config.request_defaults.reasoning_effort = Some("high".to_owned());
    config.request_defaults.reasoning_summary = Some("auto".to_owned());
    config.request_defaults.text_verbosity = Some("low".to_owned());
    config
        .request_defaults
        .include
        .push("message.output_text.logprobs".to_owned());
    config
}

fn make_proxy_app(
    config: Config,
    paths: AppPaths,
    fake_api_key: &str,
    tokens: Option<TokenStore>,
) -> Router {
    router(ProxyAppState::new(
        config,
        paths,
        fake_api_key.to_owned(),
        tokens,
    ))
}

fn make_token(account_id: &str, refresh_token: &str, expires_at: i64) -> TokenStore {
    TokenStore {
        access_token: make_access_token(account_id),
        refresh_token: refresh_token.to_owned(),
        expires_at,
        token_type: "Bearer".to_owned(),
        scope: "openid profile email offline_access".to_owned(),
        account_id: Some(account_id.to_owned()),
        obtained_at: unix_timestamp_secs(),
    }
}

fn make_access_token(account_id: &str) -> String {
    let payload = json!({
        "https://api.openai.com/auth": {
            "chatgpt_account_id": account_id
        }
    });

    format!(
        "header.{}.signature",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).expect("jwt payload"))
    )
}

async fn read_json(response: Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response bytes");
    serde_json::from_slice(&bytes).expect("json response")
}

async fn read_text(response: Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response bytes");
    String::from_utf8(bytes.to_vec()).expect("utf8 response")
}

fn get_request(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

fn auth_get_request(uri: &str, fake_api_key: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {fake_api_key}"))
        .body(Body::empty())
        .expect("request")
}

fn auth_json_request(uri: &str, fake_api_key: &str, body: &Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {fake_api_key}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(body).expect("request body")))
        .expect("request")
}

#[tokio::test]
async fn models_routes_require_fake_api_key_and_return_expected_payloads() {
    let temp = tempdir().expect("tempdir");
    let paths = test_paths(&temp);
    paths.ensure_runtime_layout().expect("runtime layout");
    let app = make_proxy_app(
        Config::default(),
        paths,
        "sk-codex-proxy-test",
        Some(make_token(
            "account-1",
            "refresh-1",
            unix_timestamp_secs() + 3600,
        )),
    );

    let unauthorized = app
        .clone()
        .oneshot(get_request("/v1/models"))
        .await
        .expect("response");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let models = app
        .clone()
        .oneshot(auth_get_request("/v1/models", "sk-codex-proxy-test"))
        .await
        .expect("response");
    assert_eq!(models.status(), StatusCode::OK);
    let payload = read_json(models).await;
    assert_eq!(payload["object"], "list");
    assert_eq!(payload["data"][0]["id"], "gpt-5.4");

    let missing_model = app
        .oneshot(auth_get_request(
            "/v1/models/not-a-real-model",
            "sk-codex-proxy-test",
        ))
        .await
        .expect("response");
    assert_eq!(missing_model.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn responses_route_forwards_normalized_request_and_collapses_sse() {
    let upstream = MockUpstream::spawn(
        StatusCode::OK,
        "text/event-stream; charset=utf-8",
        "data: {\"type\":\"response.done\",\"response\":{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5.3-codex\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"done\"}]}]}}\n",
        None,
    )
    .await;

    let temp = tempdir().expect("tempdir");
    let paths = test_paths(&temp);
    paths.ensure_runtime_layout().expect("runtime layout");
    let config = make_config(&upstream);
    let app = make_proxy_app(
        config,
        paths,
        "sk-codex-proxy-test",
        Some(make_token(
            "account-123",
            "refresh-123",
            unix_timestamp_secs() + 3600,
        )),
    );

    let request_body = json!({
        "model": "openai/gpt-5-codex",
        "prompt_cache_key": "session-123",
        "stream": false,
        "include": ["reasoning.encrypted_content"],
        "input": [
            {
                "type": "message",
                "id": "msg_1",
                "role": "user",
                "content": [{ "type": "input_text", "text": "hello" }]
            },
            { "type": "item_reference", "id": "ref_1" },
            {
                "type": "function_call",
                "id": "call_msg_1",
                "call_id": "call_1",
                "name": "write_file",
                "arguments": "{}"
            },
            {
                "type": "function_call_output",
                "id": "output_1",
                "call_id": "call_1",
                "output": "ok"
            },
            {
                "type": "function_call_output",
                "call_id": "call_orphan",
                "output": "stale"
            }
        ]
    });

    let response = app
        .oneshot(auth_json_request(
            "/v1/responses",
            "sk-codex-proxy-test",
            &request_body,
        ))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("x-codex-proxy-request-id"));
    assert_eq!(
        response
            .headers()
            .get("x-codex-proxy-upstream-request-id")
            .and_then(|value| value.to_str().ok()),
        Some(MOCK_UPSTREAM_REQUEST_ID)
    );
    let payload = read_json(response).await;
    assert_eq!(payload["id"], "resp_123");

    let recorded = upstream.responses_requests.lock().await;
    assert_eq!(recorded.len(), 1);
    let forwarded = &recorded[0];
    assert_eq!(forwarded.body["model"], "gpt-5.3-codex");
    assert_eq!(forwarded.body["store"], false);
    assert_eq!(forwarded.body["stream"], true);
    assert_eq!(forwarded.body["reasoning"]["effort"], "high");
    assert_eq!(forwarded.body["reasoning"]["summary"], "auto");
    assert_eq!(forwarded.body["text"]["verbosity"], "low");
    assert_eq!(
        forwarded.body["include"],
        json!([
            "reasoning.encrypted_content",
            "message.output_text.logprobs"
        ])
    );
    assert_eq!(forwarded.body["input"].as_array().expect("input").len(), 3);
    assert!(
        forwarded.body["input"]
            .as_array()
            .expect("input")
            .iter()
            .all(|item| item.get("id").is_none())
    );
    let expected_access_token = make_access_token("account-123");
    assert_eq!(
        forwarded.header("authorization"),
        Some(&format!("Bearer {expected_access_token}")[..])
    );
    assert_eq!(forwarded.header("chatgpt-account-id"), Some("account-123"));
    assert_eq!(
        forwarded.header("openai-beta"),
        Some("responses=experimental")
    );
    assert_eq!(forwarded.header("originator"), Some("codex_cli_rs"));
    assert_eq!(forwarded.header("conversation_id"), Some("session-123"));
    assert_eq!(forwarded.header("session_id"), Some("session-123"));
    assert!(forwarded.header("x-client-request-id").is_some());
}

#[tokio::test]
async fn responses_route_refreshes_expired_tokens_before_forwarding() {
    let refreshed_access_token = make_access_token("account-new");
    let upstream = MockUpstream::spawn(
        StatusCode::OK,
        "text/event-stream; charset=utf-8",
        "data: {\"type\":\"response.done\",\"response\":{\"id\":\"resp_refresh\",\"object\":\"response\"}}\n",
        Some(json!({
            "access_token": refreshed_access_token,
            "refresh_token": "refresh-new",
            "expires_in": 3600,
            "token_type": "Bearer",
            "scope": "openid profile email offline_access"
        })),
    )
    .await;

    let temp = tempdir().expect("tempdir");
    let paths = test_paths(&temp);
    paths.ensure_runtime_layout().expect("runtime layout");
    let app = make_proxy_app(
        make_config(&upstream),
        paths.clone(),
        "sk-codex-proxy-test",
        Some(make_token(
            "account-old",
            "refresh-old",
            unix_timestamp_secs() - 60,
        )),
    );

    let response = app
        .oneshot(auth_json_request(
            "/v1/responses",
            "sk-codex-proxy-test",
            &json!({
                "model": "gpt-5.4",
                "input": [{ "type": "message", "role": "user", "content": "hello" }]
            }),
        ))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("x-codex-proxy-request-id"));
    assert_eq!(
        response
            .headers()
            .get("x-codex-proxy-upstream-request-id")
            .and_then(|value| value.to_str().ok()),
        Some(MOCK_UPSTREAM_REQUEST_ID)
    );
    assert_eq!(upstream.token_requests.lock().await.len(), 1);

    let recorded = upstream.responses_requests.lock().await;
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].header("authorization"),
        Some(&format!("Bearer {refreshed_access_token}")[..])
    );
    assert_eq!(
        recorded[0].header("chatgpt-account-id"),
        Some("account-new")
    );
    assert!(recorded[0].header("x-client-request-id").is_some());

    let stored = load_tokens(&paths)
        .expect("load tokens")
        .expect("refreshed tokens saved");
    assert_eq!(stored.refresh_token, "refresh-new");
    assert_eq!(stored.account_id.as_deref(), Some("account-new"));
}

#[tokio::test]
async fn responses_route_maps_usage_limit_404_to_429() {
    let upstream = MockUpstream::spawn(
        StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        "usage_limit_reached",
        None,
    )
    .await;

    let temp = tempdir().expect("tempdir");
    let paths = test_paths(&temp);
    paths.ensure_runtime_layout().expect("runtime layout");
    let app = make_proxy_app(
        make_config(&upstream),
        paths,
        "sk-codex-proxy-test",
        Some(make_token(
            "account-123",
            "refresh-123",
            unix_timestamp_secs() + 3600,
        )),
    );

    let response = app
        .oneshot(auth_json_request(
            "/v1/responses",
            "sk-codex-proxy-test",
            &json!({
                "model": "gpt-5.4",
                "input": [{ "type": "message", "role": "user", "content": "hello" }]
            }),
        ))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(response.headers().contains_key("x-codex-proxy-request-id"));
    assert_eq!(
        response
            .headers()
            .get("x-codex-proxy-upstream-request-id")
            .and_then(|value| value.to_str().ok()),
        Some(MOCK_UPSTREAM_REQUEST_ID)
    );
    assert_eq!(read_text(response).await, "usage_limit_reached");
}

#[tokio::test]
async fn chat_completions_route_converts_upstream_sse_to_chat_completion_json() {
    let upstream = MockUpstream::spawn(
        StatusCode::OK,
        "text/event-stream; charset=utf-8",
        "data: {\"type\":\"response.done\",\"response\":{\"id\":\"resp_chat\",\"object\":\"response\",\"created_at\":12345,\"model\":\"gpt-5.3-codex\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Finished.\"}]}],\"usage\":{\"input_tokens\":10,\"output_tokens\":5,\"total_tokens\":15}}}\n",
        None,
    )
    .await;

    let temp = tempdir().expect("tempdir");
    let paths = test_paths(&temp);
    paths.ensure_runtime_layout().expect("runtime layout");
    let app = make_proxy_app(
        make_config(&upstream),
        paths,
        "sk-codex-proxy-test",
        Some(make_token(
            "account-123",
            "refresh-123",
            unix_timestamp_secs() + 3600,
        )),
    );

    let response = app
        .oneshot(auth_json_request(
            "/v1/chat/completions",
            "sk-codex-proxy-test",
            &json!({
                "model": "openai/gpt-5-codex",
                "messages": [
                    { "role": "user", "content": "hello" }
                ],
                "max_tokens": 64
            }),
        ))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("x-codex-proxy-request-id"));
    assert_eq!(
        response
            .headers()
            .get("x-codex-proxy-upstream-request-id")
            .and_then(|value| value.to_str().ok()),
        Some(MOCK_UPSTREAM_REQUEST_ID)
    );
    let payload = read_json(response).await;
    assert_eq!(payload["object"], "chat.completion");
    assert_eq!(payload["choices"][0]["message"]["content"], "Finished.");
    assert_eq!(payload["usage"]["total_tokens"], 15);

    let recorded = upstream.responses_requests.lock().await;
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].body["model"], "gpt-5.3-codex");
    assert_eq!(recorded[0].body["stream"], true);
    assert_eq!(recorded[0].body["max_output_tokens"], 64);
    assert_eq!(recorded[0].body["input"][0]["role"], "user");
    assert!(recorded[0].header("x-client-request-id").is_some());
}

#[tokio::test]
async fn chat_completions_route_rejects_streaming_requests() {
    let temp = tempdir().expect("tempdir");
    let paths = test_paths(&temp);
    paths.ensure_runtime_layout().expect("runtime layout");
    let app = make_proxy_app(Config::default(), paths, "sk-codex-proxy-test", None);

    let response = app
        .oneshot(auth_json_request(
            "/v1/chat/completions",
            "sk-codex-proxy-test",
            &json!({
                "model": "gpt-5.4",
                "stream": true,
                "messages": [
                    { "role": "user", "content": "hello" }
                ]
            }),
        ))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    assert!(response.headers().contains_key("x-codex-proxy-request-id"));
    let payload = read_json(response).await;
    assert_eq!(payload["error"]["type"], "proxy_error");
}
