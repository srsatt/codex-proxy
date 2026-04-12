# Streaming in `codex-proxy`

`codex-proxy` supports streaming on the client-facing API with these rules:

- `POST /v1/responses`
  - If the client sends `stream: true`, the proxy forwards the request upstream
    and pipes the SSE stream back to the client.
  - If the client sends `stream: false`, the proxy still forwards upstream in
    streaming mode and then collapses the final `response.done` or
    `response.completed` event into a single JSON response.

- `POST /v1/chat/completions`
  - Non-streaming compatibility is supported.
  - Streaming is not supported and returns `501 Not Implemented`.

Authentication split:

- Local clients authenticate to the proxy with the fake proxy API key.
- The proxy authenticates upstream to the ChatGPT Codex backend with OAuth.
