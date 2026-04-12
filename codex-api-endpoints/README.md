# `codex-api-endpoints`

This directory is reference material for the OpenAI-compatible HTTP surface
that `codex-proxy` exposes to local tools.

Important limits:

- These files are endpoint reference notes, not a promise that every listed
  endpoint is implemented by the proxy.
- The proxy currently serves:
  - `GET /healthz`
  - `GET /readyz`
  - `GET /v1/models`
  - `GET /v1/models/{model_id}`
  - `POST /v1/responses`
  - `POST /v1/chat/completions`
- Local clients authenticate with the fake proxy API key.
- Upstream authentication is separate and uses ChatGPT OAuth.

Treat the request and response schemas here as client-facing compatibility
guidance, not as a full implementation matrix.
