# Authentication in `codex-proxy`

`codex-proxy` uses two different authentication layers:

1. Local client -> proxy

Clients talk to `codex-proxy` using an OpenAI-compatible API shape and present
the locally generated fake API key:

- `Authorization: Bearer sk-codex-proxy-...`
- or `x-api-key: sk-codex-proxy-...`

2. Proxy -> upstream ChatGPT backend

The proxy itself does not forward that fake key upstream. It authenticates to
the ChatGPT Codex backend with OAuth tokens obtained through `codex-proxy auth`
and then sends:

- `Authorization: Bearer <oauth access token>`
- `chatgpt-account-id: <decoded account id>`

## What is shared vs different

- The client-facing request and response shapes are intentionally OpenAI-style,
  primarily around `/v1/responses` and `/v1/chat/completions`.
- The upstream transport is not the public OpenAI Platform API. It is the
  ChatGPT-backed Codex endpoint authenticated with OAuth.
- In practice, the upstream request body is very similar to the public
  Responses API, which is why the proxy can stay thin, but the two auth models
  are not the same thing.

## Scope note

The files under `codex-api-endpoints/` are reference material for client-facing
OpenAI-compatible payloads. They are not a claim that `codex-proxy` implements
every endpoint in that directory.
