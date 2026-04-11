# codex-proxy Agent Guide

## Mission

`codex-proxy` authenticates to ChatGPT via OAuth and exposes a local OpenAI-compatible endpoint for coding tools such as Droid.

The intended product shape is:

- authenticate a single user with ChatGPT OAuth
- generate a local fake API key with `openssl`
- expose a local endpoint that tools can treat like a Codex/OpenAI-compatible base URL
- forward those requests through the proxy to OpenAI using the OAuth-backed ChatGPT flow

The TypeScript reference implementation lives in `examples/opencode-openai-codex-auth-main`. This repository is the Rust rewrite.

## Runtime Contract

Preserve these runtime files unless the user explicitly approves a migration:

- `config.toml`
- `data/state.json`
- `data/tokens.json`

Inside Docker, the app runs with `WORKDIR /app`, so the container contract becomes:

- `/app/config.toml`
- `/app/data/state.json`
- `/app/data/tokens.json`

`config.toml` is the runtime source of truth. If it is missing, the daemon creates it on first start. Keep [`config.example.toml`](./config.example.toml) aligned with the generated schema and defaults.

## Current Skeleton

The current repository baseline already includes:

- `codex-proxy start`
- `codex-proxy auth`
- PKCE generation and local OAuth callback capture
- OAuth token exchange and persistence
- stored-token classification for missing, valid, expiring, and expired states
- refresh-token reuse from both `auth` and daemon startup paths
- forwarding for `POST /v1/responses`
- stateless request normalization for `store: false`, removed `item_reference`, and stripped item IDs
- SSE passthrough for streaming requests and SSE-to-JSON conversion for non-streaming requests
- non-streaming compatibility for `POST /v1/chat/completions`
- OpenAI-compatible model discovery via `GET /v1/models` and `GET /v1/models/{model_id}`
- config-driven model advertisement and alias normalization via `[models]` and `[models.aliases]` in `config.toml`
- readiness output at `GET /readyz` plus richer `GET /healthz` state
- structured logs around forwarding, upstream responses, and request-transformation decisions
- fake API key generation through `openssl`
- a local HTTP server with `GET /healthz` and `GET /v1/models`
- multi-stage container packaging with a bundled `/app/config.toml`, non-root runtime user, and a compose example that persists `/app/data`
- container defaults that bind the daemon to `0.0.0.0` while preserving the `/app` runtime contract
- container health checks against `GET /healthz`
- release validation via `scripts/release-check.sh`

Forwarding is currently centered on `POST /v1/responses`, with a non-streaming compatibility adapter for `POST /v1/chat/completions`. Use the phase docs in `/plans` before widening scope.

## Reference Implementation Notes

The TypeScript example is the main behavior reference. Important details to preserve when implementing forwarding:

- ChatGPT OAuth uses `https://auth.openai.com/oauth/authorize` and `https://auth.openai.com/oauth/token`
- the callback path is `/auth/callback`
- the authorize URL includes `id_token_add_organizations=true`, `codex_cli_simplified_flow=true`, and `originator=codex_cli_rs`
- the ChatGPT backend base URL is `https://chatgpt.com/backend-api`
- request forwarding should target the Codex responses path, not the ordinary platform path
- future request transformation must account for `store: false`, stripped item IDs, and filtered `item_reference` entries just like the TypeScript reference

## Working Rules For Future Agents

- Keep the path contract stable unless a migration is part of the task.
- Update `README.md`, `AGENTS.md`, `config.example.toml`, and `/plans` together when changing architecture or runtime behavior.
- Prefer small, testable diffs over broad rewrites.
- Add or update validation steps whenever you add a new phase or materially change acceptance criteria.
- Treat the example TypeScript repo as a reference, not a source to copy mechanically.

## Validation Baseline

Before closing meaningful changes, run:

- `cargo fmt`
- `cargo check`
- `cargo test`

When Docker or packaging changes are involved, also run:

- `docker build -t codex-proxy:latest .`
- `./scripts/release-check.sh`

## Phase Plans

See [`plans/README.md`](./plans/README.md) for the active phase breakdown and acceptance contracts.
