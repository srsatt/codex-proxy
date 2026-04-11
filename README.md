# codex-proxy

`codex-proxy` is a Rust daemon and CLI that authenticates a user with ChatGPT OAuth, generates a local fake API key, and exposes an OpenAI-compatible local endpoint that coding tools can target instead of a direct Codex API key.

The TypeScript reference implementation lives in [`examples/opencode-openai-codex-auth-main`](./examples/opencode-openai-codex-auth-main). This repository is the Rust rewrite. Phase 0 is complete, Phases 1 and 2 are implemented, Phase 3 is in progress, and Phase 4 packaging is implemented.

## Current Status

The skeleton now provides:

- A real Rust CLI with `codex-proxy start` and `codex-proxy auth`.
- A config and persistence contract for `config.toml`, `data/state.json`, and `data/tokens.json`.
- Fake API key generation via `openssl`.
- A real local OAuth callback listener and token exchange path for `codex-proxy auth`.
- Stored-token lifecycle handling for missing, valid, expiring, and expired credentials.
- Refresh-token reuse during `codex-proxy auth` and daemon startup.
- Request forwarding for `POST /v1/responses` to the ChatGPT Codex backend.
- Stateless request normalization for `store: false`, stripped item IDs, and filtered `item_reference` entries.
- Streaming passthrough for streaming requests and SSE-to-JSON conversion for non-streaming requests.
- A non-streaming compatibility adapter for `POST /v1/chat/completions`.
- Readiness and health endpoints at `GET /healthz` and `GET /readyz`.
- Structured forwarding logs around auth state, upstream requests, response status, and failures.
- `GET /v1/models` and `GET /v1/models/{model_id}` for local model negotiation.
- A multi-stage `Dockerfile` and `docker-compose.yml` example.

## Runtime Layout

By default the binary uses relative paths:

- `./config.toml`
- `./data/state.json`
- `./data/tokens.json`

Inside Docker the working directory is `/app`, so those same relative paths resolve to:

- `/app/config.toml`
- `/app/data/state.json`
- `/app/data/tokens.json`

That matches the repository contract described in `AGENTS.md`.

If `config.toml` does not exist yet, `codex-proxy` writes one on first startup using the built-in schema and defaults mirrored by [`config.example.toml`](./config.example.toml). The container image also ships with a bundled `/app/config.toml` built from that same example file, so `docker run` and `docker compose up` work without a source checkout inside the container. The image default command overrides the bind host to `0.0.0.0` so published Docker ports are reachable from the host machine.

## Commands

Start the local daemon:

```bash
cargo run -- start
```

Authenticate against ChatGPT OAuth:

```bash
cargo run -- auth
```

Useful flags:

- `--config-path <PATH>` overrides `config.toml`
- `--data-dir <DIR>` overrides `data/`
- `start --bind <HOST> --port <PORT>` overrides the server listen address
- `auth --callback-host <HOST> --callback-port <PORT> --no-browser --force` controls the OAuth callback listener and re-login behavior

The fake API key is stored in `state.json` and can be supplied to local clients through either `Authorization: Bearer ...` or `x-api-key`.

`codex-proxy auth` now reuses valid stored tokens and attempts refresh for expiring or expired ones before opening a browser. Use `--force` to start a new OAuth login anyway.

Compatibility notes:

- `POST /v1/responses` is the primary supported route.
- `POST /v1/chat/completions` is supported for non-streaming requests by adapting chat-completions payloads to the responses backend.
- Forwarding logs include model normalization and request-transformation details such as stripped item IDs and filtered `item_reference` entries.
- Streaming chat-completions compatibility is not implemented yet; use `/v1/responses` for streaming.

## Configuration

The example configuration is in [`config.example.toml`](./config.example.toml). The defaults intentionally mirror the TypeScript reference:

- OAuth authorize URL: `https://auth.openai.com/oauth/authorize`
- OAuth token URL: `https://auth.openai.com/oauth/token`
- ChatGPT backend base URL: `https://chatgpt.com/backend-api`
- OAuth callback default: `http://127.0.0.1:1455/auth/callback`
- Proxy listen default: `127.0.0.1:8317`
- Advertised models default to `gpt-5.4`, `gpt-5.4-mini`, and `gpt-5.3-codex`

Model advertisement and model normalization are runtime-configurable through `config.toml`:

- `[models].advertised` controls what `GET /v1/models` returns
- `[models].fallback` controls which model unknown inputs normalize to
- `[models.aliases]` maps client-sent model IDs such as provider-prefixed names to canonical upstream IDs

## Project Layout

- [`src/cli.rs`](./src/cli.rs) defines the user-facing CLI.
- [`src/config.rs`](./src/config.rs) owns the TOML config contract and defaults.
- [`src/state.rs`](./src/state.rs) persists fake API key state and OAuth tokens.
- [`src/auth`](./src/auth) contains PKCE/OAuth flow helpers and the local callback server.
- [`src/proxy`](./src/proxy) contains the local API surface, forwarding logic, and auth gate.
- [`plans`](./plans) contains phased implementation contracts for follow-on work.

## Docker

Build the local image:

```bash
docker build -t codex-proxy:latest .
```

Run the daemon directly from the image:

```bash
docker run --rm -p 127.0.0.1:8317:8317 codex-proxy:latest
```

The image includes a container health check against `GET /healthz`.

Run with Compose:

```bash
docker compose up --build
```

The committed compose example uses the bundled `/app/config.toml` from the image and persists `/app/data` in a named Docker volume. That makes `docker compose up --build` work from a fresh clone without requiring a host-side `config.toml`.

If you want to override the bundled config, mount your own file at `/app/config.toml`:

```yaml
services:
  codex-proxy:
    volumes:
      - ./config.toml:/app/config.toml:ro
```

## Known Limitations

- `POST /v1/chat/completions` only supports non-streaming compatibility right now.
- Docker auth flows are less convenient than local auth because OAuth callback handling and browser launch happen inside the container. For containerized auth, prefer explicit port mapping for the callback port and `codex-proxy auth --no-browser`.
- The proxy advertises the model surface configured in `config.toml`; it does not dynamically mirror the full OpenAI Platform model catalog.

## Release Validation

Automated validation helper:

```bash
./scripts/release-check.sh
```

Manual checklist:

- `cargo test`
- `docker build -t codex-proxy:latest .`
- `docker run --rm -p 127.0.0.1:8317:8317 codex-proxy:latest`
- Verify the container reports `healthy` or `http://127.0.0.1:8317/healthz` responds when using a local Docker daemon
- `docker compose up --build`
- Verify the compose service reports `healthy` or `http://127.0.0.1:8317/healthz` responds when using a local Docker daemon
- `docker compose down -v`

## Roadmap

The detailed implementation plan is in [`plans/README.md`](./plans/README.md). The next major milestones are:

- complete manual client smoke validation for Phase 3 compatibility work
- extend compatibility coverage, especially streaming chat-completions behavior if required by target clients
- broader request-shape parity with the TypeScript reference, including any prompt or instruction gaps
