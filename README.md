# codex-proxy

`codex-proxy` is a local Rust daemon that authenticates you with ChatGPT via
OAuth and exposes an OpenAI-compatible API endpoint. Point any coding tool that
speaks the OpenAI API at `http://127.0.0.1:8317` and it will be transparently
proxied to the ChatGPT Codex backend — no real OpenAI API key required.

```
 Coding tool  ──(fake key)──▶  codex-proxy  ──(OAuth token)──▶  ChatGPT Codex
```

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quickstart — local binary](#quickstart--local-binary)
- [Quickstart — Docker Compose](#quickstart--docker-compose)
- [Authentication](#authentication)
- [Connecting a coding tool](#connecting-a-coding-tool)
- [CLI reference](#cli-reference)
- [Configuration](#configuration)
- [HTTP API](#http-api)
- [Known limitations](#known-limitations)
- [Release validation](#release-validation)

---

## Prerequisites

**Local binary**
- Rust 1.75+ (`rustup` recommended)
- `openssl` on `PATH` (used to generate the fake API key)

**Docker**
- Docker Engine 24+ or Docker Desktop

---

## Installation

### Build from source

```bash
git clone <repo-url>
cd codex-proxy
cargo build --release
# Binary is at ./target/release/codex-proxy
```

You can add it to your PATH or run it directly with `cargo run --release --`.

### Docker image

```bash
docker build -t codex-proxy:latest .
```

---

## Quickstart — local binary

### 1. Authenticate

Run the auth flow once. This opens a browser window where you log in to
ChatGPT and grant access. Tokens are saved to `./data/tokens.json`.

```bash
cargo run --release -- auth
```

After successful login the command prints your **fake API key**:

```
Authentication complete.
Use this fake API key with local clients:

sk-codex-proxy-<hex>
```

Copy and save this key — you will use it as the API key in your coding tool.

### 2. Start the daemon

```bash
cargo run --release -- start
```

The proxy now listens on `http://127.0.0.1:8317`.

### 3. Verify

```bash
curl http://127.0.0.1:8317/healthz
```

Look for `"forwarding_ready": true` in the response. If `auth_ready` is
`false`, run `auth` again.

---

## Quickstart — Docker Compose

The included `docker-compose.yml` bundles the default config and persists data
in a named Docker volume so tokens survive container restarts.

### 1. Start the container

```bash
docker compose up --build -d
```

The proxy starts on port `8317` of your host machine.

### 2. Authenticate inside the container

Because the OAuth callback listener runs inside the container, you need to
authenticate via `docker exec` and handle the browser step manually:

```bash
docker exec -it codex-proxy-codex-proxy-1 \
  /app/codex-proxy auth --no-browser
```

> **Note**: the container name is `codex-proxy-codex-proxy-1` with the default
> Compose project name. Run `docker ps` to confirm the exact name.

The command prints an authorization URL:

```
Open this URL to authenticate codex-proxy:

https://auth.openai.com/oauth/authorize?...
```

Open that URL in a browser on your host machine. After you log in and grant
access, ChatGPT redirects to `http://127.0.0.1:1455/auth/callback` — which
must be reachable *inside* the container. To make this work, the callback port
must be published. Add or confirm this in your `docker-compose.yml`:

```yaml
services:
  codex-proxy:
    ports:
      - "127.0.0.1:8317:8317"
      - "127.0.0.1:1455:1455"   # OAuth callback port
```

Then re-run `docker compose up --build -d` before running `auth`.

After the browser redirect completes the command prints the fake API key:

```
Authentication complete.
Use this fake API key with local clients:

sk-codex-proxy-<hex>
```

### 3. Verify

```bash
curl http://127.0.0.1:8317/healthz
```

### 4. Stop

```bash
docker compose down
```

Token data is preserved in the `codex-proxy-data` Docker volume. To reset
everything (including tokens):

```bash
docker compose down -v
```

### Overriding the bundled config

The container image ships with a bundled `/app/config.toml`. Mount your own
to override it:

```yaml
services:
  codex-proxy:
    volumes:
      - ./config.toml:/app/config.toml:ro
      - codex-proxy-data:/app/data

volumes:
  codex-proxy-data:
```

---

## Authentication

`codex-proxy auth` runs the ChatGPT OAuth PKCE flow:

1. Generates a PKCE verifier/challenge pair.
2. Opens `https://auth.openai.com/oauth/authorize?...` in your browser
   (or prints the URL if `--no-browser` is set).
3. Starts a local HTTP server on `127.0.0.1:1455` to receive the callback.
4. Exchanges the authorization code for access + refresh tokens.
5. Saves tokens to `data/tokens.json` and prints the fake API key.

### Token reuse and refresh

`codex-proxy auth` reuses valid stored tokens and silently refreshes expiring
or expired ones before opening a browser. Use `--force` to start a fresh login
regardless:

```bash
codex-proxy auth --force
```

The daemon also attempts a background refresh at startup and during forwarding
when the token is within 5 minutes of expiry.

---

## Connecting a coding tool

Configure your tool with:

| Setting | Value |
|---------|-------|
| Base URL | `http://127.0.0.1:8317` |
| API key | The `sk-codex-proxy-<hex>` key printed by `auth` |

### Example — OpenAI Python SDK

```python
from openai import OpenAI

client = OpenAI(
    api_key="sk-codex-proxy-<your-key>",
    base_url="http://127.0.0.1:8317/v1",
)

response = client.chat.completions.create(
    model="gpt-5.4",
    messages=[{"role": "user", "content": "Hello!"}],
)
print(response.choices[0].message.content)
```

### Example — curl

```bash
curl http://127.0.0.1:8317/v1/responses \
  -H "Authorization: Bearer sk-codex-proxy-<your-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-5.4",
    "input": [{"type": "message", "role": "user", "content": "Hello!"}]
  }'
```

---

## CLI reference

```
codex-proxy [OPTIONS] <COMMAND>

Options:
  --config-path <PATH>   Path to config.toml [default: ./config.toml]
  --data-dir <DIR>       Directory for state.json and tokens.json [default: ./data]

Commands:
  start   Start the local proxy daemon
  auth    Authenticate with ChatGPT OAuth and persist tokens
```

### `start` flags

```
--bind <HOST>   Override the listen host (default: value in config.toml)
--port <PORT>   Override the listen port (default: value in config.toml)
```

### `auth` flags

```
--no-browser          Print the authorization URL instead of opening a browser
--force               Start a new login even when stored tokens are valid
--callback-host <HOST>  Override the OAuth callback host
--callback-port <PORT>  Override the OAuth callback port
```

---

## Configuration

If `config.toml` does not exist, the daemon creates it with defaults on first
start. See [`config.example.toml`](./config.example.toml) for a fully
annotated reference.

### Key settings

```toml
[server]
bind = "127.0.0.1"   # proxy listen host
port = 8317           # proxy listen port
log_filter = "info"   # tracing filter (same syntax as RUST_LOG)

[auth]
callback_host = "127.0.0.1"
callback_port = 1455
open_browser = true
timeout_seconds = 300

[models]
advertised = ["gpt-5.4", "gpt-5.4-mini", "gpt-5.3-codex"]
fallback = "gpt-5.4"

[models.aliases]
"openai/gpt-5.4" = "gpt-5.4"
"openai/gpt-5.4-mini" = "gpt-5.4-mini"
"openai/gpt-5.3-codex" = "gpt-5.3-codex"
```

**Model normalization**: client-sent model names are matched against
`advertised` (case-insensitive), then against `aliases`, then fall back to
`fallback`. This means tools sending `openai/gpt-5.4` or `gpt-5.4` both work.

**Log level**: override at runtime without editing config using:

```bash
RUST_LOG=debug codex-proxy start
```

---

## HTTP API

All endpoints except `/healthz` and `/readyz` require the fake API key:

```
Authorization: Bearer sk-codex-proxy-<hex>
# or
x-api-key: sk-codex-proxy-<hex>
```

| Method | Path | Description |
|--------|------|-------------|
| GET | `/healthz` | Full status JSON (auth, forwarding, token status) |
| GET | `/readyz` | `200 OK` when forwarding is ready, `503` otherwise |
| GET | `/v1/models` | OpenAI-compatible model list |
| GET | `/v1/models/{model_id}` | Single model object or 404 |
| POST | `/v1/responses` | Primary forwarding route (streaming supported) |
| POST | `/v1/chat/completions` | Non-streaming compat adapter |

### Health check response

```json
{
  "status": "ok",
  "forwarding_ready": true,
  "auth_ready": true,
  "token_status": "valid",
  "fake_api_key_configured": true,
  "chat_completions_compatibility": {
    "implemented": true,
    "streaming_supported": false
  }
}
```

---

## Runtime file layout

```
./config.toml          — TOML configuration (auto-created on first run)
./data/state.json      — fake API key and pending OAuth state
./data/tokens.json     — OAuth access and refresh tokens
```

Inside Docker (`WORKDIR /app`):

```
/app/config.toml
/app/data/state.json
/app/data/tokens.json
```

---

## Known limitations

- `POST /v1/chat/completions` does not support streaming. Use `POST /v1/responses`
  for streaming.
- The proxy advertises only the models listed in `config.toml`; it does not
  mirror the full OpenAI model catalog.
- Docker auth requires publishing the OAuth callback port (`1455` by default)
  and using `--no-browser` with `docker exec`.

---

## Release validation

```bash
# Unit and integration tests
cargo test

# Automated release checks (build + health check)
./scripts/release-check.sh

# Manual Docker checks
docker build -t codex-proxy:latest .
docker run --rm -p 127.0.0.1:8317:8317 codex-proxy:latest
curl http://127.0.0.1:8317/healthz

docker compose up --build
curl http://127.0.0.1:8317/healthz
docker compose down -v
```
