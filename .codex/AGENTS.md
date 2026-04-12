# codex-proxy — Dev / Codex Agent Guide

This file is **not tracked by git** and is only visible to Codex agents and
developers working in this local checkout. It supplements the generic
`AGENTS.md` at the repository root with context about temporary artefacts and
ongoing work.

---

## Extra Directories (Not Part of the Codebase)

These directories exist in the working tree but are intentionally excluded from
the repository's permanent documentation:

| Directory | Purpose |
|-----------|---------|
| `examples/opencode-openai-codex-auth-main/` | TypeScript reference implementation. Use it as a behaviour reference when implementing forwarding details, but do not copy code mechanically. |
| `plans/` | Phased implementation contracts. Read these before widening scope. See `plans/README.md` for the active phase breakdown and acceptance criteria. |
| `codex-api/` | Scratch / experimental area. Not part of the main build. |
| `.claude/` | Claude Code skills and project memory. Not part of the production codebase. |
| `.codex/` | This directory. Codex agent configuration and local-only docs. |

---

## Reference Implementation Notes (`examples/`)

The TypeScript example at `examples/opencode-openai-codex-auth-main` is the
primary behaviour reference for the Rust rewrite. Key details to preserve:

- OAuth authorize URL: `https://auth.openai.com/oauth/authorize`
- OAuth token URL: `https://auth.openai.com/oauth/token`
- Callback path: `/auth/callback`
- Authorize URL query params include: `id_token_add_organizations=true`,
  `codex_cli_simplified_flow=true`, `originator=codex_cli_rs`
- ChatGPT backend base URL: `https://chatgpt.com/backend-api`
- Codex responses path: `/codex/responses`
- Requests must set `store: false`, strip `item_reference`, strip item `id`
  fields, and include `reasoning.encrypted_content`

---

## Implementation Phases (`plans/`)

Current status (as of last update):

| Phase | File | Status |
|-------|------|--------|
| 0 — Skeleton & Contracts | `plans/phase-00-skeleton-and-contracts.md` | Complete |
| 1 — OAuth Lifecycle | `plans/phase-01-oauth-lifecycle.md` | Complete |
| 2 — Request Forwarding | `plans/phase-02-request-forwarding.md` | Complete |
| 3 — Client Compat & Observability | `plans/phase-03-client-compatibility-and-observability.md` | In progress |
| 4 — Packaging & Release | `plans/phase-04-packaging-and-release.md` | Complete |

Read the active phase doc before starting new work. Each phase file contains
acceptance criteria that must all pass before the phase is closed.

---

## Validation Baseline

```bash
cargo fmt
cargo check
cargo test
docker build -t codex-proxy:latest .
./scripts/release-check.sh
```

---

## Runtime Contract (same as AGENTS.md)

```
./config.toml
./data/state.json
./data/tokens.json
```

Docker: `/app/config.toml`, `/app/data/state.json`, `/app/data/tokens.json`.
