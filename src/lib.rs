//! `codex-proxy` — a local OpenAI-compatible proxy backed by ChatGPT OAuth.
//!
//! This crate provides:
//! - A CLI with `start` (runs the proxy daemon) and `auth` (OAuth login) subcommands.
//! - A PKCE-based OAuth flow that stores tokens in `data/tokens.json`.
//! - An Axum HTTP server that authenticates clients with a local fake API key
//!   and forwards requests to the ChatGPT Codex backend.
//! - Request normalization (model canonicalization, `item_reference` filtering,
//!   `store: false`, `reasoning.encrypted_content` injection).
//! - A WebSocket compatibility transport on `GET /v1/responses` for Codex-style
//!   realtime clients.
//! - A non-streaming compatibility adapter for `POST /v1/chat/completions`.

pub mod app;
pub mod auth;
pub mod browser;
pub mod cli;
pub mod commands;
pub mod config;
pub mod constants;
pub mod paths;
pub mod proxy;
pub mod state;
pub mod storage;
pub mod time;

pub use app::run;
