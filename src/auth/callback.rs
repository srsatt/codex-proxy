//! Local OAuth callback HTTP server.
//!
//! Spins up a temporary Axum server on `host:port` that listens for a single
//! `GET /auth/callback?code=...&state=...` redirect from the OAuth provider,
//! validates the `state` parameter for CSRF protection, and delivers the
//! authorization code to the caller via a oneshot channel.
//!
//! The server shuts itself down after the code is received or the timeout
//! elapses.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::Html,
    routing::get,
};
use serde::Deserialize;
use tokio::{net::TcpListener, sync::oneshot, time::timeout};

use crate::constants::{CALLBACK_PATH, OAUTH_SUCCESS_HTML};

/// Shared state injected into the Axum callback handler.
#[derive(Clone)]
struct CallbackAppState {
    /// The `state` value generated during authorization URL construction.
    /// Used to verify CSRF protection on the callback.
    expected_state: String,
    /// Oneshot sender wrapped in a `Mutex<Option<...>>` so it can be taken
    /// exactly once even when the handler is called concurrently.
    code_tx: Arc<Mutex<Option<oneshot::Sender<String>>>>,
}

/// Query parameters extracted from the OAuth callback redirect.
#[derive(Debug, Deserialize)]
struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
}

/// Bind a local HTTP server on `host:port`, wait for the OAuth callback, and
/// return the authorization code.
///
/// The server shuts down gracefully after the code is delivered. If no
/// callback is received within `timeout_window`, an error is returned and the
/// server is dropped.
pub async fn wait_for_authorization_code(
    host: &str,
    port: u16,
    expected_state: &str,
    timeout_window: Duration,
) -> Result<String> {
    let listener = TcpListener::bind(format!("{host}:{port}"))
        .await
        .with_context(|| format!("failed to bind OAuth callback listener on {host}:{port}"))?;
    let (code_tx, code_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let app = Router::new()
        .route(CALLBACK_PATH, get(handle_callback))
        .with_state(CallbackAppState {
            expected_state: expected_state.to_owned(),
            code_tx: Arc::new(Mutex::new(Some(code_tx))),
        });

    let server_task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|error| anyhow!("OAuth callback server failed: {error}"))
    });

    let code = timeout(timeout_window, code_rx)
        .await
        .with_context(|| {
            format!(
                "timed out waiting for OAuth callback after {:?}",
                timeout_window
            )
        })?
        .context("callback listener closed before a code was received")?;

    let _ = shutdown_tx.send(());
    server_task
        .await
        .context("failed to join callback server task")??;

    Ok(code)
}

/// Axum route handler for `GET /auth/callback`.
///
/// Validates `state` (CSRF check), extracts `code`, and delivers it through
/// the oneshot channel. Returns the success HTML page on success so the user
/// sees a confirmation message in their browser.
async fn handle_callback(
    State(state): State<CallbackAppState>,
    Query(query): Query<CallbackQuery>,
) -> Result<Html<&'static str>, (StatusCode, String)> {
    let Some(returned_state) = query.state else {
        return Err((
            StatusCode::BAD_REQUEST,
            "Missing state parameter".to_owned(),
        ));
    };

    if returned_state != state.expected_state {
        return Err((StatusCode::BAD_REQUEST, "State mismatch".to_owned()));
    }

    let Some(code) = query.code else {
        return Err((
            StatusCode::BAD_REQUEST,
            "Missing authorization code".to_owned(),
        ));
    };

    let mut sender = state.code_tx.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Callback lock poisoned".to_owned(),
        )
    })?;

    let code_tx = match sender.take() {
        Some(code_tx) => code_tx,
        None => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Authorization code already consumed".to_owned(),
            ));
        }
    };

    code_tx.send(code).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to deliver authorization code".to_owned(),
        )
    })?;

    Ok(Html(OAUTH_SUCCESS_HTML))
}
