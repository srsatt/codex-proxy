//! Lightweight time utility used throughout the codebase.
//!
//! Everything is expressed in Unix seconds so it can be compared directly
//! against the `expires_at` field stored in [`crate::state::TokenStore`].

use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the current time as whole seconds since the Unix epoch.
///
/// Falls back to `0` on the (practically impossible) case where
/// `SystemTime::now()` is earlier than `UNIX_EPOCH`.
pub fn unix_timestamp_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
