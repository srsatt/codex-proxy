//! Thin wrapper around the [`webbrowser`] crate for opening URLs in the
//! system's default browser during the OAuth flow.

/// Attempt to open `url` in the system browser.
///
/// Returns `true` if the OS reported success, `false` otherwise.
/// Failures are non-fatal: the caller prints the URL so the user can open it
/// manually.
pub fn open_browser_url(url: &str) -> bool {
    webbrowser::open(url).is_ok()
}
