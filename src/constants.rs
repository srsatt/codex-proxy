//! Compile-time constants shared across the codebase.

/// URL path that the local OAuth callback server listens on.
/// Must match the `redirect_uri` registered with the OAuth provider.
pub const CALLBACK_PATH: &str = "/auth/callback";

/// Prefix applied to every generated fake API key so they are visually
/// distinguishable from real OpenAI keys (e.g. `sk-codex-proxy-<hex>`).
pub const FAKE_API_KEY_PREFIX: &str = "sk-codex-proxy-";

/// How many seconds before token expiry the daemon considers a token
/// "expiring" and proactively attempts a refresh.
pub const TOKEN_REFRESH_WINDOW_SECONDS: i64 = 300;

/// Static HTML page returned to the browser after a successful OAuth callback.
/// Instructs the user to return to the terminal.
pub const OAUTH_SUCCESS_HTML: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>codex-proxy Authentication</title>
    <style>
      :root {
        color-scheme: light;
        font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }
      body {
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        background: linear-gradient(180deg, #eef6ff 0%, #ffffff 100%);
        color: #10203a;
      }
      main {
        max-width: 34rem;
        padding: 2rem;
        background: rgba(255, 255, 255, 0.92);
        border: 1px solid rgba(16, 32, 58, 0.12);
        border-radius: 1rem;
        box-shadow: 0 20px 60px rgba(16, 32, 58, 0.12);
      }
      h1 {
        margin-top: 0;
      }
      p {
        line-height: 1.6;
      }
      code {
        background: rgba(16, 32, 58, 0.08);
        padding: 0.15rem 0.35rem;
        border-radius: 0.35rem;
      }
    </style>
  </head>
  <body>
    <main>
      <h1>Authentication complete</h1>
      <p>You can return to <code>codex-proxy</code>. The local callback was received successfully.</p>
    </main>
  </body>
</html>
"#;
