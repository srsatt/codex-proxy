pub fn open_browser_url(url: &str) -> bool {
    webbrowser::open(url).is_ok()
}
