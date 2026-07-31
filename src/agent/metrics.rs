use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use super::status::AgentStats;
use crate::output;

/// Namespace for the exposed metrics. Fixed rather than configurable: telling
/// two agents apart is what a scrape target's own labels are for.
const METRIC_PREFIX: &str = "btwattch2";
/// The whole exchange, from the first byte of the request to the last of the
/// response. Bounds a client that connects and then says nothing.
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(10);
/// Cap on the request head we will read. Nothing in a scrape needs more, and
/// the limit is what keeps a client from making us buffer without end.
const MAX_REQUEST_BYTES: u64 = 8 * 1024;
/// The exposition format's content type, as scrapers expect it.
const EXPOSITION_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
/// The content type of every other reply: the index and the error bodies.
const TEXT_TYPE: &str = "text/plain; charset=utf-8";

/// Claim the metrics port. Separate from `serve` so a port already in use fails
/// `agent start` outright, rather than after the tens of seconds a BLE connect
/// can take — by which time the operator has stopped watching.
pub async fn bind(addr: SocketAddr) -> Result<TcpListener> {
    TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind the metrics endpoint to {addr}"))
}

/// Serve `/metrics` until the task is dropped. Each connection is answered from
/// `stats`, so a scrape never waits on the BLE link.
pub async fn serve(listener: TcpListener, stats: Arc<AgentStats>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let stats = Arc::clone(&stats);
                tokio::spawn(async move {
                    // A per-connection failure is the client's problem, not the
                    // agent's: a scraper that hangs up mid-response is routine.
                    let _ = tokio::time::timeout(EXCHANGE_TIMEOUT, handle(stream, &stats)).await;
                });
            }
            Err(e) => {
                eprintln!("[WARN] Metrics accept failed: {e}");
                tokio::time::sleep(super::ACCEPT_BACKOFF).await;
            }
        }
    }
}

/// What a request line asks for, once it is understood.
#[derive(Debug, PartialEq, Eq)]
enum Route {
    Metrics,
    Index,
    NotFound,
    MethodNotAllowed,
}

/// Route a request line such as `GET /metrics?x=1 HTTP/1.1`. `None` means it was
/// not a request line at all.
fn route(line: &str) -> Option<Route> {
    let mut parts = line.split_whitespace();
    let method = parts.next()?;
    let target = parts.next()?;

    if method != "GET" {
        return Some(Route::MethodNotAllowed);
    }
    // A scraper may append a query string; the path is what selects the route.
    let path = target.split(['?', '#']).next().unwrap_or(target);
    Some(match path {
        "/metrics" => Route::Metrics,
        "/" => Route::Index,
        _ => Route::NotFound,
    })
}

async fn handle(stream: TcpStream, stats: &AgentStats) {
    let (reader, mut writer) = stream.into_split();
    // The bound applies to the request head as a whole, so neither a single
    // enormous line nor an endless run of small ones can grow the buffer.
    let mut reader = BufReader::new(reader.take(MAX_REQUEST_BYTES));

    let mut line = String::new();
    if reader.read_line(&mut line).await.is_err() || line.is_empty() {
        return;
    }

    let response = match route(&line) {
        Some(Route::Metrics) => {
            let reading = stats.reading();
            let body = output::metrics_exposition(
                METRIC_PREFIX,
                reading.measurement.as_ref(),
                reading.fresh,
            );
            response("200 OK", EXPOSITION_TYPE, "", &body)
        }
        Some(Route::Index) => response(
            "200 OK",
            TEXT_TYPE,
            "",
            "btwattch2 agent\nmetrics: /metrics\n",
        ),
        Some(Route::MethodNotAllowed) => {
            response("405 Method Not Allowed", TEXT_TYPE, "Allow: GET\r\n", "")
        }
        Some(Route::NotFound) | None => response("404 Not Found", TEXT_TYPE, "", ""),
    };

    // Drain the rest of the head before replying. A client still writing when
    // its socket is closed sees the reset rather than the response, which turns
    // a plain 404 into a connection error.
    let mut rest = String::new();
    while let Ok(n) = reader.read_line(&mut rest).await {
        if n == 0 || rest.trim_end().is_empty() {
            break;
        }
        rest.clear();
    }

    writer.write_all(response.as_bytes()).await.ok();
    writer.flush().await.ok();
}

/// Build a complete HTTP/1.1 response. Every reply closes the connection:
/// keeping it alive would buy nothing for a scrape that arrives once an
/// interval, and costs the state machine that goes with it.
fn response(status: &str, content_type: &str, extra_headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         {extra_headers}\
         \r\n\
         {body}",
        body.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::testutil::measurement;

    #[test]
    fn routes_are_read_off_the_request_line() {
        for (line, expected) in [
            ("GET /metrics HTTP/1.1", Route::Metrics),
            ("GET /metrics?collect=all HTTP/1.1", Route::Metrics),
            ("GET / HTTP/1.1", Route::Index),
            ("GET /nope HTTP/1.1", Route::NotFound),
            ("POST /metrics HTTP/1.1", Route::MethodNotAllowed),
            ("DELETE / HTTP/1.1", Route::MethodNotAllowed),
        ] {
            assert_eq!(route(line), Some(expected), "routing {line:?}");
        }
        assert_eq!(route("garbage"), None);
        assert_eq!(route(""), None);
    }

    /// Start a server on an ephemeral port and return where to reach it.
    async fn serve_stats(stats: Arc<AgentStats>) -> SocketAddr {
        let listener = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, stats));
        addr
    }

    /// Send `request` verbatim and read the whole reply.
    async fn get(addr: SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn a_scrape_reports_the_latest_measurement() {
        let stats = Arc::new(AgentStats::new("1s".parse().unwrap(), None));
        stats.record_sample(&measurement(123.5));
        let addr = serve_stats(Arc::clone(&stats)).await;

        let response = get(addr, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        assert!(response.contains(EXPOSITION_TYPE), "{response}");
        assert!(response.contains("btwattch2_wattage 123.5\n"), "{response}");
        assert!(response.contains("btwattch2_up 1\n"), "{response}");

        // Content-Length has to match, or a scraper reads a truncated body.
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        let declared: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(declared, body.len());
    }

    /// A running agent that has read nothing must say so rather than serve a
    /// blank 200 that looks like a healthy zero-watt load.
    #[tokio::test]
    async fn a_scrape_before_any_measurement_reports_down() {
        let stats = Arc::new(AgentStats::new("1s".parse().unwrap(), None));
        let addr = serve_stats(stats).await;

        let response = get(addr, "GET /metrics HTTP/1.1\r\n\r\n").await;
        assert!(response.contains("btwattch2_up 0\n"), "{response}");
        assert!(!response.contains("btwattch2_wattage"), "{response}");
    }

    #[tokio::test]
    async fn other_requests_are_refused_in_kind() {
        let stats = Arc::new(AgentStats::new("1s".parse().unwrap(), None));
        let addr = serve_stats(stats).await;

        let response = get(addr, "GET /favicon.ico HTTP/1.1\r\n\r\n").await;
        assert!(
            response.starts_with("HTTP/1.1 404 Not Found\r\n"),
            "{response}"
        );

        // `route` covers the methods themselves; what matters over the wire is
        // that a rejected one still gets a well-formed reply.
        let response = get(addr, "DELETE /metrics HTTP/1.1\r\n\r\n").await;
        assert!(
            response.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"),
            "{response}"
        );
        assert!(response.contains("\r\nAllow: GET\r\n"), "{response}");

        // A human who opens the port in a browser gets a pointer, not a 404.
        let response = get(addr, "GET / HTTP/1.1\r\n\r\n").await;
        assert!(response.contains("/metrics"), "{response}");
    }

    /// An oversized head must not be buffered without end; the connection is
    /// answered or dropped, but the agent stays up either way.
    #[tokio::test]
    async fn an_oversized_request_is_bounded() {
        let stats = Arc::new(AgentStats::new("1s".parse().unwrap(), None));
        let addr = serve_stats(Arc::clone(&stats)).await;

        let padding = "X".repeat(MAX_REQUEST_BYTES as usize * 2);
        let request = format!("GET /metrics HTTP/1.1\r\nX-Pad: {padding}\r\n\r\n");
        get(addr, &request).await;

        // Still serving afterwards.
        let response = get(addr, "GET /metrics HTTP/1.1\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    }
}
