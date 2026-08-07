use std::ops::ControlFlow;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;

use super::protocol::{self, Request, Response};

pub async fn execute(
    request: &Request,
    paths: &super::AgentPaths,
    mut on_response: impl FnMut(Response) -> ControlFlow<()>,
) -> Result<()> {
    let mut stream = connect(paths).await?;
    protocol::write_message(&mut stream, request).await?;

    let (reader, _writer) = stream.split();
    let mut lines = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let n = lines.read_line(&mut line).await?;
        if n == 0 {
            bail!("agent closed the connection before completing the response");
        }

        let resp: Response =
            serde_json::from_str(line.trim()).context("failed to parse agent response")?;

        // The agent reporting the request failed, so it becomes this call's
        // error rather than something to hand onward.
        if let Response::Error { message } = resp {
            bail!(message);
        }

        let is_terminal = matches!(resp, Response::StreamEnd);
        if on_response(resp).is_break() || is_terminal {
            break;
        }
    }

    Ok(())
}

pub async fn request(request: &Request, paths: &super::AgentPaths) -> Result<Response> {
    let mut response = None;
    execute(request, paths, |resp| {
        response = Some(resp);
        ControlFlow::Break(())
    })
    .await?;
    response.context("agent returned no response")
}

/// Ping the agent, returning what it reports about itself. A `None` address or
/// status means the agent answered but did not send that field.
pub async fn ping(paths: &super::AgentPaths) -> Result<super::DaemonInfo> {
    let response = tokio::time::timeout(Duration::from_millis(500), request(&Request::Ping, paths))
        .await
        .map_err(|_| anyhow::anyhow!("agent ping timed out"))??;
    match response {
        Response::Pong { addr, status } => Ok(super::DaemonInfo {
            addr: addr
                .as_deref()
                .map(str::parse)
                .transpose()
                .context("agent reported an unparsable address")?,
            status,
        }),
        _ => bail!("agent returned an unexpected ping response"),
    }
}

pub async fn send_shutdown(paths: &super::AgentPaths) -> Result<()> {
    match request(&Request::Shutdown, paths).await? {
        Response::Ok => Ok(()),
        _ => bail!("agent returned an unexpected shutdown response"),
    }
}

async fn connect(paths: &super::AgentPaths) -> Result<UnixStream> {
    let path = &paths.socket;
    UnixStream::connect(path)
        .await
        .with_context(|| format!("failed to connect to agent at {}", path.display()))
}

#[cfg(test)]
mod tests {
    // Only for the raw-bytes test below; everything else frames through
    // `protocol::write_message`.
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixListener;

    use super::super::protocol::AgentStatus;
    use super::super::testutil::TempPath;
    use super::*;

    /// A socket path plus the guard that removes it; both must stay alive for
    /// the duration of the test.
    fn test_paths() -> (super::super::AgentPaths, TempPath) {
        let temp = TempPath::new(".sock");
        (
            super::super::paths_from_socket(temp.path().to_path_buf()),
            temp,
        )
    }

    async fn run_ping_test(response: Option<Response>) -> Result<super::super::DaemonInfo> {
        let (paths, _temp) = test_paths();
        let listener = UnixListener::bind(&paths.socket)?;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = String::new();
            BufReader::new(&mut stream)
                .read_line(&mut request)
                .await
                .unwrap();
            if let Some(response) = response {
                protocol::write_message(&mut stream, &response).await.ok();
            }
        });

        let result = ping(&paths).await;
        server.await.unwrap();
        result
    }

    #[tokio::test]
    async fn ping_requires_pong() {
        let pong = Response::Pong {
            addr: None,
            status: None,
        };
        assert!(run_ping_test(Some(pong)).await.is_ok());
        assert!(run_ping_test(Some(Response::Ok)).await.is_err());
    }

    #[tokio::test]
    async fn ping_reports_the_agent_address_and_status() {
        let addr = "CB:DF:6B:12:34:56";
        let response = Response::Pong {
            addr: Some(addr.to_string()),
            status: Some(AgentStatus {
                samples: 42,
                ..AgentStatus::default()
            }),
        };
        let info = run_ping_test(Some(response)).await.unwrap();
        assert_eq!(info.addr, Some(addr.parse().unwrap()));
        assert_eq!(info.status.unwrap().samples, 42);
    }

    #[tokio::test]
    async fn ping_rejects_an_unparsable_address() {
        let response = Response::Pong {
            addr: Some("not-an-address".to_string()),
            status: None,
        };
        assert!(run_ping_test(Some(response)).await.is_err());
    }

    /// A ping reply from an agent that predates the `addr` field must still
    /// parse, rather than making the agent look absent.
    #[tokio::test]
    async fn ping_accepts_a_reply_without_an_address() {
        let (paths, _temp) = test_paths();
        let listener = UnixListener::bind(&paths.socket).unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = String::new();
            BufReader::new(&mut stream)
                .read_line(&mut request)
                .await
                .unwrap();
            stream.write_all(b"{\"type\":\"pong\"}\n").await.ok();
        });

        let info = ping(&paths).await.unwrap();
        assert_eq!(info.addr, None);
        assert!(info.status.is_none());
    }

    #[tokio::test]
    async fn ping_rejects_eof() {
        assert!(run_ping_test(None).await.is_err());
    }

    #[tokio::test]
    async fn ping_rejects_error_response() {
        let result = run_ping_test(Some(Response::Error {
            message: "actor failed".to_string(),
        }))
        .await;
        assert_eq!(result.unwrap_err().to_string(), "actor failed");
    }
}
