use std::ops::ControlFlow;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use super::protocol::{Request, Response};

pub async fn execute(
    request: &Request,
    paths: &super::AgentPaths,
    mut on_response: impl FnMut(Response) -> ControlFlow<()>,
) -> Result<()> {
    let mut stream = connect(paths).await?;

    let mut buf = serde_json::to_vec(request)?;
    buf.push(b'\n');
    stream.write_all(&buf).await?;
    stream.flush().await?;

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

        // An error reply is the agent reporting the request failed, so it
        // becomes this call's error rather than something to hand onward.
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

pub async fn ping(paths: &super::AgentPaths) -> Result<()> {
    let response = tokio::time::timeout(Duration::from_millis(500), request(&Request::Ping, paths))
        .await
        .map_err(|_| anyhow::anyhow!("agent ping timed out"))??;
    match response {
        Response::Pong => Ok(()),
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::net::UnixListener;

    use super::*;

    static NEXT_SOCKET: AtomicUsize = AtomicUsize::new(0);

    struct SocketGuard(std::path::PathBuf);

    impl Drop for SocketGuard {
        fn drop(&mut self) {
            std::fs::remove_file(&self.0).ok();
        }
    }

    fn test_paths() -> super::super::AgentPaths {
        let id = NEXT_SOCKET.fetch_add(1, Ordering::Relaxed);
        let socket = std::env::temp_dir().join(format!(
            "btwattch2-client-test-{}-{id}.sock",
            std::process::id()
        ));
        super::super::paths_from_socket(socket)
    }

    async fn run_ping_test(response: Option<Response>) -> Result<()> {
        let paths = test_paths();
        let _socket = SocketGuard(paths.socket.clone());
        let listener = UnixListener::bind(&paths.socket)?;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = String::new();
            BufReader::new(&mut stream)
                .read_line(&mut request)
                .await
                .unwrap();
            if let Some(response) = response {
                let mut buf = serde_json::to_vec(&response).unwrap();
                buf.push(b'\n');
                stream.write_all(&buf).await.ok();
            }
        });

        let result = ping(&paths).await;
        server.await.unwrap();
        result
    }

    #[tokio::test]
    async fn ping_requires_pong() {
        assert!(run_ping_test(Some(Response::Pong)).await.is_ok());
        assert!(run_ping_test(Some(Response::Ok)).await.is_err());
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
