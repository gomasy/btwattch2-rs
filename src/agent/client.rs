use std::ops::ControlFlow;
use std::time::Duration;

use anyhow::{Context, Result};
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
            break;
        }

        let resp: Response =
            serde_json::from_str(line.trim()).context("failed to parse agent response")?;

        let is_terminal = matches!(resp, Response::StreamEnd | Response::Error { .. });
        if on_response(resp).is_break() || is_terminal {
            break;
        }
    }

    Ok(())
}

pub async fn ping(paths: &super::AgentPaths) -> Result<()> {
    tokio::time::timeout(Duration::from_millis(500), {
        execute(&Request::Ping, paths, |_| ControlFlow::Break(()))
    })
    .await
    .map_err(|_| anyhow::anyhow!("agent ping timed out"))?
}

pub async fn send_shutdown(paths: &super::AgentPaths) -> Result<()> {
    execute(&Request::Shutdown, paths, |_| ControlFlow::Break(())).await
}

async fn connect(paths: &super::AgentPaths) -> Result<UnixStream> {
    let path = &paths.socket;
    UnixStream::connect(path)
        .await
        .with_context(|| format!("failed to connect to agent at {}", path.display()))
}
