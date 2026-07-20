use std::path::Path;

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, mpsc};

use super::protocol::{Request, Response};
use crate::cli::ConnectionConfig;
use crate::connection::{self, Connection, FrameAssembler, Notifications};
use crate::payload;

const MEASUREMENT_FRAME_MIN_LEN: usize = 29;

struct ActorCommand {
    request: Request,
    tx: mpsc::UnboundedSender<Response>,
}

enum FrameKind {
    Command,
    Measurement,
}

pub async fn run(config: &ConnectionConfig) -> Result<()> {
    let sock = super::socket_path();
    let pid_file = super::pid_path();

    cleanup_stale(&sock, &pid_file)?;
    let listener =
        UnixListener::bind(&sock).with_context(|| format!("failed to bind {}", sock.display()))?;
    std::fs::write(&pid_file, std::process::id().to_string()).ok();

    eprintln!("[INFO] Agent listening on {}", sock.display());

    let conn = Connection::new(config).await?;
    eprintln!("[INFO] Connected to device");

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ActorCommand>();
    let shutdown = std::sync::Arc::new(Notify::new());

    let actor = tokio::spawn(actor_loop(conn, cmd_rx, shutdown.clone()));

    let result = accept_loop(listener, cmd_tx.clone(), &shutdown).await;

    drop(cmd_tx);
    actor.await.ok();
    cleanup_files(&sock, &pid_file);
    result
}

fn cleanup_stale(sock: &Path, pid_file: &Path) -> Result<()> {
    if let Ok(pid_str) = std::fs::read_to_string(pid_file)
        && let Ok(pid) = pid_str.trim().parse::<u32>()
        && Path::new(&format!("/proc/{pid}")).exists()
    {
        bail!("agent is already running (pid {pid})");
    }
    std::fs::remove_file(sock).ok();
    std::fs::remove_file(pid_file).ok();
    Ok(())
}

fn cleanup_files(sock: &Path, pid_file: &Path) {
    std::fs::remove_file(sock).ok();
    std::fs::remove_file(pid_file).ok();
    eprintln!("[INFO] Agent stopped");
}

async fn accept_loop(
    listener: UnixListener,
    cmd_tx: mpsc::UnboundedSender<ActorCommand>,
    shutdown: &Notify,
) -> Result<()> {
    let sigterm = async {
        #[cfg(unix)]
        {
            let mut sig = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to register SIGTERM handler");
            sig.recv().await;
        }
        #[cfg(not(unix))]
        futures::future::pending::<()>().await;
    };

    tokio::select! {
        _ = async {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let tx = cmd_tx.clone();
                        tokio::spawn(handle_client(stream, tx));
                    }
                    Err(e) => {
                        eprintln!("[WARN] Accept failed: {e}");
                    }
                }
            }
        } => {}
        _ = tokio::signal::ctrl_c() => {
            eprintln!("\n[INFO] Received SIGINT, shutting down...");
        }
        _ = sigterm => {
            eprintln!("[INFO] Received SIGTERM, shutting down...");
        }
        _ = shutdown.notified() => {
            eprintln!("[INFO] Shutdown requested, shutting down...");
        }
    }
    Ok(())
}

async fn handle_client(stream: UnixStream, cmd_tx: mpsc::UnboundedSender<ActorCommand>) {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let line = match lines.next_line().await {
        Ok(Some(line)) => line,
        _ => return,
    };

    let request: Request = match serde_json::from_str(&line) {
        Ok(r) => r,
        Err(e) => {
            let resp = Response::Error {
                message: format!("invalid request: {e}"),
            };
            send_response(&mut writer, &resp).await.ok();
            return;
        }
    };

    let (resp_tx, mut resp_rx) = mpsc::unbounded_channel::<Response>();
    let cmd = ActorCommand {
        request,
        tx: resp_tx,
    };

    if cmd_tx.send(cmd).is_err() {
        let resp = Response::Error {
            message: "agent shutting down".to_string(),
        };
        send_response(&mut writer, &resp).await.ok();
        return;
    }

    while let Some(resp) = resp_rx.recv().await {
        if send_response(&mut writer, &resp).await.is_err() {
            break;
        }
        if !matches!(resp, Response::Measurement { .. }) {
            break;
        }
    }
}

async fn send_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    resp: &Response,
) -> Result<()> {
    let mut buf = serde_json::to_vec(resp)?;
    buf.push(b'\n');
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

async fn actor_loop(
    mut conn: Connection,
    mut cmd_rx: mpsc::UnboundedReceiver<ActorCommand>,
    shutdown: std::sync::Arc<Notify>,
) {
    let mut streaming_client: Option<mpsc::UnboundedSender<Response>> = None;
    let mut notifications: Option<Notifications> = None;
    let mut assembler = FrameAssembler::new();
    let mut ticker = tokio::time::interval(conn.interval());
    let monitoring_payload = payload::monitoring();

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break };
                let should_stop = handle_actor_command(
                    &mut conn,
                    cmd,
                    &mut streaming_client,
                    &mut notifications,
                    &mut assembler,
                    &mut ticker,
                ).await;
                if should_stop {
                    shutdown.notify_one();
                    break;
                }
            }

            _ = ticker.tick(), if streaming_client.is_some() => {
                match conn.write(&monitoring_payload).await {
                    Ok(reconnected) => {
                        if reconnected {
                            match conn.listen().await {
                                Ok(n) => {
                                    notifications = Some(n);
                                    assembler.clear();
                                }
                                Err(e) => {
                                    eprintln!("[ERR] Re-listen failed: {e}");
                                    end_stream(&mut streaming_client);
                                    notifications = None;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[ERR] Write failed: {e}");
                        end_stream(&mut streaming_client);
                        notifications = None;
                    }
                }
            }

            event = async {
                match notifications.as_mut() {
                    Some(n) => n.next().await,
                    None => futures::future::pending().await,
                }
            } => {
                let Some(event) = event else {
                    eprintln!("[WARN] Notification stream closed, reconnecting...");
                    match conn.connect().await {
                        Ok(()) => match conn.listen().await {
                            Ok(n) => {
                                notifications = Some(n);
                                assembler.clear();
                            }
                            Err(e) => {
                                eprintln!("[ERR] Re-listen failed: {e}");
                                end_stream(&mut streaming_client);
                                notifications = None;
                            }
                        },
                        Err(e) => {
                            eprintln!("[ERR] Reconnect failed: {e}");
                            end_stream(&mut streaming_client);
                            notifications = None;
                        }
                    }
                    continue;
                };

                if event.uuid != connection::C_RX {
                    continue;
                }

                if let Some(frame) = assembler.feed(&event.value)
                    && let Some(ref tx) = streaming_client
                {
                    match connection::read_measure(&frame) {
                        Ok(m) => {
                            if tx.send(Response::from_measurement(&m)).is_err() {
                                streaming_client = None;
                                notifications = None;
                            }
                        }
                        Err(e) => {
                            eprintln!("[ERR] Failed to parse measurement: {e}");
                        }
                    }
                }
            }
        }
    }

    conn.disconnect().await.ok();
}

/// Returns `true` if the actor should shut down.
async fn handle_actor_command(
    conn: &mut Connection,
    cmd: ActorCommand,
    streaming_client: &mut Option<mpsc::UnboundedSender<Response>>,
    notifications: &mut Option<Notifications>,
    assembler: &mut FrameAssembler,
    ticker: &mut tokio::time::Interval,
) -> bool {
    match cmd.request {
        Request::Ping => {
            cmd.tx.send(Response::Pong).ok();
        }

        Request::Shutdown => {
            end_stream(streaming_client);
            cmd.tx.send(Response::Ok).ok();
            return true;
        }

        Request::Subscribe => {
            if streaming_client.is_some() {
                cmd.tx
                    .send(Response::Error {
                        message: "another client is already streaming".to_string(),
                    })
                    .ok();
                return false;
            }

            match conn.listen().await {
                Ok(n) => {
                    *notifications = Some(n);
                    assembler.clear();
                    ticker.reset();
                    *streaming_client = Some(cmd.tx);
                }
                Err(e) => {
                    cmd.tx
                        .send(Response::Error {
                            message: format!("failed to start listening: {e}"),
                        })
                        .ok();
                }
            }
        }

        Request::GetRtc => {
            let resp = send_oneshot(
                conn,
                &payload::monitoring(),
                FrameKind::Measurement,
                notifications,
                assembler,
            )
            .await
            .and_then(|frame| {
                connection::read_measure(&frame)
                    .map(|m| Response::from_measurement(&m))
                    .map_err(|e| format!("get_rtc failed: {e}"))
            });
            send_result(&cmd.tx, resp);
        }

        Request::Power { on } => {
            let p = if on { payload::on() } else { payload::off() };
            let resp = send_oneshot(conn, &p, FrameKind::Command, notifications, assembler)
                .await
                .map(|frame| parse_command_result(&frame));
            send_result(&cmd.tx, resp);
        }

        Request::SetRtc { time } => {
            let parsed = chrono::DateTime::parse_from_rfc3339(&time)
                .map(|t| t.with_timezone(&chrono::Local));
            match parsed {
                Ok(dt) => {
                    let p = payload::rtc(&dt);
                    let resp = send_oneshot(conn, &p, FrameKind::Command, notifications, assembler)
                        .await
                        .map(|frame| parse_command_result(&frame));
                    send_result(&cmd.tx, resp);
                }
                Err(e) => {
                    cmd.tx
                        .send(Response::Error {
                            message: format!("invalid time: {e}"),
                        })
                        .ok();
                }
            }
        }

        Request::TestLed => {
            let p = payload::blink_led();
            let resp = send_oneshot(conn, &p, FrameKind::Command, notifications, assembler)
                .await
                .map(|frame| parse_command_result(&frame));
            send_result(&cmd.tx, resp);
        }
    }
    false
}

fn parse_command_result(frame: &[u8]) -> Response {
    match frame.get(4).copied() {
        Some(0x00) => Response::CommandResult {
            success: true,
            code: Some(0x00),
        },
        Some(code) => Response::CommandResult {
            success: false,
            code: Some(code),
        },
        None => Response::Error {
            message: "response frame too short".to_string(),
        },
    }
}

fn send_result(
    tx: &mpsc::UnboundedSender<Response>,
    result: std::result::Result<Response, String>,
) {
    match result {
        Ok(r) => {
            tx.send(r).ok();
        }
        Err(msg) => {
            tx.send(Response::Error { message: msg }).ok();
        }
    }
}

async fn send_oneshot(
    conn: &mut Connection,
    cmd_payload: &[u8],
    kind: FrameKind,
    notifications: &mut Option<Notifications>,
    assembler: &mut FrameAssembler,
) -> std::result::Result<Vec<u8>, String> {
    let was_streaming = notifications.is_some();

    if notifications.is_none() {
        match conn.listen().await {
            Ok(n) => {
                *notifications = Some(n);
                assembler.clear();
            }
            Err(e) => return Err(format!("listen failed: {e}")),
        }
    }

    match conn.write(cmd_payload).await {
        Ok(reconnected) => {
            if reconnected {
                match conn.listen().await {
                    Ok(n) => {
                        *notifications = Some(n);
                        assembler.clear();
                    }
                    Err(e) => return Err(format!("re-listen failed: {e}")),
                }
            }
        }
        Err(e) => return Err(format!("write failed: {e}")),
    }

    let result = wait_for_frame(notifications, kind).await;

    if !was_streaming {
        *notifications = None;
    }

    result
}

async fn wait_for_frame(
    notifications: &mut Option<Notifications>,
    kind: FrameKind,
) -> std::result::Result<Vec<u8>, String> {
    let mut assembler = FrameAssembler::new();
    let wait = async {
        loop {
            let event = match notifications.as_mut() {
                Some(n) => n.next().await,
                None => return Err("no notification stream".to_string()),
            };
            let Some(event) = event else {
                return Err("notification stream closed".to_string());
            };
            if event.uuid != connection::C_RX {
                continue;
            }
            if let Some(frame) = assembler.feed(&event.value) {
                let is_measurement = frame.len() >= MEASUREMENT_FRAME_MIN_LEN;
                match kind {
                    FrameKind::Command if is_measurement => continue,
                    FrameKind::Measurement if !is_measurement => continue,
                    _ => return Ok(frame),
                }
            }
        }
    };

    tokio::time::timeout(std::time::Duration::from_secs(10), wait)
        .await
        .unwrap_or(Err("command timed out".to_string()))
}

fn end_stream(client: &mut Option<mpsc::UnboundedSender<Response>>) {
    if let Some(tx) = client.take() {
        tx.send(Response::StreamEnd).ok();
    }
}
