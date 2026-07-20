use std::ops::ControlFlow;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use btleplug::api::ValueNotification;
use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, mpsc};

use super::protocol::{Request, Response};
use crate::cli::ConnectionConfig;
use crate::connection::{
    self, Connection, FrameAssembler, MEASUREMENT_FRAME_MIN_LEN, Notifications,
};
use crate::payload;

/// How long a one-shot command waits for its reply frame.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

struct ActorCommand {
    request: Request,
    tx: mpsc::UnboundedSender<Response>,
}

enum FrameKind {
    Command,
    Measurement,
}

pub async fn run(config: &ConnectionConfig, paths: &super::AgentPaths) -> Result<()> {
    let sock = &paths.socket;
    let pid_file = &paths.pid;

    cleanup_stale(sock, pid_file)?;
    let listener =
        UnixListener::bind(sock).with_context(|| format!("failed to bind {}", sock.display()))?;
    std::fs::write(pid_file, std::process::id().to_string()).ok();

    eprintln!("[INFO] Agent listening on {}", sock.display());

    let conn = match Connection::new(config).await {
        Ok(conn) => conn,
        Err(e) => {
            cleanup_files(sock, pid_file);
            return Err(e);
        }
    };

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ActorCommand>();
    let shutdown = std::sync::Arc::new(Notify::new());

    let actor = tokio::spawn(actor_loop(conn, cmd_rx, shutdown.clone()));

    let result = accept_loop(listener, cmd_tx.clone(), &shutdown).await;

    drop(cmd_tx);
    actor.await.ok();
    cleanup_files(sock, pid_file);
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

/// A failure that costs us the notification stream. The streaming client is
/// dropped and the link re-established on the next request.
type StreamError = String;

/// What woke the actor loop. The select! only *produces* these; acting on one
/// happens afterwards, so the handler can borrow the actor exclusively.
enum Event {
    Command(Option<ActorCommand>),
    Tick,
    Notification(Option<ValueNotification>),
}

/// Owns the single BLE connection and serializes every client request onto it.
/// At most one client streams measurements at a time; one-shot commands are
/// interleaved on the same link.
struct Actor {
    conn: Connection,
    notifications: Option<Notifications>,
    assembler: FrameAssembler,
    ticker: tokio::time::Interval,
    streaming_client: Option<mpsc::UnboundedSender<Response>>,
    monitoring_payload: Vec<u8>,
}

async fn actor_loop(
    conn: Connection,
    mut cmd_rx: mpsc::UnboundedReceiver<ActorCommand>,
    shutdown: std::sync::Arc<Notify>,
) {
    let mut actor = Actor::new(conn);

    loop {
        let event = tokio::select! {
            cmd = cmd_rx.recv() => Event::Command(cmd),
            _ = actor.ticker.tick(), if actor.streaming_client.is_some() => Event::Tick,
            event = next_notification(&mut actor.notifications) => Event::Notification(event),
        };

        match event {
            // The last client handle is gone: nothing left to serve.
            Event::Command(None) => break,
            Event::Command(Some(cmd)) => {
                if actor.handle(cmd).await.is_break() {
                    shutdown.notify_one();
                    break;
                }
            }
            Event::Tick => actor.poll_device().await,
            Event::Notification(event) => actor.handle_notification(event).await,
        }
    }

    actor.conn.disconnect().await.ok();
}

/// Next RX notification, or a future that never resolves while no stream is
/// subscribed, so the select! branch simply stays inert.
async fn next_notification(notifications: &mut Option<Notifications>) -> Option<ValueNotification> {
    match notifications.as_mut() {
        Some(n) => n.next().await,
        None => futures::future::pending().await,
    }
}

impl Actor {
    fn new(conn: Connection) -> Self {
        let ticker = tokio::time::interval(conn.interval());
        Self {
            conn,
            notifications: None,
            assembler: FrameAssembler::new(),
            ticker,
            streaming_client: None,
            monitoring_payload: payload::monitoring(),
        }
    }

    /// Adopt a fresh notification stream, discarding any half-assembled frame.
    fn apply_subscription(&mut self, n: Notifications) {
        self.notifications = Some(n);
        self.assembler.clear();
    }

    /// (Re-)subscribe to RX notifications, discarding any half-assembled frame.
    async fn relisten(&mut self) -> Result<(), StreamError> {
        match self.conn.listen().await {
            Ok(n) => {
                self.apply_subscription(n);
                Ok(())
            }
            Err(e) => {
                self.notifications = None;
                Err(format!("listen failed: {e}"))
            }
        }
    }

    /// Tear the streaming state down. When `reason` is `Some`, it is logged as
    /// an `[ERR]`; write failures are already logged by `Connection::write`, so
    /// the agent passes `None` to avoid a duplicate line.
    fn abort_stream(&mut self, reason: Option<StreamError>) {
        if let Some(e) = reason {
            eprintln!("[ERR] {e}");
        }
        end_stream(&mut self.streaming_client);
        self.notifications = None;
    }

    /// Ask the device for a measurement. Only runs while a client is streaming.
    async fn poll_device(&mut self) {
        match self.conn.write(&self.monitoring_payload).await {
            // A reconnect along the way invalidated the old subscription.
            Ok(true) => {
                if let Err(e) = self.relisten().await {
                    self.abort_stream(Some(e));
                }
            }
            Ok(false) => {}
            // `Connection::write` already logged this at [WARN]; just drop the
            // stream so the next request re-establishes it.
            Err(_) => self.abort_stream(None),
        }
    }

    async fn handle_notification(&mut self, event: Option<ValueNotification>) {
        let Some(event) = event else {
            match self.conn.reconnect_stream().await {
                Ok(n) => self.apply_subscription(n),
                // `reconnect_stream` already logged the "[WARN] ... reconnecting"
                // line; surface the fatal failure and tear the stream down.
                Err(e) => self.abort_stream(Some(format!("reconnect failed: {e}"))),
            }
            return;
        };

        if event.uuid != connection::C_RX {
            return;
        }

        // Frames arriving with no subscriber are stray replies; drop them.
        let Some(frame) = self.assembler.feed(&event.value) else {
            return;
        };
        let Some(tx) = self.streaming_client.as_ref() else {
            return;
        };

        let Some(m) = connection::try_measurement(&frame) else {
            return;
        };
        // The client hung up mid-stream.
        if tx.send(Response::from_measurement(&m)).is_err() {
            self.streaming_client = None;
            self.notifications = None;
        }
    }

    /// Serve one client request. `Break` means the agent should shut down.
    async fn handle(&mut self, cmd: ActorCommand) -> ControlFlow<()> {
        match cmd.request {
            Request::Ping => {
                cmd.tx.send(Response::Pong).ok();
            }

            Request::Shutdown => {
                end_stream(&mut self.streaming_client);
                cmd.tx.send(Response::Ok).ok();
                return ControlFlow::Break(());
            }

            Request::Subscribe => {
                if self.streaming_client.is_some() {
                    send_error(&cmd.tx, "another client is already streaming".to_string());
                } else if let Err(e) = self.relisten().await {
                    send_error(&cmd.tx, e);
                } else {
                    self.ticker.reset();
                    self.streaming_client = Some(cmd.tx);
                }
            }

            Request::GetRtc => {
                let resp = self
                    .oneshot(&payload::monitoring(), FrameKind::Measurement)
                    .await
                    .and_then(|frame| {
                        connection::read_measure(&frame)
                            .map(|m| Response::from_measurement(&m))
                            .map_err(|e| format!("get_rtc failed: {e}"))
                    });
                reply(&cmd.tx, resp);
            }

            Request::Power { on } => {
                let p = if on { payload::on() } else { payload::off() };
                self.command(&p, &cmd.tx).await;
            }

            Request::TestLed => {
                self.command(&payload::blink_led(), &cmd.tx).await;
            }

            Request::SetRtc { time } => match chrono::DateTime::parse_from_rfc3339(&time) {
                Ok(t) => {
                    let p = payload::rtc(&t.with_timezone(&chrono::Local));
                    self.command(&p, &cmd.tx).await;
                }
                Err(e) => send_error(&cmd.tx, format!("invalid time: {e}")),
            },
        }
        ControlFlow::Continue(())
    }

    /// Send a one-shot command frame and reply with its status byte.
    async fn command(&mut self, cmd_payload: &[u8], tx: &mpsc::UnboundedSender<Response>) {
        let resp = self
            .oneshot(cmd_payload, FrameKind::Command)
            .await
            .map(|frame| parse_command_result(&frame));
        reply(tx, resp);
    }

    /// Write `cmd_payload` and wait for the matching reply frame. When no
    /// client is streaming, the temporary subscription is torn down after.
    async fn oneshot(
        &mut self,
        cmd_payload: &[u8],
        kind: FrameKind,
    ) -> Result<Vec<u8>, StreamError> {
        let was_streaming = self.notifications.is_some();

        if !was_streaming {
            self.relisten().await?;
        }

        match self.conn.write(cmd_payload).await {
            Ok(true) => self.relisten().await?,
            Ok(false) => {}
            Err(e) => return Err(format!("write failed: {e}")),
        }

        let result = wait_for_frame(&mut self.notifications, kind).await;

        if !was_streaming {
            self.notifications = None;
        }

        result
    }
}

fn parse_command_result(frame: &[u8]) -> Response {
    match frame.get(4).copied() {
        Some(code) => Response::CommandResult {
            success: code == 0x00,
            code: Some(code),
        },
        None => Response::Error {
            message: "response frame too short".to_string(),
        },
    }
}

fn send_error(tx: &mpsc::UnboundedSender<Response>, message: String) {
    tx.send(Response::Error { message }).ok();
}

fn reply(tx: &mpsc::UnboundedSender<Response>, result: Result<Response, StreamError>) {
    match result {
        Ok(r) => {
            tx.send(r).ok();
        }
        Err(message) => send_error(tx, message),
    }
}

/// Read notifications until one reassembles into a frame of the expected kind.
/// Measurement and command replies are told apart by length, since a streaming
/// measurement can arrive while a one-shot command is in flight.
async fn wait_for_frame(
    notifications: &mut Option<Notifications>,
    kind: FrameKind,
) -> Result<Vec<u8>, StreamError> {
    if notifications.is_none() {
        return Err("no notification stream".to_string());
    }

    let mut assembler = FrameAssembler::new();
    let wait = async {
        loop {
            let Some(event) = next_notification(notifications).await else {
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

    tokio::time::timeout(COMMAND_TIMEOUT, wait)
        .await
        .unwrap_or(Err("command timed out".to_string()))
}

fn end_stream(client: &mut Option<mpsc::UnboundedSender<Response>>) {
    if let Some(tx) = client.take() {
        tx.send(Response::StreamEnd).ok();
    }
}
