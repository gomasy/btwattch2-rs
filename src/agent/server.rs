use std::future::Future;
use std::ops::ControlFlow;
use std::path::Path;

use anyhow::{Context, Result, bail};
use btleplug::api::ValueNotification;
use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, mpsc};

use super::protocol::{Request, Response};
use crate::cli::ConnectionConfig;
use crate::connection::{
    self, COMMAND_TIMEOUT, Connection, FrameAssembler, FrameKind, Notifications,
};
use crate::payload;

struct ActorCommand {
    request: Request,
    tx: mpsc::UnboundedSender<Response>,
}

pub async fn run(config: &ConnectionConfig, paths: &super::AgentPaths) -> Result<()> {
    let sock = &paths.socket;
    let pid_file = &paths.pid;

    cleanup_stale(paths).await?;
    let listener =
        UnixListener::bind(sock).with_context(|| format!("failed to bind {}", sock.display()))?;
    if let Err(e) = std::fs::write(pid_file, std::process::id().to_string()) {
        std::fs::remove_file(sock).ok();
        return Err(e).with_context(|| format!("failed to write {}", pid_file.display()));
    }

    eprintln!("[INFO] Agent listening on {}", sock.display());

    let conn = match Connection::new(config).await {
        Ok(conn) => conn,
        Err(e) => {
            cleanup_files(sock, pid_file);
            return Err(e);
        }
    };

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ActorCommand>();
    // Two separate signals rather than one: the accept loop must stop first so
    // the actor can finish in-flight work, and `notify_one` leaves a permit
    // behind when the target is momentarily not parked on `notified()`, which
    // a shared `notify_waiters` would drop on the floor.
    let shutdown = std::sync::Arc::new(Notify::new());
    let actor_shutdown = std::sync::Arc::new(Notify::new());

    let mut actor = tokio::spawn(actor_loop(conn, cmd_rx, actor_shutdown.clone()));
    let result = tokio::select! {
        result = accept_loop(listener, cmd_tx.clone(), shutdown) => result,
        actor_result = &mut actor => {
            cleanup_files(sock, pid_file);
            actor_result.context("agent actor failed")?;
            bail!("agent actor stopped unexpectedly");
        }
    };

    actor_shutdown.notify_one();
    drop(cmd_tx);
    actor.await.ok();
    cleanup_files(sock, pid_file);
    result
}

/// Refuse to start when a previous agent is still alive, and clear its leftover
/// files when it is not. The pid check is a free `/proc` stat, so the socket
/// round trip is only worth paying for when there is no usable pid file left.
async fn cleanup_stale(paths: &super::AgentPaths) -> Result<()> {
    if let Ok(pid_str) = std::fs::read_to_string(&paths.pid)
        && let Ok(pid) = pid_str.trim().parse::<u32>()
    {
        if Path::new(&format!("/proc/{pid}")).exists() {
            bail!("agent is already running (pid {pid})");
        }
    } else if paths.socket.exists() && super::client::ping(paths).await.is_ok() {
        bail!("agent is already running");
    }

    std::fs::remove_file(&paths.socket).ok();
    std::fs::remove_file(&paths.pid).ok();
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
    shutdown: std::sync::Arc<Notify>,
) -> Result<()> {
    let sigterm = async {
        #[cfg(unix)]
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            // Losing the graceful SIGTERM path is not worth killing a working
            // agent over: the signal keeps its default disposition (which still
            // stops the process, just without the socket/pid cleanup), and
            // SIGINT and `agent stop` remain unaffected.
            Err(e) => {
                eprintln!("[WARN] Failed to register SIGTERM handler: {e}");
                futures::future::pending::<()>().await;
            }
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
                        let shutdown = shutdown.clone();
                        tokio::spawn(handle_client(stream, tx, shutdown));
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

async fn handle_client(
    stream: UnixStream,
    cmd_tx: mpsc::UnboundedSender<ActorCommand>,
    shutdown: std::sync::Arc<Notify>,
) {
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

    if let Some(response) = control_response(&request, cmd_tx.is_closed()) {
        if send_response(&mut writer, &response).await.is_ok()
            && matches!(request, Request::Shutdown)
        {
            shutdown.notify_one();
        }
        return;
    }

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

/// The requests the connection handler answers itself, so `agent status` and
/// `agent stop` stay responsive while the actor is busy on the BLE link.
/// `None` means the request is actor-bound work.
fn control_response(request: &Request, actor_gone: bool) -> Option<Response> {
    match request {
        Request::Ping if actor_gone => Some(Response::Error {
            message: "agent actor is not running".to_string(),
        }),
        Request::Ping => Some(Response::Pong),
        Request::Shutdown => Some(Response::Ok),
        _ => None,
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
            _ = shutdown.notified() => break,
            cmd = cmd_rx.recv() => Event::Command(cmd),
            _ = actor.ticker.tick(), if actor.streaming_client.is_some() => Event::Tick,
            event = next_notification(&mut actor.notifications) => Event::Notification(event),
        };

        let flow = match event {
            // The last client handle is gone: nothing left to serve.
            Event::Command(None) => ControlFlow::Break(()),
            Event::Command(Some(cmd)) => until_shutdown(actor.handle(cmd), &shutdown).await,
            Event::Tick => until_shutdown(actor.poll_device(), &shutdown).await,
            Event::Notification(event) => {
                until_shutdown(actor.handle_notification(event), &shutdown).await
            }
        };
        if flow.is_break() {
            break;
        }
    }

    // However the loop ended, the streaming client is owed a clean end: it
    // reads a closed socket as a protocol error, not as a normal stop.
    end_stream(&mut actor.streaming_client);
    tokio::time::timeout(COMMAND_TIMEOUT, actor.conn.disconnect())
        .await
        .ok();
}

/// Run `work` to completion, abandoning it if shutdown is signalled first.
/// `Break` means the actor should stop.
async fn until_shutdown(work: impl Future<Output = ()>, shutdown: &Notify) -> ControlFlow<()> {
    tokio::select! {
        _ = work => ControlFlow::Continue(()),
        _ = shutdown.notified() => ControlFlow::Break(()),
    }
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

    /// Tear the streaming state down and report the failure to the client.
    fn abort_stream(&mut self, reason: StreamError) {
        eprintln!("[ERR] {reason}");
        if let Some(tx) = self.streaming_client.take() {
            tx.send(Response::Error { message: reason }).ok();
        }
        self.notifications = None;
    }

    /// Ask the device for a measurement. Only runs while a client is streaming.
    async fn poll_device(&mut self) {
        match self.conn.write(&self.monitoring_payload).await {
            // A reconnect along the way invalidated the old subscription.
            Ok(true) => {
                if let Err(e) = self.relisten().await {
                    self.abort_stream(e);
                }
            }
            Ok(false) => {}
            Err(e) => self.abort_stream(format!("measurement write failed: {e}")),
        }
    }

    async fn handle_notification(&mut self, event: Option<ValueNotification>) {
        let Some(event) = event else {
            match self.conn.reconnect_stream().await {
                Ok(n) => self.apply_subscription(n),
                // `reconnect_stream` already logged the "[WARN] ... reconnecting"
                // line; surface the fatal failure and tear the stream down.
                Err(e) => self.abort_stream(format!("reconnect failed: {e}")),
            }
            return;
        };

        if event.uuid != connection::C_RX {
            return;
        }

        let Some(tx) = self.streaming_client.clone() else {
            return;
        };

        for frame in self.assembler.feed(&event.value) {
            let Some(m) = connection::try_measurement(&frame) else {
                continue;
            };
            // The client hung up mid-stream.
            if tx.send(Response::from_measurement(&m)).is_err() {
                self.streaming_client = None;
                self.notifications = None;
                break;
            }
        }
    }

    /// Serve one client request.
    async fn handle(&mut self, cmd: ActorCommand) {
        match cmd.request {
            // `handle_client` answers these itself so they stay responsive
            // while the actor is busy on the link; they never reach here.
            Request::Ping | Request::Shutdown => {}

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
                let p = self.monitoring_payload.clone();
                let resp = self
                    .oneshot(&p, FrameKind::Measurement)
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

        let result = self.write_and_wait(cmd_payload, kind).await;

        if !was_streaming {
            self.notifications = None;
        }

        result
    }

    async fn write_and_wait(
        &mut self,
        cmd_payload: &[u8],
        kind: FrameKind,
    ) -> Result<Vec<u8>, StreamError> {
        match self.conn.write(cmd_payload).await {
            Ok(true) => self.relisten().await?,
            Ok(false) => {}
            Err(e) => return Err(format!("write failed: {e}")),
        }

        wait_for_frame(&mut self.notifications, kind).await
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
    let resp = result.unwrap_or_else(|message| Response::Error { message });
    tx.send(resp).ok();
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
            for frame in assembler.feed(&event.value) {
                if kind.matches(&frame) {
                    return Ok(frame);
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
