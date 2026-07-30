use std::future::Future;
use std::io::Write;
use std::ops::ControlFlow;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use btleplug::api::{BDAddr, ValueNotification};
use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::{Notify, mpsc};

use super::protocol::{Request, Response};
use super::status::AgentStats;
use crate::cli::ConnectionConfig;
use crate::connection::{
    self, COMMAND_TIMEOUT, Connection, FrameAssembler, FrameKind, Notifications,
};
use crate::payload;

struct ActorCommand {
    request: Request,
    tx: mpsc::UnboundedSender<Response>,
}

/// Owns the socket and pid file for as long as the agent is serving. Every exit
/// path below — an early `?`, a signal during the initial connect, `agent stop`
/// — has to take both files with it, and a guard is the only way to say that
/// once rather than at each `return`. Removal only; whether the agent stopped
/// What the connection handlers need to answer a request without troubling the
/// actor: which device this agent holds, and its live counters.
struct AgentInfo {
    addr: BDAddr,
    stats: Arc<AgentStats>,
}

/// cleanly or never got going is `run`'s to report, not a destructor's.
struct AgentFiles<'a>(&'a super::AgentPaths);

impl Drop for AgentFiles<'_> {
    fn drop(&mut self) {
        self.0.remove_files();
    }
}

pub async fn run(config: &ConnectionConfig, paths: &super::AgentPaths) -> Result<()> {
    let sock = &paths.socket;

    // Register before creating anything, so no window exists where a signal
    // still has its default disposition and kills the process between the bind
    // and the guard below. Nothing polls these until the connect, which is fine:
    // a signal arriving earlier is queued rather than lost.
    let mut signals = Shutdown::new();

    cleanup_stale(paths).await?;
    ensure_socket_dir(sock)?;
    let listener =
        UnixListener::bind(sock).with_context(|| format!("failed to bind {}", sock.display()))?;
    let _files = AgentFiles(paths);
    write_pid_file(&paths.pid)?;

    eprintln!("[INFO] Agent listening on {}", sock.display());

    let stats = Arc::new(AgentStats::new(config.interval));

    // Connecting can take tens of seconds of scanning and retries. Watch for a
    // signal throughout, so a Ctrl-C here runs the cleanup above instead of
    // killing the process outright and stranding the socket and pid file.
    let conn = tokio::select! {
        result = Connection::new(config) => result?,
        reason = signals.recv() => {
            eprintln!("[INFO] Received {reason} while connecting, shutting down...");
            return Ok(());
        }
    };

    // The link is up before anything has been read from it, so record it here:
    // otherwise an agent sitting idle with a healthy connection would report
    // itself as disconnected until the first measurement.
    stats.set_connected(true);

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ActorCommand>();
    // Two separate signals rather than one: the accept loop must stop first so
    // the actor can finish in-flight work, and `notify_one` leaves a permit
    // behind when the target is momentarily not parked on `notified()`, which
    // a shared `notify_waiters` would drop on the floor.
    let shutdown = Arc::new(Notify::new());
    let actor_shutdown = Arc::new(Notify::new());

    let info = Arc::new(AgentInfo {
        addr: config.addr,
        stats: Arc::clone(&stats),
    });

    let mut actor = tokio::spawn(actor_loop(conn, cmd_rx, actor_shutdown.clone(), stats));
    let result = tokio::select! {
        result = accept_loop(listener, cmd_tx.clone(), shutdown, info, signals) => result,
        actor_result = &mut actor => {
            actor_result.context("agent actor failed")?;
            bail!("agent actor stopped unexpectedly");
        }
    };

    actor_shutdown.notify_one();
    drop(cmd_tx);
    actor.await.ok();
    // Reached only after the agent actually served, so a startup failure no
    // longer reports a clean stop on its way out.
    eprintln!("[INFO] Agent stopped");
    result
}

/// The signals that end the agent, registered once so no window exists in which
/// one arrives with nothing listening for it.
struct Shutdown {
    interrupt: Option<Signal>,
    terminate: Option<Signal>,
}

impl Shutdown {
    fn new() -> Self {
        Self {
            interrupt: Self::register(SignalKind::interrupt()),
            terminate: Self::register(SignalKind::terminate()),
        }
    }

    /// Losing a graceful signal path is not worth refusing to start over: the
    /// signal keeps its default disposition, which still stops the process,
    /// just without the socket and pid file cleanup.
    fn register(kind: SignalKind) -> Option<Signal> {
        match signal(kind) {
            Ok(sig) => Some(sig),
            Err(e) => {
                eprintln!("[WARN] Failed to register handler for signal {kind:?}: {e}");
                None
            }
        }
    }

    /// Wait for whichever arrives first, naming it for the log line.
    async fn recv(&mut self) -> &'static str {
        let interrupt = Self::wait(self.interrupt.as_mut());
        let terminate = Self::wait(self.terminate.as_mut());
        tokio::select! {
            _ = interrupt => "SIGINT",
            _ = terminate => "SIGTERM",
        }
    }

    /// A signal that failed to register simply never fires.
    async fn wait(sig: Option<&mut Signal>) {
        match sig {
            Some(sig) => {
                sig.recv().await;
            }
            None => futures::future::pending().await,
        }
    }
}

/// Refuse to start when a previous agent is still alive, and clear its leftover
/// files when it is not.
///
/// A live pid is not proof on its own — pids get recycled, and a leftover file
/// naming an unrelated process must not lock the agent out of ever starting
/// again — so it counts only when it still belongs to a btwattch2. That check is
/// a cheap `/proc` read taken first; a socket that answers is the authority and
/// is consulted whenever the pid file leaves any doubt, including when it names
/// a process that is not ours. Skipping the socket in that case would let a
/// second agent unlink a live one's socket and fight it for the device.
async fn cleanup_stale(paths: &super::AgentPaths) -> Result<()> {
    if let Some(pid) = paths.read_pid().filter(|&pid| is_agent_process(pid)) {
        bail!("agent is already running (pid {pid})");
    }
    if super::probe_daemon(paths).await.is_some() {
        bail!("agent is already running");
    }

    paths.remove_files();
    Ok(())
}

/// Whether `pid` names a live process running this same program. Comparing
/// `comm` is what tells a still-running agent apart from a recycled pid.
fn is_agent_process(pid: u32) -> bool {
    let ours = std::fs::read_to_string("/proc/self/comm");
    let theirs = std::fs::read_to_string(format!("/proc/{pid}/comm"));
    matches!((ours, theirs), (Ok(ours), Ok(theirs)) if ours == theirs)
}

/// Create the socket's parent directory, private to us. The default runtime
/// directory does not exist until the first agent start; an existing directory
/// (including one an explicit `--socket` points into) is left as it is.
fn ensure_socket_dir(sock: &Path) -> Result<()> {
    let Some(dir) = sock.parent().filter(|d| !d.as_os_str().is_empty()) else {
        return Ok(());
    };
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .with_context(|| format!("failed to create {}", dir.display()))
}

/// Write the pid file, refusing to follow a symlink. `--pid-file` can name any
/// path, so an attacker who can predict it must not be able to turn the write
/// into a clobber of an unrelated file the agent happens to be able to write.
fn write_pid_file(path: &Path) -> Result<()> {
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_NOFOLLOW)
        .mode(0o600)
        .open(path)
        .and_then(|mut file| file.write_all(std::process::id().to_string().as_bytes()))
        .with_context(|| format!("failed to write {}", path.display()))
}

async fn accept_loop(
    listener: UnixListener,
    cmd_tx: mpsc::UnboundedSender<ActorCommand>,
    shutdown: Arc<Notify>,
    info: Arc<AgentInfo>,
    mut signals: Shutdown,
) -> Result<()> {
    tokio::select! {
        _ = async {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let tx = cmd_tx.clone();
                        let shutdown = shutdown.clone();
                        let info = Arc::clone(&info);
                        tokio::spawn(handle_client(stream, tx, shutdown, info));
                    }
                    Err(e) => {
                        eprintln!("[WARN] Accept failed: {e}");
                    }
                }
            }
        } => {}
        reason = signals.recv() => {
            eprintln!("[INFO] Received {reason}, shutting down...");
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
    shutdown: Arc<Notify>,
    info: Arc<AgentInfo>,
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

    if let Some(response) = control_response(&request, cmd_tx.is_closed(), &info) {
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
fn control_response(request: &Request, actor_gone: bool, info: &AgentInfo) -> Option<Response> {
    match request {
        Request::Ping if actor_gone => Some(Response::Error {
            message: "agent actor is not running".to_string(),
        }),
        // The address rides along on the pong so a client can tell whether the
        // agent it found is holding the device its --addr asked for; the status
        // is read from shared counters, which is why answering here does not
        // mean answering with less.
        Request::Ping => Some(Response::Pong {
            addr: Some(info.addr.to_string()),
            status: Some(info.stats.snapshot()),
        }),
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

/// A failure that costs us the notification stream. The streaming clients are
/// dropped and the link re-established on the next request.
type StreamError = String;

/// The clients currently streaming measurements.
///
/// A list rather than the single slot this used to be: the device is polled once
/// per interval whatever the audience, so a second subscriber costs nothing and
/// no longer has to be turned away. Owning the reported count alongside the list
/// is what keeps `agent status` from drifting out of step with it.
struct Clients {
    list: Vec<mpsc::UnboundedSender<Response>>,
    stats: Arc<AgentStats>,
}

impl Clients {
    fn new(stats: Arc<AgentStats>) -> Self {
        Self {
            list: Vec::new(),
            stats,
        }
    }

    fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    fn add(&mut self, tx: mpsc::UnboundedSender<Response>) {
        self.list.push(tx);
        self.publish_count();
    }

    /// Hand `response` to everyone still listening.
    fn broadcast(&mut self, response: &Response) {
        let mut hung_up = false;
        for tx in &self.list {
            // One departure must not interrupt anyone else's feed, so the
            // failures are collected and swept up afterwards.
            hung_up |= tx.send(response.clone()).is_err();
        }
        if hung_up {
            self.reap();
        }
    }

    /// Drop the clients that have hung up. Doing this eagerly rather than
    /// waiting for the next failed send keeps the reported count current.
    fn reap(&mut self) {
        if self.list.iter().any(|tx| tx.is_closed()) {
            self.list.retain(|tx| !tx.is_closed());
            self.publish_count();
        }
    }

    /// Report a failure to everyone and forget them.
    fn abort(&mut self, reason: &StreamError) {
        for tx in self.take() {
            tx.send(Response::Error {
                message: reason.clone(),
            })
            .ok();
        }
    }

    /// Tell everyone the stream is over and forget them. They read a closed
    /// socket as a protocol error, not as a normal stop.
    fn end(&mut self) {
        for tx in self.take() {
            tx.send(Response::StreamEnd).ok();
        }
    }

    fn take(&mut self) -> Vec<mpsc::UnboundedSender<Response>> {
        let previous = std::mem::take(&mut self.list);
        self.publish_count();
        previous
    }

    fn publish_count(&self) {
        self.stats.set_clients(self.list.len());
    }
}

/// What woke the actor loop. The select! only *produces* these; acting on one
/// happens afterwards, so the handler can borrow the actor exclusively.
enum Event {
    Command(Option<ActorCommand>),
    Tick,
    Notification(Option<ValueNotification>),
}

/// Owns the single BLE connection and serializes every client request onto it.
/// Any number of clients stream measurements at once, all fed from the same poll;
/// one-shot commands are interleaved on the same link.
struct Actor {
    conn: Connection,
    notifications: Option<Notifications>,
    assembler: FrameAssembler,
    ticker: tokio::time::Interval,
    /// Everyone currently subscribed; one poll of the device answers all of them.
    clients: Clients,
    monitoring_payload: Vec<u8>,
    stats: Arc<AgentStats>,
}

async fn actor_loop(
    conn: Connection,
    mut cmd_rx: mpsc::UnboundedReceiver<ActorCommand>,
    shutdown: Arc<Notify>,
    stats: Arc<AgentStats>,
) {
    let mut actor = Actor::new(conn, stats);

    loop {
        let event = tokio::select! {
            _ = shutdown.notified() => break,
            cmd = cmd_rx.recv() => Event::Command(cmd),
            _ = actor.ticker.tick(), if !actor.clients.is_empty() => Event::Tick,
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

    // However the loop ended, the streaming clients are owed a clean end.
    actor.clients.end();
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
    fn new(conn: Connection, stats: Arc<AgentStats>) -> Self {
        let ticker = tokio::time::interval(conn.interval());
        Self {
            conn,
            notifications: None,
            assembler: FrameAssembler::new(),
            ticker,
            clients: Clients::new(Arc::clone(&stats)),
            monitoring_payload: payload::monitoring(),
            stats,
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

    /// Tear the streaming state down and report the failure to every client.
    /// Every path that gives up on streaming goes through here, so what "torn
    /// down" means stays in one place.
    fn abort_stream(&mut self, reason: StreamError) {
        eprintln!("[ERR] {reason}");
        self.notifications = None;
        self.stats.set_connected(false);
        self.clients.abort(&reason);
    }

    /// Ask the device for a measurement. Only runs while a client is streaming.
    async fn poll_device(&mut self) {
        self.clients.reap();
        if self.clients.is_empty() {
            return;
        }

        match self.conn.write(&self.monitoring_payload).await {
            // A reconnect along the way invalidated the old subscription.
            Ok(true) => {
                self.stats.record_reconnect();
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
                Ok(n) => {
                    self.stats.record_reconnect();
                    self.apply_subscription(n);
                }
                // `reconnect_stream` already logged the "[WARN] ... reconnecting"
                // line; surface the fatal failure and tear the stream down.
                Err(e) => self.abort_stream(format!("reconnect failed: {e}")),
            }
            return;
        };

        if event.uuid != connection::C_RX {
            return;
        }

        let Self {
            assembler,
            clients,
            stats,
            ..
        } = self;
        for frame in assembler.feed(&event.value) {
            let Some(m) = connection::try_measurement(&frame) else {
                continue;
            };
            stats.record_sample();
            clients.broadcast(&Response::from_measurement(&m));
        }
    }

    /// Serve one client request.
    async fn handle(&mut self, cmd: ActorCommand) {
        self.clients.reap();

        match cmd.request {
            // `handle_client` answers these itself so they stay responsive
            // while the actor is busy on the link; they never reach here.
            Request::Ping | Request::Shutdown => {}

            Request::Subscribe => {
                // Whether anything was being polled before this client arrived.
                let idle = self.clients.is_empty();
                // A subscription already in place is reused: taking out another
                // would discard a part-received frame and cost the clients
                // already streaming a sample.
                if self.notifications.is_none()
                    && let Err(e) = self.relisten().await
                {
                    send_error(&cmd.tx, e);
                    return;
                }
                self.clients.add(cmd.tx);
                // Only a client that starts the polling waits on the ticker's
                // own schedule. Resetting it for a later arrival would push back
                // the sample the others are already waiting for.
                if idle {
                    self.ticker.reset();
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

        self.wait_for_frame(kind).await
    }

    /// Read notifications until one reassembles into a frame of the expected
    /// kind. Measurement and command replies are told apart by length, since a
    /// streaming measurement can arrive while a one-shot command is in flight;
    /// those are forwarded to the streaming client rather than dropped.
    ///
    /// This shares `self.assembler` rather than using a local one on purpose. A
    /// second assembler would split a part-received frame across the two, and
    /// both halves would then have to resynchronize on CRC failures — a burst
    /// of warnings and dropped samples every time a command interrupts a
    /// stream.
    async fn wait_for_frame(&mut self, kind: FrameKind) -> Result<Vec<u8>, StreamError> {
        // Destructured so the three fields can be borrowed independently: the
        // stream and assembler mutably, the client for the forwarding closure.
        let Self {
            notifications,
            assembler,
            clients,
            stats,
            ..
        } = self;
        let Some(notifications) = notifications.as_mut() else {
            return Err("no notification stream".to_string());
        };

        let wait = connection::next_matching_frame(notifications, assembler, &kind, |frame| {
            if let Some(m) = connection::try_measurement(frame) {
                stats.record_sample();
                // A hung-up client is reaped on the next request; here the
                // command reply is what matters.
                clients.broadcast(&Response::from_measurement(&m));
            }
        });

        match tokio::time::timeout(COMMAND_TIMEOUT, wait).await {
            Ok(Some(frame)) => Ok(frame),
            Ok(None) => Err("notification stream closed".to_string()),
            Err(_) => Err("command timed out".to_string()),
        }
    }
}

fn parse_command_result(frame: &[u8]) -> Response {
    match connection::command_status(frame) {
        Some(code) => Response::CommandResult { code },
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

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::super::testutil::TempPath;
    use super::*;

    fn test_stats() -> Arc<AgentStats> {
        Arc::new(AgentStats::new("1s".parse().unwrap()))
    }

    /// A subscriber and the receiving end it would be streaming to.
    fn subscriber() -> (
        mpsc::UnboundedSender<Response>,
        mpsc::UnboundedReceiver<Response>,
    ) {
        mpsc::unbounded_channel()
    }

    fn wattage(resp: &Response) -> Option<f64> {
        match resp {
            Response::Measurement { wattage, .. } => Some(*wattage),
            _ => None,
        }
    }

    /// A measurement as it travels: built through the real conversion, so the
    /// fan-out is exercised on the shape clients actually receive.
    fn sample(wattage: f64) -> Response {
        Response::from_measurement(&crate::connection::testutil::measurement(wattage))
    }

    /// The point of the client list: one poll of the device feeds everyone, so
    /// concurrent subscribers no longer have to be turned away.
    #[test]
    fn every_client_gets_every_measurement() {
        let stats = test_stats();
        let mut clients = Clients::new(Arc::clone(&stats));

        let (tx_a, mut rx_a) = subscriber();
        let (tx_b, mut rx_b) = subscriber();
        clients.add(tx_a);
        clients.add(tx_b);
        assert_eq!(stats.snapshot().clients, 2);

        clients.broadcast(&sample(42.0));
        assert_eq!(rx_a.try_recv().map(|r| wattage(&r)), Ok(Some(42.0)));
        assert_eq!(rx_b.try_recv().map(|r| wattage(&r)), Ok(Some(42.0)));
    }

    /// One client hanging up must not cost the others a sample, and must not
    /// leave the reported count overstated either.
    #[test]
    fn a_departure_does_not_disturb_the_rest() {
        let stats = test_stats();
        let mut clients = Clients::new(Arc::clone(&stats));

        let (tx_gone, rx_gone) = subscriber();
        let (tx_stays, mut rx_stays) = subscriber();
        clients.add(tx_gone);
        clients.add(tx_stays);
        drop(rx_gone);

        clients.broadcast(&sample(1.0));
        clients.broadcast(&sample(2.0));

        assert_eq!(rx_stays.try_recv().map(|r| wattage(&r)), Ok(Some(1.0)));
        assert_eq!(rx_stays.try_recv().map(|r| wattage(&r)), Ok(Some(2.0)));
        assert_eq!(stats.snapshot().clients, 1);
        assert!(!clients.is_empty());
    }

    #[test]
    fn a_failure_is_reported_to_everyone() {
        let stats = test_stats();
        let mut clients = Clients::new(Arc::clone(&stats));
        let (tx_a, mut rx_a) = subscriber();
        let (tx_b, mut rx_b) = subscriber();
        clients.add(tx_a);
        clients.add(tx_b);

        clients.abort(&"link went away".to_string());

        for rx in [&mut rx_a, &mut rx_b] {
            assert!(matches!(
                rx.try_recv(),
                Ok(Response::Error { message }) if message == "link went away"
            ));
        }
        assert!(clients.is_empty());
        assert_eq!(stats.snapshot().clients, 0);
    }

    /// A closed socket reads as a protocol error at the far end, so shutdown
    /// owes every client an explicit end.
    #[test]
    fn shutdown_ends_every_stream() {
        let stats = test_stats();
        let mut clients = Clients::new(Arc::clone(&stats));
        let (tx_a, mut rx_a) = subscriber();
        let (tx_b, mut rx_b) = subscriber();
        clients.add(tx_a);
        clients.add(tx_b);

        clients.end();

        assert!(matches!(rx_a.try_recv(), Ok(Response::StreamEnd)));
        assert!(matches!(rx_b.try_recv(), Ok(Response::StreamEnd)));
        assert!(clients.is_empty());
        assert_eq!(stats.snapshot().clients, 0);
    }

    /// `agent status` reads its answer from the shared counters, which is what
    /// lets the connection handler reply while the actor is busy on the link.
    #[test]
    fn a_ping_carries_the_address_and_status() {
        let addr: BDAddr = "CB:DF:6B:12:34:56".parse().unwrap();
        let stats = test_stats();
        stats.set_clients(2);
        let info = AgentInfo {
            addr,
            stats: Arc::clone(&stats),
        };

        let Some(Response::Pong {
            addr: reported,
            status: Some(status),
        }) = control_response(&Request::Ping, false, &info)
        else {
            panic!("a ping must be answered with a pong carrying a status");
        };
        assert_eq!(reported, Some(addr.to_string()));
        assert_eq!(status.clients, 2);
        assert!(!status.connected, "no link has been established yet");

        // With the actor gone there is nothing to report about, so the ping
        // becomes an error rather than a pong that looks healthy.
        assert!(matches!(
            control_response(&Request::Ping, true, &info),
            Some(Response::Error { .. })
        ));
    }

    /// Streaming and one-shot requests are the actor's work; these two are not,
    /// which is what keeps `agent status` and `agent stop` answerable.
    #[test]
    fn only_ping_and_shutdown_are_answered_without_the_actor() {
        let info = AgentInfo {
            addr: "CB:DF:6B:12:34:56".parse().unwrap(),
            stats: test_stats(),
        };
        assert!(matches!(
            control_response(&Request::Shutdown, false, &info),
            Some(Response::Ok)
        ));
        for request in [Request::Subscribe, Request::GetRtc, Request::TestLed] {
            assert!(
                control_response(&request, false, &info).is_none(),
                "{request:?} is the actor's to serve"
            );
        }
    }

    fn mode_of(path: &Path) -> u32 {
        std::fs::metadata(path)
            .expect("path exists")
            .permissions()
            .mode()
            & 0o777
    }

    #[test]
    fn pid_file_is_written_private() {
        let temp = TempPath::new(".pid");
        write_pid_file(temp.path()).unwrap();

        let paths = super::super::paths_from_socket(temp.path().with_extension("sock"));
        assert_eq!(paths.read_pid(), Some(std::process::id()));
        assert_eq!(mode_of(temp.path()), 0o600);
    }

    /// `cleanup_stale` normally unlinks a planted symlink before we get here.
    /// O_NOFOLLOW covers the case where it cannot — a symlink planted in the
    /// window between the two, or a directory whose entries we may not remove.
    #[test]
    fn pid_file_refuses_to_follow_a_symlink() {
        let temp = TempPath::new(".pid");
        let victim = temp.sibling(".victim");
        std::fs::write(&victim, b"untouched").unwrap();
        std::os::unix::fs::symlink(&victim, temp.path()).unwrap();

        assert!(write_pid_file(temp.path()).is_err());
        assert_eq!(std::fs::read(&victim).unwrap(), b"untouched");
    }

    /// A pid file holding anything but a number is the same as none at all.
    #[test]
    fn unparsable_pid_file_reads_as_absent() {
        let temp = TempPath::new(".pid");
        let paths = super::super::paths_from_socket(temp.path().with_extension("sock"));

        std::fs::write(temp.path(), b"  4321\n").unwrap();
        assert_eq!(paths.read_pid(), Some(4321));

        for junk in ["", "   ", "not-a-pid", "-1"] {
            std::fs::write(temp.path(), junk).unwrap();
            assert_eq!(paths.read_pid(), None, "accepted {junk:?}");
        }
    }

    #[test]
    fn our_own_pid_is_recognised_as_an_agent() {
        assert!(is_agent_process(std::process::id()));
    }

    /// The check that keeps a recycled pid in a leftover file from locking the
    /// agent out for good. Pid 1 is always live and never a btwattch2.
    #[test]
    fn an_unrelated_live_process_is_not_an_agent() {
        assert!(!is_agent_process(1));
    }

    #[test]
    fn socket_dir_is_created_private() {
        let temp = TempPath::new(".d");
        let sock = temp.path().join("deeper/a.sock");
        let dir = sock.parent().expect("socket path has a parent");

        ensure_socket_dir(&sock).unwrap();
        assert_eq!(mode_of(dir), 0o700);

        // An existing directory is accepted as it is, not re-permissioned.
        ensure_socket_dir(&sock).unwrap();
    }
}
