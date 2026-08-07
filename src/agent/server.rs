use std::future::Future;
use std::io::Write;
use std::ops::ControlFlow;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use btleplug::api::{BDAddr, ValueNotification};
use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::{Notify, mpsc};

use super::protocol::{self, Request, Response};
use super::status::AgentStats;
use crate::cli::{ConnectionConfig, Settings, SocketMode};
use crate::connection::{
    self, COMMAND_TIMEOUT, Connection, FrameAssembler, FrameKind, Notifications,
};
use crate::payload;

/// What the connection handlers need to answer a request without troubling the
/// actor: which device this agent holds, and its live counters.
struct AgentInfo {
    addr: BDAddr,
    stats: Arc<AgentStats>,
}

/// How many measurements may queue for one client before it is dropped.
///
/// The queue absorbs a client that is briefly slow to read its socket. Past that
/// it would be absorbing one that has stopped reading altogether, which nothing
/// bounds — a single wedged subscriber would cost the agent unbounded memory.
/// Sized for a minute at the default one-second interval.
const CLIENT_QUEUE_LEN: usize = 64;

struct ActorCommand {
    request: Request,
    tx: mpsc::Sender<Response>,
}

/// Owns the socket and pid file for as long as the agent is serving. Every exit
/// path below — an early `?`, a signal during the initial connect, `agent stop`
/// — has to take both files with it, which a guard says once rather than at each
/// `return`. Removal only; reporting how the agent stopped is `run`'s job.
struct AgentFiles<'a> {
    paths: &'a super::AgentPaths,
    /// Held for the guard's lifetime, so the claim on the pid file lasts exactly
    /// as long as the agent is serving.
    pid: PidFile,
}

impl<'a> AgentFiles<'a> {
    fn new(paths: &'a super::AgentPaths, pid: PidFile) -> Self {
        Self { paths, pid }
    }

    /// Record our pid, now that the agent is committed to serving.
    fn write_pid(&self) -> Result<()> {
        self.pid.write(&self.paths.pid)
    }
}

impl Drop for AgentFiles<'_> {
    fn drop(&mut self) {
        self.paths.remove_files();
    }
}

/// The agent's pid file, held open for as long as the agent runs.
///
/// The open file is itself the claim: an exclusive `flock` on it is what tells a
/// live agent from a file a dead one left behind, and the kernel drops it however
/// the process exits. A pid *written inside* a file cannot do that job — it can
/// name a recycled pid, and two agents starting at the same moment can both read
/// it, both conclude nothing is running, and unlink each other's socket.
#[derive(Debug)]
struct PidFile(std::fs::File);

impl PidFile {
    /// Claim the pid file, failing when another agent already holds it.
    ///
    /// The open refuses to follow a symlink: `--pid-file` can name any path, so
    /// someone who can predict it must not be able to turn the write into a
    /// clobber of an unrelated file the agent can write.
    ///
    /// Nothing is written yet. The startup checks after this one can still bail,
    /// and until they pass, what a previous agent left in the file says more
    /// than a pid of ours that is about to stop being true.
    fn claim(paths: &super::AgentPaths) -> Result<Self> {
        let path = &paths.pid;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(libc::O_NOFOLLOW)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;

        // SAFETY: `flock` on a descriptor this function owns and keeps alive for
        // the returned value's lifetime.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(Self(file));
        }

        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::WouldBlock {
            match paths.read_pid() {
                Some(pid) => bail!("agent is already running (pid {pid})"),
                None => bail!("agent is already running"),
            }
        }
        // A filesystem that cannot lock is not worth refusing to start over: the
        // socket probe below still catches the ordinary case.
        eprintln!(
            "[WARN] Failed to lock {}: {error}; a concurrent `agent start` will not be detected",
            path.display()
        );
        Ok(Self(file))
    }

    /// Write our pid over whatever the file held.
    fn write(&self, path: &Path) -> Result<()> {
        let mut file = &self.0;
        file.set_len(0)
            .and_then(|()| file.write_all(std::process::id().to_string().as_bytes()))
            .and_then(|()| file.flush())
            .with_context(|| format!("failed to write {}", path.display()))
    }
}

/// Take over the agent's files and socket, in the one order that is safe — kept
/// in one function rather than strung through `run`.
///
/// The guard comes back with the listener because the two belong to the same
/// claim: whoever holds the guard is the agent, and dropping it removes both
/// files.
async fn take_over(
    paths: &super::AgentPaths,
    mode: SocketMode,
) -> Result<(AgentFiles<'_>, UnixListener)> {
    // The directory first: the pid file the claim below opens lives in it.
    ensure_socket_dir(&paths.socket)?;
    // Before anything is unlinked, so two agents starting at once cannot each
    // decide the other is not there.
    let pid_file = PidFile::claim(paths)?;
    cleanup_stale(paths).await?;
    // Before the bind, not after: claiming the pid file created it, so from here
    // on a failure has a file to take with it. `cleanup_stale` has already
    // unlinked any leftover socket, so this guard never removes someone else's.
    let files = AgentFiles::new(paths, pid_file);
    let listener = bind_socket(&paths.socket, mode)?;
    files.write_pid()?;
    Ok((files, listener))
}

pub async fn run(
    config: &ConnectionConfig,
    settings: &Settings,
    paths: &super::AgentPaths,
) -> Result<()> {
    // Registered before anything is created, so no window exists where a signal
    // still has its default disposition and kills the process between the bind
    // and the guard below. One arriving before anything polls these is queued
    // rather than lost.
    let mut signals = Shutdown::new();

    // Bound for the whole of `run`: the guard's `Drop` is what takes the socket
    // and pid file with it, however this returns.
    let (_files, listener) = take_over(paths, settings.socket_mode()).await?;

    eprintln!("[INFO] Agent listening on {}", paths.socket.display());

    // Claimed before the connect: a port already in use should fail now rather
    // than after the tens of seconds a BLE connect can take.
    let metrics = match settings.metrics_listen {
        Some(addr) => {
            let listener = super::metrics::bind(addr).await?;
            eprintln!("[INFO] Metrics endpoint listening on http://{addr}/metrics");
            Some(listener)
        }
        None => None,
    };
    let stats = Arc::new(AgentStats::new(config.interval, settings.metrics_listen));

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

    // Recorded before anything has been read: otherwise an agent sitting idle
    // with a healthy connection — nobody streaming, no endpoint polling — would
    // report itself disconnected until the first measurement.
    stats.set_connected(true);

    // Started only now that there is something to serve, so a scrape landing
    // during the connect is refused outright rather than answered with `up 0`
    // by an agent that has not finished starting.
    let metrics_task =
        metrics.map(|listener| tokio::spawn(super::metrics::serve(listener, Arc::clone(&stats))));

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ActorCommand>();
    // Two separate signals rather than one: the accept loop must stop first so
    // the actor can finish in-flight work. `notify_one` also leaves a permit
    // behind when the target is momentarily not parked on `notified()`, which a
    // shared `notify_waiters` would drop on the floor.
    let shutdown = Arc::new(Notify::new());
    let actor_shutdown = Arc::new(Notify::new());

    let info = Arc::new(AgentInfo {
        addr: config.addr,
        stats: Arc::clone(&stats),
    });
    // A serving endpoint keeps the device polled even with no client attached:
    // an exporter that only sampled while someone watched would have nothing to
    // serve the scraper it exists for.
    let poll_always = metrics_task.is_some();

    let mut actor = tokio::spawn(actor_loop(
        conn,
        cmd_rx,
        actor_shutdown.clone(),
        stats,
        poll_always,
    ));
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
    if let Some(task) = metrics_task {
        task.abort();
    }
    // Reached only after the agent actually served, so a startup failure does
    // not report a clean stop on its way out.
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

/// Refuse to start when a previous agent is still alive, and clear the socket it
/// left behind when it is not.
///
/// The pid file claim has already ruled out another agent that takes it. A socket
/// that answers catches the one case it cannot: an agent from before the claim
/// existed, still holding the device across an upgrade in place. Without this,
/// that agent's socket would be unlinked and the two would fight for the device.
async fn cleanup_stale(paths: &super::AgentPaths) -> Result<()> {
    if super::probe_daemon(paths).await.is_some() {
        bail!("agent is already running");
    }

    // The socket alone: the pid file is the one this process holds the claim on,
    // and it is rewritten rather than removed.
    std::fs::remove_file(&paths.socket).ok();
    Ok(())
}

/// Bind the agent socket, and have it carry exactly `mode` from the moment it
/// exists.
///
/// Both halves are needed. `bind` derives the mode from the umask, so a `chmod`
/// on its own would leave a window in which the socket is reachable at whatever
/// the inherited umask allowed; narrowing the umask first closes it. The `chmod`
/// then makes the mode exactly the one asked for rather than at most it, since a
/// umask can only clear bits.
fn bind_socket(sock: &Path, mode: SocketMode) -> Result<UnixListener> {
    let listener = {
        let _umask = Umask::narrowed_to(mode);
        UnixListener::bind(sock).with_context(|| format!("failed to bind {}", sock.display()))?
    };
    std::fs::set_permissions(sock, std::fs::Permissions::from_mode(mode.bits()))
        .with_context(|| format!("failed to set mode {mode} on {}", sock.display()))?;
    Ok(listener)
}

/// The process umask, restored when the guard drops.
///
/// Process-wide, and so only safe to touch because nothing else is creating
/// files at this point: the runtime directory and the pid file are already in
/// place, and no task that could race with it is spawned until the socket is
/// bound.
struct Umask(libc::mode_t);

impl Umask {
    /// Mask off every permission bit `mode` does not grant.
    fn narrowed_to(mode: SocketMode) -> Self {
        let mask = !(mode.bits() as libc::mode_t) & 0o777;
        // SAFETY: `umask` cannot fail and touches nothing but this process's
        // own mask, which the guard puts back.
        Self(unsafe { libc::umask(mask) })
    }
}

impl Drop for Umask {
    fn drop(&mut self) {
        // SAFETY: as above, restoring what the process started with.
        unsafe { libc::umask(self.0) };
    }
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
                        tokio::time::sleep(super::ACCEPT_BACKOFF).await;
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

    let Ok(Some(line)) = lines.next_line().await else {
        return;
    };

    let request: Request = match serde_json::from_str(&line) {
        Ok(r) => r,
        Err(e) => {
            let resp = Response::Error {
                message: format!("invalid request: {e}"),
            };
            protocol::write_message(&mut writer, &resp).await.ok();
            return;
        }
    };

    if let Some(response) = control_response(&request, cmd_tx.is_closed(), &info) {
        if protocol::write_message(&mut writer, &response)
            .await
            .is_ok()
            && matches!(request, Request::Shutdown)
        {
            shutdown.notify_one();
        }
        return;
    }

    let (resp_tx, mut resp_rx) = mpsc::channel::<Response>(CLIENT_QUEUE_LEN);
    let cmd = ActorCommand {
        request,
        tx: resp_tx,
    };

    if cmd_tx.send(cmd).is_err() {
        let resp = Response::Error {
            message: "agent shutting down".to_string(),
        };
        protocol::write_message(&mut writer, &resp).await.ok();
        return;
    }

    while let Some(resp) = resp_rx.recv().await {
        if protocol::write_message(&mut writer, &resp).await.is_err() {
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
        // The address rides along so a client can tell whether the agent it
        // found holds the device its --addr asked for; the status comes from
        // shared counters, so answering here costs nothing.
        Request::Ping => Some(Response::Pong {
            addr: Some(info.addr.to_string()),
            status: Some(info.stats.snapshot()),
        }),
        Request::Shutdown => Some(Response::Ok),
        _ => None,
    }
}

/// A failure that costs us the notification stream. The streaming clients are
/// dropped and the link re-established on the next request.
type StreamError = String;

/// The clients currently streaming measurements. The device is polled once per
/// interval whatever the audience, so a second subscriber costs nothing.
///
/// Owns the reported count alongside the list, which is what keeps
/// `agent status` from drifting out of step with it.
struct Clients {
    list: Vec<mpsc::Sender<Response>>,
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

    fn add(&mut self, tx: mpsc::Sender<Response>) {
        self.list.push(tx);
        self.publish_count();
    }

    /// Hand `response` to everyone still listening. One client going away must
    /// not interrupt anyone else's feed, so every send is attempted and the
    /// departures are swept up in the same pass.
    fn broadcast(&mut self, response: &Response) {
        let before = self.list.len();
        let mut lagging = 0;

        self.list.retain(|tx| match tx.try_send(response.clone()) {
            Ok(()) => true,
            // Hung up.
            Err(mpsc::error::TrySendError::Closed(_)) => false,
            // Still connected, but no longer reading its socket. Waiting would
            // hold samples for a client that may never read again; dropping it
            // closes the socket, which is how it learns its feed ended.
            Err(mpsc::error::TrySendError::Full(_)) => {
                lagging += 1;
                false
            }
        });

        if lagging > 0 {
            eprintln!(
                "[WARN] Dropped {lagging} client(s) that stopped reading, \
                 having fallen {CLIENT_QUEUE_LEN} measurements behind"
            );
        }
        if self.list.len() != before {
            self.publish_count();
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

    /// Report a failure to everyone and forget them. Best-effort: a client whose
    /// queue is full has stopped reading, and dropping its sender tells it as
    /// much anyway.
    fn abort(&mut self, reason: &StreamError) {
        for tx in self.take() {
            tx.try_send(Response::Error {
                message: reason.clone(),
            })
            .ok();
        }
    }

    /// Tell everyone the stream is over and forget them. They read a closed
    /// socket as a protocol error, not as a normal stop.
    fn end(&mut self) {
        for tx in self.take() {
            tx.try_send(Response::StreamEnd).ok();
        }
    }

    fn take(&mut self) -> Vec<mpsc::Sender<Response>> {
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
/// Any number of clients stream at once, all fed from the same poll, with
/// one-shot commands interleaved on the same link.
struct Actor {
    conn: Connection,
    notifications: Option<Notifications>,
    assembler: FrameAssembler,
    ticker: tokio::time::Interval,
    /// Everyone currently subscribed; one poll of the device answers all of them.
    clients: Clients,
    monitoring_payload: Vec<u8>,
    stats: Arc<AgentStats>,
    /// Keep polling even with no client attached, for the metrics endpoint.
    poll_always: bool,
}

async fn actor_loop(
    conn: Connection,
    mut cmd_rx: mpsc::UnboundedReceiver<ActorCommand>,
    shutdown: Arc<Notify>,
    stats: Arc<AgentStats>,
    poll_always: bool,
) {
    // With the endpoint serving, nothing may ever ask for a subscription, so
    // the actor polls on its own from the first tick — which `interval` fires
    // straight away, and which takes the subscription out itself.
    let mut actor = Actor::new(conn, stats, poll_always);

    loop {
        let event = tokio::select! {
            _ = shutdown.notified() => break,
            cmd = cmd_rx.recv() => Event::Command(cmd),
            _ = actor.ticker.tick(), if actor.wants_poll() => Event::Tick,
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
    fn new(conn: Connection, stats: Arc<AgentStats>, poll_always: bool) -> Self {
        let mut ticker = tokio::time::interval(conn.interval());
        // A poll that overruns its period — a write that retries through a
        // reconnect, say — must not leave a backlog of ticks to fire
        // back-to-back. The default would answer a minute of failed writes with
        // a minute of instant retries, unattended when the endpoint is serving.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Self {
            conn,
            notifications: None,
            assembler: FrameAssembler::new(),
            ticker,
            clients: Clients::new(Arc::clone(&stats)),
            monitoring_payload: payload::monitoring(),
            stats,
            poll_always,
        }
    }

    /// Whether the device should be polled: somebody is streaming, or the
    /// metrics endpoint needs a current reading to serve.
    fn wants_poll(&self) -> bool {
        self.poll_always || !self.clients.is_empty()
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
    /// Every path that gives up on streaming goes through here.
    fn abort_stream(&mut self, reason: StreamError) {
        eprintln!("[ERR] {reason}");
        self.notifications = None;
        self.stats.set_connected(false);
        self.clients.abort(&reason);
    }

    /// Ask the device for a measurement. Only runs while something wants one.
    async fn poll_device(&mut self) {
        self.clients.reap();
        if !self.wants_poll() {
            return;
        }

        // An aborted stream leaves no subscription behind, and when the endpoint
        // is what keeps the polling going nobody else will take one out. Routed
        // through `abort_stream` rather than just logged, so a link this agent
        // cannot subscribe to is not still reported as connected.
        if self.notifications.is_none()
            && let Err(e) = self.relisten().await
        {
            self.abort_stream(e);
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
                // `reconnect_stream` already logged the attempt; this is the
                // fatal failure after it.
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
            // Recorded even with no client attached: the metrics endpoint serves
            // this, and `agent status` counts it.
            stats.record_sample(&m);
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
                let idle = !self.wants_poll();
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

            // Both the parse and the frame can reject the time — the wire format
            // carries the year in a single byte — and either way the client is
            // owed the reason rather than a wrongly set clock.
            Request::SetRtc { time } => {
                let frame = chrono::DateTime::parse_from_rfc3339(&time)
                    .map_err(|e| format!("invalid time: {e}"))
                    .and_then(|t| {
                        payload::rtc(&t.with_timezone(&chrono::Local)).map_err(|e| e.to_string())
                    });
                match frame {
                    Ok(p) => self.command(&p, &cmd.tx).await,
                    Err(e) => send_error(&cmd.tx, e),
                }
            }
        }
    }

    /// Send a one-shot command frame and reply with its status byte.
    async fn command(&mut self, cmd_payload: &[u8], tx: &mpsc::Sender<Response>) {
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
    /// kind. A streaming measurement can arrive while a one-shot command is in
    /// flight, so those are forwarded to the clients rather than dropped.
    ///
    /// Shares `self.assembler` on purpose: a second one would split a
    /// part-received frame across the two, leaving both halves to resynchronize
    /// on CRC failures — a burst of warnings and dropped samples every time a
    /// command interrupts a stream.
    async fn wait_for_frame(&mut self, kind: FrameKind) -> Result<Vec<u8>, StreamError> {
        // Destructured so the fields can be borrowed independently: the stream
        // and assembler mutably, the clients for the forwarding closure.
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
                stats.record_sample(&m);
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

fn send_error(tx: &mpsc::Sender<Response>, message: String) {
    tx.try_send(Response::Error { message }).ok();
}

/// Answer a one-shot request. `try_send` cannot fail for want of room here: the
/// channel is this request's own, and one reply is all that is ever put in it.
fn reply(tx: &mpsc::Sender<Response>, result: Result<Response, StreamError>) {
    let resp = result.unwrap_or_else(|message| Response::Error { message });
    tx.try_send(resp).ok();
}

#[cfg(test)]
mod tests {
    use super::super::testutil::TempPath;
    use super::*;

    fn test_stats() -> Arc<AgentStats> {
        Arc::new(AgentStats::new("1s".parse().unwrap(), None))
    }

    /// A subscriber and the receiving end it would be streaming to.
    fn subscriber() -> (mpsc::Sender<Response>, mpsc::Receiver<Response>) {
        mpsc::channel(CLIENT_QUEUE_LEN)
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

    /// The point of the client list: one poll of the device feeds everyone.
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

    /// A client that stops reading is dropped rather than queued for without
    /// limit, and the others carry on unaffected.
    #[test]
    fn a_client_that_stops_reading_is_dropped_not_queued_for() {
        let stats = test_stats();
        let mut clients = Clients::new(Arc::clone(&stats));

        let (tx_stuck, _rx_stuck) = subscriber();
        let (tx_reads, mut rx_reads) = subscriber();
        clients.add(tx_stuck);
        clients.add(tx_reads);

        // `_rx_stuck` is held but never read, so its queue fills; the other end
        // is drained every round, so it never does.
        for i in 0..CLIENT_QUEUE_LEN + 1 {
            clients.broadcast(&sample(i as f64));
            assert_eq!(rx_reads.try_recv().map(|r| wattage(&r)), Ok(Some(i as f64)));
        }

        assert_eq!(
            stats.snapshot().clients,
            1,
            "the stuck client is still held"
        );
        assert!(!clients.is_empty(), "the reading client was not disturbed");
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

    /// The shared counters are what let the connection handler answer while the
    /// actor is busy on the link.
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

        // With the actor gone the ping becomes an error rather than a pong that
        // looks healthy.
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

    /// Paths whose pid file is the temp path itself, so the guard cleans it up.
    fn pid_paths(temp: &TempPath) -> super::super::AgentPaths {
        super::super::paths_from_socket(temp.path().with_extension("sock"))
    }

    #[test]
    fn pid_file_is_written_private() {
        let temp = TempPath::new(".pid");
        let paths = pid_paths(&temp);

        let _guard = umask_guard();
        let claim = PidFile::claim(&paths).unwrap();
        claim.write(&paths.pid).unwrap();

        assert_eq!(paths.read_pid(), Some(std::process::id()));
        assert_eq!(mode_of(temp.path()), 0o600);
    }

    /// The pid file is a claim rather than a note: while one agent holds it, no
    /// second agent may start, whatever the file happens to contain. Without
    /// that, two `agent start`s racing each other both read the file, both
    /// conclude nothing is running, and both unlink the other's socket.
    #[test]
    fn the_pid_file_is_an_exclusive_claim() {
        let temp = TempPath::new(".pid");
        let paths = pid_paths(&temp);

        let held = PidFile::claim(&paths).expect("the first claim succeeds");
        held.write(&paths.pid).unwrap();

        let err = PidFile::claim(&paths).unwrap_err().to_string();
        assert!(err.contains("already running"), "{err}");
        assert!(
            err.contains(&std::process::id().to_string()),
            "the holder's pid names who to go and look at: {err}"
        );

        // Released with the file — by the kernel, so however the holder exits.
        drop(held);
        assert!(PidFile::claim(&paths).is_ok());
    }

    /// A stale pid file must not lock the agent out for good: nothing holds the
    /// claim, so it is taken and overwritten.
    #[test]
    fn a_leftover_pid_file_is_claimed_and_overwritten() {
        let temp = TempPath::new(".pid");
        let paths = pid_paths(&temp);
        std::fs::write(temp.path(), b"999999").unwrap();

        let claim = PidFile::claim(&paths).expect("nothing holds a leftover file");
        claim.write(&paths.pid).unwrap();
        assert_eq!(paths.read_pid(), Some(std::process::id()));
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

        assert!(PidFile::claim(&pid_paths(&temp)).is_err());
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

    /// A bind takes its mode from the umask, which an agent holding a mains
    /// switch must not depend on. A configured mode is carried exactly as
    /// written, and — thanks to the narrowed umask — never wider in between.
    #[tokio::test]
    async fn the_socket_is_bound_at_the_configured_mode() {
        for text in ["0600", "0660", "0666"] {
            let mode: SocketMode = text.parse().unwrap();
            let temp = TempPath::new(".sock");

            let _guard = umask_guard();
            let _listener = bind_socket(temp.path(), mode).unwrap();
            assert_eq!(mode_of(temp.path()), mode.bits(), "binding at {text}");
        }
    }

    /// A config file that says nothing about the socket gets owner-only.
    #[tokio::test]
    async fn the_default_socket_mode_admits_its_owner_alone() {
        let temp = TempPath::new(".sock");

        let _guard = umask_guard();
        let _listener = bind_socket(temp.path(), Settings::default().socket_mode()).unwrap();
        assert_eq!(mode_of(temp.path()), 0o600);
    }

    /// The umask a bind narrows is process-wide, so the tests that assert a mode
    /// take turns rather than reading one another's window.
    fn umask_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn socket_dir_is_created_private() {
        let temp = TempPath::new(".d");
        let sock = temp.path().join("deeper/a.sock");
        let dir = sock.parent().expect("socket path has a parent");

        let _guard = umask_guard();
        ensure_socket_dir(&sock).unwrap();
        assert_eq!(mode_of(dir), 0o700);

        // An existing directory is accepted as it is, not re-permissioned.
        ensure_socket_dir(&sock).unwrap();
    }
}
