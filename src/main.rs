mod agent;
mod cli;
mod connection;
mod output;
mod payload;
mod signal;

use std::future::Future;
use std::ops::ControlFlow;
use std::path::Path;
use std::time::Duration;

use anyhow::{Result, bail};
use chrono::Local;
use clap::Parser;

use agent::protocol::{Request, Response};
use cli::{AgentAction, Cli, Command, LogLevel, Mode, ScanMode};
use connection::{Connection, Measurement, ScannedDevice};
use output::StreamRenderer;
use signal::Sigpipe;

const DEFAULT_SCAN_WINDOW: Duration = Duration::from_secs(10);

/// Parse the command line, then hand off to the runtime. Kept synchronous so
/// the SIGPIPE decision lands before any thread is spawned, which is what makes
/// `set_sigpipe` safe to call at all.
fn main() -> Result<()> {
    let cli = Cli::parse();
    signal::set_sigpipe(if cli.is_agent_start() {
        Sigpipe::Ignored
    } else {
        Sigpipe::Fatal
    });

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_cli(cli))
}

async fn run_cli(cli: Cli) -> Result<()> {
    // Resolves the config file and the profile `--device` selects, so every
    // path below — including `agent stop` finding the right socket — agrees on
    // what device this invocation is about.
    let settings = cli.settings()?;
    let paths = cli.agent_paths(&settings);

    if let Some(Command::Agent { action }) = &cli.command {
        return run_agent_command(action, &cli, &settings, &paths).await;
    }

    let mode = cli.mode();
    cli.validate_prefix(&mode)?;

    // Stay quiet in Mackerel mode unless --debug is given, so nothing but
    // metrics reaches mackerel-agent.
    let log_level = cli.log_level(matches!(mode, Mode::Metric(_)));
    connection::set_log_level(log_level);

    if let Some(scan_mode) = cli.scan_mode() {
        let window = cli
            .duration
            .map_or(DEFAULT_SCAN_WINDOW, Duration::from_secs);
        let devices = Connection::scan(cli.adapter_index(&settings), window).await?;
        print_scan(&devices, scan_mode);
        return Ok(());
    }

    if let Some(daemon) = agent::probe_daemon(&paths).await {
        ensure_addr_matches(&cli, &daemon, &paths.socket)?;
        warn_ignored_flags(&cli);
        return run_via_daemon(mode, &cli, log_level, &paths).await;
    }

    let mut conn = Connection::new(&cli.connection_config(&settings)?).await?;

    let result = tokio::select! {
        result = run(&mut conn, mode, &cli, log_level) => result,
        _ = tokio::signal::ctrl_c() => Ok(()),
    };

    let disconnect = conn.disconnect().await;
    result?;
    disconnect
}

/// Stop before handing a command to an agent attached to some other device.
/// Routing is automatic, so without this check `--addr <B>` while an agent
/// holds device A quietly operates A instead — on a mains switch that is not a
/// mistake worth making twice.
fn ensure_addr_matches(cli: &Cli, daemon: &agent::DaemonInfo, socket: &Path) -> Result<()> {
    let (Some(want), Some(have)) = (cli.explicit_addr(), daemon.addr) else {
        return Ok(());
    };
    if want != have {
        bail!(
            "the agent on {} is attached to {have}, but --addr asks for {want}; \
             stop that agent or point --socket at the one holding {want}",
            socket.display()
        );
    }
    Ok(())
}

/// Note the options the agent decides for itself. Not an error: unlike the
/// address, getting one of these wrong cannot operate the wrong device.
fn warn_ignored_flags(cli: &Cli) {
    let warn = |flag: &str| {
        eprintln!(
            "[WARN] {flag} is ignored while the agent is running; the value it was started with applies"
        );
    };
    if cli.connect.interval.is_some() {
        warn("--interval");
    }
    if cli.connect.index.is_some() {
        warn("--index");
    }
}

async fn run_agent_command(
    action: &AgentAction,
    cli: &Cli,
    settings: &cli::Settings,
    paths: &agent::AgentPaths,
) -> Result<()> {
    match action {
        AgentAction::Start => {
            let conn_cfg = cli.connection_config(settings)?;
            let log_level = cli.log_level(false);
            connection::set_log_level(log_level);
            agent::server::run(&conn_cfg, paths).await
        }
        AgentAction::Stop => {
            if agent::probe_daemon(paths).await.is_none() {
                eprintln!("Agent is not running");
                return Ok(());
            }
            agent::client::send_shutdown(paths).await?;
            eprintln!("Agent stopped");
            Ok(())
        }
        AgentAction::Status => {
            if let Some(daemon) = agent::probe_daemon(paths).await {
                // The socket answered, so the agent is up even if its pid file
                // is missing or unreadable; say so rather than print a blank.
                let contents = std::fs::read_to_string(&paths.pid).ok();
                let pid = contents
                    .as_deref()
                    .map(str::trim)
                    .filter(|pid| !pid.is_empty())
                    .unwrap_or("unknown");
                println!("Agent is running (pid {pid})");
                println!("Socket:      {}", paths.socket.display());
                if let Some(addr) = daemon.addr {
                    println!("Attached to: {addr}");
                }
            } else {
                // Name the socket: with a profile per device there are several
                // an invocation could have meant, and which one was checked is
                // the first question when the answer is "not running".
                println!("Agent is not running ({})", paths.socket.display());
            }
            Ok(())
        }
    }
}

async fn run_via_daemon(
    mode: Mode,
    cli: &Cli,
    log_level: LogLevel,
    paths: &agent::AgentPaths,
) -> Result<()> {
    match mode {
        Mode::SetRtc(time) => {
            let req = Request::SetRtc {
                time: time.to_rfc3339(),
            };
            send_daemon_command(&req, "RTC set", paths).await
        }
        Mode::GetRtc => {
            let resp = agent::client::request(&Request::GetRtc, paths).await?;
            let Some(m) = resp.to_measurement() else {
                bail!("agent returned an unexpected RTC response");
            };
            print_rtc_drift(&m);
            Ok(())
        }
        Mode::Power(on) => {
            let action = if on { "Power on" } else { "Power off" };
            send_daemon_command(&Request::Power { on }, action, paths).await
        }
        Mode::TestLed => send_daemon_command(&Request::TestLed, "Blink", paths).await,
        Mode::Metric(_) | Mode::Monitor => {
            let mut renderer = StreamRenderer::new(
                cli.output_format(),
                mode.prefix(),
                cli.sample_count(&mode),
                log_level,
            );

            let work = agent::client::execute(&Request::Subscribe, paths, |resp| {
                if let Some(m) = resp.to_measurement() {
                    renderer.record(&m)
                } else {
                    ControlFlow::Continue(())
                }
            });

            until_deadline(work, cli.duration).await
        }
    }
}

/// Report the device RTC against the system clock.
fn print_rtc_drift(m: &Measurement) {
    let now = Local::now();
    let drift = m.timestamp.signed_duration_since(now);
    println!("device_time = {}", m.timestamp.to_rfc3339());
    println!("system_time = {}", now.to_rfc3339());
    println!("drift_seconds = {}", drift.num_seconds());
}

/// Run `work` until it finishes, `duration` seconds elapse, or Ctrl-C. The
/// early exits are not errors: a `--duration` run ending is a normal stop, and
/// dropping `work` here lets its renderer print the summary.
async fn until_deadline<F>(work: F, duration: Option<u64>) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let deadline = async {
        match duration {
            Some(secs) => tokio::time::sleep(Duration::from_secs(secs)).await,
            None => std::future::pending().await,
        }
    };

    tokio::select! {
        result = work => result,
        _ = deadline => Ok(()),
        _ = tokio::signal::ctrl_c() => Ok(()),
    }
}

async fn send_daemon_command(req: &Request, action: &str, paths: &agent::AgentPaths) -> Result<()> {
    match agent::client::request(req, paths).await? {
        Response::CommandResult { code } => connection::report_command(action, code),
        _ => bail!("agent returned an unexpected command response"),
    }
}

async fn run(conn: &mut Connection, mode: Mode, cli: &Cli, log_level: LogLevel) -> Result<()> {
    match mode {
        Mode::SetRtc(time) => conn.set_rtc(&time).await,
        Mode::GetRtc => {
            let m = conn.measure_once().await?;
            print_rtc_drift(&m);
            Ok(())
        }
        Mode::Power(on) => conn.power(on).await,
        Mode::TestLed => conn.blink_led().await,
        Mode::Metric(_) | Mode::Monitor => {
            let mut renderer = StreamRenderer::new(
                cli.output_format(),
                mode.prefix(),
                cli.sample_count(&mode),
                log_level,
            );
            let work = conn.subscribe_measure(|m| renderer.record(&m));
            until_deadline(work, cli.duration).await
        }
    }
}

fn print_scan(devices: &[ScannedDevice], mode: ScanMode) {
    let listed: Vec<&ScannedDevice> = devices
        .iter()
        .filter(|d| mode == ScanMode::Everything || d.is_watt_checker())
        .collect();

    if listed.is_empty() {
        println!("No devices found.");
    }
    for d in &listed {
        let name = d.name.as_deref().unwrap_or("(unknown)");
        println!("{}\t{}\trssi={}", d.addr, name, d.rssi);
    }

    // Say what was left out, so a meter whose name never arrived does not look
    // like a meter that is not there. Informational, so `--quiet` drops it and
    // the device list stays the only thing on stdout either way.
    let hidden = devices.len() - listed.len();
    if hidden > 0 && connection::info_enabled() {
        eprintln!("[INFO] {hidden} other device(s) hidden; use --scan-all to list them");
    }
}
