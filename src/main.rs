mod agent;
mod cli;
mod connection;
mod output;
mod payload;

use std::ops::ControlFlow;
use std::time::Duration;

use anyhow::Result;
use chrono::Local;
use clap::Parser;

use agent::protocol::{Request, Response};
use cli::{AgentAction, Cli, Command, LogLevel, Mode, OutputFormat};
use connection::{Connection, ScannedDevice};
use output::{Printer, Stats};

const DEFAULT_SCAN_WINDOW: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = cli.load_config()?;

    if let Some(Command::Agent { action }) = &cli.command {
        return run_agent_command(action, &cli, cfg.as_ref()).await;
    }

    let mode = cli.mode();

    // Stay quiet in Mackerel mode unless --debug is given, so nothing but
    // metrics reaches mackerel-agent.
    let log_level = cli.log_level(matches!(mode, Mode::Metric(_)));
    connection::set_log_level(log_level);

    if cli.scan {
        let window = cli
            .duration
            .map_or(DEFAULT_SCAN_WINDOW, Duration::from_secs);
        let devices = Connection::scan(cli.adapter_index(cfg.as_ref()), window).await?;
        print_scan(&devices);
        return Ok(());
    }

    if agent::is_daemon_available().await {
        return run_via_daemon(mode, &cli, log_level).await;
    }

    let mut conn = Connection::new(&cli.connection_config(cfg.as_ref())?).await?;

    tokio::select! {
        result = run(&mut conn, mode, &cli, log_level) => result?,
        _ = tokio::signal::ctrl_c() => {}
    }

    conn.disconnect().await
}

async fn run_agent_command(
    action: &AgentAction,
    cli: &Cli,
    cfg: Option<&cli::ConnectOpts>,
) -> Result<()> {
    match action {
        AgentAction::Start => {
            let conn_cfg = cli.connection_config(cfg)?;
            let log_level = cli.log_level(false);
            connection::set_log_level(log_level);
            agent::server::run(&conn_cfg).await
        }
        AgentAction::Stop => {
            if !agent::is_daemon_available().await {
                eprintln!("Agent is not running");
                return Ok(());
            }
            agent::client::send_shutdown().await?;
            eprintln!("Agent stopped");
            Ok(())
        }
        AgentAction::Status => {
            if agent::is_daemon_available().await {
                let pid_file = agent::pid_path();
                let pid = std::fs::read_to_string(&pid_file).unwrap_or_default();
                println!("Agent is running (pid {})", pid.trim());
            } else {
                println!("Agent is not running");
            }
            Ok(())
        }
    }
}

async fn run_via_daemon(mode: Mode, cli: &Cli, log_level: LogLevel) -> Result<()> {
    match mode {
        Mode::SetRtc(time) => {
            let req = Request::SetRtc {
                time: time.to_rfc3339(),
            };
            send_daemon_command(&req, "RTC set").await
        }
        Mode::GetRtc => {
            agent::client::execute(&Request::GetRtc, |resp| {
                match resp {
                    resp @ Response::Measurement { .. } => {
                        if let Some(m) = resp.to_measurement() {
                            let now = Local::now();
                            let drift = m.timestamp.signed_duration_since(now);
                            println!("device_time = {}", m.timestamp.to_rfc3339());
                            println!("system_time = {}", now.to_rfc3339());
                            println!("drift_seconds = {}", drift.num_seconds());
                        }
                    }
                    Response::Error { ref message } => eprintln!("[ERR] {message}"),
                    _ => {}
                }
                ControlFlow::Break(())
            })
            .await
        }
        Mode::Power(on) => {
            let action = if on { "Power on" } else { "Power off" };
            send_daemon_command(&Request::Power { on }, action).await
        }
        Mode::TestLed => send_daemon_command(&Request::TestLed, "Blink").await,
        Mode::Metric(_) | Mode::Monitor => {
            let prefix = if let Mode::Metric(ref name) = mode {
                name.clone()
            } else {
                "btwattch2".to_string()
            };

            let count = if matches!(mode, Mode::Metric(_)) {
                match (cli.count, cli.duration) {
                    (None, None) => Some(1),
                    (count, _) => count,
                }
            } else {
                cli.count
            };

            let mut printer = Printer::new(cli.output_format(), &prefix);
            let mut stats = Stats::new();
            let mut samples = 0u64;

            let work = agent::client::execute(&Request::Subscribe, |resp| {
                if let Some(m) = resp.to_measurement() {
                    let energy_wh = stats.record(&m);
                    printer.print(&m, energy_wh);
                    samples += 1;
                    if count.is_some_and(|c| samples >= c) {
                        return ControlFlow::Break(());
                    }
                } else if let Response::Error { message } = resp {
                    eprintln!("[ERR] {message}");
                    return ControlFlow::Break(());
                }
                ControlFlow::Continue(())
            });

            match cli.duration {
                Some(secs) => tokio::select! {
                    result = work => result?,
                    _ = tokio::time::sleep(Duration::from_secs(secs)) => {},
                    _ = tokio::signal::ctrl_c() => {},
                },
                None => tokio::select! {
                    result = work => result?,
                    _ = tokio::signal::ctrl_c() => {},
                },
            }

            if log_level == LogLevel::Info {
                stats.print_summary();
            }
            Ok(())
        }
    }
}

async fn send_daemon_command(req: &Request, action: &str) -> Result<()> {
    agent::client::execute(req, |resp| {
        match resp {
            Response::CommandResult { success, code } => {
                if success {
                    eprintln!("[INFO] {action} succeeded");
                } else {
                    eprintln!("[ERR] {action} failed, CODE: {:#04x}", code.unwrap_or(0xff));
                }
            }
            Response::Error { message } => eprintln!("[ERR] {message}"),
            _ => {}
        }
        ControlFlow::Break(())
    })
    .await
}

async fn run(conn: &mut Connection, mode: Mode, cli: &Cli, log_level: LogLevel) -> Result<()> {
    match mode {
        Mode::SetRtc(time) => conn.set_rtc(&time).await,
        Mode::GetRtc => {
            conn.subscribe_measure(|m| {
                let now = Local::now();
                let drift = m.timestamp.signed_duration_since(now);
                println!("device_time = {}", m.timestamp.to_rfc3339());
                println!("system_time = {}", now.to_rfc3339());
                println!("drift_seconds = {}", drift.num_seconds());
                ControlFlow::Break(())
            })
            .await
        }
        Mode::Power(on) => conn.power(on).await,
        Mode::TestLed => conn.blink_led().await,
        Mode::Metric(name) => {
            // One sample by default; --count/--duration extend the run.
            let count = match (cli.count, cli.duration) {
                (None, None) => Some(1),
                (count, _) => count,
            };
            run_stream(
                conn,
                &name,
                cli.output_format(),
                count,
                cli.duration,
                LogLevel::Off,
            )
            .await
        }
        Mode::Monitor => {
            run_stream(
                conn,
                "btwattch2",
                cli.output_format(),
                cli.count,
                cli.duration,
                log_level,
            )
            .await
        }
    }
}

/// Drive a measurement subscription, rendering each sample. `count` stops
/// after N samples; `duration` stops after the given seconds; either way —
/// including Ctrl-C, which cancels this future from main's select — the
/// summary is printed on drop when `log_level` allows it.
async fn run_stream(
    conn: &mut Connection,
    prefix: &str,
    format: OutputFormat,
    count: Option<u64>,
    duration: Option<u64>,
    log_level: LogLevel,
) -> Result<()> {
    /// Prints the summary when dropped, so cancellation paths (Ctrl-C,
    /// --duration timeout) report it too.
    struct SummaryOnDrop {
        stats: Stats,
        enabled: bool,
    }
    impl Drop for SummaryOnDrop {
        fn drop(&mut self) {
            if self.enabled {
                self.stats.print_summary();
            }
        }
    }

    let mut printer = Printer::new(format, prefix);
    let mut summary = SummaryOnDrop {
        stats: Stats::new(),
        enabled: log_level == LogLevel::Info,
    };

    let mut samples = 0u64;
    let work = conn.subscribe_measure(|m| {
        let energy_wh = summary.stats.record(&m);
        printer.print(&m, energy_wh);
        samples += 1;
        if count.is_some_and(|c| samples >= c) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });

    match duration {
        Some(secs) => tokio::select! {
            result = work => result,
            _ = tokio::time::sleep(Duration::from_secs(secs)) => Ok(()),
        },
        None => work.await,
    }
}

fn print_scan(devices: &[ScannedDevice]) {
    if devices.is_empty() {
        println!("No devices found.");
        return;
    }
    for d in devices {
        let name = d.name.as_deref().unwrap_or("(unknown)");
        let rssi = d
            .rssi
            .map(|r| r.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!("{}\t{}\trssi={}", d.addr, name, rssi);
    }
}
