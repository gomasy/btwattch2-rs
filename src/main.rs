mod agent;
mod cli;
mod connection;
mod output;
mod payload;

use std::future::Future;
use std::ops::ControlFlow;
use std::time::Duration;

use anyhow::Result;
use chrono::Local;
use clap::Parser;

use agent::protocol::{Request, Response};
use cli::{AgentAction, Cli, Command, LogLevel, Mode};
use connection::{Connection, Measurement, ScannedDevice};
use output::StreamRenderer;

const DEFAULT_SCAN_WINDOW: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = cli.load_config()?;
    let paths = cli.agent_paths();

    if let Some(Command::Agent { action }) = &cli.command {
        return run_agent_command(action, &cli, cfg.as_ref(), &paths).await;
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

    if agent::is_daemon_available(&paths).await {
        return run_via_daemon(mode, &cli, log_level, &paths).await;
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
    paths: &agent::AgentPaths,
) -> Result<()> {
    match action {
        AgentAction::Start => {
            let conn_cfg = cli.connection_config(cfg)?;
            let log_level = cli.log_level(false);
            connection::set_log_level(log_level);
            agent::server::run(&conn_cfg, paths).await
        }
        AgentAction::Stop => {
            if !agent::is_daemon_available(paths).await {
                eprintln!("Agent is not running");
                return Ok(());
            }
            agent::client::send_shutdown(paths).await?;
            eprintln!("Agent stopped");
            Ok(())
        }
        AgentAction::Status => {
            if agent::is_daemon_available(paths).await {
                let pid = std::fs::read_to_string(&paths.pid).unwrap_or_default();
                println!("Agent is running (pid {})", pid.trim());
            } else {
                println!("Agent is not running");
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
            agent::client::execute(&Request::GetRtc, paths, |resp| {
                match resp.to_measurement() {
                    Some(m) => print_rtc_drift(&m),
                    None => {
                        if let Response::Error { message } = resp {
                            eprintln!("[ERR] {message}");
                        }
                    }
                }
                ControlFlow::Break(())
            })
            .await
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
                } else if let Response::Error { message } = resp {
                    eprintln!("[ERR] {message}");
                    ControlFlow::Break(())
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
    agent::client::execute(req, paths, |resp| {
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
                print_rtc_drift(&m);
                ControlFlow::Break(())
            })
            .await
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
