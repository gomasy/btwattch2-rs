use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use btleplug::api::BDAddr;
use chrono::{DateTime, Local, NaiveDateTime, TimeDelta, TimeZone};
use clap::{Args, Parser, Subcommand, ValueEnum};

pub const DEFAULT_INDEX: usize = 0;
pub const DEFAULT_INTERVAL: u64 = 1;

/// How measurements are rendered to stdout.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable `V = .., A = .., W = ..` lines.
    Plain,
    /// One JSON object per measurement (JSON Lines).
    Json,
    /// CSV with a header row.
    Csv,
    /// Label: value tab-separated lines.
    Ltsv,
    /// Prometheus / OpenMetrics text exposition format.
    Prometheus,
    /// Mackerel custom metrics (`name.metric<TAB>value<TAB>epoch`).
    Mackerel,
}

/// Verbosity of informational messages on stderr.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum LogLevel {
    /// No informational messages.
    Off,
    /// Print `[INFO]` progress messages.
    Info,
}

/// Options needed to reach the device, whatever the mode. Also the shape of
/// the config file: `load_config` parses into this and `or` overlays the CLI.
#[derive(Args, Debug, Default)]
pub struct ConnectOpts {
    /// Specify adapter index, e.g. hci0 [default: 0].
    #[arg(short, long, value_name = "index")]
    pub index: Option<usize>,

    /// Specify the destination address.
    #[arg(short, long, value_name = "addr")]
    pub addr: Option<BDAddr>,

    /// Specify the seconds to wait between updates [default: 1].
    #[arg(short = 'n', long, value_name = "second(s)",
          value_parser = clap::value_parser!(u64).range(1..))]
    pub interval: Option<u64>,
}

impl ConnectOpts {
    /// Overlay `self` on `fallback`: any field set here wins.
    fn or(&self, fallback: Option<&ConnectOpts>) -> ConnectOpts {
        ConnectOpts {
            index: self.index.or_else(|| fallback.and_then(|c| c.index)),
            addr: self.addr.or_else(|| fallback.and_then(|c| c.addr)),
            interval: self.interval.or_else(|| fallback.and_then(|c| c.interval)),
        }
    }
}

/// Resolved connection parameters after merging the config file with the CLI.
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub index: usize,
    pub addr: BDAddr,
    pub interval: u64,
}

/// Toolkit for the RS-BTWATTCH2 Bluetooth power meter.
#[derive(Parser, Debug)]
pub struct Cli {
    #[command(flatten)]
    pub connect: ConnectOpts,

    /// Path to a config file (`key = value` lines). Defaults to
    /// $XDG_CONFIG_HOME/btwattch2/config.toml or ~/.config/btwattch2/config.toml.
    #[arg(short = 'c', long, value_name = "path")]
    pub config: Option<PathBuf>,

    /// Turn on the power switch.
    #[arg(long, group = "mode")]
    pub on: bool,

    /// Turn off the power switch.
    #[arg(long, group = "mode")]
    pub off: bool,

    /// Specify the time to set to RTC.
    #[arg(long, value_name = "time", value_parser = parse_time, group = "mode")]
    pub set_rtc: Option<DateTime<Local>>,

    /// Set the current time of this system to RTC.
    #[arg(long, group = "mode")]
    pub set_rtc_now: bool,

    /// Blink the LED on the main unit.
    #[arg(long, group = "mode")]
    pub test_led: bool,

    /// Print a measurement as Mackerel custom metrics and exit.
    #[arg(long, value_name = "name", group = "mode")]
    pub metric_name: Option<String>,

    /// Scan for nearby BTWATTCH2 devices and list them, then exit.
    #[arg(long, group = "mode")]
    pub scan: bool,

    /// Read the device RTC and report its drift from the system clock.
    #[arg(long, group = "mode")]
    pub get_rtc: bool,

    /// How to render measurements.
    #[arg(long, value_enum, value_name = "format")]
    pub format: Option<OutputFormat>,

    /// Stop after this many measurements.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u64).range(1..))]
    pub count: Option<u64>,

    /// Stop after this many seconds (with --scan: the scan window).
    #[arg(long, value_name = "seconds", value_parser = clap::value_parser!(u64).range(1..))]
    pub duration: Option<u64>,

    /// Print informational messages to stderr (suppressed by default when
    /// --metric-name is given).
    #[arg(short, long)]
    pub debug: bool,

    /// Suppress informational messages on stderr.
    #[arg(short, long)]
    pub quiet: bool,

    /// Set the verbosity of informational messages. Overrides --debug/--quiet.
    #[arg(long, value_enum, value_name = "level")]
    pub log_level: Option<LogLevel>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Manage the persistent connection agent.
    Agent {
        #[command(subcommand)]
        action: AgentAction,
    },
}

#[derive(Subcommand, Debug)]
pub enum AgentAction {
    /// Start the agent daemon (runs in the foreground).
    Start,
    /// Stop a running agent daemon.
    Stop,
    /// Show agent daemon status.
    Status,
}

/// What the invocation asks the tool to do, `--scan` aside (main handles it
/// before connecting). The clap `mode` group guarantees at most one of the
/// flags below is set.
#[derive(Debug)]
pub enum Mode {
    SetRtc(DateTime<Local>),
    GetRtc,
    Power(bool),
    TestLed,
    Metric(String),
    Monitor,
}

impl Mode {
    /// Namespace prefix for rendered metrics.
    pub fn prefix(&self) -> &str {
        match self {
            Mode::Metric(name) => name,
            _ => "btwattch2",
        }
    }
}

impl Cli {
    pub fn mode(&self) -> Mode {
        if self.get_rtc {
            Mode::GetRtc
        } else if self.set_rtc_now {
            Mode::SetRtc(Local::now())
        } else if let Some(time) = self.set_rtc {
            Mode::SetRtc(time)
        } else if self.on {
            Mode::Power(true)
        } else if self.off {
            Mode::Power(false)
        } else if self.test_led {
            Mode::TestLed
        } else if let Some(name) = &self.metric_name {
            Mode::Metric(name.clone())
        } else {
            Mode::Monitor
        }
    }

    /// How many samples a streaming run should take. Metric mode takes a
    /// single sample by default; `--count`/`--duration` extend the run, and
    /// monitor mode streams until stopped.
    pub fn sample_count(&self, mode: &Mode) -> Option<u64> {
        match (matches!(mode, Mode::Metric(_)), self.count, self.duration) {
            (true, None, None) => Some(1),
            _ => self.count,
        }
    }

    /// Effective output format: an explicit `--format` always wins, otherwise
    /// `--metric-name` defaults to Mackerel and everything else to Plain.
    pub fn output_format(&self) -> OutputFormat {
        self.format.unwrap_or(if self.metric_name.is_some() {
            OutputFormat::Mackerel
        } else {
            OutputFormat::Plain
        })
    }

    /// Effective log level, applying the precedence
    /// --log-level > --quiet > --debug > mode default.
    pub fn log_level(&self, is_metric: bool) -> LogLevel {
        if let Some(level) = self.log_level {
            return level;
        }
        if self.quiet {
            return LogLevel::Off;
        }
        if self.debug || !is_metric {
            LogLevel::Info
        } else {
            LogLevel::Off
        }
    }

    /// Adapter index to use, merging the config file under the CLI. Shared by
    /// the scan path (which needs no address) and `connection_config`.
    pub fn adapter_index(&self, cfg: Option<&ConnectOpts>) -> usize {
        self.connect.or(cfg).index.unwrap_or(DEFAULT_INDEX)
    }

    /// Resolve the device address and other connection parameters, merging the
    /// config file (if any) under the CLI. Fails when no address is available.
    pub fn connection_config(&self, cfg: Option<&ConnectOpts>) -> Result<ConnectionConfig> {
        let merged = self.connect.or(cfg);
        Ok(ConnectionConfig {
            index: merged.index.unwrap_or(DEFAULT_INDEX),
            interval: merged.interval.unwrap_or(DEFAULT_INTERVAL),
            addr: merged.addr.ok_or_else(|| {
                anyhow!("no device address given; pass --addr or set it in the config file")
            })?,
        })
    }

    /// Load a config file if one is requested or present at the default path.
    /// Malformed lines, unknown keys, and invalid values are hard errors so a
    /// typo can't silently fall back to defaults.
    pub fn load_config(&self) -> Result<Option<ConnectOpts>> {
        let path = match &self.config {
            Some(p) => {
                if !p.exists() {
                    bail!("config file not found: {}", p.display());
                }
                p.clone()
            }
            None => match default_config_path() {
                Some(p) if p.exists() => p,
                _ => return Ok(None),
            },
        };

        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read config: {}", path.display()))?;

        let mut cfg = ConnectOpts::default();
        for (lineno, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let place = || format!("{}:{}", path.display(), lineno + 1);
            let Some((key, value)) = line.split_once('=') else {
                bail!("{}: expected `key = value`, got: {line}", place());
            };
            let key = key.trim();
            let value = value.trim().trim_matches('"');
            match key {
                "index" => {
                    cfg.index = Some(
                        value
                            .parse()
                            .with_context(|| format!("{}: invalid index: {value}", place()))?,
                    )
                }
                "interval" => {
                    let interval: u64 = value
                        .parse()
                        .with_context(|| format!("{}: invalid interval: {value}", place()))?;
                    if interval == 0 {
                        bail!("{}: interval must be at least 1", place());
                    }
                    cfg.interval = Some(interval);
                }
                "addr" => {
                    cfg.addr = Some(
                        value
                            .parse()
                            .map_err(|e| anyhow!("{}: invalid addr {value}: {e}", place()))?,
                    )
                }
                _ => bail!("{}: unknown key: {key}", place()),
            }
        }
        Ok(Some(cfg))
    }
}

/// $XDG_CONFIG_HOME/btwattch2/config.toml, falling back to
/// ~/.config/btwattch2/config.toml.
fn default_config_path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))?;
    Some(base.join("btwattch2").join("config.toml"))
}

/// Resolve a naive local time to a `DateTime<Local>`, tolerating DST
/// transitions: ambiguous times take the earlier offset, and times inside a
/// DST gap are resolved via the offset in effect one hour later.
pub fn local_datetime(naive: NaiveDateTime) -> Option<DateTime<Local>> {
    Local.from_local_datetime(&naive).earliest().or_else(|| {
        Local
            .from_local_datetime(&(naive + TimeDelta::hours(1)))
            .earliest()
            .map(|t| t - TimeDelta::hours(1))
    })
}

fn parse_time(s: &str) -> Result<DateTime<Local>> {
    if let Ok(time) = DateTime::parse_from_rfc3339(s) {
        return Ok(time.with_timezone(&Local));
    }

    const FORMATS: &[&str] = &[
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
    ];

    FORMATS
        .iter()
        .find_map(|f| NaiveDateTime::parse_from_str(s, f).ok())
        .and_then(local_datetime)
        .ok_or_else(|| anyhow!("unrecognized time format: {s}"))
}
