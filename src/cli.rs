use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use btleplug::api::BDAddr;
use chrono::{DateTime, Local, NaiveDateTime, TimeDelta, TimeZone};
use clap::{Args, Parser, Subcommand, ValueEnum};

pub const DEFAULT_INDEX: usize = 0;
pub const DEFAULT_INTERVAL: Interval = Interval(Duration::from_secs(1));

/// Shortest polling period accepted. The device answers a measurement request
/// over BLE, and below a few milliseconds the requests only queue up behind
/// replies that cannot arrive any faster — so the floor is a guard against a
/// value that would look like it worked while merely flooding the link.
const MIN_INTERVAL: Duration = Duration::from_millis(10);

/// How long to wait between measurement requests.
///
/// A newtype rather than a `Duration`, so "positive and not absurdly small" is
/// established once at parse time: the value becomes a `tokio::time::interval`
/// period, which panics on a zero duration. That invariant used to be carried
/// by `NonZeroU64` seconds, which also ruled out every sub-second period.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Interval(Duration);

impl Interval {
    pub fn duration(self) -> Duration {
        self.0
    }

    pub fn as_secs_f64(self) -> f64 {
        self.0.as_secs_f64()
    }
}

impl FromStr for Interval {
    type Err = anyhow::Error;

    /// Accepts seconds (`1`, `0.5`, `2s`) or milliseconds (`500ms`).
    fn from_str(s: &str) -> Result<Self> {
        let text = s.trim();
        // `ms` first: `strip_suffix('s')` would otherwise leave a trailing `m`.
        let (number, scale) = match text.strip_suffix("ms") {
            Some(number) => (number, 1e-3),
            None => (text.strip_suffix('s').unwrap_or(text), 1.0),
        };

        let seconds: f64 = number
            .trim()
            .parse()
            .map_err(|_| anyhow!("invalid interval {s:?}: expected 0.5, 500ms, or 2s"))?;
        let duration = Duration::try_from_secs_f64(seconds * scale)
            .map_err(|e| anyhow!("invalid interval {s:?}: {e}"))?;

        if duration < MIN_INTERVAL {
            bail!("interval {s:?} is shorter than the {MIN_INTERVAL:?} minimum");
        }
        Ok(Self(duration))
    }
}

/// `Duration`'s own `Debug` is the format wanted here — `1s`, `500ms`, `1.5s` —
/// and it round-trips through `FromStr` above.
impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

fn parse_interval(s: &str) -> Result<Interval> {
    s.parse()
}

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

/// Characters a Prometheus metric name may contain. `parse_metric_name` accepts
/// this set plus `.` and `-` for Mackerel's benefit, so keeping the narrow set
/// in one predicate is what makes the subset relation between the two real
/// rather than a claim in a doc comment.
fn prometheus_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | ':')
}

impl OutputFormat {
    /// Reject a metric prefix this format cannot express. Prometheus metric
    /// names allow only `[A-Za-z_:][A-Za-z0-9_:]*`, so a name carrying a `.` or
    /// `-` would emit an exposition no scraper will parse.
    pub fn validate_prefix(self, prefix: &str) -> Result<()> {
        if self == OutputFormat::Prometheus
            && let Some(bad) = prefix.chars().find(|&c| !prometheus_char(c))
        {
            bail!(
                "metric name {prefix:?} contains {bad:?}, which a Prometheus metric name \
                 cannot; use letters, digits, `_`, or `:`"
            );
        }
        Ok(())
    }
}

/// Verbosity of informational messages on stderr.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum LogLevel {
    /// No informational messages.
    Off,
    /// Print `[INFO]` progress messages.
    Info,
}

/// Options needed to reach the device, whatever the mode.
#[derive(Args, Clone, Debug, Default)]
pub struct ConnectOpts {
    /// Specify adapter index, e.g. hci0 [default: 0].
    #[arg(short, long, value_name = "index")]
    pub index: Option<usize>,

    /// Specify the destination address.
    #[arg(short, long, value_name = "addr")]
    pub addr: Option<BDAddr>,

    /// Specify the time to wait between updates, e.g. 2s or 500ms [default: 1s].
    #[arg(short = 'n', long, value_name = "interval", value_parser = parse_interval)]
    pub interval: Option<Interval>,
}

impl ConnectOpts {
    /// Overlay `self` on `fallback`: any field set here wins.
    fn or(&self, fallback: &ConnectOpts) -> ConnectOpts {
        ConnectOpts {
            index: self.index.or(fallback.index),
            addr: self.addr.or(fallback.addr),
            interval: self.interval.or(fallback.interval),
        }
    }
}

/// Everything one device needs: how to reach it, and where the agent holding it
/// keeps its socket and metrics endpoint.
///
/// One type for three roles, because they have the same shape and the same
/// merge rule: a `[devices.*]` section, the config file's own top-level keys,
/// and what an invocation finally runs with once the command line has been
/// overlaid on both.
#[derive(Clone, Debug, Default)]
pub struct Settings {
    pub connect: ConnectOpts,
    pub socket: Option<PathBuf>,
    pub metrics_listen: Option<SocketAddr>,
}

impl Settings {
    /// Overlay `self` on `fallback`: any field set here wins. Used twice, with
    /// the same meaning both times — a `[devices.*]` section over the file's
    /// top-level keys, then the command line over the result.
    fn or(&self, fallback: &Settings) -> Settings {
        Settings {
            connect: self.connect.or(&fallback.connect),
            socket: self.socket.clone().or_else(|| fallback.socket.clone()),
            metrics_listen: self.metrics_listen.or(fallback.metrics_listen),
        }
    }
}

/// Resolved connection parameters after merging the config file with the CLI.
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub index: usize,
    pub addr: BDAddr,
    /// Positive by construction: it becomes a `tokio::time::interval` period,
    /// which panics on a zero duration. Enforcing it in the type keeps that out
    /// of reach instead of resting on the two parsers that feed this struct.
    pub interval: Interval,
}

/// The config file: keys given outside any section, the `[devices.NAME]`
/// sections, and which of them `--device` defaults to.
#[derive(Debug, Default)]
pub struct FileConfig {
    defaults: Settings,
    devices: BTreeMap<String, Settings>,
    default_device: Option<String>,
    path: PathBuf,
}

impl FileConfig {
    /// The profile to use, inheriting the file's top-level keys. `name` is
    /// `--device`; without it the file's `default` applies, and without that
    /// the top-level keys are the whole configuration.
    fn profile(&self, name: Option<&str>) -> Result<Settings> {
        let Some(name) = name.or(self.default_device.as_deref()) else {
            return Ok(self.defaults.clone());
        };
        let device = self.devices.get(name).ok_or_else(|| {
            let known = self.device_names();
            anyhow!(
                "unknown device {name:?} in {}; {known}",
                self.path.display()
            )
        })?;
        Ok(device.or(&self.defaults))
    }

    fn device_names(&self) -> String {
        if self.devices.is_empty() {
            return "the file defines no [devices.*] sections".to_string();
        }
        let names: Vec<&str> = self.devices.keys().map(String::as_str).collect();
        format!("known devices: {}", names.join(", "))
    }
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

    /// Use the named `[devices.<name>]` section of the config file.
    #[arg(long, value_name = "name")]
    pub device: Option<String>,

    /// Path to the agent's unix socket. Defaults to
    /// $XDG_RUNTIME_DIR/btwattch2.sock (or /run/btwattch2/btwattch2.sock).
    #[arg(long, value_name = "path")]
    pub socket: Option<PathBuf>,

    /// Path to the agent's pid file. Defaults to the socket path with a
    /// `.pid` extension, i.e. $XDG_RUNTIME_DIR/btwattch2.pid (or
    /// /run/btwattch2/btwattch2.pid). Overrides the derived location.
    #[arg(long, value_name = "path")]
    pub pid_file: Option<PathBuf>,

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
    #[arg(long, value_name = "name", value_parser = parse_metric_name, group = "mode")]
    pub metric_name: Option<String>,

    /// Scan for nearby BTWATTCH2 devices and list them, then exit.
    #[arg(long, group = "mode")]
    pub scan: bool,

    /// Scan like --scan, but list every Bluetooth device rather than only
    /// watt checkers.
    #[arg(long, group = "mode")]
    pub scan_all: bool,

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
    Start {
        /// Serve Prometheus metrics over HTTP on this address, e.g.
        /// 127.0.0.1:9101. The endpoint is unauthenticated, so bind it to a
        /// loopback address unless something in front of it provides access
        /// control.
        #[arg(long, value_name = "addr")]
        metrics_listen: Option<SocketAddr>,
    },
    /// Stop a running agent daemon.
    Stop,
    /// Show agent daemon status.
    Status,
}

/// Which devices a scan reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanMode {
    /// Only devices whose advertised name marks them as a watt checker.
    WattCheckers,
    /// Every device currently advertising.
    Everything,
}

/// What the invocation asks the tool to do, the scans aside (main handles those
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
    /// Whether this invocation is the long-running daemon, which needs the
    /// opposite SIGPIPE disposition from every short-lived CLI command.
    pub fn is_agent_start(&self) -> bool {
        matches!(
            self.command,
            Some(Command::Agent {
                action: AgentAction::Start { .. }
            })
        )
    }

    /// Whether this invocation only lists nearby devices, and whether it wants
    /// the unfiltered list. `--scan` names watt checkers alone, as its help says;
    /// `--scan-all` is the escape hatch for a device whose name has not arrived.
    pub fn scan_mode(&self) -> Option<ScanMode> {
        match (self.scan, self.scan_all) {
            (true, _) => Some(ScanMode::WattCheckers),
            (_, true) => Some(ScanMode::Everything),
            _ => None,
        }
    }

    /// The address named on the command line, ignoring the config file. Only an
    /// explicit one means "this device and no other"; a configured one is a
    /// default that a running agent legitimately supersedes.
    pub fn explicit_addr(&self) -> Option<BDAddr> {
        self.connect.addr
    }

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

    /// Reject a metric prefix the chosen format cannot represent, before
    /// anything connects to the device.
    pub fn validate_prefix(&self, mode: &Mode) -> Result<()> {
        self.output_format().validate_prefix(mode.prefix())
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

    /// The metrics endpoint asked for on the command line. Only `agent start`
    /// takes one: it is the daemon that would serve it.
    fn metrics_listen_flag(&self) -> Option<SocketAddr> {
        match &self.command {
            Some(Command::Agent {
                action: AgentAction::Start { metrics_listen },
            }) => *metrics_listen,
            _ => None,
        }
    }

    /// Resolve everything the invocation needs from the command line and the
    /// config file, the command line winning. Done once per run so the config
    /// file is read and validated a single time.
    pub fn settings(&self) -> Result<Settings> {
        let file = self.load_config()?;
        let profile = match (&file, &self.device) {
            (Some(file), device) => file.profile(device.as_deref())?,
            (None, None) => Settings::default(),
            // Naming a profile that cannot exist is a typo worth reporting:
            // falling back to the defaults would connect to some other device.
            (None, Some(device)) => bail!(
                "--device {device} needs a config file defining [devices.{device}], and none was found"
            ),
        };

        let cli = Settings {
            connect: self.connect.clone(),
            socket: self.socket.clone(),
            metrics_listen: self.metrics_listen_flag(),
        };
        Ok(cli.or(&profile))
    }

    /// Adapter index to use. Shared by the scan path (which needs no address)
    /// and `connection_config`.
    pub fn adapter_index(&self, settings: &Settings) -> usize {
        settings.connect.index.unwrap_or(DEFAULT_INDEX)
    }

    /// Resolve the device address and other connection parameters. Fails when
    /// no address is available.
    pub fn connection_config(&self, settings: &Settings) -> Result<ConnectionConfig> {
        Ok(ConnectionConfig {
            index: self.adapter_index(settings),
            interval: settings.connect.interval.unwrap_or(DEFAULT_INTERVAL),
            addr: settings.connect.addr.ok_or_else(|| {
                anyhow!("no device address given; pass --addr or set it in the config file")
            })?,
        })
    }

    /// Load a config file if one is requested or present at the default path.
    /// Malformed lines, unknown keys, and invalid values are hard errors so a
    /// typo can't silently fall back to defaults.
    pub fn load_config(&self) -> Result<Option<FileConfig>> {
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

        parse_config(&text, path).map(Some)
    }

    /// Resolve the agent socket/pid paths, honouring `--socket` (or the selected
    /// profile's) and `--pid-file` if given. The pid file defaults to the socket
    /// path with a `.pid` extension, which `--pid-file` overrides.
    pub fn agent_paths(&self, settings: &Settings) -> crate::agent::AgentPaths {
        let mut paths = match &settings.socket {
            Some(s) => crate::agent::paths_from_socket(s.clone()),
            None => crate::agent::default_paths(),
        };
        if let Some(pid) = &self.pid_file {
            paths.pid = pid.clone();
        }
        paths
    }
}

/// Parse a config file: `key = value` lines, optionally grouped into
/// `[devices.NAME]` sections. Keys before the first section are defaults every
/// section inherits, which is also the whole configuration for a file with no
/// sections at all — the only shape that existed before profiles.
fn parse_config(text: &str, path: PathBuf) -> Result<FileConfig> {
    let mut config = FileConfig {
        path,
        ..FileConfig::default()
    };
    // Which profile the keys currently being read belong to. Held by name
    // rather than as a borrow so the map stays writable underneath.
    let mut section: Option<String> = None;

    for (lineno, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let place = || format!("{}:{}", config.path.display(), lineno + 1);

        if let Some(header) = line.strip_prefix('[') {
            let name = header
                .strip_suffix(']')
                .ok_or_else(|| anyhow!("{}: unterminated section header: {line}", place()))?
                .trim();
            let device = name.strip_prefix("devices.").ok_or_else(|| {
                anyhow!(
                    "{}: unknown section [{name}]; only [devices.<name>] is understood",
                    place()
                )
            })?;
            let device = unquote(device.trim());
            if device.is_empty() {
                bail!("{}: a [devices.<name>] section needs a name", place());
            }
            // Re-entering a section adds to it rather than replacing it, which
            // is what TOML does with a repeated table.
            config.devices.entry(device.to_string()).or_default();
            section = Some(device.to_string());
            continue;
        }

        let Some((key, value)) = line.split_once('=') else {
            bail!("{}: expected `key = value`, got: {line}", place());
        };
        let key = key.trim();
        let value = unquote(value.trim());

        // `default` selects a section, so it belongs to the file rather than to
        // any one profile.
        if key == "default" {
            if section.is_some() {
                bail!(
                    "{}: `default` belongs before the first [devices.<name>] section",
                    place()
                );
            }
            config.default_device = Some(value.to_string());
            continue;
        }

        let profile = match &section {
            Some(name) => config
                .devices
                .get_mut(name)
                .expect("the section was inserted when its header was read"),
            None => &mut config.defaults,
        };
        assign(profile, key, value, &place())?;
    }

    // A `default` naming nothing is a typo that would otherwise only surface as
    // the wrong device — or as no device at all — much later.
    if let Some(name) = &config.default_device
        && !config.devices.contains_key(name)
    {
        bail!(
            "{}: default = {name:?} names no [devices.{name}] section; {}",
            config.path.display(),
            config.device_names()
        );
    }
    Ok(config)
}

/// Apply one `key = value` pair to `profile`. Unknown keys are hard errors, so
/// a typo cannot silently leave a default in place.
fn assign(profile: &mut Settings, key: &str, value: &str, place: &str) -> Result<()> {
    match key {
        "index" => {
            profile.connect.index = Some(
                value
                    .parse()
                    .with_context(|| format!("{place}: invalid index: {value}"))?,
            );
        }
        // `Interval` rejects zero and anything below its floor as a parse
        // error, so there is no separate range check to keep in step with clap's.
        "interval" => {
            profile.connect.interval = Some(
                value
                    .parse()
                    .with_context(|| format!("{place}: invalid interval"))?,
            );
        }
        "addr" => {
            profile.connect.addr = Some(
                value
                    .parse()
                    .map_err(|e| anyhow!("{place}: invalid addr {value}: {e}"))?,
            );
        }
        "socket" => profile.socket = Some(PathBuf::from(value)),
        "metrics_listen" => {
            profile.metrics_listen = Some(
                value
                    .parse()
                    .with_context(|| format!("{place}: invalid metrics_listen: {value}"))?,
            );
        }
        _ => bail!("{place}: unknown key: {key}"),
    }
    Ok(())
}

/// Strip one matching pair of double quotes, so `addr = "..."` and `addr = ...`
/// both work. `trim_matches` would peel off every quote at both ends, quietly
/// accepting `""""` and the like; leaving the extras in makes the value fail to
/// parse with an error naming the line, which is the point of this parser.
fn unquote(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(value)
}

/// Accept only metric names every supported backend can carry. Beyond the
/// obviously broken, this keeps a name containing a newline or a tab from
/// forging extra metric lines in the line-oriented formats.
/// The union of what every supported backend accepts: `prometheus_char` plus
/// `.` and `-` for Mackerel. `OutputFormat::validate_prefix` narrows it again
/// per format. Rejecting the rest also keeps a name containing a newline or a
/// tab from forging extra metric lines in the line-oriented formats.
fn parse_metric_name(s: &str) -> Result<String> {
    let mut chars = s.chars();
    let head = chars
        .next()
        .is_some_and(|c| prometheus_char(c) && !c.is_ascii_digit());
    let tail = chars.all(|c| prometheus_char(c) || matches!(c, '.' | '-'));
    if !head || !tail {
        bail!(
            "invalid metric name {s:?}: use a letter, `_`, or `:`, followed by letters, \
             digits, `_`, `:`, `.`, or `-`"
        );
    }
    Ok(s.to_string())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_cli(args: &[&str]) -> Cli {
        Cli::parse_from(std::iter::once("btwattch2").chain(args.iter().copied()))
    }

    #[test]
    fn unquote_strips_one_pair_only() {
        assert_eq!(unquote("\"abc\""), "abc");
        assert_eq!(unquote("abc"), "abc");
        // Leftover quotes are kept so the value fails to parse with a message
        // naming the line, rather than being silently accepted.
        assert_eq!(unquote("\"\"abc\"\""), "\"abc\"");
        assert_eq!(unquote("\"abc"), "\"abc");
    }

    #[test]
    fn metric_name_accepts_backend_safe_names() {
        // `:` is legal in a Prometheus metric name, so it must survive here too.
        for name in ["wattchecker1", "_x", "a.b-c_1", "job:power", ":x"] {
            assert_eq!(parse_metric_name(name).unwrap(), name);
        }
    }

    #[test]
    fn metric_name_rejects_injection_and_bad_leading_chars() {
        // A newline would forge extra lines in every line-oriented format.
        for name in ["", "1abc", "a b", "a\nb.wattage\t0\t0", "a\tb", ".x"] {
            assert!(parse_metric_name(name).is_err(), "accepted {name:?}");
        }
    }

    #[test]
    fn prometheus_rejects_a_prefix_it_cannot_express() {
        let cli = parse_cli(&["--format", "prometheus", "--metric-name", "a.b"]);
        assert!(cli.validate_prefix(&cli.mode()).is_err());
    }

    #[test]
    fn prometheus_accepts_underscored_prefixes_and_the_default() {
        for args in [
            vec!["--format", "prometheus", "--metric-name", "a_b1"],
            vec!["--format", "prometheus"],
        ] {
            let cli = parse_cli(&args);
            assert!(cli.validate_prefix(&cli.mode()).is_ok());
        }
    }

    /// A dotted name is fine for Mackerel, so only Prometheus may reject it.
    #[test]
    fn other_formats_accept_a_dotted_prefix() {
        let cli = parse_cli(&["--metric-name", "a.b"]);
        assert!(cli.validate_prefix(&cli.mode()).is_ok());
    }

    /// Parse `text` as the config file at a throwaway path.
    fn config(text: &str) -> Result<FileConfig> {
        parse_config(text, PathBuf::from("config.toml"))
    }

    /// Resolve `args` against `text` written to a real config file, which is the
    /// only way through `settings` — it reads the file itself.
    fn settings(args: &[&str], text: &str) -> Result<Settings> {
        let temp = crate::agent::testutil::TempPath::new(".toml");
        std::fs::write(temp.path(), text).unwrap();
        let path = temp.path().to_str().unwrap().to_string();
        let args: Vec<&str> = ["-c", &path]
            .into_iter()
            .chain(args.iter().copied())
            .collect();
        parse_cli(&args).settings()
    }

    #[test]
    fn cli_overlays_the_config_file() {
        let text = "index = 1\naddr = \"CB:DF:6B:12:34:56\"\ninterval = 9";
        let settings = settings(&["-n", "3"], text).unwrap();
        let resolved = parse_cli(&["-n", "3"])
            .connection_config(&settings)
            .unwrap();
        assert_eq!(resolved.interval, "3".parse().unwrap());
        assert_eq!(resolved.index, 1);
        assert_eq!(resolved.addr, "CB:DF:6B:12:34:56".parse().unwrap());
    }

    #[test]
    fn connection_config_needs_an_address() {
        assert!(
            parse_cli(&[])
                .connection_config(&Settings::default())
                .is_err()
        );
    }

    #[test]
    fn connection_config_defaults_the_interval_and_index() {
        let settings = Settings {
            connect: ConnectOpts {
                addr: "CB:DF:6B:12:34:56".parse().ok(),
                ..ConnectOpts::default()
            },
            ..Settings::default()
        };
        let resolved = parse_cli(&[]).connection_config(&settings).unwrap();
        assert_eq!(resolved.interval, DEFAULT_INTERVAL);
        assert_eq!(resolved.index, DEFAULT_INDEX);
    }

    #[test]
    fn metric_mode_takes_one_sample_unless_told_otherwise() {
        let cli = parse_cli(&["--metric-name", "x"]);
        assert_eq!(cli.sample_count(&cli.mode()), Some(1));

        let cli = parse_cli(&["--metric-name", "x", "--duration", "10"]);
        assert_eq!(cli.sample_count(&cli.mode()), None);

        // Monitor mode streams until stopped.
        let cli = parse_cli(&[]);
        assert_eq!(cli.sample_count(&cli.mode()), None);
    }

    #[test]
    fn log_level_precedence() {
        assert_eq!(parse_cli(&[]).log_level(false), LogLevel::Info);
        // Metric mode stays quiet so only metrics reach mackerel-agent.
        assert_eq!(parse_cli(&[]).log_level(true), LogLevel::Off);
        assert_eq!(parse_cli(&["-d"]).log_level(true), LogLevel::Info);
        assert_eq!(parse_cli(&["-q"]).log_level(false), LogLevel::Off);
        // --quiet beats --debug, and --log-level beats both.
        assert_eq!(parse_cli(&["-d", "-q"]).log_level(false), LogLevel::Off);
        assert_eq!(
            parse_cli(&["-q", "--log-level", "info"]).log_level(true),
            LogLevel::Info
        );
    }

    #[test]
    fn output_format_defaults_to_mackerel_only_for_metric_mode() {
        assert_eq!(parse_cli(&[]).output_format(), OutputFormat::Plain);
        assert_eq!(
            parse_cli(&["--metric-name", "x"]).output_format(),
            OutputFormat::Mackerel
        );
        assert_eq!(
            parse_cli(&["--metric-name", "x", "--format", "json"]).output_format(),
            OutputFormat::Json
        );
    }

    #[test]
    fn parse_time_accepts_every_documented_format() {
        let expected = local_datetime(
            NaiveDateTime::parse_from_str("2021-01-02 03:04:05", "%Y-%m-%d %H:%M:%S").unwrap(),
        )
        .unwrap();
        for s in [
            "2021-01-02 03:04:05",
            "2021-01-02T03:04:05",
            "2021/01/02 03:04:05",
        ] {
            assert_eq!(parse_time(s).unwrap(), expected, "parsing {s}");
        }
        assert!(parse_time("yesterday").is_err());
    }

    /// Drives the SIGPIPE choice, so only the long-running daemon may say yes:
    /// a CLI wants a closed pipe to be fatal, the agent must survive one.
    #[test]
    fn only_agent_start_is_the_daemon() {
        assert!(parse_cli(&["agent", "start"]).is_agent_start());
        for args in [
            vec!["agent", "stop"],
            vec!["agent", "status"],
            vec!["--scan"],
            vec![],
        ] {
            assert!(!parse_cli(&args).is_agent_start(), "claimed {args:?}");
        }
    }

    #[test]
    fn explicit_addr_ignores_the_config_file() {
        let text = "addr = \"CB:DF:6B:12:34:56\"";
        // Only what the command line asked for counts, so a configured address
        // never makes a running agent look like the wrong device.
        assert_eq!(parse_cli(&[]).explicit_addr(), None);
        assert_eq!(
            parse_cli(&["-a", "CB:DF:6B:AA:BB:CC"]).explicit_addr(),
            "CB:DF:6B:AA:BB:CC".parse().ok()
        );
        assert_eq!(
            settings(&[], text).unwrap().connect.addr,
            "CB:DF:6B:12:34:56".parse().ok()
        );
    }

    #[test]
    fn interval_accepts_sub_second_periods() {
        for (text, expected) in [
            ("1", Duration::from_secs(1)),
            ("2s", Duration::from_secs(2)),
            ("0.5", Duration::from_millis(500)),
            ("0.5s", Duration::from_millis(500)),
            ("500ms", Duration::from_millis(500)),
            (" 250 ms ", Duration::from_millis(250)),
        ] {
            let parsed: Interval = text.parse().unwrap_or_else(|e| panic!("{text:?}: {e}"));
            assert_eq!(parsed.duration(), expected, "parsing {text:?}");
        }
    }

    /// Zero would panic `tokio::time::interval`, and the rest are values that
    /// would otherwise be silently truncated or accepted as nonsense.
    #[test]
    fn interval_rejects_zero_and_junk() {
        for text in ["0", "0s", "0ms", "1ms", "-1", "", "abc", "1m", "NaN", "inf"] {
            assert!(text.parse::<Interval>().is_err(), "accepted {text:?}");
        }
        assert!(Cli::try_parse_from(["btwattch2", "-n", "0"]).is_err());
    }

    /// The format is what `FromStr` accepts, so status output can be pasted
    /// back into `--interval`.
    #[test]
    fn interval_display_round_trips() {
        for text in ["1s", "500ms", "1.5s"] {
            let parsed: Interval = text.parse().unwrap();
            assert_eq!(parsed.to_string(), text);
            assert_eq!(text.parse::<Interval>().unwrap(), parsed);
        }
    }

    #[test]
    fn a_file_without_sections_is_the_whole_configuration() {
        let cfg = config("addr = \"CB:DF:6B:12:34:56\"\ninterval = 500ms").unwrap();
        let profile = cfg.profile(None).unwrap();
        assert_eq!(profile.connect.addr, "CB:DF:6B:12:34:56".parse().ok());
        assert_eq!(profile.connect.interval, "500ms".parse().ok());
    }

    #[test]
    fn a_device_section_inherits_the_top_level_keys() {
        let cfg = config(
            "interval = 2s\n\
             [devices.living]\n\
             addr = \"CB:DF:6B:12:34:56\"\n\
             [devices.rack]\n\
             addr = \"CB:DF:6B:AA:BB:CC\"\n\
             interval = 500ms\n\
             socket = \"/run/btwattch2/rack.sock\"\n\
             metrics_listen = \"127.0.0.1:9101\"\n",
        )
        .unwrap();

        let living = cfg.profile(Some("living")).unwrap();
        assert_eq!(living.connect.addr, "CB:DF:6B:12:34:56".parse().ok());
        assert_eq!(living.connect.interval, "2s".parse().ok());
        assert_eq!(living.socket, None);

        // The section wins over the inherited default.
        let rack = cfg.profile(Some("rack")).unwrap();
        assert_eq!(rack.connect.interval, "500ms".parse().ok());
        assert_eq!(rack.socket, Some(PathBuf::from("/run/btwattch2/rack.sock")));
        assert_eq!(rack.metrics_listen, "127.0.0.1:9101".parse().ok());
    }

    #[test]
    fn default_selects_a_section_when_no_device_is_named() {
        let cfg = config(
            "default = \"rack\"\n\
             [devices.living]\n\
             addr = \"CB:DF:6B:12:34:56\"\n\
             [devices.rack]\n\
             addr = \"CB:DF:6B:AA:BB:CC\"\n",
        )
        .unwrap();
        assert_eq!(
            cfg.profile(None).unwrap().connect.addr,
            "CB:DF:6B:AA:BB:CC".parse().ok()
        );
        // --device still overrides it.
        assert_eq!(
            cfg.profile(Some("living")).unwrap().connect.addr,
            "CB:DF:6B:12:34:56".parse().ok()
        );
    }

    /// Every way of naming a device that does not exist has to fail loudly:
    /// falling back to the defaults would operate some other meter.
    #[test]
    fn a_missing_profile_is_an_error() {
        let cfg = config("[devices.living]\naddr = \"CB:DF:6B:12:34:56\"").unwrap();
        let err = cfg.profile(Some("rack")).unwrap_err().to_string();
        assert!(err.contains("known devices: living"), "{err}");

        assert!(config("default = \"rack\"").is_err());
        assert!(settings(&["--device", "rack"], "addr = \"CB:DF:6B:12:34:56\"").is_err());
    }

    #[test]
    fn config_rejects_malformed_sections_and_keys() {
        for text in [
            "[devices.living",
            "[wattage]",
            "[devices.]",
            "[devices.living]\nunknown = 1",
            "[devices.living]\ndefault = \"living\"",
            "metrics_listen = \"not-an-address\"",
            "interval = 0",
            "addr = nonsense",
            "just a line",
        ] {
            assert!(config(text).is_err(), "accepted {text:?}");
        }
    }

    /// A profile supplies the socket, so `--device` alone routes to the right
    /// agent; an explicit `--socket` still wins.
    #[test]
    fn the_profile_supplies_the_socket() {
        let text = "[devices.rack]\naddr = \"CB:DF:6B:AA:BB:CC\"\nsocket = \"/run/rack.sock\"";
        let cli = parse_cli(&["--device", "rack"]);
        let resolved = settings(&["--device", "rack"], text).unwrap();
        assert_eq!(
            cli.agent_paths(&resolved).socket,
            PathBuf::from("/run/rack.sock")
        );

        let overridden =
            settings(&["--device", "rack", "--socket", "/run/other.sock"], text).unwrap();
        assert_eq!(overridden.socket, Some(PathBuf::from("/run/other.sock")));
    }

    /// `--metrics-listen` only exists on `agent start`, and the profile's value
    /// applies when the flag is absent.
    #[test]
    fn metrics_listen_comes_from_the_flag_or_the_profile() {
        let text = "metrics_listen = \"127.0.0.1:9101\"\naddr = \"CB:DF:6B:12:34:56\"";
        assert_eq!(
            settings(&["agent", "start"], text).unwrap().metrics_listen,
            "127.0.0.1:9101".parse().ok()
        );
        assert_eq!(
            settings(
                &["agent", "start", "--metrics-listen", "127.0.0.1:9999"],
                text
            )
            .unwrap()
            .metrics_listen,
            "127.0.0.1:9999".parse().ok()
        );
    }

    #[test]
    fn scan_mode_distinguishes_the_two_scans() {
        assert_eq!(parse_cli(&[]).scan_mode(), None);
        assert_eq!(
            parse_cli(&["--scan"]).scan_mode(),
            Some(ScanMode::WattCheckers)
        );
        assert_eq!(
            parse_cli(&["--scan-all"]).scan_mode(),
            Some(ScanMode::Everything)
        );
        // Both at once is a contradiction the mode group rejects.
        assert!(Cli::try_parse_from(["btwattch2", "--scan", "--scan-all"]).is_err());
    }
}
