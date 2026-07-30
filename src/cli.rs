use std::fmt;
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

    /// Specify the time to wait between updates, e.g. 2s or 500ms [default: 1s].
    #[arg(short = 'n', long, value_name = "interval", value_parser = parse_interval)]
    pub interval: Option<Interval>,
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
    /// Positive by construction: it becomes a `tokio::time::interval` period,
    /// which panics on a zero duration. Enforcing it in the type keeps that out
    /// of reach instead of resting on the two parsers that feed this struct.
    pub interval: Interval,
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
    Start,
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
                action: AgentAction::Start
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
            let value = unquote(value.trim());
            match key {
                "index" => {
                    cfg.index = Some(
                        value
                            .parse()
                            .with_context(|| format!("{}: invalid index: {value}", place()))?,
                    )
                }
                // `Interval` rejects zero and anything below its floor as a
                // parse error, so there is no separate range check to keep in
                // step with clap's.
                "interval" => {
                    cfg.interval = Some(
                        value
                            .parse()
                            .with_context(|| format!("{}: invalid interval", place()))?,
                    )
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

    /// Resolve the agent socket/pid paths, honouring `--socket` and
    /// `--pid-file` if given. The pid file defaults to the socket path with a
    /// `.pid` extension, which `--pid-file` overrides.
    pub fn agent_paths(&self) -> crate::agent::AgentPaths {
        let mut paths = match &self.socket {
            Some(s) => crate::agent::paths_from_socket(s.clone()),
            None => crate::agent::default_paths(),
        };
        if let Some(pid) = &self.pid_file {
            paths.pid = pid.clone();
        }
        paths
    }
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

    #[test]
    fn cli_overlays_the_config_file() {
        let cfg = ConnectOpts {
            index: Some(1),
            addr: Some("CB:DF:6B:12:34:56".parse().unwrap()),
            interval: "9s".parse().ok(),
        };
        let resolved = parse_cli(&["-n", "3"])
            .connection_config(Some(&cfg))
            .unwrap();
        assert_eq!(resolved.interval, "3s".parse().unwrap());
        assert_eq!(resolved.index, 1);
        assert_eq!(resolved.addr, cfg.addr.unwrap());
    }

    #[test]
    fn connection_config_needs_an_address() {
        assert!(parse_cli(&[]).connection_config(None).is_err());
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
        let cfg = ConnectOpts {
            addr: Some("CB:DF:6B:12:34:56".parse().unwrap()),
            ..ConnectOpts::default()
        };
        // Only what the command line asked for counts, so a configured address
        // never makes a running agent look like the wrong device.
        assert_eq!(parse_cli(&[]).explicit_addr(), None);
        assert_eq!(
            parse_cli(&["-a", "CB:DF:6B:AA:BB:CC"]).explicit_addr(),
            "CB:DF:6B:AA:BB:CC".parse().ok()
        );
        assert_eq!(
            parse_cli(&[]).connection_config(Some(&cfg)).unwrap().addr,
            cfg.addr.unwrap()
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
