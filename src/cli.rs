use anyhow::{Result, anyhow};
use btleplug::api::BDAddr;
use chrono::{DateTime, Local, NaiveDateTime, TimeDelta, TimeZone};
use clap::{Args, Parser};

/// Options needed to reach the device, whatever the mode.
#[derive(Args, Debug)]
pub struct ConnectOpts {
    /// Specify adapter index, e.g. hci0.
    #[arg(short, long, value_name = "index", default_value_t = 0)]
    pub index: usize,

    /// Specify the destination address.
    #[arg(short, long, value_name = "addr")]
    pub addr: BDAddr,

    /// Specify the seconds to wait between updates.
    #[arg(short = 'n', long, value_name = "second(s)", default_value_t = 1)]
    pub interval: u64,
}

/// Toolkit for the RS-BTWATTCH2 Bluetooth power meter.
#[derive(Parser, Debug)]
pub struct Cli {
    #[command(flatten)]
    pub connect: ConnectOpts,

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

    /// Print a single measurement as Mackerel custom metrics and exit.
    #[arg(long, value_name = "name", group = "mode")]
    pub metric_name: Option<String>,

    /// Print informational messages to stderr (suppressed by default when
    /// --metric-name is given).
    #[arg(short, long)]
    pub debug: bool,
}

impl Cli {
    pub fn rtc_time(&self) -> Option<DateTime<Local>> {
        if self.set_rtc_now {
            Some(Local::now())
        } else {
            self.set_rtc
        }
    }
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
