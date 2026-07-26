use std::ops::ControlFlow;
use std::time::Instant;

use serde::ser::{Serialize, SerializeMap, Serializer};

use crate::cli::{LogLevel, OutputFormat};
use crate::connection::Measurement;

/// (name, help text, accessor) of one measurement channel.
type ChannelSpec = (&'static str, &'static str, fn(&Measurement) -> f64);

/// The measurement channels every format renders, in output order. Keeping
/// the list in one place keeps the per-format schemas from drifting apart.
const CHANNELS: [ChannelSpec; 4] = [
    ("voltage", "Instantaneous voltage in volts", |m| m.voltage),
    ("ampere", "Instantaneous current in amperes", |m| m.ampere),
    ("wattage", "Instantaneous power in watts", |m| m.wattage),
    (
        "power_factor",
        "Power factor (wattage / (voltage * ampere))",
        |m| m.power_factor,
    ),
];
const ENERGY_NAME: &str = "energy_wh";
const ENERGY_HELP: &str = "Energy accumulated this session in watt-hours";

/// The fields of a record, in output order: the channels, then session energy.
/// Every format that carries energy composes its record from this, so a channel
/// added or renamed shows up everywhere at once instead of in one arm.
fn fields(m: &Measurement, energy_wh: f64) -> impl Iterator<Item = (&'static str, f64)> {
    CHANNELS
        .iter()
        .map(move |(name, _, value)| (*name, value(m)))
        .chain([(ENERGY_NAME, energy_wh)])
}

/// The same names without needing a measurement, for headers.
fn field_names() -> impl Iterator<Item = &'static str> {
    CHANNELS
        .iter()
        .map(|(name, _, _)| *name)
        .chain([ENERGY_NAME])
}

/// One JSON Lines record. Serialized through serde, which gets the escaping and
/// the `null` for a non-finite float that a formatted string would not, but
/// written entry by entry: a `Value` or `json!` map sorts keys alphabetically
/// and would reorder the documented column layout, and a `#[derive]`d struct
/// would re-list the channels that `CHANNELS` already owns.
struct JsonLine<'a> {
    measurement: &'a Measurement,
    energy_wh: f64,
}

impl Serialize for JsonLine<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(CHANNELS.len() + 2))?;
        map.serialize_entry("time", &self.measurement.timestamp.timestamp())?;
        for (name, value) in fields(self.measurement, self.energy_wh) {
            map.serialize_entry(name, &value)?;
        }
        map.end()
    }
}

/// Renders measurements to stdout in the requested format. Formats that need
/// a header (CSV) or a metric declaration block (Prometheus) print it once,
/// before the first measurement.
///
/// Machine-readable formats emit every value at full precision, `energy_wh`
/// included, and leave rounding to whatever consumes them. Only `plain` and the
/// end-of-run summary round, because those are read by people.
pub struct Printer {
    format: OutputFormat,
    prefix: String,
    header_printed: bool,
}

impl Printer {
    pub fn new(format: OutputFormat, prefix: &str) -> Self {
        Self {
            format,
            prefix: prefix.to_string(),
            header_printed: false,
        }
    }

    /// Render one measurement. `energy_wh` is the session energy so far,
    /// computed by the caller's `Stats`.
    pub fn print(&mut self, m: &Measurement, energy_wh: f64) {
        match self.format {
            OutputFormat::Plain => println!(
                "V = {}, A = {}, W = {}, PF = {:.4}, Wh = {:.3}",
                m.voltage, m.ampere, m.wattage, m.power_factor, energy_wh
            ),
            OutputFormat::Mackerel => {
                let name = &self.prefix;
                let epoch = m.timestamp.timestamp();
                for (suffix, _, value) in CHANNELS {
                    println!("{name}.{suffix}\t{}\t{epoch}", value(m));
                }
            }
            OutputFormat::Csv => {
                if !self.header_printed {
                    let names: Vec<&str> = field_names().collect();
                    println!("time,{}", names.join(","));
                    self.header_printed = true;
                }
                let values: Vec<String> = fields(m, energy_wh)
                    .map(|(_, value)| value.to_string())
                    .collect();
                println!("{},{}", m.timestamp.timestamp(), values.join(","));
            }
            OutputFormat::Ltsv => {
                let labelled: Vec<String> = fields(m, energy_wh)
                    .map(|(name, value)| format!("{name}:{value}"))
                    .collect();
                println!("time:{}\t{}", m.timestamp.timestamp(), labelled.join("\t"));
            }
            OutputFormat::Json => {
                let line = JsonLine {
                    measurement: m,
                    energy_wh,
                };
                match serde_json::to_string(&line) {
                    Ok(json) => println!("{json}"),
                    // Nothing in `JsonLine` can fail to serialize, but skipping
                    // one sample beats taking the whole run down if that ever
                    // stops being true.
                    Err(e) => eprintln!("[ERR] Failed to encode measurement: {e}"),
                }
            }
            OutputFormat::Prometheus => {
                let epoch_ms = m.timestamp.timestamp_millis();
                if !self.header_printed {
                    for (suffix, help) in CHANNELS
                        .iter()
                        .map(|(n, h, _)| (*n, *h))
                        .chain([(ENERGY_NAME, ENERGY_HELP)])
                    {
                        println!("# HELP {}_{suffix} {help}", self.prefix);
                        println!("# TYPE {}_{suffix} gauge", self.prefix);
                    }
                    self.header_printed = true;
                }
                for (suffix, value) in fields(m, energy_wh) {
                    println!("{}_{suffix} {value} {epoch_ms}", self.prefix);
                }
            }
        }
    }
}

/// Drives a measurement stream: renders each sample, folds it into the running
/// statistics, and stops after `count` samples. The summary is printed when the
/// renderer is dropped, so cancellation paths (Ctrl-C, `--duration` timeout)
/// report it as well as a run that finishes on its own.
pub struct StreamRenderer {
    printer: Printer,
    stats: Stats,
    samples: u64,
    count: Option<u64>,
    summary: bool,
}

impl StreamRenderer {
    pub fn new(
        format: OutputFormat,
        prefix: &str,
        count: Option<u64>,
        log_level: LogLevel,
    ) -> Self {
        Self {
            printer: Printer::new(format, prefix),
            stats: Stats::new(),
            samples: 0,
            count,
            summary: log_level == LogLevel::Info,
        }
    }

    /// Render one measurement, breaking once `count` samples have been seen.
    pub fn record(&mut self, m: &Measurement) -> ControlFlow<()> {
        let energy_wh = self.stats.record(m);
        self.printer.print(m, energy_wh);
        self.samples += 1;
        if self.count.is_some_and(|c| self.samples >= c) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
}

impl Drop for StreamRenderer {
    fn drop(&mut self) {
        if self.summary {
            self.stats.print_summary();
        }
    }
}

/// Per-channel running min/max/sum.
struct Channel {
    min: f64,
    max: f64,
    sum: f64,
}

impl Default for Channel {
    fn default() -> Self {
        Self {
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum: 0.0,
        }
    }
}

impl Channel {
    fn record(&mut self, value: f64) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value;
    }
}

/// Running statistics over a stream of measurements, printed as a summary to
/// stderr when a monitoring run ends. Energy is integrated over the actual
/// wall-clock time between samples, so the first sample contributes nothing
/// and reconnect gaps are accounted for at their real length.
pub struct Stats {
    count: u64,
    channels: [Channel; 4],
    last_sample: Option<Instant>,
    energy_wh: f64,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            count: 0,
            channels: Default::default(),
            last_sample: None,
            energy_wh: 0.0,
        }
    }

    /// Fold a measurement into the running statistics and return the session
    /// energy so far in watt-hours.
    pub fn record(&mut self, m: &Measurement) -> f64 {
        let now = Instant::now();
        if let Some(last) = self.last_sample {
            self.energy_wh += m.wattage * (now - last).as_secs_f64() / 3600.0;
        }
        self.last_sample = Some(now);

        self.count += 1;
        for ((_, _, value), channel) in CHANNELS.iter().zip(&mut self.channels) {
            channel.record(value(m));
        }
        self.energy_wh
    }

    /// Print min/max/avg per channel and total energy to stderr.
    pub fn print_summary(&self) {
        if self.count == 0 {
            return;
        }
        let n = self.count as f64;
        eprintln!("--- summary ({}-sample run) ---", self.count);
        for ((name, _, _), c) in CHANNELS.iter().zip(&self.channels) {
            eprintln!(
                "{name}: min={:.3}  max={:.3}  avg={:.3}",
                c.min,
                c.max,
                c.sum / n
            );
        }
        eprintln!("energy: {:.3} Wh", self.energy_wh);
    }
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Local, TimeZone};

    /// A fixed timestamp, so a rendered line can be asserted whole.
    const EPOCH: i64 = 1609304963;

    fn measurement(wattage: f64) -> Measurement {
        Measurement {
            voltage: 100.0,
            ampere: 1.0,
            wattage,
            power_factor: 1.0,
            timestamp: Local
                .timestamp_opt(EPOCH, 0)
                .single()
                .expect("fixed epoch is unambiguous in every timezone"),
        }
    }

    fn json_line(m: &Measurement, energy_wh: f64) -> String {
        serde_json::to_string(&JsonLine {
            measurement: m,
            energy_wh,
        })
        .unwrap()
    }

    /// The documented column order, which a `serde_json::Map` would resort.
    #[test]
    fn json_keys_stay_in_channel_order() {
        let json = json_line(&measurement(2.0), 0.5);
        let keys: Vec<&str> = json
            .split(',')
            .filter_map(|f| f.split(':').next())
            .map(|k| k.trim_matches(['{', '"']))
            .collect();
        assert_eq!(
            keys,
            [
                "time",
                "voltage",
                "ampere",
                "wattage",
                "power_factor",
                "energy_wh"
            ]
        );
    }

    /// JSON has no NaN token; going through serde turns one into `null` rather
    /// than emitting a line no parser accepts.
    #[test]
    fn json_encodes_a_non_finite_value_as_null() {
        let json = json_line(&measurement(f64::NAN), f64::INFINITY);
        assert!(json.contains("\"wattage\":null"), "{json}");
        assert!(json.contains("\"energy_wh\":null"), "{json}");
    }

    #[test]
    fn first_sample_contributes_no_energy() {
        let mut stats = Stats::new();
        assert_eq!(stats.record(&measurement(3600.0)), 0.0);
    }

    #[test]
    fn renderer_breaks_at_count() {
        let mut r = StreamRenderer::new(OutputFormat::Json, "t", Some(2), LogLevel::Off);
        assert!(r.record(&measurement(1.0)).is_continue());
        assert!(r.record(&measurement(1.0)).is_break());
    }

    #[test]
    fn renderer_without_count_never_breaks() {
        let mut r = StreamRenderer::new(OutputFormat::Json, "t", None, LogLevel::Off);
        for _ in 0..3 {
            assert!(r.record(&measurement(1.0)).is_continue());
        }
    }

    #[test]
    fn energy_integrates_elapsed_time() {
        let mut stats = Stats::new();
        stats.record(&measurement(3600.0));
        std::thread::sleep(std::time::Duration::from_millis(20));
        // 3600 W for >= 20 ms is >= 0.02 Wh.
        assert!(stats.record(&measurement(3600.0)) >= 0.02);
    }
}
