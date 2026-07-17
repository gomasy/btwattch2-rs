# btwattch2-rs

Rust toolkit for the RS-BTWATTCH2 Bluetooth power meter

A tool for controlling the RATOC Systems RS-BTWATTCH2 Bluetooth Watt Checker from Linux. It can measure voltage, current, and power, operate the power switch, and synchronize the RTC on the main unit.

This is a Rust port of [ruby-btwattch2](https://github.com/gomasy/ruby-btwattch2).

## Requirements

- Linux (BlueZ)
- Rust (stable)

## Build

```console
$ cargo build --release
```

A single binary, `btwattch2`, is generated in `target/release/`.

## Usage

```
Usage: btwattch2 [OPTIONS]

Options:
  -i, --index <index>       Specify adapter index, e.g. hci0 [default: 0]
  -a, --addr <addr>         Specify the destination address
  -n, --interval <second(s)>
                             Specify the seconds to wait between updates [default: 1]
  -c, --config <path>       Path to a config file (TOML-like `key = value`)
  --on                      Turn on the power switch
  --off                     Turn off the power switch
  --set-rtc <time>          Specify the time to set to RTC
  --set-rtc-now             Set the current time of this system to RTC
  --test-led                Blink the LED on the main unit
  --metric-name <name>      Print a measurement as Mackerel custom metrics and exit
  --scan                    Scan for nearby BTWATTCH2 devices and list them, then exit
  --get-rtc                 Read the device RTC and report its drift from the system clock
  --format <format>         How to render measurements
                             [plain|json|csv|ltsv|prometheus|mackerel]
  --count <N>               Stop after this many measurements
  --duration <seconds>      Stop after this many seconds (with --scan: the scan window)
  -d, --debug               Print informational messages to stderr
  -q, --quiet               Suppress informational messages on stderr
      --log-level <level>   Set the verbosity [off|info]
  -h, --help                Print help
```

### Discovering the device (`--scan`)

List nearby Bluetooth devices (address, name, RSSI) without connecting. Useful for finding the `addr` to pass to the other commands.

```console
# btwattch2 --scan
CB:DF:6B:12:34:56    RS-BTWATTCH2    rssi=-62
```

### Measurement

Run with `--addr` set to the Bluetooth address of the device, and measurements are printed every `--interval` seconds (default: 1 second).

```console
# btwattch2 --addr CB:DF:6B:12:34:56
V = 104.29123878479004, A = 1.1373979076743126, W = 106.03327941894531, PF = 0.8936, Wh = 0.029
V = 104.19976472854614, A = 1.1281732693314552, W = 105.39636832475662, PF = 0.8971, Wh = 0.059
...
```

`PF` is the power factor, derived as `wattage / (voltage * ampere)`. `Wh` is the energy accumulated during this run, integrated from the wattage and the interval.

On exit (Ctrl-C, `--count`, or `--duration`) a summary of min/max/avg per channel and the total energy is printed to stderr.

### Output formats (`--format`)

Measurements can be rendered in several machine-friendly formats for piping into other tools. An explicit `--format` always wins; otherwise `--metric-name` defaults to `mackerel` and everything else to `plain`.

- `plain` — the human-readable line above (default).
- `json` — one JSON object per line (JSON Lines):

  ```console
  # btwattch2 --addr CB:DF:6B:12:34:56 --format json
  {"time":1609304963,"voltage":104.807,"ampere":1.120,"wattage":104.895,"power_factor":0.894,"energy_wh":0.029}
  ```

- `csv` — a header row followed by one row per measurement:

  ```console
  # btwattch2 --addr CB:DF:6B:12:34:56 --format csv
  time,voltage,ampere,wattage,power_factor,energy_wh
  1609304963,104.807,1.120,104.895,0.894,0.029
  ```

- `ltsv` — Label: value tab-separated lines.
- `prometheus` — Prometheus / OpenMetrics text exposition format (gauge metrics, prefixed by `--metric-name` or `btwattch2`):

  ```console
  # btwattch2 --addr CB:DF:6B:12:34:56 --format prometheus --metric-name wattchecker1
  # HELP wattchecker1_voltage Instantaneous voltage in volts
  # TYPE wattchecker1_voltage gauge
  wattchecker1_voltage 104.807 1609304963000
  ...
  ```

- `mackerel` — Mackerel custom metrics, line-per-metric (used with `--metric-name`).

### Limiting a run (`--count`, `--duration`)

Stop automatically instead of waiting for Ctrl-C. Handy for cron/systemd-timer or periodic sampling.

```console
# btwattch2 --addr CB:DF:6B:12:34:56 --count 60 --format csv > sample.csv
# btwattch2 --addr CB:DF:6B:12:34:56 --duration 300 --format json
```

### Mackerel integration

When `--metric-name` is given, a single measurement is printed in Mackerel custom-metrics format and the process exits.

```console
# btwattch2 --addr CB:DF:6B:12:34:56 --metric-name wattchecker1
wattchecker1.voltage    104.80763912200928      1609304963
wattchecker1.ampere     1.120739296078682       1609304963
wattchecker1.wattage    104.89565205574036       1609304963
```

Informational (`[INFO]`) messages on stderr are suppressed in this mode so that the output stays quiet when invoked from mackerel-agent. Pass `-d` / `--debug` to print them for troubleshooting.

Note: The epoch of the metrics is based on the RTC of the device. Synchronize the RTC periodically. (See "Time synchronization" below.)

### Operating the power switch

```console
# btwattch2 --addr CB:DF:6B:12:34:56 --on
# btwattch2 --addr CB:DF:6B:12:34:56 --off
```

### Time synchronization

Timestamps of the measurements are based on the RTC of the device, so run the following command periodically via systemd-timer, cron, or similar to keep it in sync.

```console
# btwattch2 --addr CB:DF:6B:12:34:56 --set-rtc-now
```

### Reading the device RTC (`--get-rtc`)

Report the device's RTC and its drift from the system clock. This reuses the timestamp carried in a measurement frame, so no separate read command is needed.

```console
# btwattch2 --addr CB:DF:6B:12:34:56 --get-rtc
device_time = 2026-07-17T12:00:03+09:00
system_time = 2026-07-17T12:00:05+09:00
drift_seconds = -2
```

### Configuration file

Connection defaults can be stored in a config file so you don't have to pass `--addr` every time. By default `$XDG_CONFIG_HOME/btwattch2/config.toml` (or `~/.config/btwattch2/config.toml`) is read when present. Override with `-c/--config <path>`. The CLI wins over the file.

```toml
# ~/.config/btwattch2/config.toml
addr = "CB:DF:6B:12:34:56"
index = 0
interval = 1
```

### Verbosity

Informational messages go to stderr. By default they are shown for monitoring but suppressed in `--metric-name` mode. Use `-d/--debug` (or `--log-level info`) to force them on, `-q/--quiet` (or `--log-level off`) to force them off.

## License

[MIT License](LICENSE)
