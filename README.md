# btwattch2-rs

Rust toolkit for controlling the RATOC Systems RS-BTWATTCH2 Bluetooth Watt Checker from Linux. It can measure voltage, current, and power, operate the power switch, and synchronize the RTC on the main unit.

This is a Rust port of [ruby-btwattch2](https://github.com/gomasy/ruby-btwattch2).

## Requirements

- Linux (BlueZ)
- Rust 1.88+

## Build

```console
$ cargo build --release
```

A single binary, `btwattch2`, is generated in `target/release/`.

## Usage

```
Usage: btwattch2 [OPTIONS] [COMMAND]

Commands:
  agent  Manage the persistent agent (start, stop, status)

Options:
  -i, --index <index>       Specify adapter index, e.g. hci0 [default: 0]
  -a, --addr <addr>         Specify the destination address
  -n, --interval <interval> Specify the time to wait between updates,
                             e.g. 2s or 500ms [default: 1s]
  -c, --config <path>       Path to a config file (TOML-like `key = value`)
  --device <name>           Use the named [devices.<name>] section of the config file
  --socket <path>           Path to the agent's unix socket
                             [default: $XDG_RUNTIME_DIR/btwattch2.sock]
  --pid-file <path>         Path to the agent's pid file
                             [default: the socket path with a `.pid` extension]
  --on                      Turn on the power switch
  --off                     Turn off the power switch
  --set-rtc <time>          Specify the time to set to RTC
  --set-rtc-now             Set the current time of this system to RTC
  --test-led                Blink the LED on the main unit
  --metric-name <name>      Print a measurement as Mackerel custom metrics and exit
  --scan                    Scan for nearby BTWATTCH2 devices and list them, then exit
  --scan-all                Scan like --scan, but list every Bluetooth device
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

List nearby watt checkers (address, name, RSSI) without connecting, to find the `addr` the other commands take. Only devices currently advertising are listed: BlueZ also remembers everything it has ever seen, and those would otherwise pad the results with neighbours long out of range.

```console
# btwattch2 --scan
CB:DF:6B:12:34:56    RS-BTWATTCH2    rssi=-62
[INFO] 7 other device(s) hidden; use --scan-all to list them
```

Devices are recognised by their advertised name, so one that has not sent it yet is counted as hidden. `--scan-all` lists every device instead — worth trying if a meter you expect does not appear:

```console
# btwattch2 --scan-all
CB:DF:6B:12:34:56    RS-BTWATTCH2    rssi=-62
F4:12:00:AB:CD:EF    (unknown)       rssi=-88
```

### Measurement

Run with `--addr` set to the Bluetooth address of the device, and measurements are printed every `--interval` (default: 1 second).

`--interval` accepts seconds or milliseconds — `2s`, `0.5`, `500ms` — between a floor of 10 ms and a ceiling of a day. Sub-second polling is useful for catching inrush current and switching transients, but the device answers each request at its own pace: ask faster than it can reply and the extra requests only queue up.

```console
# btwattch2 --addr CB:DF:6B:12:34:56
V = 104.29123878479004, A = 1.1373979076743126, W = 106.03327941894531, PF = 0.8936, Wh = 0.029
V = 104.19976472854614, A = 1.1281732693314552, W = 105.39636832475662, PF = 0.8971, Wh = 0.059
...
```

`PF` is the power factor, derived as `wattage / (voltage * ampere)`. `Wh` is the energy accumulated during this run, integrated over the wall-clock time actually elapsed between samples — so the first sample contributes nothing, and a reconnect gap is accounted for at its real length.

On exit (Ctrl-C, `--count`, or `--duration`) a summary of min/max/avg per channel and the total energy is printed to stderr.

### Output formats (`--format`)

Measurements can be rendered in several machine-friendly formats for piping into other tools. An explicit `--format` always wins; otherwise `--metric-name` defaults to `mackerel` and everything else to `plain`.

Only `plain` and the end-of-run summary round their values, since those are read by people; every other format emits full precision and leaves rounding to whatever consumes it. (The examples below are shortened for readability.)

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

`--metric-name` accepts a letter or `_` followed by letters, digits, `_`, `.`, or `-`. The `prometheus` format is stricter still and rejects `.` and `-`, which would produce an exposition no scraper will parse.

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
wattchecker1.wattage    104.89565205574036      1609304963
```

Informational (`[INFO]`) messages are suppressed in this mode so that nothing but metrics reaches mackerel-agent. Pass `-d` / `--debug` to print them for troubleshooting.

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

### Agent (daemon) mode

Every command normally connects and disconnects BLE, which takes several seconds. The agent keeps the connection alive in the foreground so subsequent commands execute instantly via a Unix domain socket.

```console
# Start the agent (stays in the foreground; use systemd/nohup to daemonize)
# btwattch2 --addr CB:DF:6B:12:34:56 agent start

# In another terminal — commands go through the agent automatically
# btwattch2 --on
# btwattch2 --count 5

# Check status / stop
# btwattch2 agent status
# btwattch2 agent stop
```

Any number of commands may stream at once. The device is polled once per interval however many are listening, and each measurement is handed to all of them, so a live `btwattch2 --count 5` and a periodic `--metric-name` run no longer collide.

A client that stops reading its end is dropped once it falls 64 measurements behind, with a warning on the agent's stderr, so one wedged subscriber cannot make the agent hold samples indefinitely. The clients still reading are unaffected.

When the agent is running, CLI commands detect it and route through the socket; when it is not, they fall back to direct BLE. No flags needed either way.

#### Status

```console
# btwattch2 agent status
Agent is running (pid 2417)
Socket:      /run/btwattch2/btwattch2.sock
Attached to: CB:DF:6B:12:34:56
Link:        connected
Interval:    1s
Uptime:      3h 12m 4s
Samples:     11524 (last 0.9s ago)
Reconnects:  3
Clients:     1
Metrics:     http://127.0.0.1:9101/metrics
```

`Link` is the state worth watching: an agent whose BLE link has dropped keeps answering commands, and every one of them fails until it recovers. `Reconnects` counts the links re-established since start — a number that keeps climbing points at range or interference rather than at the tool.

The counters are read from shared state rather than from the connection actor, so `agent status` answers immediately even while the agent is blocked on the link — which is exactly when it is worth asking.

#### Prometheus endpoint

`agent start --metrics-listen <addr>` serves the latest measurement over HTTP at `/metrics`, so Prometheus can scrape the agent directly instead of going through a textfile collector:

```console
# btwattch2 --addr CB:DF:6B:12:34:56 agent start --metrics-listen 127.0.0.1:9101
$ curl -s localhost:9101/metrics
# HELP btwattch2_voltage Instantaneous voltage in volts
# TYPE btwattch2_voltage gauge
btwattch2_voltage 104.807
...
# TYPE btwattch2_last_sample_timestamp_seconds gauge
btwattch2_last_sample_timestamp_seconds 1609304963
# TYPE btwattch2_up gauge
btwattch2_up 1
```

With the endpoint enabled the agent polls the device continuously, whether or not anything is streaming — otherwise there would be nothing current to serve. Scrape it like any other exporter:

```yaml
scrape_configs:
  - job_name: btwattch2
    static_configs:
      - targets: ['127.0.0.1:9101']
```

Notes:

- `btwattch2_up` is `0` when the last reading is too old to describe the device — no sample yet, or a link that dropped. The channel gauges are then omitted rather than repeated, since a gauge that keeps returning the last value it saw makes a dead link look like a steady load. The freshness window is three intervals, or five seconds, whichever is longer.
- Unlike `--format prometheus`, the endpoint carries no per-sample timestamp: a scraper stamps what it reads.
- Session energy is not exposed. It is accumulated per run by the streaming client, so there is no meaningful value for a scrape to read.
- The endpoint is plain HTTP with no authentication. Bind it to a loopback address unless something in front of it provides access control.
- The metric names are fixed. Two agents scraped by one Prometheus are told apart by the target's own labels, not by renaming their metrics.

#### Socket and pid file

The agent listens on `$XDG_RUNTIME_DIR/btwattch2.sock`. When `XDG_RUNTIME_DIR` is unset — as it usually is under systemd or `sudo` — it falls back to `/run/btwattch2/btwattch2.sock`, creating `/run/btwattch2` mode 0700 on first start. Override with `--socket <path>`, passing it on *every* command (including `agent start`, so the daemon and its clients agree on the location):

```console
# btwattch2 --socket /run/btwattch2/device-a.sock --addr CB:DF:6B:12:34:56 agent start
# btwattch2 --socket /run/btwattch2/device-a.sock --on
```

The pid file defaults to the socket path with a `.pid` extension (`btwattch2.sock` → `btwattch2.pid`); `--pid-file <path>` overrides it independently. Run multiple agents on separate sockets to manage several devices at once — the config file can hold each one's socket, so `--device` alone routes to the right agent (see "Configuration file" below).

Under systemd, let the unit own the runtime directory rather than relying on the fallback:

```ini
[Service]
RuntimeDirectory=btwattch2
RuntimeDirectoryMode=0700
ExecStart=/usr/local/bin/btwattch2 --socket /run/btwattch2/agent.sock --addr CB:DF:6B:12:34:56 agent start
```

The agent itself removes its socket and pid file on exit, including on SIGINT and SIGTERM. While it runs it holds an exclusive lock on the pid file, so a second `agent start` on the same paths fails with `agent is already running (pid N)` rather than unlinking the first one's socket — including when the two are started at the same moment. The kernel releases the lock however the agent exits, so a pid file left behind by a crash never locks the agent out of starting again.

#### Socket permissions

The socket and the pid file are created mode 0600 by default, so the agent's permissions do not depend on the umask it inherited — connecting to a unix socket takes write permission on it, and the agent holds a mains switch.

To let other users reach the agent, set `socket_mode` in the config file, written as octal exactly as `chmod` takes it:

```toml
socket_mode = 0666
```

Only `agent start` acts on it; clients merely connect to the socket it created. Like `socket`, it can sit in a `[devices.<name>]` section so each agent gets its own. A value that is not octal permission bits is an error rather than a silent fall back to 0600, which would lock out the users it was set to admit.

Reaching a socket also takes search permission on every directory above it, and the default runtime directory is 0700 — so widening the socket alone is not enough. Give the directory a matching mode as well: `RuntimeDirectoryMode=0755` in the unit above, or create the directory beforehand (an existing one is left as it is).

#### Device selection

Because the agent holds the connection, its `--addr`, `--index`, and `--interval` are fixed at `agent start` and a client cannot change them. `--index` and `--interval` on a client are ignored with a warning. An explicit `--addr` naming a *different* device is an error rather than a warning — otherwise a command meant for one meter would silently operate another:

```console
# btwattch2 --addr CB:DF:6B:AA:BB:CC --off
Error: the agent on /run/btwattch2/btwattch2.sock is attached to CB:DF:6B:12:34:56, but
--addr asks for CB:DF:6B:AA:BB:CC; stop that agent or point --socket at the one holding it
```

> **Upgrading: restart the agent when you replace the binary.** The socket protocol changed between 1.0 and 1.1: a client and an agent from those two versions fail on `--on`, `--off`, `--set-rtc`, and `--test-led` with a parse error, though measurement streaming is unaffected. Later additions are backward compatible — a new client asking an older agent for its status gets the fields that agent knows about — but the agent still has to be restarted before it can serve the metrics endpoint or report the new fields.

### Configuration file

Connection defaults can be stored in a config file so you don't have to pass `--addr` every time. By default `$XDG_CONFIG_HOME/btwattch2/config.toml` (or `~/.config/btwattch2/config.toml`) is read when present. Override with `-c/--config <path>`. The CLI wins over the file.

```toml
# ~/.config/btwattch2/config.toml
addr = "CB:DF:6B:12:34:56"
index = 0
interval = 1s
```

Recognised keys are `addr`, `index`, `interval`, `socket`, `socket_mode`, and `metrics_listen`. Unknown keys, malformed lines, and invalid values are errors rather than silently ignored, so a typo cannot leave you talking to the wrong device.

#### Device profiles

With more than one meter, give each a `[devices.<name>]` section and select it with `--device <name>`. Keys before the first section are defaults every profile inherits, and `default` picks the profile to use when `--device` is absent:

```toml
# ~/.config/btwattch2/config.toml
interval = 1s
default = "living"

[devices.living]
addr = "CB:DF:6B:12:34:56"

[devices.rack]
addr = "CB:DF:6B:AA:BB:CC"
interval = 500ms
socket = "/run/btwattch2/rack.sock"
socket_mode = 0660
metrics_listen = "127.0.0.1:9101"
```

```console
# btwattch2 --off                     # the default profile: living
# btwattch2 --device rack --off       # the rack meter, via its own socket
# btwattch2 --device rack agent start  # serves metrics on 127.0.0.1:9101
```

Because `socket` travels with the profile, `--device rack` reaches the agent holding that meter without repeating `--socket` on every command. A `--device` naming no section is an error listing the ones that exist.

### Verbosity

Informational messages go to stderr. By default they are shown for monitoring but suppressed in `--metric-name` mode. Use `-d/--debug` (or `--log-level info`) to force them on, `-q/--quiet` (or `--log-level off`) to force them off.

## License

[MIT License](LICENSE)
