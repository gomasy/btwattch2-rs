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

Two binaries, `btwattch2` and `mackerel`, are generated in `target/release/`.

## Usage

```
Usage: btwattch2 [OPTIONS] --addr <addr>

Options:
  -i, --index <index>         Specify adapter index, e.g. hci0 [default: 0]
  -a, --addr <addr>           Specify the destination address
  -n, --interval <second(s)>  Specify the seconds to wait between updates [default: 1]
      --on                    Turn on the power switch
      --off                   Turn off the power switch
      --set-rtc <time>        Specify the time to set to RTC
      --set-rtc-now           Set the current time of this system to RTC
      --test-led              Blink the LED on the main unit
  -h, --help                  Print help
```

### Measurement

Run with `--addr` set to the Bluetooth address of the device, and measurements are printed every `--interval` seconds (default: 1 second).

```console
# btwattch2 --addr CB:DF:6B:12:34:56
V = 104.29123878479004, A = 1.1373979076743126, W = 106.03327941894531
V = 104.19976472854614, A = 1.1281732693314552, W = 105.39636832475662
V = 104.17768478393555, A = 1.1366924941539764, W = 105.92031782865524
...
```

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

### Mackerel integration

Measurements can also be printed in a format compliant with [Mackerel](https://mackerel.io) custom metrics.

```console
# mackerel --addr CB:DF:6B:12:34:56 --metric-name wattchecker1
wattchecker1.voltage    104.80763912200928      1609304963
wattchecker1.ampere     1.120739296078682       1609304963
wattchecker1.wattage    104.89565205574036      1609304963
```

Unlike `btwattch2`, the `mackerel` binary suppresses informational (`[INFO]`) messages on stderr so that it stays quiet when invoked from mackerel-agent. Pass `-d` / `--debug` to print them for troubleshooting.

Note: The epoch of the metrics is based on the RTC of the device. Synchronize the RTC periodically. (See "Time synchronization" above.)

## License

[MIT License](LICENSE)
