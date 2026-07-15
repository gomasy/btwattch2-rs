use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use btleplug::api::{
    BDAddr, Central, CharPropFlags, Characteristic, Manager as _, Peripheral as _, ScanFilter,
    ValueNotification, WriteType,
};
use btleplug::platform::{Adapter, Manager, Peripheral};
use chrono::{DateTime, Local, NaiveDate};
use futures::{Stream, StreamExt};
use uuid::{Uuid, uuid};

use crate::cli::{ConnectOpts, local_datetime};
use crate::payload::{self, FRAME_OVERHEAD, HEADER};

// TX/RX characteristics of the Nordic UART service the device exposes.
const C_TX: Uuid = uuid!("6e400002-b5a3-f393-e0a9-e50e24dcca9e");
const C_RX: Uuid = uuid!("6e400003-b5a3-f393-e0a9-e50e24dcca9e");

const SCAN_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_RETRIES: usize = 5;
const CONNECT_RETRIES: usize = 5;

const VOLTAGE_SCALE: f64 = (1u64 << 24) as f64;
const AMPERE_SCALE: f64 = (1u64 << 30) as f64;
const WATTAGE_SCALE: f64 = (1u64 << 24) as f64;

type Notifications = Pin<Box<dyn Stream<Item = ValueNotification> + Send>>;

static DEBUG: AtomicBool = AtomicBool::new(false);

/// Enable informational output. Called once at startup from the CLI flag.
pub fn set_debug(enabled: bool) {
    DEBUG.store(enabled, Ordering::Relaxed);
}

fn enabled() -> bool {
    DEBUG.load(Ordering::Relaxed)
}

// Print an informational message to stderr, only when --debug is on.
// Warnings and errors are printed unconditionally with plain `eprintln!`.
macro_rules! info {
    ($($arg:tt)*) => {
        if enabled() {
            eprintln!("[INFO] {}", format_args!($($arg)*));
        }
    };
}

#[derive(Debug)]
pub struct Measurement {
    pub voltage: f64,
    pub ampere: f64,
    pub wattage: f64,
    pub timestamp: DateTime<Local>,
}

pub struct Connection {
    name: String,
    addr: BDAddr,
    interval: Duration,
    device: Peripheral,
    tx: Characteristic,
    rx: Characteristic,
}

impl Connection {
    pub async fn new(opts: &ConnectOpts) -> Result<Self> {
        let manager = Manager::new().await?;
        let name = format!("hci{}", opts.index);
        let adapter = Self::find_adapter(&manager, &name).await?;
        let device = Self::find_device(&adapter, opts.addr).await?;

        let (tx, rx) = Self::establish(&device, opts.addr, &name).await?;

        Ok(Self {
            name,
            addr: opts.addr,
            interval: Duration::from_secs(opts.interval.max(1)),
            device,
            tx,
            rx,
        })
    }

    async fn find_adapter(manager: &Manager, name: &str) -> Result<Adapter> {
        for adapter in manager.adapters().await? {
            // On BlueZ this is "hciN (modalias)".
            match adapter.adapter_info().await {
                Ok(info) if info == name || info.starts_with(&format!("{name} ")) => {
                    return Ok(adapter);
                }
                _ => {}
            }
        }
        bail!("adapter {name} not found")
    }

    async fn find_device(adapter: &Adapter, addr: BDAddr) -> Result<Peripheral> {
        if let Some(device) = Self::lookup(adapter, addr).await? {
            return Ok(device);
        }

        info!("Scanning for {addr}...");
        adapter
            .start_scan(ScanFilter::default())
            .await
            .context("failed to start scanning (is Bluetooth powered on?)")?;

        let deadline = tokio::time::Instant::now() + SCAN_TIMEOUT;
        let device = loop {
            if let Some(device) = Self::lookup(adapter, addr).await? {
                break device;
            }
            if tokio::time::Instant::now() >= deadline {
                adapter.stop_scan().await.ok();
                bail!("device {addr} not found");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };

        adapter.stop_scan().await.ok();
        Ok(device)
    }

    async fn lookup(adapter: &Adapter, addr: BDAddr) -> Result<Option<Peripheral>> {
        Ok(adapter
            .peripherals()
            .await?
            .into_iter()
            .find(|device| device.address() == addr))
    }

    async fn establish(
        device: &Peripheral,
        addr: BDAddr,
        name: &str,
    ) -> Result<(Characteristic, Characteristic)> {
        for attempt in 1.. {
            info!("Connecting to {addr} via {name} (attempt {attempt})...");

            let result = async {
                device.connect().await?;
                device.discover_services().await
            }
            .await;

            match result {
                Ok(()) => break,
                Err(e) if attempt < CONNECT_RETRIES => {
                    info!("Connection failed: {e}, retrying...");
                    // On BlueZ, Connect() can succeed at the D-Bus level even
                    // when service discovery times out, leaving a half-open
                    // connection that makes every later attempt fail too.
                    // Drop it before retrying.
                    Self::drop_connection(device).await;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => {
                    info!("Connection failed: {e}");
                    Self::drop_connection(device).await;
                    return Err(e)
                        .with_context(|| format!("connect failed after {attempt} attempts"));
                }
            }
        }

        let chars = device.characteristics();
        let find = |uuid: Uuid| {
            chars
                .iter()
                .find(|c| c.uuid == uuid)
                .cloned()
                .ok_or_else(|| anyhow!("characteristic {uuid} not found"))
        };
        let tx = find(C_TX)?;
        let rx = find(C_RX)?;

        info!("Connected to {addr} via {name}");
        Ok((tx, rx))
    }

    async fn connect(&mut self) -> Result<()> {
        let (tx, rx) = Self::establish(&self.device, self.addr, &self.name).await?;
        self.tx = tx;
        self.rx = rx;
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<()> {
        Self::drop_connection(&self.device).await;
        info!("Disconnected");
        Ok(())
    }

    /// Disconnect only if a link is actually established. Calling
    /// Disconnect() while a connect attempt is still in flight aborts it,
    /// which makes bluetoothd log "No matching connection for device".
    async fn drop_connection(device: &Peripheral) {
        if device.is_connected().await.unwrap_or(false) {
            device.disconnect().await.ok();
        }
    }

    pub async fn set_rtc(&mut self, time: &DateTime<Local>) -> Result<()> {
        self.command(payload::rtc(time), "RTC set").await
    }

    pub async fn power(&mut self, on: bool) -> Result<()> {
        let (payload, action) = if on {
            (payload::on(), "Power on")
        } else {
            (payload::off(), "Power off")
        };
        self.command(payload, action).await
    }

    pub async fn blink_led(&mut self) -> Result<()> {
        self.command(payload::blink_led(), "Blink").await
    }

    /// Send a one-shot command and report the status byte of the reply.
    async fn command(&mut self, payload: Vec<u8>, action: &str) -> Result<()> {
        self.subscribe(payload, |frame| {
            match frame.get(4).copied() {
                Some(0x00) => info!("{action} succeeded"),
                Some(code) => eprintln!("[ERR] {action} failed, CODE: {code:#04x}"),
                None => eprintln!("[ERR] {action} failed, frame too short"),
            }
            ControlFlow::Break(())
        })
        .await
    }

    async fn subscribe<F>(&mut self, payload: Vec<u8>, mut on_frame: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> ControlFlow<()>,
    {
        let mut notifications = self.listen().await?;

        let mut buf: Vec<u8> = Vec::new();
        let mut ticker = tokio::time::interval(self.interval);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if self.write(&payload).await? {
                        // Reconnected: the old stream belongs to the previous
                        // connection, so re-subscribe and listen again.
                        notifications = self.listen().await?;
                    }
                }
                event = notifications.next() => {
                    let Some(event) = event else {
                        eprintln!("[WARN] Notification stream closed, reconnecting...");
                        self.connect().await?;
                        notifications = self.listen().await?;
                        buf.clear();
                        continue;
                    };

                    if event.uuid != C_RX {
                        continue;
                    }

                    if !buf.is_empty() && event.value.first() == Some(&HEADER) {
                        buf.clear();
                    }
                    buf.extend_from_slice(&event.value);

                    if buf.len() >= FRAME_OVERHEAD {
                        let payload_len = u16::from_be_bytes([buf[1], buf[2]]) as usize;
                        if buf.len() - FRAME_OVERHEAD != payload_len {
                            continue;
                        }
                        if on_frame(&buf).is_break() {
                            return Ok(());
                        }
                        buf.clear();
                    }
                }
            }
        }
    }

    pub async fn subscribe_measure<F>(&mut self, mut on_measure: F) -> Result<()>
    where
        F: FnMut(Measurement) -> ControlFlow<()>,
    {
        self.subscribe(payload::monitoring(), |frame| match read_measure(frame) {
            Ok(m) => on_measure(m),
            Err(e) => {
                eprintln!("[ERR] Failed to parse measurement: {e}");
                ControlFlow::Continue(())
            }
        })
        .await
    }

    async fn listen(&self) -> Result<Notifications> {
        self.device.subscribe(&self.rx).await?;
        Ok(self.device.notifications().await?)
    }

    /// Write `payload`, reconnecting on failure. Returns whether the
    /// connection was re-established along the way.
    async fn write(&mut self, payload: &[u8]) -> Result<bool> {
        let mut reconnected = false;

        for attempt in 1.. {
            // Re-read every attempt: a reconnect along the way replaces `tx`.
            let write_type = if self.tx.properties.contains(CharPropFlags::WRITE) {
                WriteType::WithResponse
            } else {
                WriteType::WithoutResponse
            };

            match self.device.write(&self.tx, payload, write_type).await {
                Ok(()) => return Ok(reconnected),
                Err(e) if attempt < WRITE_RETRIES => {
                    eprintln!("[WARN] Write failed: {e}");
                    match self.connect().await {
                        Ok(()) => reconnected = true,
                        Err(e) => {
                            eprintln!("[WARN] Reconnect failed: {e}");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("write failed after {attempt} attempts"));
                }
            }
        }

        unreachable!()
    }
}

fn read_measure(frame: &[u8]) -> Result<Measurement> {
    if frame.len() < 29 {
        bail!("frame too short: {} bytes", frame.len());
    }

    // [sec, min, hour, day, mon, year - 1900]
    let date = &frame[23..29];
    let timestamp =
        NaiveDate::from_ymd_opt(1900 + date[5] as i32, date[4] as u32 + 1, date[3] as u32)
            .and_then(|d| d.and_hms_opt(date[2] as u32, date[1] as u32, date[0] as u32))
            .and_then(local_datetime)
            .ok_or_else(|| anyhow!("invalid timestamp in frame"))?;

    Ok(Measurement {
        voltage: u48_le(&frame[5..11]) as f64 / VOLTAGE_SCALE,
        ampere: u48_le(&frame[11..17]) as f64 / AMPERE_SCALE,
        wattage: u48_le(&frame[17..23]) as f64 / WATTAGE_SCALE,
        timestamp,
    })
}

fn u48_le(payload: &[u8]) -> u64 {
    payload
        .iter()
        .rev()
        .fold(0, |acc, &byte| (acc << 8) | u64::from(byte))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};

    #[test]
    fn u48_le_is_little_endian() {
        assert_eq!(u48_le(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00]), 1);
        assert_eq!(u48_le(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x01]), 1 << 40);
    }

    #[test]
    fn read_measure_parses_frame() {
        let mut frame = vec![0u8; 30];
        frame[0] = HEADER;
        // voltage = 100.5 * 2^24, little-endian
        frame[5..11].copy_from_slice(&(100 * (1u64 << 24) + (1 << 23)).to_le_bytes()[..6]);
        // ampere = 1.25 * 2^30
        frame[11..17].copy_from_slice(&((1u64 << 30) + (1 << 28)).to_le_bytes()[..6]);
        // wattage = 2^24
        frame[17..23].copy_from_slice(&(1u64 << 24).to_le_bytes()[..6]);
        // 2021-01-02 03:04:05 => [sec, min, hour, day, mon, year - 1900]
        frame[23..29].copy_from_slice(&[5, 4, 3, 2, 0, 121]);

        let m = read_measure(&frame).unwrap();
        assert_eq!(m.voltage, 100.5);
        assert_eq!(m.ampere, 1.25);
        assert_eq!(m.wattage, 1.0);
        assert_eq!(
            (
                m.timestamp.year(),
                m.timestamp.month(),
                m.timestamp.day(),
                m.timestamp.hour(),
                m.timestamp.minute(),
                m.timestamp.second()
            ),
            (2021, 1, 2, 3, 4, 5)
        );
    }

    #[test]
    fn read_measure_rejects_short_frame() {
        assert!(read_measure(&[0xAA, 0x00, 0x00, 0x00]).is_err());
    }
}
