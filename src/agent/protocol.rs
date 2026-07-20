use serde::{Deserialize, Serialize};

use crate::connection::Measurement;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Request {
    SetRtc { time: String },
    GetRtc,
    Power { on: bool },
    TestLed,
    Subscribe,
    Ping,
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Ok,
    Pong,
    Error {
        message: String,
    },
    Measurement {
        voltage: f64,
        ampere: f64,
        wattage: f64,
        power_factor: f64,
        timestamp: i64,
    },
    CommandResult {
        success: bool,
        code: Option<u8>,
    },
    StreamEnd,
}

impl Response {
    pub fn from_measurement(m: &Measurement) -> Self {
        Self::Measurement {
            voltage: m.voltage,
            ampere: m.ampere,
            wattage: m.wattage,
            power_factor: m.power_factor,
            timestamp: m.timestamp.timestamp(),
        }
    }

    pub fn to_measurement(&self) -> Option<Measurement> {
        use chrono::{Local, TimeZone};
        match self {
            Self::Measurement {
                voltage,
                ampere,
                wattage,
                power_factor,
                timestamp,
            } => {
                let dt = Local.timestamp_opt(*timestamp, 0).single()?;
                Some(Measurement {
                    voltage: *voltage,
                    ampere: *ampere,
                    wattage: *wattage,
                    power_factor: *power_factor,
                    timestamp: dt,
                })
            }
            _ => None,
        }
    }
}
