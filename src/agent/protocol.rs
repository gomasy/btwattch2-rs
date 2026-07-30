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

/// What the agent reports about itself on a ping, for `agent status`.
///
/// Every field is defaulted, so a reply from an agent too old to send them
/// parses as a status with nothing to say rather than failing outright — the
/// same tolerance `addr` gets on `Pong`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentStatus {
    /// How long the agent has been serving.
    pub uptime_seconds: u64,
    /// The polling period it was started with.
    pub interval_seconds: f64,
    /// Whether it currently believes the BLE link is up. A running agent with a
    /// dead link is the state this exists to make visible.
    pub connected: bool,
    /// Measurements read since start, and how long ago the last one arrived.
    pub samples: u64,
    pub last_sample_age_seconds: Option<f64>,
    /// Links re-established since start.
    pub reconnects: u64,
    /// Clients currently streaming.
    pub clients: usize,
    /// Where the metrics endpoint is bound, if it is serving.
    pub metrics_listen: Option<String>,
}

/// Cloneable so one measurement can be handed to every streaming client without
/// re-deriving it per client.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Ok,
    Pong {
        /// The device the agent is attached to. Defaulted so a ping reply from
        /// an agent that predates the field still parses, as `None`.
        #[serde(default)]
        addr: Option<String>,
        #[serde(default)]
        status: Option<AgentStatus>,
    },
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
        /// The device's status byte; zero means it accepted the command. Only
        /// the code travels — a `success` flag alongside it would be derived
        /// state that the wire could contradict.
        code: u8,
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

#[cfg(test)]
mod tests {
    use chrono::{Local, TimeZone};

    use super::*;

    #[test]
    fn measurement_survives_a_round_trip() {
        let m = Measurement {
            voltage: 100.5,
            ampere: 1.25,
            wattage: 125.0,
            power_factor: 0.99,
            timestamp: Local.timestamp_opt(1609304963, 0).unwrap(),
        };
        let json = serde_json::to_string(&Response::from_measurement(&m)).unwrap();
        let back: Response = serde_json::from_str(&json).unwrap();
        let back = back.to_measurement().unwrap();

        assert_eq!(back.voltage, m.voltage);
        assert_eq!(back.ampere, m.ampere);
        assert_eq!(back.wattage, m.wattage);
        assert_eq!(back.power_factor, m.power_factor);
        assert_eq!(back.timestamp, m.timestamp);
    }

    /// `to_measurement` is how clients recognise a measurement reply, so every
    /// other variant has to yield `None` rather than a bogus reading.
    #[test]
    fn other_responses_are_not_measurements() {
        for resp in [
            Response::Ok,
            Response::Pong {
                addr: None,
                status: None,
            },
            Response::StreamEnd,
            Response::CommandResult { code: 0 },
        ] {
            assert!(resp.to_measurement().is_none(), "{resp:?}");
        }
    }

    /// A pong from an agent that predates the `addr` and `status` fields must
    /// still parse, rather than making the agent look absent.
    #[test]
    fn pong_without_an_address_or_status_parses() {
        let resp: Response = serde_json::from_str(r#"{"type":"pong"}"#).unwrap();
        assert!(matches!(
            resp,
            Response::Pong {
                addr: None,
                status: None
            }
        ));
    }

    /// Likewise for a status that gained fields since the peer was built: what
    /// it does send survives, and the rest defaults.
    #[test]
    fn a_partial_status_parses() {
        let resp: Response =
            serde_json::from_str(r#"{"type":"pong","status":{"samples":7}}"#).unwrap();
        let Response::Pong {
            status: Some(status),
            ..
        } = resp
        else {
            panic!("expected a pong carrying a status");
        };
        assert_eq!(status.samples, 7);
        assert_eq!(status.reconnects, 0);
        assert_eq!(status.last_sample_age_seconds, None);
    }

    #[test]
    fn requests_are_tagged_by_cmd() {
        let json = serde_json::to_string(&Request::Power { on: true }).unwrap();
        assert_eq!(json, r#"{"cmd":"power","on":true}"#);
        assert!(matches!(
            serde_json::from_str(r#"{"cmd":"subscribe"}"#).unwrap(),
            Request::Subscribe
        ));
    }
}
