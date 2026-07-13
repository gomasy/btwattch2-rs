use chrono::{DateTime, Datelike, Local, Timelike};

/// Every frame, in both directions, starts with this byte.
pub const HEADER: u8 = 0xAA;
/// Length of the header (1) + size field (2) + CRC (1).
pub const FRAME_OVERHEAD: usize = 4;

const RTC_TIMER: u8 = 0x01;
const MONITORING: u8 = 0x08;
const TURN_OFF: [u8; 2] = [0xA7, 0x00];
const TURN_ON: [u8; 2] = [0xA7, 0x01];
const BLINK_LED: [u8; 5] = [0x3E, 0x01, 0x02, 0x02, 0x0F];

pub fn rtc(time: &DateTime<Local>) -> Vec<u8> {
    generate(&[
        RTC_TIMER,
        time.second() as u8,
        time.minute() as u8,
        time.hour() as u8,
        time.day() as u8,
        (time.month() - 1) as u8,
        (time.year() - 1900) as u8,
        time.weekday().num_days_from_sunday() as u8,
    ])
}

pub fn monitoring() -> Vec<u8> {
    generate(&[MONITORING])
}

pub fn on() -> Vec<u8> {
    generate(&TURN_ON)
}

pub fn off() -> Vec<u8> {
    generate(&TURN_OFF)
}

pub fn blink_led() -> Vec<u8> {
    generate(&BLINK_LED)
}

fn generate(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(payload.len() + FRAME_OVERHEAD);
    frame.push(HEADER);
    frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    frame.extend_from_slice(payload);
    frame.push(crc8(payload));
    frame
}

/// CRC-8 over the payload bytes, as the device expects: polynomial 0x85,
/// zero initial value, no reflection. Not any catalogued standard variant.
fn crc8(payload: &[u8]) -> u8 {
    const POLYNOMIAL: u8 = 0x85;

    payload.iter().fold(0x00, |mut crc, &byte| {
        crc ^= byte;
        for _ in 0..8 {
            crc = if crc & 0x80 == 0x80 {
                (crc << 1) ^ POLYNOMIAL
            } else {
                crc << 1
            };
        }
        crc
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn monitoring_frame() {
        assert_eq!(monitoring(), vec![0xAA, 0x00, 0x01, 0x08, 0xB3]);
    }

    #[test]
    fn crc8_of_monitoring_command() {
        assert_eq!(crc8(&[0x08]), 0xB3);
    }

    #[test]
    fn crc8_of_empty_payload() {
        assert_eq!(crc8(&[]), 0x00);
    }

    #[test]
    fn rtc_frame_layout() {
        let time = Local.with_ymd_and_hms(2021, 1, 2, 3, 4, 5).unwrap();
        let frame = rtc(&time);

        // header + size (BE) + payload (8 bytes) + crc
        assert_eq!(frame.len(), 12);
        assert_eq!(&frame[..3], &[0xAA, 0x00, 0x08]);
        // [cmd, sec, min, hour, day, mon - 1, year - 1900, wday]
        assert_eq!(&frame[3..11], &[0x01, 5, 4, 3, 2, 0, 121, 6]);
        assert_eq!(frame[11], crc8(&frame[3..11]));
    }
}
