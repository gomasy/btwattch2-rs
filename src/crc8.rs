const POLYNOMIAL: u8 = 0x85;

pub fn crc8(payload: &[u8]) -> u8 {
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

    #[test]
    fn crc8_of_monitoring_command() {
        assert_eq!(crc8(&[0x08]), 0xB3);
    }

    #[test]
    fn crc8_of_empty_payload() {
        assert_eq!(crc8(&[]), 0x00);
    }
}
