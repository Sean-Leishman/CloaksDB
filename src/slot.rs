use std::fmt::Debug;

#[derive(Debug)]
pub struct Slot {
    pub offset: u16,
    pub key_length: u16,
    pub value_length: u16,
}

impl Slot {
    pub const SIZE: usize = 6;

    pub fn total_length(&self) -> u16 {
        self.key_length + self.value_length
    }

    pub fn serialize(&self) -> [u8; Self::SIZE] {
        let mut buffer = [0u8; Self::SIZE];
        buffer[0..2].copy_from_slice(&self.offset.to_le_bytes());
        buffer[2..4].copy_from_slice(&self.key_length.to_le_bytes());
        buffer[4..6].copy_from_slice(&self.value_length.to_le_bytes());

        buffer
    }

    pub fn deserialize(buffer: &[u8]) -> Self {
        let offset = u16::from_le_bytes(buffer[0..2].try_into().unwrap());
        let key_length = u16::from_le_bytes(buffer[2..4].try_into().unwrap());
        let value_length = u16::from_le_bytes(buffer[4..6].try_into().unwrap());

        Slot {
            offset: offset,
            key_length: key_length,
            value_length: value_length,
        }
    }

    pub fn clone(&self) -> Self {
        Slot {
            offset: self.offset,
            key_length: self.key_length,
            value_length: self.value_length,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_roundtrip() {
        let slot = Slot {
            offset: 100,
            key_length: 50,
            value_length: 200,
        };

        let bytes = slot.serialize();
        let restored = Slot::deserialize(&bytes);

        assert_eq!(restored.offset, 100);
        assert_eq!(restored.key_length, 50);
        assert_eq!(restored.value_length, 200);
    }

    #[test]
    fn slot_roundtrip_max_values() {
        let slot = Slot {
            offset: u16::MAX,
            key_length: u16::MAX,
            value_length: u16::MAX,
        };

        let bytes = slot.serialize();
        let restored = Slot::deserialize(&bytes);

        assert_eq!(restored.offset, u16::MAX);
        assert_eq!(restored.key_length, u16::MAX);
        assert_eq!(restored.value_length, u16::MAX);
    }

    #[test]
    fn slot_roundtrip_zero_values() {
        let slot = Slot {
            offset: 0,
            key_length: 0,
            value_length: 0,
        };

        let bytes = slot.serialize();
        let restored = Slot::deserialize(&bytes);

        assert_eq!(restored.offset, 0);
        assert_eq!(restored.key_length, 0);
        assert_eq!(restored.value_length, 0);
    }

    #[test]
    fn slot_size_is_correct() {
        let slot = Slot {
            offset: 0,
            key_length: 0,
            value_length: 0,
        };

        assert_eq!(slot.serialize().len(), Slot::SIZE);
        assert_eq!(Slot::SIZE, 6);
    }

    #[test]
    fn slot_total_length() {
        let slot = Slot {
            offset: 0,
            key_length: 10,
            value_length: 20,
        };

        assert_eq!(slot.total_length(), 30);
    }
}
