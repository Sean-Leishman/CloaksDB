use std::fmt::Debug;

#[derive(Debug)]
pub struct Slot {
    pub offset: u16,
    pub key_length: u16,
    pub value_length: u16,
}

impl Slot {
    pub const SIZE: usize = 6;

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
}
