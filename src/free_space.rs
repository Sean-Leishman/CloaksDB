use std::fmt::Debug;

#[derive(Debug)]
pub struct FreeSpaceRegion {
    pub offset: u16,
    pub length: u16,
}

impl FreeSpaceRegion {
    pub const SIZE: usize = 4;
    pub fn serialize(&self) -> [u8; FreeSpaceRegion::SIZE] {
        let mut buffer = [0u8; FreeSpaceRegion::SIZE];
        buffer[0..2].copy_from_slice(&self.offset.to_le_bytes());
        buffer[2..4].copy_from_slice(&self.length.to_le_bytes());
        buffer
    }

    pub fn deserialize(buffer: &[u8; FreeSpaceRegion::SIZE]) -> Self {
        let offset = u16::from_le_bytes(buffer[0..2].try_into().unwrap());
        let length = u16::from_le_bytes(buffer[2..4].try_into().unwrap());

        FreeSpaceRegion { offset, length }
    }
}
