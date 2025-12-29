#[derive(Debug)]
pub struct Header {
    magic_number: u16,
    version: u16,
    pub page_size: u64,
    pub root_page_id: u64,
    page_count: u64,
}

#[derive(Debug)]
pub enum HeaderError {
    InvalidMagicNumber(u16),
    InvalidBufferSize { expected: usize, got: usize },
    CorruptedData(String),
}

impl std::fmt::Display for HeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            HeaderError::InvalidMagicNumber(num) => {
                write!(f, "Invalid magic number: {} (must be > 0)", num)
            }
            HeaderError::InvalidBufferSize { expected, got } => {
                write!(f, "Invalid buffer size: expected {}, got {}", expected, got)
            }
            HeaderError::CorruptedData(msg) => {
                write!(f, "Corrupted header data: {}", msg)
            }
        }
    }
}

impl Header {
    pub const SIZE: usize = 28;

    pub fn new(
        magic_number: u16,
        version: u16,
        page_size: u64,
        root_page_id: u64,
        page_count: u64,
    ) -> Self {
        Header {
            magic_number,
            version,
            page_size,
            root_page_id,
            page_count,
        }
    }

    pub fn pages_empty(&self) -> bool {
        self.page_count == 0
    }

    pub fn add_root_page(&mut self, root_page_id: u64) {
        self.root_page_id = root_page_id;
        self.add_page();
    }

    pub fn add_page(&mut self) {
        self.page_count += 1;
    }

    pub fn serialize(&self) -> [u8; Self::SIZE] {
        let mut buffer = [0u8; Self::SIZE];
        buffer[0..2].copy_from_slice(&self.magic_number.to_le_bytes());
        buffer[2..4].copy_from_slice(&self.version.to_le_bytes());
        buffer[4..12].copy_from_slice(&self.page_size.to_le_bytes());
        buffer[12..20].copy_from_slice(&self.root_page_id.to_le_bytes());
        buffer[20..28].copy_from_slice(&self.page_count.to_le_bytes());

        buffer
    }

    pub fn deserialize(buffer: &[u8]) -> Result<Self, HeaderError> {
        if buffer.len() < Header::SIZE {
            return Err(HeaderError::InvalidBufferSize {
                expected: Header::SIZE,
                got: buffer.len(),
            });
        }

        let magic_number = u16::from_le_bytes(buffer[0..2].try_into().unwrap());
        if magic_number == 0 {
            return Err(HeaderError::InvalidMagicNumber(magic_number));
        }

        let version = u16::from_le_bytes(buffer[2..4].try_into().unwrap());
        let page_size = u64::from_le_bytes(buffer[4..12].try_into().unwrap());
        let root_page_id = u64::from_le_bytes(buffer[12..20].try_into().unwrap());
        let page_count = u64::from_le_bytes(buffer[20..28].try_into().unwrap());

        Ok(Header {
            magic_number,
            version,
            page_size,
            root_page_id,
            page_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─────────────────────────────────────────────────────────
    // Serialization / Deserialization
    // ─────────────────────────────────────────────────────────

    #[test]
    fn header_roundtrip_basic() {
        let header = Header {
            magic_number: 1,
            version: 0,
            page_size: 4096,
            root_page_id: 0,
            page_count: 1,
        };

        let bytes = header.serialize();
        let restored = Header::deserialize(&bytes).unwrap();

        assert_eq!(restored.magic_number, 1);
        assert_eq!(restored.version, 0);
        assert_eq!(restored.page_size, 4096);
        assert_eq!(restored.root_page_id, 0);
        assert_eq!(restored.page_count, 1);
    }

    #[test]
    fn header_roundtrip_large_values() {
        let header = Header {
            magic_number: u16::MAX,
            version: u16::MAX,
            page_size: u64::MAX,
            root_page_id: u64::MAX,
            page_count: u64::MAX,
        };

        let bytes = header.serialize();
        let restored = Header::deserialize(&bytes).unwrap();

        assert_eq!(restored.magic_number, u16::MAX);
        assert_eq!(restored.version, u16::MAX);
        assert_eq!(restored.page_size, u64::MAX);
        assert_eq!(restored.root_page_id, u64::MAX);
        assert_eq!(restored.page_count, u64::MAX);
    }

    #[test]
    fn header_serialize_size_is_correct() {
        let header = Header {
            magic_number: 1,
            version: 0,
            page_size: 4096,
            root_page_id: 0,
            page_count: 1,
        };

        let bytes = header.serialize();
        assert_eq!(bytes.len(), Header::SIZE);
    }

    // ─────────────────────────────────────────────────────────
    // Error Cases
    // ─────────────────────────────────────────────────────────

    #[test]
    fn header_rejects_zero_magic_number() {
        let mut bytes = [0u8; Header::SIZE];
        // magic_number = 0 (invalid)
        bytes[0..2].copy_from_slice(&0u16.to_le_bytes());

        let result = Header::deserialize(&bytes);
        assert!(matches!(result, Err(HeaderError::InvalidMagicNumber(0))));
    }

    #[test]
    fn header_rejects_short_buffer() {
        let bytes = [0u8; Header::SIZE - 1];
        let result = Header::deserialize(&bytes);

        assert!(matches!(
            result,
            Err(HeaderError::InvalidBufferSize {
                expected: _,
                got: _
            })
        ));
    }

    #[test]
    fn header_accepts_longer_buffer() {
        let mut bytes = vec![0u8; Header::SIZE + 100];
        bytes[0..2].copy_from_slice(&1u16.to_le_bytes()); // valid magic number

        let result = Header::deserialize(&bytes);
        assert!(result.is_ok());
    }

    // ─────────────────────────────────────────────────────────
    // Field Position Tests (catch layout bugs)
    // ─────────────────────────────────────────────────────────

    #[test]
    fn header_field_positions_are_correct() {
        let header = Header {
            magic_number: 0x1234,
            version: 0x5678,
            page_size: 0x1111_2222_3333_4444,
            root_page_id: 0x5555_6666_7777_8888,
            page_count: 0x9999_AAAA_BBBB_CCCC,
        };

        let bytes = header.serialize();

        // Check each field at expected offset
        assert_eq!(u16::from_le_bytes(bytes[0..2].try_into().unwrap()), 0x1234);
        assert_eq!(u16::from_le_bytes(bytes[2..4].try_into().unwrap()), 0x5678);
        assert_eq!(
            u64::from_le_bytes(bytes[4..12].try_into().unwrap()),
            0x1111_2222_3333_4444
        );
        assert_eq!(
            u64::from_le_bytes(bytes[12..20].try_into().unwrap()),
            0x5555_6666_7777_8888
        );
        assert_eq!(
            u64::from_le_bytes(bytes[20..28].try_into().unwrap()),
            0x9999_AAAA_BBBB_CCCC
        );
    }
}
