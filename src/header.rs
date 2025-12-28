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
