use crate::header::Header;
use std::fs::File;
use std::io::{Read, Seek, Write};

#[derive(Debug)]
pub enum PageManagerError {
    Io(std::io::Error),
    HeaderNotWritten,
}

impl std::fmt::Display for PageManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PageManagerError::Io(e) => {
                write!(f, "IO error: {}", e)
            }
            PageManagerError::HeaderNotWritten {} => {
                write!(f, "Header has not been written")
            }
        }
    }
}

impl From<std::io::Error> for PageManagerError {
    fn from(err: std::io::Error) -> PageManagerError {
        PageManagerError::Io(err)
    }
}

pub struct PageManager {
    file: File,
    pub page_size: u64,
    pub header_size: u64,
}

impl PageManager {
    pub fn new(file: File, page_size: u64, header_size: u64) -> Self {
        let mut file_clone = file.try_clone().unwrap();
        let file_length = file_clone.seek(std::io::SeekFrom::End(0)).unwrap();
        if file_length < header_size {
            let header_buffer = vec![0u8; header_size as usize];
            file_clone.write_all(&header_buffer).unwrap();
        }

        PageManager {
            file,
            page_size,
            header_size,
        }
    }

    fn from_pageid(&self, page_id: u64) -> u64 {
        (page_id * self.page_size) + self.header_size
    }

    fn to_pageid(&self, byte_offset: u64) -> u64 {
        (byte_offset - self.header_size) / self.page_size
    }

    pub fn allocate_page(&mut self) -> Result<u64, PageManagerError> {
        self.file.seek(std::io::SeekFrom::End(0))?;

        let byte_offset = self.file.seek(std::io::SeekFrom::Current(0))?;
        if byte_offset < Header::SIZE as u64 {
            return Err(PageManagerError::HeaderNotWritten);
        }

        let page_id = self.to_pageid(byte_offset);

        self.file
            .write(&vec![0u8; self.page_size.try_into().unwrap()])?;

        return Ok(page_id);
    }

    pub fn write_header(&mut self, data: &[u8]) -> Result<(), std::io::Error> {
        if data.len() > self.header_size as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Buffer too large: expected {} got {}",
                    self.header_size,
                    data.len()
                ),
            ));
        }

        let _ = self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.write_all(data)?;
        Ok(())
    }

    pub fn read_header(&mut self) -> Result<Vec<u8>, std::io::Error> {
        let mut buffer = vec![0u8; self.header_size as usize];
        let _ = self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.read(&mut buffer)?;
        Ok(buffer)
    }

    pub fn write_page(&mut self, page_id: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.file
            .seek(std::io::SeekFrom::Start(self.from_pageid(page_id)))?;

        self.file.write_all(data)?;
        Ok(())
    }

    pub fn read_page(&mut self, page_id: u64) -> Result<(Box<Vec<u8>>, usize), std::io::Error> {
        self.file
            .seek(std::io::SeekFrom::Start(self.from_pageid(page_id)))?;

        let buffer_size: usize = self.page_size.try_into().unwrap();
        let mut buffer = vec![0u8; buffer_size];
        let bytes_read = self.file.read(&mut buffer)?;
        Ok((Box::new(buffer), bytes_read))
    }
}
