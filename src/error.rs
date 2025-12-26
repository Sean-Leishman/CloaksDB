use crate::header::HeaderError;
use crate::page_manager::PageManagerError;
use crate::slotted_page::SlottedPageError;

impl From<SlottedPageError> for BTreeError {
    fn from(err: SlottedPageError) -> BTreeError {
        BTreeError::SlottedPage(err)
    }
}

#[derive(Debug)]
pub enum BTreeError {
    Io(std::io::Error),
    Serialization(bincode::Error),
    Header(HeaderError),
    PageManager(PageManagerError),
    SlottedPage(SlottedPageError),
    KeyNotFound(String),
    InvalidNodeType(u8),
    PageOverflow { page_id: u64 },
}

impl std::fmt::Display for BTreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BTreeError::Io(e) => {
                write!(f, "IO error: {}", e)
            }
            BTreeError::Serialization(e) => {
                write!(f, "Serialization error: {}", e)
            }
            BTreeError::Header(e) => {
                write!(f, "Header error: {}", e)
            }
            BTreeError::PageManager(e) => {
                write!(f, "PageManager error: {}", e)
            }
            BTreeError::SlottedPage(e) => {
                write!(f, "SlottedPage error: {}", e)
            }
            BTreeError::KeyNotFound(key) => {
                write!(f, "KeyNotFound: {}", key)
            }
            BTreeError::InvalidNodeType(node_type) => {
                write!(f, "InvalidNodeType: {}", node_type)
            }
            BTreeError::PageOverflow { page_id } => {
                write!(f, "PageOverflow: page_id={}", page_id)
            }
        }
    }
}

impl From<std::io::Error> for BTreeError {
    fn from(err: std::io::Error) -> BTreeError {
        BTreeError::Io(err)
    }
}

impl From<HeaderError> for BTreeError {
    fn from(err: HeaderError) -> BTreeError {
        BTreeError::Header(err)
    }
}
