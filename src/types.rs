use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum NodeType {
    INTERNAL = 0,
    LEAF = 1,
}

impl From<u8> for NodeType {
    fn from(value: u8) -> NodeType {
        match value {
            0 => NodeType::INTERNAL,
            1 => NodeType::LEAF,
            _ => panic!("Invalid node type"),
        }
    }
}
