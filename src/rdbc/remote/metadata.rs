use anyhow::Result;
use core::fmt;
use serde::{Deserialize, Serialize};

use crate::rdbc::resultsetmetadataadapter::{DataType, ResultSetMetaDataAdapter};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteMetaData {}

impl fmt::Display for RemoteMetaData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl RemoteMetaData {
    pub fn new() -> Self {
        Self {}
    }
}

impl ResultSetMetaDataAdapter for RemoteMetaData {
    fn get_column_count(&self) -> usize {
        panic!("TODO")
    }
    fn get_column_name(&self, column: usize) -> Option<&String> {
        panic!("TODO")
    }
    fn get_column_type(&self, column: usize) -> Option<DataType> {
        panic!("TODO")
    }
    fn get_column_display_size(&self, column: usize) -> Option<usize> {
        panic!("TODO")
    }
}
