use anyhow::Result;
use core::fmt;
use serde::{Deserialize, Serialize};

use super::metadata::RemoteMetaData;
use crate::rdbc::resultsetadapter::ResultSetAdapter;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteResultSet {}

impl fmt::Display for RemoteResultSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl RemoteResultSet {
    pub fn new() -> Self {
        Self {}
    }
}

impl ResultSetAdapter for RemoteResultSet {
    type Meta = RemoteMetaData;

    fn next(&self) -> bool {
        panic!("TODO")
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        panic!("TODO")
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        panic!("TODO")
    }
    fn get_meta_data(&self) -> Result<Self::Meta> {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
}
