use anyhow::Result;

use super::metadata::NetworkResultSetMetaData;
use crate::{rdbc::resultsetadapter::ResultSetAdapter, remote_capnp};
use remote_capnp::remote_result_set;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Value {
    Int32(i32),
    String(String),
}

pub struct NetworkResultSet {
    resultset: remote_result_set::Client,
}
impl NetworkResultSet {
    pub fn new(resultset: remote_result_set::Client) -> Self {
        Self { resultset }
    }
}

impl ResultSetAdapter for NetworkResultSet {
    type Meta = NetworkResultSetMetaData;

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
