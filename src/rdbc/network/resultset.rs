use anyhow::Result;

use super::metadata::NetworkResultSetMetaData;
use crate::rdbc::resultsetadapter::ResultSetAdapter;

pub struct NetworkResultSet {
    // TODO
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
