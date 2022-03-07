use rdbc::{Error, Result, ResultSet, ResultSetMetaData};
use std::rc::Rc;

use crate::rdbc::resultsetadapter::ResultSetAdapter;

pub struct EmbeddedResultSet {}

impl ResultSet for EmbeddedResultSet {
    fn meta_data(&self) -> Result<Rc<dyn ResultSetMetaData>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn next(&mut self) -> bool {
        false
    }
    fn get_i8(&self, i: u64) -> Result<Option<i8>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn get_i16(&self, i: u64) -> Result<Option<i16>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn get_i32(&self, i: u64) -> Result<Option<i32>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn get_i64(&self, i: u64) -> Result<Option<i64>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn get_f32(&self, i: u64) -> Result<Option<f32>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn get_f64(&self, i: u64) -> Result<Option<f64>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn get_string(&self, i: u64) -> Result<Option<String>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn get_bytes(&self, i: u64) -> Result<Option<Vec<u8>>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
}

impl ResultSetAdapter for EmbeddedResultSet {
    fn close(&mut self) -> Result<()> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
}
