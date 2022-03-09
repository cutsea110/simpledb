use anyhow::Result;
use core::fmt;

use super::resultsetmetadataadapter::ResultSetMetaDataAdapter;

#[derive(Debug)]
pub enum ResultSetError {
    ScanFailed,
}

impl std::error::Error for ResultSetError {}
impl fmt::Display for ResultSetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ResultSetError::ScanFailed => {
                write!(f, "failed to scan")
            }
        }
    }
}

pub trait ResultSetAdapter {
    type Meta: ResultSetMetaDataAdapter;

    fn next(&self) -> bool;
    fn get_i32(&self, fldname: &str) -> Result<i32>;
    fn get_string(&self, fldname: &str) -> Result<String>;
    fn get_meta_data(&self) -> Result<Self::Meta>;
    fn close(&mut self) -> Result<()>;
}
