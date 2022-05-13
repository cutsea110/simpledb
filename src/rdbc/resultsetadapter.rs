use anyhow::Result;
use core::fmt;

use super::resultsetmetadataadapter::ResultSetMetaDataAdapter;

#[derive(Debug)]
pub enum ResultSetError {
    ScanFailed,
    RollbackFailed,
    CloseFailed,
    UnknownField(String),
}

impl std::error::Error for ResultSetError {}
impl fmt::Display for ResultSetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ResultSetError::ScanFailed => {
                write!(f, "failed to scan")
            }
            ResultSetError::RollbackFailed => {
                write!(f, "failed to rollback")
            }
            ResultSetError::CloseFailed => {
                write!(f, "failed to close")
            }
            ResultSetError::UnknownField(fldname) => {
                write!(f, "unknown field {}", fldname)
            }
        }
    }
}

pub trait ResultSetAdapter {
    type Meta: ResultSetMetaDataAdapter;
    type Next;
    type Int32Value;
    type StringValue;

    fn next(&self) -> Self::Next;
    fn get_i32(&mut self, fldname: &str) -> Result<Self::Int32Value>;
    fn get_string(&mut self, fldname: &str) -> Result<Self::StringValue>;
    fn get_meta_data(&self) -> Result<Self::Meta>;
    fn close(&mut self) -> Result<()>;
}
