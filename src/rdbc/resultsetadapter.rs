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
    type Int8Value;
    type UInt8Value;
    type Int16Value;
    type UInt16Value;
    type Int32Value;
    type UInt32Value;
    type StringValue;
    type BoolValue;
    type DateValue;
    type Res;

    fn next(&self) -> Self::Next;
    fn get_i8(&mut self, fldname: &str) -> Result<Self::Int8Value>;
    fn get_u8(&mut self, fldname: &str) -> Result<Self::UInt8Value>;
    fn get_i16(&mut self, fldname: &str) -> Result<Self::Int16Value>;
    fn get_u16(&mut self, fldname: &str) -> Result<Self::UInt16Value>;
    fn get_i32(&mut self, fldname: &str) -> Result<Self::Int32Value>;
    fn get_u32(&mut self, fldname: &str) -> Result<Self::UInt32Value>;
    fn get_string(&mut self, fldname: &str) -> Result<Self::StringValue>;
    fn get_bool(&mut self, fldname: &str) -> Result<Self::BoolValue>;
    fn get_date(&mut self, fldname: &str) -> Result<Self::DateValue>;
    fn get_meta_data(&self) -> Result<Self::Meta>;
    fn close(&mut self) -> Result<Self::Res>;
}
