use anyhow::Result;
use core::fmt;

use super::resultsetadapter::ResultSetAdapter;

#[derive(Debug)]
pub enum StatementError {
    RuntimeError,
    CloseFailed,
}

impl std::error::Error for StatementError {}
impl fmt::Display for StatementError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StatementError::RuntimeError => {
                write!(f, "runtime error")
            }
            StatementError::CloseFailed => {
                write!(f, "failed to close")
            }
        }
    }
}

pub trait StatementAdapter<'a> {
    type Set: ResultSetAdapter;

    fn execute_query(&'a mut self) -> Result<Self::Set>;
    fn execute_update(&mut self) -> Result<i32>;
    fn close(&mut self) -> Result<()>;
}
