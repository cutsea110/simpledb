use anyhow::Result;
use core::fmt;
use std::{cell::RefCell, rc::Rc};

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

pub trait StatementAdapter {
    fn execute_query(&mut self) -> Result<Rc<RefCell<dyn ResultSetAdapter>>>;
    fn execute_update(&mut self) -> Result<i32>;
    fn close(&mut self) -> Result<()>;
}
