use anyhow::Result;
use core::fmt;
use serde::{Deserialize, Serialize};

use super::resultset::RemoteResultSet;
use crate::rdbc::statementadapter::StatementAdapter;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteStatement {}

impl fmt::Display for RemoteStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl RemoteStatement {
    pub fn new() -> Self {
        Self {}
    }
}

impl<'a> StatementAdapter<'a> for RemoteStatement {
    type Set = RemoteResultSet;

    fn execute_query(&'a mut self) -> Result<Self::Set> {
        panic!("TODO")
    }
    fn execute_update(&mut self) -> Result<i32> {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
}
