use anyhow::Result;

use super::{planrepr::NetworkPlanRepr, resultset::NetworkResultSet};
use crate::rdbc::statementadapter::StatementAdapter;

pub struct NetworkStatement {}

impl NetworkStatement {
    pub fn new() -> Self {
        Self {}
    }
    pub fn explain_plan(&mut self) -> Result<NetworkPlanRepr> {
        panic!("TODO")
    }
}

impl<'a> StatementAdapter<'a> for NetworkStatement {
    type Set = NetworkResultSet;

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
