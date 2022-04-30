use anyhow::Result;

use super::{planrepr::NetworkPlanRepr, resultset::NetworkResultSet};
use crate::rdbc::statementadapter::StatementAdapter;

pub struct NetworkStatement {
    // TODO
}

impl<'a> StatementAdapter<'a> for NetworkStatement {
    type Set = NetworkResultSet;
    type PlanRepr = NetworkPlanRepr;

    fn execute_query(&'a mut self) -> Result<Self::Set> {
        panic!("TODO")
    }
    fn execute_update(&mut self) -> Result<i32> {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn explain_plan(&mut self) -> Result<Self::PlanRepr> {
        panic!("TODO")
    }
}
