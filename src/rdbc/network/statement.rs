use anyhow::Result;

use super::{planrepr::NetworkPlanRepr, resultset::NetworkResultSet};
use crate::{
    rdbc::statementadapter::StatementAdapter,
    remote_capnp::{
        remote_result_set,
        remote_statement::{self, *},
    },
};

pub struct NetworkStatement {
    client: remote_statement::Client,
}

impl NetworkStatement {
    pub fn new(client: remote_statement::Client) -> Self {
        Self { client }
    }
}

impl<'a> StatementAdapter<'a> for NetworkStatement {
    type Set = NetworkResultSet;
    type PlanRepr = NetworkPlanRepr;

    fn execute_query(&'a mut self) -> Result<Self::Set> {
        let rt = tokio::runtime::Runtime::new().unwrap(); // TODO
        let resultset = rt.block_on(async {
            let request = self.client.execute_query_request();
            request.send().pipeline.get_result()
        });

        Ok(NetworkResultSet::new(resultset))
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
