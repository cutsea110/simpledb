use anyhow::Result;

use super::{planrepr::NetworkPlanRepr, resultset::NetworkResultSet};
use crate::{rdbc::statementadapter::StatementAdapter, remote_capnp::remote_statement};

pub struct NetworkStatement {
    client: remote_statement::Client,
}

impl NetworkStatement {
    pub fn new(client: remote_statement::Client) -> Self {
        Self { client }
    }
    pub fn explain_plan(&mut self) -> Result<NetworkPlanRepr> {
        let rt = tokio::runtime::Runtime::new().unwrap(); // TODO
        let planrepr = rt.block_on(async {
            let request = self.client.explain_plan_request();
            let reply = request.send().promise.await.unwrap();
            let repr = reply.get().unwrap().get_planrepr().unwrap();

            NetworkPlanRepr::from(repr)
        });

        Ok(planrepr)
    }
}

impl<'a> StatementAdapter<'a> for NetworkStatement {
    type Set = NetworkResultSet;

    fn execute_query(&'a mut self) -> Result<Self::Set> {
        let rt = tokio::runtime::Runtime::new().unwrap(); // TODO
        let resultset = rt.block_on(async {
            let request = self.client.execute_query_request();
            request.send().pipeline.get_result()
        });

        Ok(NetworkResultSet::new(resultset))
    }
    fn execute_update(&mut self) -> Result<i32> {
        let rt = tokio::runtime::Runtime::new().unwrap(); // TODO
        let affected = rt.block_on(async {
            let request = self.client.execute_update_request();
            let reply = request.send().promise.await.unwrap();
            reply.get().unwrap().get_affected() // TODO
        });

        Ok(affected)
    }
    fn close(&mut self) -> Result<()> {
        let rt = tokio::runtime::Runtime::new().unwrap(); // TODO
        rt.block_on(async {
            let request = self.client.close_request();
            request.send().promise.await.unwrap();
        });

        Ok(())
    }
}
