use anyhow::Result;

use super::{planrepr::NetworkPlanRepr, resultset::NetworkResultSet};
use crate::{rdbc::model::Schema, remote_capnp::remote_statement};

pub struct NetworkStatement {
    stmt: remote_statement::Client,
}

impl NetworkStatement {
    pub fn new(stmt: remote_statement::Client) -> Self {
        Self { stmt }
    }

    pub async fn execute_query(&mut self) -> Result<NetworkResultSet> {
        let response = self.stmt.execute_query_request().send().promise.await?;
        let response = response.get()?;
        let resultset = response.get_result()?;
        let schema = Schema::from(response.get_schema()?);
        Ok(NetworkResultSet::new(resultset, schema))
    }

    pub async fn execute_update(&mut self) -> Result<(i32, i32)> {
        let response = self.stmt.execute_update_request().send().promise.await?;
        let response = response.get()?;
        Ok((response.get_affected(), response.get_committed_tx()))
    }

    pub async fn close(&mut self) -> Result<i32> {
        let response = self.stmt.close_request().send().promise.await?;
        Ok(response.get()?.get_tx())
    }

    pub async fn explain_plan(&mut self) -> Result<NetworkPlanRepr> {
        let response = self.stmt.explain_plan_request().send().promise.await?;
        let planrepr = response.get()?.get_planrepr()?;
        Ok(NetworkPlanRepr::from(planrepr))
    }
}
