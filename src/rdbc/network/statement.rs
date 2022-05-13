use anyhow::Result;

use super::{planrepr::NetworkPlanRepr, resultset::NetworkResultSet};
use crate::{
    rdbc::statementadapter::StatementAdapter,
    remote_capnp::{affected, remote_statement},
};

pub struct AffectedImpl {
    affected: affected::Client,
}
impl AffectedImpl {
    pub fn new(affected: affected::Client) -> Self {
        Self { affected }
    }
}

pub struct NetworkStatement {
    stmt: remote_statement::Client,
}

impl NetworkStatement {
    pub fn new(stmt: remote_statement::Client) -> Self {
        Self { stmt }
    }
    pub async fn explain_plan(&mut self) -> Result<NetworkPlanRepr> {
        let request = self.stmt.explain_plan_request();
        let reply = request.send().promise.await?;
        let planrepr = reply.get()?.get_planrepr()?;

        Ok(NetworkPlanRepr::from(planrepr))
    }
}

impl<'a> StatementAdapter<'a> for NetworkStatement {
    type Set = NetworkResultSet;
    type Aeffected = AffectedImpl;

    fn execute_query(&'a mut self) -> Result<Self::Set> {
        let resultset = self
            .stmt
            .execute_query_request()
            .send()
            .pipeline
            .get_result();

        Ok(Self::Set::new(resultset))
    }
    fn execute_update(&mut self) -> Result<Self::Aeffected> {
        let request = self.stmt.execute_update_request();
        let affected = request.send().pipeline.get_affected();

        Ok(AffectedImpl::new(affected))
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
}
