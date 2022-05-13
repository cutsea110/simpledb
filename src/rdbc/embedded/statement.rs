use anyhow::Result;

use super::connection::EmbeddedConnection;
use super::planrepr::EmbeddedPlanRepr;
use super::resultset::EmbeddedResultSet;
use crate::plan::planner::Planner;
use crate::rdbc::connectionadapter::ConnectionAdapter;
use crate::rdbc::statementadapter::{StatementAdapter, StatementError};

pub struct EmbeddedStatement<'a> {
    conn: &'a mut EmbeddedConnection,
    planner: Planner,
    sql: String,
}

impl<'a> EmbeddedStatement<'a> {
    pub fn new(conn: &'a mut EmbeddedConnection, planner: Planner, sql: &str) -> Self {
        Self {
            conn,
            planner,
            sql: sql.to_string(),
        }
    }
    pub fn sql(&self) -> &str {
        &self.sql
    }
    pub fn explain_plan(&mut self) -> Result<EmbeddedPlanRepr> {
        let tx = self.conn.get_transaction();
        match self.planner.create_query_plan(&self.sql, tx) {
            Ok(pln) => Ok(EmbeddedPlanRepr::new(pln.repr())),
            Err(_) => self
                .conn
                .rollback()
                .and_then(|_| Err(From::from(StatementError::RuntimeError))),
        }
    }
}

impl<'a> StatementAdapter<'a> for EmbeddedStatement<'a> {
    type Set = EmbeddedResultSet<'a>;
    type Aeffected = i32;

    fn execute_query(&'a mut self) -> Result<Self::Set> {
        let tx = self.conn.get_transaction();
        match self.planner.create_query_plan(&self.sql, tx) {
            Ok(pln) => EmbeddedResultSet::new(pln, &mut self.conn),
            Err(_) => self
                .conn
                .rollback()
                .and_then(|_| Err(From::from(StatementError::RuntimeError))),
        }
    }
    fn execute_update(&mut self) -> Result<Self::Aeffected> {
        let tx = self.conn.get_transaction();
        match self.planner.execute_update(&self.sql, tx) {
            Ok(affected) => self.conn.commit().and_then(|_| Ok(affected)),
            Err(_) => self
                .conn
                .rollback()
                .and_then(|_| Err(From::from(StatementError::RuntimeError))),
        }
    }
    fn close(&mut self) -> Result<()> {
        self.conn.close()
    }
}
