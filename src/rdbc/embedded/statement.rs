use anyhow::Result;

use super::connection::EmbeddedConnection;
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
}

impl<'a> StatementAdapter<'a> for EmbeddedStatement<'a> {
    type Set = EmbeddedResultSet<'a>;

    fn execute_query(&'a mut self) -> Result<Self::Set> {
        let tx = self.conn.get_transaction();
        if let Ok(pln) = self.planner.create_query_plan(&self.sql, tx) {
            if let Ok(result) = EmbeddedResultSet::new(pln, &mut self.conn) {
                return Ok(result);
            }
        }

        Err(From::from(StatementError::RuntimeError))
    }
    fn execute_update(&mut self) -> Result<i32> {
        let tx = self.conn.get_transaction();
        if let Ok(result) = self.planner.execute_update(&self.sql, tx) {
            if self.conn.commit().is_err() {
                return Err(From::from(StatementError::CommitFailed));
            }
            return Ok(result);
        }

        self.conn.rollback()?;
        Err(From::from(StatementError::RuntimeError))
    }
    fn close(&mut self) -> Result<()> {
        self.conn
            .close()
            .or_else(|_| Err(From::from(StatementError::CloseFailed)))
    }
}
