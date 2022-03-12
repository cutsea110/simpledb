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
        self.planner
            .create_query_plan(&self.sql, tx)
            .and_then(|pln| EmbeddedResultSet::new(pln, &mut self.conn))
            .or_else(|_| Err(From::from(StatementError::RuntimeError)))
    }
    fn execute_update(&mut self) -> Result<i32> {
        let tx = self.conn.get_transaction();
        self.planner
            .execute_update(&self.sql, tx)
            .and_then(|affected| {
                self.conn
                    .commit()
                    .and(Ok(affected))
                    .or(Err(From::from(StatementError::CommitFailed)))
            })
            .or({
                self.conn
                    .rollback()
                    .and(Err(From::from(StatementError::RuntimeError)))
                    .or(Err(From::from(StatementError::RollbackFailed)))
            })
    }
    fn close(&mut self) -> Result<()> {
        self.conn
            .close()
            .or_else(|_| Err(From::from(StatementError::CloseFailed)))
    }
}
