use anyhow::Result;

use super::connection::EmbeddedConnection;
use super::resultset::EmbeddedResultSet;
use crate::plan::planner::Planner;
use crate::rdbc::connectionadapter::ConnectionAdapter;
use crate::rdbc::statementadapter::StatementAdapter;

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
}

impl<'a> StatementAdapter<'a> for EmbeddedStatement<'a> {
    type Result = EmbeddedResultSet<'a>;

    fn execute_query(&'a mut self) -> Result<Self::Result> {
        let tx = self.conn.get_transaction()?;
        let pln = self.planner.create_query_plan(&self.sql, tx)?;
        Ok(EmbeddedResultSet::new(pln, self.conn)?)
    }
    fn execute_update(&mut self) -> Result<i32> {
        let tx = self.conn.get_transaction()?;
        let result = self.planner.execute_update(&self.sql, tx)?;
        self.conn.commit()?;
        Ok(result)
    }
    fn close(&mut self) -> Result<()> {
        self.conn.close()
    }
}
