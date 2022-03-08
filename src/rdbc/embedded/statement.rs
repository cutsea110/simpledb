use rdbc::{Error, Result, ResultSet, Statement, Value};
use std::{cell::RefCell, rc::Rc};

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

impl<'a> Statement for EmbeddedStatement<'a> {
    fn execute_query(&mut self, params: &[Value]) -> Result<Rc<RefCell<dyn ResultSet + '_>>> {
        if let Ok(tx) = self.conn.get_transaction() {
            if let Ok(pln) = self.planner.create_query_plan(&self.sql, tx) {
                return Ok(Rc::new(RefCell::new(EmbeddedResultSet::new(
                    pln,
                    &mut self.conn,
                ))));
            }
            return Err(From::from(Error::General(
                "failed to create query plan".to_string(),
            )));
        }
        Err(From::from(Error::General(
            "failed to get current transaction".to_string(),
        )))
    }
    fn execute_update(&mut self, params: &[Value]) -> Result<u64> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
}

impl<'a> StatementAdapter for EmbeddedStatement<'a> {
    fn close(&mut self) -> Result<()> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
}
