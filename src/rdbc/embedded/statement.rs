use anyhow::Result;
use std::{cell::RefCell, rc::Rc};

use super::connection::EmbeddedConnection;
use super::resultset::EmbeddedResultSet;
use crate::plan::planner::Planner;
use crate::rdbc::connectionadapter::ConnectionAdapter;
use crate::rdbc::resultsetadapter::ResultSetAdapter;
use crate::rdbc::statementadapter::StatementAdapter;

pub struct EmbeddedStatement {
    conn: Rc<RefCell<EmbeddedConnection>>,
    planner: Planner,
    sql: String,
}

impl EmbeddedStatement {
    pub fn new(conn: Rc<RefCell<EmbeddedConnection>>, planner: Planner, sql: &str) -> Self {
        Self {
            conn,
            planner,
            sql: sql.to_string(),
        }
    }
}

impl StatementAdapter for EmbeddedStatement {
    fn execute_query(&mut self) -> Result<Rc<RefCell<dyn ResultSetAdapter>>> {
        let tx = self.conn.borrow_mut().get_transaction()?;
        let pln = self.planner.create_query_plan(&self.sql, tx)?;
        Ok(Rc::new(RefCell::new(EmbeddedResultSet::new(
            pln,
            Rc::clone(&self.conn),
        )?)))
    }
    fn execute_update(&mut self) -> Result<i32> {
        let tx = self.conn.borrow_mut().get_transaction()?;
        let result = self.planner.execute_update(&self.sql, tx)?;
        self.conn.borrow_mut().commit()?;
        Ok(result)
    }
    fn close(&mut self) -> Result<()> {
        self.conn.borrow_mut().close()
    }
}
