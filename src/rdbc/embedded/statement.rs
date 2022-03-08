use anyhow::Result;
use std::{cell::RefCell, rc::Rc};

use super::connection::EmbeddedConnection;
use crate::plan::planner::Planner;
use crate::rdbc::resultsetadapter::ResultSetAdapter;
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

impl<'a> StatementAdapter for EmbeddedStatement<'a> {
    fn execute_query(&mut self) -> Result<Rc<RefCell<dyn ResultSetAdapter>>> {
        panic!("TODO")
    }
    fn execute_update(&mut self) -> Result<i32> {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
}
