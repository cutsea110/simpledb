use rdbc::{Error, Result, ResultSet, Statement, Value};
use std::{cell::RefCell, rc::Rc};

use crate::rdbc::statementadapter::StatementAdapter;

pub struct EmbeddedStatement {
    sql: String,
}

impl EmbeddedStatement {
    pub fn new(sql: &str) -> Self {
        Self {
            sql: sql.to_string(),
        }
    }
}

impl Statement for EmbeddedStatement {
    fn execute_query(&mut self, params: &[Value]) -> Result<Rc<RefCell<dyn ResultSet + '_>>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn execute_update(&mut self, params: &[Value]) -> Result<u64> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
}

impl StatementAdapter for EmbeddedStatement {
    fn close(&mut self) -> Result<()> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
}
