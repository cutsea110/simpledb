use rdbc::{Error, Result, ResultSet, Statement, Value};
use std::{cell::RefCell, rc::Rc};

pub struct EmbeddedStatement {}

impl Statement for EmbeddedStatement {
    fn execute_query(&mut self, params: &[Value]) -> Result<Rc<RefCell<dyn ResultSet + '_>>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn execute_update(&mut self, params: &[Value]) -> Result<u64> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
}
