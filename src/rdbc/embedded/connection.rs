use rdbc::{Connection, Error, Result, Statement};
use std::{cell::RefCell, rc::Rc};

pub struct EmbeddedConnection {}

impl Connection for EmbeddedConnection {
    fn create(&mut self, sql: &str) -> Result<Rc<RefCell<dyn Statement + '_>>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn prepare(&mut self, sql: &str) -> Result<Rc<RefCell<dyn Statement + '_>>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
}
