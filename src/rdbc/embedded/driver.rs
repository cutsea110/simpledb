use std::{cell::RefCell, rc::Rc};

use rdbc::{Connection, Driver, Error, Result};

pub struct EmbeddedDriver {}

impl Driver for EmbeddedDriver {
    fn connect(&self, url: &str) -> Result<Rc<RefCell<dyn Connection>>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
}
