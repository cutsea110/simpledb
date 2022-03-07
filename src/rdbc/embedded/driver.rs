use std::{cell::RefCell, rc::Rc};

use rdbc::{Connection, Driver, Error, Result};

use crate::server::simpledb::SimpleDB;

use super::connection::EmbeddedConnection;

pub struct EmbeddedDriver {}

impl EmbeddedDriver {
    pub fn new() -> Self {
        Self {}
    }
}

impl Driver for EmbeddedDriver {
    fn connect(&self, url: &str) -> Result<Rc<RefCell<dyn Connection>>> {
        if let Ok(db) = SimpleDB::new(url) {
            return Ok(Rc::new(RefCell::new(EmbeddedConnection::new(db))));
        }

        Err(From::from(Error::General(
            "couldn't connect database".to_string(),
        )))
    }
}
