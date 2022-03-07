use rdbc::{Connection, Driver, Error, Result};
use std::{cell::RefCell, rc::Rc};

use super::connection::EmbeddedConnection;
use crate::{rdbc::driveradapter::DriverAdapter, server::simpledb::SimpleDB};

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

impl DriverAdapter for EmbeddedDriver {
    fn get_major_version(&self) -> i32 {
        0
    }
    fn get_minor_version(&self) -> i32 {
        1
    }
}
