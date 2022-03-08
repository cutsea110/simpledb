use anyhow::Result;
use std::{cell::RefCell, rc::Rc};

use super::connection::EmbeddedConnection;
use crate::{
    rdbc::{
        connectionadapter::ConnectionAdapter,
        driveradapter::{DriverAdapter, DriverError},
    },
    server::simpledb::SimpleDB,
};

pub struct EmbeddedDriver {}

impl EmbeddedDriver {
    pub fn new() -> Self {
        Self {}
    }
}

impl DriverAdapter for EmbeddedDriver {
    fn connect(&self, url: &str) -> Result<Rc<RefCell<dyn ConnectionAdapter>>> {
        if let Ok(db) = SimpleDB::new(url) {
            return Ok(Rc::new(RefCell::new(EmbeddedConnection::new(db))));
        }

        Err(From::from(DriverError::ConnectFailed))
    }
    fn get_major_version(&self) -> i32 {
        0
    }
    fn get_minor_version(&self) -> i32 {
        1
    }
}
