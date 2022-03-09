use anyhow::Result;
use std::{cell::RefCell, rc::Rc};

use super::connection::EmbeddedConnection;
use crate::{
    rdbc::driveradapter::{DriverAdapter, DriverError},
    server::simpledb::SimpleDB,
};

pub struct EmbeddedDriver {}

impl EmbeddedDriver {
    pub fn new() -> Self {
        Self {}
    }
}

impl DriverAdapter for EmbeddedDriver {
    type Con = EmbeddedConnection;

    fn connect(&self, url: &str) -> Result<Self::Con> {
        if let Ok(db) = SimpleDB::new(url) {
            let edb = Rc::new(RefCell::new(db));
            return Ok(EmbeddedConnection::new(edb));
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
