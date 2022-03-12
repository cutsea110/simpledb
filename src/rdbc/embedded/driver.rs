use anyhow::Result;

use super::connection::EmbeddedConnection;
use crate::{
    rdbc::driveradapter::{DriverAdapter, DriverError},
    server::simpledb::SimpleDB,
};

pub struct EmbeddedDriver;

impl EmbeddedDriver {
    pub fn new() -> Self {
        Self
    }
}

impl DriverAdapter<'_> for EmbeddedDriver {
    type Con = EmbeddedConnection;

    fn connect(&self, url: &str) -> Result<Self::Con> {
        if let Ok(db) = SimpleDB::new(url) {
            return Ok(EmbeddedConnection::new(db));
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
