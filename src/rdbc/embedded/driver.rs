use std::{cell::RefCell, rc::Rc};

use rdbc::{Connection, Driver, Result};

use crate::server::simpledb::SimpleDB;

pub struct EmbeddedDriver {
    db: SimpleDB,
}

impl EmbeddedDriver {
    fn new(db: SimpleDB) -> Self {
        Self { db }
    }
}

impl Driver for EmbeddedDriver {
    fn connect(&self, url: &str) -> Result<Rc<RefCell<dyn Connection>>> {
        panic!("TODO")
    }
}
