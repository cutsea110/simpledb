use anyhow::Result;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{
    rdbc::{connectionadapter::ConnectionAdapter, statementadapter::StatementAdapter},
    server::simpledb::SimpleDB,
    tx::transaction::Transaction,
};

pub struct EmbeddedConnection {
    db: SimpleDB,
    current_tx: Arc<Mutex<Transaction>>,
}

impl EmbeddedConnection {
    pub fn new(db: SimpleDB) -> Self {
        let tx = db.new_tx().unwrap();

        Self {
            db,
            current_tx: Arc::new(Mutex::new(tx)),
        }
    }
}

impl ConnectionAdapter for EmbeddedConnection {
    fn create(&mut self, sql: &str) -> Result<Rc<RefCell<dyn StatementAdapter>>> {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn commit(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn rollback(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn get_transaction(&self) -> Result<Arc<Mutex<Transaction>>> {
        panic!("TODO")
    }
}
