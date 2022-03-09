use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::statement::EmbeddedStatement;
use crate::{
    rdbc::connectionadapter::{ConnectionAdapter, ConnectionError},
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

impl<'a> ConnectionAdapter<'a> for EmbeddedConnection {
    type State = EmbeddedStatement<'a>;

    fn create(&'a mut self, sql: &str) -> Result<Self::State> {
        let planner = self.db.planner()?;
        Ok(EmbeddedStatement::new(self, planner, sql))
    }
    fn close(&mut self) -> Result<()> {
        self.commit()
    }
    fn commit(&mut self) -> Result<()> {
        self.current_tx.lock().unwrap().commit()?;
        if let Ok(tx) = self.db.new_tx() {
            self.current_tx = Arc::new(Mutex::new(tx));
            return Ok(());
        }

        Err(From::from(ConnectionError::StartNewTransactionFailed))
    }
    fn rollback(&mut self) -> Result<()> {
        self.current_tx.lock().unwrap().rollback()?;
        if let Ok(tx) = self.db.new_tx() {
            self.current_tx = Arc::new(Mutex::new(tx));
            return Ok(());
        }

        Err(From::from(ConnectionError::StartNewTransactionFailed))
    }
    fn get_transaction(&self) -> Result<Arc<Mutex<Transaction>>> {
        Ok(Arc::clone(&self.current_tx))
    }
}
