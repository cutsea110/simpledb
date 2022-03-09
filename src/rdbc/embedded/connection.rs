use anyhow::Result;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
};

use super::statement::EmbeddedStatement;
use crate::{
    rdbc::connectionadapter::{ConnectionAdapter, ConnectionError},
    server::simpledb::SimpleDB,
    tx::transaction::Transaction,
};

#[derive(Clone)]
pub struct EmbeddedConnection {
    db: Rc<RefCell<SimpleDB>>,
    current_tx: Arc<Mutex<Transaction>>,
}

impl EmbeddedConnection {
    pub fn new(db: Rc<RefCell<SimpleDB>>) -> Self {
        let tx = db.borrow_mut().new_tx().unwrap();

        Self {
            db,
            current_tx: Arc::new(Mutex::new(tx)),
        }
    }
}

impl<'a> ConnectionAdapter<'a> for EmbeddedConnection {
    type State = EmbeddedStatement<'a>;

    fn create(&'a mut self, sql: &str) -> Result<Self::State> {
        let planner = self.db.borrow_mut().planner()?;
        Ok(EmbeddedStatement::new(self, planner, sql))
    }
    fn close(&mut self) -> Result<()> {
        self.commit()
    }
    fn commit(&mut self) -> Result<()> {
        self.current_tx.lock().unwrap().commit()?;
        if let Ok(tx) = self.db.borrow_mut().new_tx() {
            self.current_tx = Arc::new(Mutex::new(tx));
            return Ok(());
        }

        Err(From::from(ConnectionError::StartNewTransactionFailed))
    }
    fn rollback(&mut self) -> Result<()> {
        self.current_tx.lock().unwrap().rollback()?;
        if let Ok(tx) = self.db.borrow_mut().new_tx() {
            self.current_tx = Arc::new(Mutex::new(tx));
            return Ok(());
        }

        Err(From::from(ConnectionError::StartNewTransactionFailed))
    }
    fn get_transaction(&self) -> Result<Arc<Mutex<Transaction>>> {
        Ok(Arc::clone(&self.current_tx))
    }
}
