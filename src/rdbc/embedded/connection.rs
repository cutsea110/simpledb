use rdbc::{Connection, Error, Result, Statement};
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{
    rdbc::connectionadapter::ConnectionAdapter, server::simpledb::SimpleDB,
    tx::transaction::Transaction,
};

use super::statement::EmbeddedStatement;

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

impl Connection for EmbeddedConnection {
    fn create(&mut self, sql: &str) -> Result<Rc<RefCell<dyn Statement + '_>>> {
        if let Ok(planner) = self.db.planner() {
            return Ok(Rc::new(RefCell::new(EmbeddedStatement::new(
                self, planner, sql,
            ))));
        }

        Err(From::from(Error::General(
            "couldn't get planner".to_string(),
        )))
    }
    fn prepare(&mut self, sql: &str) -> Result<Rc<RefCell<dyn Statement + '_>>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
}

impl ConnectionAdapter for EmbeddedConnection {
    fn close(&mut self) -> Result<()> {
        self.commit()
    }
    fn commit(&mut self) -> Result<()> {
        if self.current_tx.lock().unwrap().commit().is_err() {
            return Err(From::from(Error::General(
                "failed to commit current transaction.".to_string(),
            )));
        }
        if let Ok(tx) = self.db.new_tx() {
            self.current_tx = Arc::new(Mutex::new(tx));
        } else {
            return Err(From::from(Error::General(
                "failed to start new transaction.".to_string(),
            )));
        }

        Ok(())
    }
    fn rollback(&mut self) -> Result<()> {
        if self.current_tx.lock().unwrap().rollback().is_err() {
            return Err(From::from(Error::General(
                "failed to rollback current transaction.".to_string(),
            )));
        }
        if let Ok(tx) = self.db.new_tx() {
            self.current_tx = Arc::new(Mutex::new(tx));
        } else {
            return Err(From::from(Error::General(
                "failed to start new transaction.".to_string(),
            )));
        }

        Ok(())
    }
    fn get_transaction(&self) -> Result<Arc<Mutex<Transaction>>> {
        Ok(Arc::clone(&self.current_tx))
    }
}
