use rdbc::{Connection, Error, Result, Statement};
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{
    plan::planner::Planner, rdbc::connectionadapter::ConnectionAdapter, server::simpledb::SimpleDB,
    tx::transaction::Transaction,
};

use super::statement::EmbeddedStatement;

pub struct EmbeddedConnection {
    db: SimpleDB,
    current_tx: Arc<Mutex<Transaction>>,
    planner: Planner,
}

impl EmbeddedConnection {
    pub fn new(db: SimpleDB) -> Self {
        let tx = db.new_tx().unwrap();
        let planner = db.planner().unwrap();

        Self {
            db,
            current_tx: Arc::new(Mutex::new(tx)),
            planner,
        }
    }
}

impl Connection for EmbeddedConnection {
    fn create(&mut self, sql: &str) -> Result<Rc<RefCell<dyn Statement + '_>>> {
        Ok(Rc::new(RefCell::new(EmbeddedStatement::new(sql))))
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
