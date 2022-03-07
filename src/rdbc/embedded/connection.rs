use rdbc::{Connection, Error, Result, Statement};
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{plan::planner::Planner, server::simpledb::SimpleDB, tx::transaction::Transaction};

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
