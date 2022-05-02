use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::statement::EmbeddedStatement;
use crate::{
    metadata::indexmanager::IndexInfo,
    rdbc::connectionadapter::{ConnectionAdapter, ConnectionError},
    record::schema::Schema,
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
    pub fn commit(&mut self) -> Result<()> {
        if self.current_tx.lock().unwrap().commit().is_err() {
            return Err(From::from(ConnectionError::CommitFailed));
        }
        if let Ok(tx) = self.db.new_tx() {
            self.current_tx = Arc::new(Mutex::new(tx));
            return Ok(());
        }

        Err(From::from(ConnectionError::StartNewTransactionFailed))
    }
    pub fn rollback(&mut self) -> Result<()> {
        if self.current_tx.lock().unwrap().rollback().is_err() {
            return Err(From::from(ConnectionError::RollbackFailed));
        }
        if let Ok(tx) = self.db.new_tx() {
            self.current_tx = Arc::new(Mutex::new(tx));
            return Ok(());
        }

        Err(From::from(ConnectionError::StartNewTransactionFailed))
    }
    pub fn get_transaction(&self) -> Arc<Mutex<Transaction>> {
        Arc::clone(&self.current_tx)
    }
    pub fn get_table_schema(&self, tblname: &str) -> Result<Arc<Schema>> {
        self.db
            .get_table_schema(tblname, Arc::clone(&self.current_tx))
    }
    pub fn get_view_definition(&self, viewname: &str) -> Result<(String, String)> {
        self.db
            .get_view_definitoin(viewname, Arc::clone(&self.current_tx))
    }
    pub fn get_index_info(&self, tblname: &str) -> Result<HashMap<String, IndexInfo>> {
        self.db
            .get_index_info(tblname, Arc::clone(&self.current_tx))
    }
}

impl<'a> ConnectionAdapter<'a> for EmbeddedConnection {
    type Stmt = EmbeddedStatement<'a>;

    fn create_statement(&'a mut self, sql: &str) -> Result<Self::Stmt> {
        self.db
            .planner()
            .and_then(|planner| Ok(EmbeddedStatement::new(self, planner, sql)))
            .or_else(|_| Err(From::from(ConnectionError::CreateStatementFailed)))
    }
    fn close(&mut self) -> Result<()> {
        self.commit()
            .or_else(|_| Err(From::from(ConnectionError::CloseFailed)))
    }
}
