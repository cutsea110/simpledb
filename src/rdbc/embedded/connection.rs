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
}

impl<'a> ConnectionAdapter<'a> for EmbeddedConnection {
    type Stmt = EmbeddedStatement<'a>;

    fn create(&'a mut self, sql: &str) -> Result<Self::Stmt> {
        if let Ok(planner) = self.db.planner() {
            return Ok(EmbeddedStatement::new(self, planner, sql));
        }

        Err(From::from(ConnectionError::CreateStatementFailed))
    }
    fn close(&mut self) -> Result<()> {
        self.commit()
            .or_else(|_| Err(From::from(ConnectionError::CloseFailed)))
    }
    fn commit(&mut self) -> Result<()> {
        if self.current_tx.lock().unwrap().commit().is_err() {
            return Err(From::from(ConnectionError::CommitFailed));
        }
        if let Ok(tx) = self.db.new_tx() {
            self.current_tx = Arc::new(Mutex::new(tx));
            return Ok(());
        }

        Err(From::from(ConnectionError::StartNewTransactionFailed))
    }
    fn rollback(&mut self) -> Result<()> {
        if self.current_tx.lock().unwrap().rollback().is_err() {
            return Err(From::from(ConnectionError::RollbackFailed));
        }
        if let Ok(tx) = self.db.new_tx() {
            self.current_tx = Arc::new(Mutex::new(tx));
            return Ok(());
        }

        Err(From::from(ConnectionError::StartNewTransactionFailed))
    }
    fn get_transaction(&self) -> Arc<Mutex<Transaction>> {
        Arc::clone(&self.current_tx)
    }
    fn get_table_schema(&self, tblname: &str) -> Result<Arc<Schema>> {
        self.db
            .get_table_schema(tblname, Arc::clone(&self.current_tx))
    }
    fn get_view_definition(&self, viewname: &str) -> Result<(String, String)> {
        self.db
            .get_view_definitoin(viewname, Arc::clone(&self.current_tx))
    }
    fn get_index_info(&self, tblname: &str) -> Result<HashMap<String, IndexInfo>> {
        self.db
            .get_index_info(tblname, Arc::clone(&self.current_tx))
    }
}
