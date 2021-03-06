use anyhow::Result;
use log::info;
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
        self.dump_statistics();

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
        self.dump_statistics();

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

    fn dump_statistics(&self) {
        self.nums_of_read_written_blocks();
        self.nums_of_available_buffers();
        self.nums_of_total_pinned_unpinned();
        self.buffer_cache_hit_assigned();
    }

    // extends for statistic by exercise 3.15
    fn nums_of_read_written_blocks(&self) {
        let (r, w) = self
            .db
            .file_mgr()
            .lock()
            .unwrap()
            .nums_of_read_written_blocks();
        info!("numbers of read/written blocks: {}/{}", r, w);
    }
    // extends for statistic by exercise 4.18
    fn nums_of_available_buffers(&self) {
        let available = self.db.buffer_mgr().lock().unwrap().available();
        info!("numbers of available buffers: {}", available);
    }
    fn nums_of_total_pinned_unpinned(&self) {
        let (p, u) = self
            .db
            .buffer_mgr()
            .lock()
            .unwrap()
            .nums_total_pinned_unpinned();
        info!("numbers of total pinned/unpinned buffers: {}/{}", p, u);
    }
    fn buffer_cache_hit_assigned(&self) {
        let (hit, assigned) = self
            .db
            .buffer_mgr()
            .lock()
            .unwrap()
            .buffer_cache_hit_assigned();
        let ratio = (hit as f32 / assigned as f32) * 100.0;
        info!(
            "buffer cache hit/assigned(ratio): {}/{}({:.3}%)",
            hit, assigned, ratio
        );
    }
}

impl<'a> ConnectionAdapter<'a> for EmbeddedConnection {
    type Stmt = EmbeddedStatement<'a>;
    type Res = ();

    fn create_statement(&'a mut self, sql: &str) -> Result<Self::Stmt> {
        self.db
            .planner()
            .and_then(|planner| Ok(EmbeddedStatement::new(self, planner, sql)))
            .or_else(|_| Err(From::from(ConnectionError::CreateStatementFailed)))
    }
    fn close(&mut self) -> Result<Self::Res> {
        self.commit()
            .or_else(|_| Err(From::from(ConnectionError::CloseFailed)))
    }
}
