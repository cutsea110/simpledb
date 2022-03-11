use anyhow::Result;
use core::fmt;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::statementadapter::StatementAdapter;
use crate::{
    metadata::indexmanager::IndexInfo, record::schema::Schema, tx::transaction::Transaction,
};

#[derive(Debug)]
pub enum ConnectionError {
    CreateStatementFailed,
    StartNewTransactionFailed,
    CommitFailed,
    RollbackFailed,
    CloseFailed,
}

impl std::error::Error for ConnectionError {}
impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConnectionError::CreateStatementFailed => {
                write!(f, "failed to create statement")
            }
            ConnectionError::StartNewTransactionFailed => {
                write!(f, "failed to start new transaction")
            }
            ConnectionError::CommitFailed => {
                write!(f, "failed to commit")
            }
            ConnectionError::RollbackFailed => {
                write!(f, "failed to rollback")
            }
            ConnectionError::CloseFailed => {
                write!(f, "failed to close")
            }
        }
    }
}

pub trait ConnectionAdapter<'a> {
    type Stmt: StatementAdapter<'a>;

    fn create(&'a mut self, sql: &str) -> Result<Self::Stmt>;
    fn close(&mut self) -> Result<()>;
    fn commit(&mut self) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
    fn get_transaction(&self) -> Arc<Mutex<Transaction>>;
    // my own extend
    fn get_table_schema(&self, tblname: &str) -> Result<Arc<Schema>>;
    // my own extend
    fn get_view_definition(&self, viewname: &str) -> Result<(String, String)>;
    // my own extend
    fn get_index_info(&self, tblname: &str) -> Result<HashMap<String, IndexInfo>>;
}
