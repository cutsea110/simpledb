use anyhow::Result;
use core::fmt;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::statement::RemoteStatement;
use crate::{
    metadata::indexmanager::IndexInfo, rdbc::connectionadapter::ConnectionAdapter,
    record::schema::Schema, tx::transaction::Transaction,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConnection {}

impl<'a> fmt::Display for RemoteConnection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl RemoteConnection {
    pub fn new() -> Self {
        panic!("TODO")
    }
}

impl<'a> ConnectionAdapter<'a> for RemoteConnection {
    type Stmt = RemoteStatement;

    fn create(&'a mut self, sql: &str) -> anyhow::Result<Self::Stmt> {
        panic!("TODO")
    }
    fn close(&mut self) -> anyhow::Result<()> {
        panic!("TODO")
    }
    fn commit(&mut self) -> anyhow::Result<()> {
        panic!("TODO")
    }
    fn rollback(&mut self) -> anyhow::Result<()> {
        panic!("TODO")
    }
    fn get_transaction(&self) -> Arc<Mutex<Transaction>> {
        panic!("TODO")
    }
    fn get_table_schema(&self, tblname: &str) -> Result<Arc<Schema>> {
        panic!("TODO")
    }
    fn get_view_definition(&self, viewname: &str) -> Result<(String, String)> {
        panic!("TODO")
    }
    fn get_index_info(&self, tblname: &str) -> Result<HashMap<String, IndexInfo>> {
        panic!("TODO")
    }
}
