use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::statement::NetworkStatement;
use crate::{
    metadata::indexmanager::IndexInfo, rdbc::connectionadapter::ConnectionAdapter,
    record::schema::Schema, tx::transaction::Transaction,
};

pub struct NetworkConnection {
    // TODO
}
impl<'a> ConnectionAdapter<'a> for NetworkConnection {
    type Stmt = NetworkStatement;

    fn create(&'a mut self, sql: &str) -> Result<Self::Stmt> {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn commit(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn rollback(&mut self) -> Result<()> {
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
