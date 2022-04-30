use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::statement::NetworkStatement;
use crate::{
    metadata::indexmanager::IndexInfo, rdbc::connectionadapter::ConnectionAdapter,
    record::schema::Schema, remote_capnp, tx::transaction::Transaction,
};
use remote_capnp::remote_connection;

pub struct NetworkConnection {
    client: remote_connection::Client,
}
impl NetworkConnection {
    pub fn new(client: remote_connection::Client) -> Self {
        Self { client }
    }
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
