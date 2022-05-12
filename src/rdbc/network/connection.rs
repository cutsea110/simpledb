use anyhow::Result;

use super::statement::NetworkStatement;
use crate::{rdbc::connectionadapter::ConnectionAdapter, remote_capnp};

pub struct NetworkConnection {}
impl NetworkConnection {
    pub fn new() -> Self {
        Self {}
    }
    pub fn commit(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn rollback(&mut self) -> Result<()> {
        panic!("TODO")
    }
}

impl<'a> ConnectionAdapter<'a> for NetworkConnection {
    type Stmt = NetworkStatement;

    fn create_statement(&'a mut self, sql: &str) -> Result<Self::Stmt> {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
}
