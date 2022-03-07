use std::sync::{Arc, Mutex};

use rdbc::{Connection, Result};

use crate::tx::transaction::Transaction;

pub trait ConnectionAdapter: Connection {
    fn close(&mut self) -> Result<()>;
    fn commit(&mut self) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
    fn get_transaction(&self) -> Result<Arc<Mutex<Transaction>>>;
}
