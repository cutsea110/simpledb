use anyhow::Result;
use core::fmt;
use std::{
    mem,
    sync::{Arc, Mutex},
};

use super::{LogRecord, TxType};
use crate::{file::page::Page, log::manager::LogMgr, tx::transaction::Transaction};

pub struct CheckpointRecord {}

impl fmt::Display for CheckpointRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<CHECKPOINT>")
    }
}

impl LogRecord for CheckpointRecord {
    fn op(&self) -> TxType {
        TxType::CHECKPOINT
    }
    fn tx_number(&self) -> i32 {
        -1 // dummy
    }
    fn undo(&mut self, _tx: Arc<Mutex<Transaction>>) -> Result<()> {
        // nop
        Ok(())
    }
}
impl CheckpointRecord {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }
    pub fn write_to_log(lm: Arc<Mutex<LogMgr>>) -> Result<i32> {
        let reclen = mem::size_of::<i32>();

        let mut p = Page::new_from_size(reclen);
        p.set_i32(0, TxType::CHECKPOINT as i32)?;

        lm.lock().unwrap().append(p.contents())
    }
}
