use anyhow::Result;
use core::fmt;
use std::{
    mem,
    sync::{Arc, Mutex},
};

use super::{LogRecord, TxType};
use crate::{file::page::Page, log::manager::LogMgr, tx::transaction::Transaction};

pub struct CommitRecord {
    txnum: i32,
}

impl fmt::Display for CommitRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<COMMIT {}>", self.txnum)
    }
}

impl LogRecord for CommitRecord {
    fn op(&self) -> TxType {
        TxType::COMMIT
    }
    fn tx_number(&self) -> i32 {
        self.txnum
    }
    fn undo(&mut self, _tx: Arc<Mutex<Transaction>>) -> Result<()> {
        // nop
        Ok(())
    }
}
impl CommitRecord {
    pub fn new(p: Page) -> Result<Self> {
        let tpos = mem::size_of::<i32>();
        let txnum = p.get_i32(tpos)?;

        Ok(Self { txnum })
    }
    pub fn write_to_log(lm: Arc<Mutex<LogMgr>>, txnum: i32) -> Result<i32> {
        let tpos = mem::size_of::<i32>();
        let reclen = tpos + mem::size_of::<i32>();

        let mut p = Page::new_from_size(reclen as usize);
        p.set_i32(0, TxType::COMMIT as i32)?;
        p.set_i32(tpos, txnum)?;

        lm.lock().unwrap().append(p.contents())
    }
}
