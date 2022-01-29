use anyhow::Result;
use core::fmt;
use std::{cell::RefCell, mem, sync::Arc};

use crate::{file::page::Page, log::manager::LogMgr, tx::transaction::Transaction};

use super::{LogRecord, TxType};

pub struct RollbackRecord {
    txnum: i32,
}

impl fmt::Display for RollbackRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<ROLLBACK {}>", self.txnum)
    }
}

impl LogRecord for RollbackRecord {
    fn op(&self) -> TxType {
        TxType::ROLLBACK
    }
    fn tx_number(&self) -> i32 {
        self.txnum
    }
    fn undo(&mut self, _tx: &mut Transaction) -> Result<()> {
        // nop
        Ok(())
    }
}
impl RollbackRecord {
    pub fn new(p: Page) -> Result<Self> {
        let tpos = mem::size_of::<i32>();
        let txnum = p.get_i32(tpos)?;

        Ok(Self { txnum })
    }
    pub fn write_to_log(lm: Arc<RefCell<LogMgr>>, txnum: i32) -> Result<u64> {
        let tpos = mem::size_of::<i32>();
        let reclen = tpos + mem::size_of::<i32>();

        let mut p = Page::new_from_size(reclen as usize);
        p.set_i32(0, TxType::ROLLBACK as i32)?;
        p.set_i32(tpos, txnum)?;

        lm.borrow_mut().append(p.contents())
    }
}
