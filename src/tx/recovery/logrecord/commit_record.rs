use anyhow::Result;
use core::fmt;
use std::{cell::RefCell, mem, sync::Arc};

use crate::{file::page::Page, log::manager::LogMgr};

use super::{LogRecord, TxType};

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
}
impl CommitRecord {
    pub fn new(p: Page) -> Result<Self> {
        let tpos = mem::size_of::<i32>();
        let txnum = p.get_i32(tpos)?;

        Ok(Self { txnum })
    }
    pub fn write_to_log(lm: Arc<RefCell<LogMgr>>, txnum: i32) -> Result<u64> {
        let tpos = mem::size_of::<i32>();
        let reclen = tpos + mem::size_of::<i32>();

        let mut p = Page::new_from_size(reclen as usize);
        p.set_i32(0, TxType::COMMIT as i32)?;
        p.set_i32(tpos, txnum)?;

        lm.borrow_mut().append(p.contents())
    }
}
