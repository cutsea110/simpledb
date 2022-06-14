use anyhow::Result;
use core::fmt;
use std::{
    mem,
    sync::{Arc, Mutex},
};

use super::{LogRecord, TxType};
use crate::{
    file::{block_id::BlockId, page::Page},
    log::manager::LogMgr,
    tx::transaction::Transaction,
};

pub struct SetBoolRecord {
    txnum: i32,
    offset: i32,
    val: bool,
    blk: BlockId,
}

impl fmt::Display for SetBoolRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "<SETBOOL {} {} {} {}>",
            self.txnum, self.blk, self.offset, self.val
        )
    }
}

impl LogRecord for SetBoolRecord {
    fn op(&self) -> TxType {
        TxType::SETBOOL
    }
    fn tx_number(&self) -> i32 {
        self.txnum
    }
    fn undo(&mut self, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        tx.lock().unwrap().pin(&self.blk)?;
        tx.lock()
            .unwrap()
            .set_bool(&self.blk, self.offset, self.val, false)?; // don't log the undo!
        tx.lock().unwrap().unpin(&self.blk)
    }
}
impl SetBoolRecord {
    pub fn new(p: Page) -> Result<Self> {
        let tpos = mem::size_of::<i32>();
        let txnum = p.get_i32(tpos)?;
        let fpos = tpos + mem::size_of::<i32>();
        let filename = p.get_string(fpos)?;
        let bpos = fpos + Page::max_length(filename.len());
        let blknum = p.get_i32(bpos)?;
        let blk = BlockId::new(&filename, blknum);
        let opos = bpos + mem::size_of::<i32>();
        let offset = p.get_i32(opos)?;
        let vpos = opos + mem::size_of::<i32>();
        let val = p.get_bool(vpos)?;

        Ok(Self {
            txnum,
            offset,
            val,
            blk,
        })
    }
    pub fn write_to_log(
        lm: Arc<Mutex<LogMgr>>,
        txnum: i32,
        blk: &BlockId,
        offset: i32,
        val: bool,
    ) -> Result<i32> {
        let tpos = mem::size_of::<i32>();
        let fpos = tpos + mem::size_of::<i32>();
        let bpos = fpos + Page::max_length(blk.file_name().len());
        let opos = bpos + mem::size_of::<i32>();
        let vpos = opos + mem::size_of::<i32>();
        let reclen = vpos + mem::size_of::<i32>();

        let mut p = Page::new_from_size(reclen as usize);
        p.set_i32(0, TxType::SETBOOL as i32)?;
        p.set_i32(tpos, txnum)?;
        p.set_string(fpos, blk.file_name())?;
        p.set_i32(bpos, blk.number() as i32)?;
        p.set_i32(opos, offset)?;
        p.set_bool(vpos, val)?;

        lm.lock().unwrap().append(p.contents())
    }
}
