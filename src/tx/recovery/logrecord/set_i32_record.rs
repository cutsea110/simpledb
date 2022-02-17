use anyhow::Result;
use core::fmt;
use std::{
    mem,
    sync::{Arc, Mutex},
};

use crate::{
    file::{block_id::BlockId, page::Page},
    log::manager::LogMgr,
    tx::transaction::Transaction,
};

use super::{LogRecord, TxType};

pub struct SetI32Record {
    txnum: i32,
    offset: i32,
    val: i32,
    blk: BlockId,
}

impl fmt::Display for SetI32Record {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "<SETI32 {} {} {} {}>",
            self.txnum, self.blk, self.offset, self.val
        )
    }
}

impl LogRecord for SetI32Record {
    fn op(&self) -> TxType {
        TxType::SETI32
    }
    fn tx_number(&self) -> i32 {
        self.txnum
    }
    fn undo(&mut self, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        tx.lock().unwrap().pin(&self.blk)?;
        tx.lock()
            .unwrap()
            .set_i32(&self.blk, self.offset, self.val, false)?; // don't log the undo!
        tx.lock().unwrap().unpin(&self.blk)
    }
}
impl SetI32Record {
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
        let val = p.get_i32(vpos)?;

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
        val: i32,
    ) -> Result<u64> {
        let tpos = mem::size_of::<i32>();
        let fpos = tpos + mem::size_of::<i32>();
        let bpos = fpos + Page::max_length(blk.file_name().len());
        let opos = bpos + mem::size_of::<i32>();
        let vpos = opos + mem::size_of::<i32>();
        let reclen = vpos + mem::size_of::<i32>();

        let mut p = Page::new_from_size(reclen as usize);
        p.set_i32(0, TxType::SETI32 as i32)?;
        p.set_i32(tpos, txnum)?;
        p.set_string(fpos, blk.file_name())?;
        p.set_i32(bpos, blk.number() as i32)?;
        p.set_i32(opos, offset)?;
        p.set_i32(vpos, val)?;

        lm.lock().unwrap().append(p.contents())
    }
}
