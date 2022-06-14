use anyhow::Result;
use chrono::NaiveDate;
use std::{
    sync::{Arc, Mutex},
    usize,
};

use super::{
    bufferlist::BufferList,
    concurrency::{locktable::LockTable, manager::ConcurrencyMgr},
    recovery::manager::RecoveryMgr,
};
use crate::{
    buffer::manager::BufferMgr,
    file::{block_id::BlockId, manager::FileMgr},
    log::manager::LogMgr,
};

static END_OF_FILE: i32 = -1;

#[derive(Debug, Clone)]
pub struct Transaction {
    // static member (shared by all Transaction)
    next_tx_num: Arc<Mutex<i32>>,

    recovery_mgr: Option<Arc<Mutex<RecoveryMgr>>>,
    concur_mgr: ConcurrencyMgr,
    bm: Arc<Mutex<BufferMgr>>,
    fm: Arc<Mutex<FileMgr>>,
    txnum: i32,
    mybuffers: BufferList,
}

impl Transaction {
    pub fn new(
        next_tx_num: Arc<Mutex<i32>>,
        locktbl: Arc<Mutex<LockTable>>,

        fm: Arc<Mutex<FileMgr>>,
        lm: Arc<Mutex<LogMgr>>,
        bm: Arc<Mutex<BufferMgr>>,
    ) -> Result<Self> {
        let mut tran = Self {
            next_tx_num,
            recovery_mgr: None, // dummy
            concur_mgr: ConcurrencyMgr::new(locktbl),
            bm: Arc::clone(&bm),
            fm,
            txnum: 0, // dummy
            mybuffers: BufferList::new(Arc::clone(&bm)),
        };

        // update txnum
        let next_tx_num = tran.next_tx_number();
        tran.txnum = next_tx_num;
        // update recovery_mgr field (cyclic reference)
        let tx = Arc::new(Mutex::new(tran.clone()));
        tran.recovery_mgr = Arc::new(Mutex::new(RecoveryMgr::new(tx, next_tx_num, lm, bm)?)).into();

        Ok(tran)
    }
    pub fn commit(&mut self) -> Result<()> {
        self.recovery_mgr
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .commit()?;
        self.concur_mgr.release()?;
        self.mybuffers.unpin_all()?;
        println!("transaction {} committed", self.txnum);

        Ok(())
    }
    pub fn rollback(&mut self) -> Result<()> {
        self.recovery_mgr
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .rollback()?;
        self.concur_mgr.release()?;
        self.mybuffers.unpin_all()?;
        println!("transaction {} rolled back", self.txnum);

        Ok(())
    }
    pub fn recover(&mut self) -> Result<()> {
        self.bm.lock().unwrap().flush_all(self.txnum)?;
        self.recovery_mgr
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .recover()
    }
    pub fn pin(&mut self, blk: &BlockId) -> Result<()> {
        self.mybuffers.pin(blk)
    }
    pub fn unpin(&mut self, blk: &BlockId) -> Result<()> {
        self.mybuffers.unpin(blk)
    }
    pub fn get_i8(&mut self, blk: &BlockId, offset: i32) -> Result<i8> {
        self.concur_mgr.s_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        buff.contents().get_i8(offset as usize)
    }
    pub fn set_i8(&mut self, blk: &BlockId, offset: i32, val: i8, ok_to_log: bool) -> Result<()> {
        self.concur_mgr.x_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        let mut lsn: i32 = -1;
        if ok_to_log {
            let mut rm = self.recovery_mgr.as_ref().unwrap().lock().unwrap();
            lsn = rm.set_i8(&mut buff, offset, val)?.try_into().unwrap();
        }
        let p = buff.contents();
        p.set_i8(offset as usize, val)?;
        buff.set_modified(self.txnum, lsn);

        Ok(())
    }
    pub fn get_u8(&mut self, blk: &BlockId, offset: i32) -> Result<u8> {
        self.concur_mgr.s_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        buff.contents().get_u8(offset as usize)
    }
    pub fn set_u8(&mut self, blk: &BlockId, offset: i32, val: u8, ok_to_log: bool) -> Result<()> {
        self.concur_mgr.x_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        let mut lsn: i32 = -1;
        if ok_to_log {
            let mut rm = self.recovery_mgr.as_ref().unwrap().lock().unwrap();
            lsn = rm.set_u8(&mut buff, offset, val)?.try_into().unwrap();
        }
        let p = buff.contents();
        p.set_u8(offset as usize, val)?;
        buff.set_modified(self.txnum, lsn);

        Ok(())
    }
    pub fn get_i16(&mut self, blk: &BlockId, offset: i32) -> Result<i16> {
        self.concur_mgr.s_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        buff.contents().get_i16(offset as usize)
    }
    pub fn set_i16(&mut self, blk: &BlockId, offset: i32, val: i16, ok_to_log: bool) -> Result<()> {
        self.concur_mgr.x_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        let mut lsn: i32 = -1;
        if ok_to_log {
            let mut rm = self.recovery_mgr.as_ref().unwrap().lock().unwrap();
            lsn = rm.set_i16(&mut buff, offset, val)?.try_into().unwrap();
        }
        let p = buff.contents();
        p.set_i16(offset as usize, val)?;
        buff.set_modified(self.txnum, lsn);

        Ok(())
    }
    pub fn get_u16(&mut self, blk: &BlockId, offset: i32) -> Result<u16> {
        self.concur_mgr.s_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        buff.contents().get_u16(offset as usize)
    }
    pub fn set_u16(&mut self, blk: &BlockId, offset: i32, val: u16, ok_to_log: bool) -> Result<()> {
        self.concur_mgr.x_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        let mut lsn: i32 = -1;
        if ok_to_log {
            let mut rm = self.recovery_mgr.as_ref().unwrap().lock().unwrap();
            lsn = rm.set_u16(&mut buff, offset, val)?.try_into().unwrap();
        }
        let p = buff.contents();
        p.set_u16(offset as usize, val)?;
        buff.set_modified(self.txnum, lsn);

        Ok(())
    }
    pub fn get_i32(&mut self, blk: &BlockId, offset: i32) -> Result<i32> {
        self.concur_mgr.s_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        buff.contents().get_i32(offset as usize)
    }
    pub fn set_i32(&mut self, blk: &BlockId, offset: i32, val: i32, ok_to_log: bool) -> Result<()> {
        self.concur_mgr.x_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        let mut lsn: i32 = -1;
        if ok_to_log {
            let mut rm = self.recovery_mgr.as_ref().unwrap().lock().unwrap();
            lsn = rm.set_i32(&mut buff, offset, val)?.try_into().unwrap();
        }
        let p = buff.contents();
        p.set_i32(offset as usize, val)?;
        buff.set_modified(self.txnum, lsn);

        Ok(())
    }
    pub fn get_u32(&mut self, blk: &BlockId, offset: i32) -> Result<u32> {
        self.concur_mgr.s_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        buff.contents().get_u32(offset as usize)
    }
    pub fn set_u32(&mut self, blk: &BlockId, offset: i32, val: u32, ok_to_log: bool) -> Result<()> {
        self.concur_mgr.x_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        let mut lsn: i32 = -1;
        if ok_to_log {
            let mut rm = self.recovery_mgr.as_ref().unwrap().lock().unwrap();
            lsn = rm.set_u32(&mut buff, offset, val)?.try_into().unwrap();
        }
        let p = buff.contents();
        p.set_u32(offset as usize, val)?;
        buff.set_modified(self.txnum, lsn);

        Ok(())
    }
    pub fn get_string(&mut self, blk: &BlockId, offset: i32) -> Result<String> {
        self.concur_mgr.s_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        buff.contents().get_string(offset as usize)
    }
    pub fn set_string(
        &mut self,
        blk: &BlockId,
        offset: i32,
        val: &str,
        ok_to_log: bool,
    ) -> Result<()> {
        self.concur_mgr.x_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        let mut lsn: i32 = -1;
        if ok_to_log {
            let mut rm = self.recovery_mgr.as_ref().unwrap().lock().unwrap();
            lsn = rm.set_string(&mut buff, offset, val)?.try_into().unwrap();
        }
        let p = buff.contents();
        p.set_string(offset as usize, val.to_string())?;
        buff.set_modified(self.txnum, lsn);

        Ok(())
    }
    pub fn get_bool(&mut self, blk: &BlockId, offset: i32) -> Result<bool> {
        self.concur_mgr.s_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        buff.contents().get_bool(offset as usize)
    }
    pub fn set_bool(
        &mut self,
        blk: &BlockId,
        offset: i32,
        val: bool,
        ok_to_log: bool,
    ) -> Result<()> {
        self.concur_mgr.x_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        let mut lsn: i32 = -1;
        if ok_to_log {
            let mut rm = self.recovery_mgr.as_ref().unwrap().lock().unwrap();
            lsn = rm.set_bool(&mut buff, offset, val)?.try_into().unwrap();
        }
        let p = buff.contents();
        p.set_bool(offset as usize, val)?;
        buff.set_modified(self.txnum, lsn);

        Ok(())
    }
    pub fn get_date(&mut self, blk: &BlockId, offset: i32) -> Result<NaiveDate> {
        self.concur_mgr.s_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        buff.contents().get_date(offset as usize)
    }
    pub fn set_date(
        &mut self,
        blk: &BlockId,
        offset: i32,
        val: NaiveDate,
        ok_to_log: bool,
    ) -> Result<()> {
        self.concur_mgr.x_lock(blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        let mut lsn: i32 = -1;
        if ok_to_log {
            let mut rm = self.recovery_mgr.as_ref().unwrap().lock().unwrap();
            lsn = rm.set_date(&mut buff, offset, val)?.try_into().unwrap();
        }
        let p = buff.contents();
        p.set_date(offset as usize, val)?;
        buff.set_modified(self.txnum, lsn);

        Ok(())
    }
    pub fn size(&mut self, filename: &str) -> Result<i32> {
        let dummyblk = BlockId::new(filename, END_OF_FILE);
        self.concur_mgr.s_lock(&dummyblk)?;
        self.fm.lock().unwrap().length(filename)
    }
    pub fn append(&mut self, filename: &str) -> Result<BlockId> {
        let dummyblk = BlockId::new(filename, END_OF_FILE);
        self.concur_mgr.x_lock(&dummyblk)?;
        self.fm.lock().unwrap().append(filename)
    }
    pub fn block_size(&self) -> i32 {
        self.fm.lock().unwrap().block_size()
    }
    pub fn available_buffs(&self) -> usize {
        self.bm.lock().unwrap().available()
    }
    pub fn tx_num(&self) -> i32 {
        self.txnum
    }
    fn next_tx_number(&mut self) -> i32 {
        let mut next_tx_num = self.next_tx_num.lock().unwrap();
        *next_tx_num += 1;

        *next_tx_num
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::simpledb::SimpleDB;

    use anyhow::Result;
    use std::fs;
    use std::path::Path;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/txtest").exists() {
            fs::remove_dir_all("_test/txtest")?;
        }

        let simpledb = SimpleDB::new_with("_test/txtest", 400, 8);

        let mut tx1 = simpledb.new_tx()?;
        let blk = BlockId::new("testfile", 1);
        tx1.pin(&blk)?;
        // Don't log initial block values.
        tx1.set_i32(&blk, 80, 1, false)?;
        tx1.set_string(&blk, 40, "one", false)?;
        tx1.commit()?;

        let mut tx2 = simpledb.new_tx()?;
        tx2.pin(&blk)?;
        let ival = tx2.get_i32(&blk, 80)?;
        let sval = tx2.get_string(&blk, 40)?;
        println!("initial value at location 80 = {}", ival);
        println!("initial value at location 40 = {}", sval);
        assert_eq!(1, ival);
        assert_eq!("one".to_string(), sval);
        let newival = ival + 1;
        let newsval = format!("{}!", sval);
        tx2.set_i32(&blk, 80, newival, true)?;
        tx2.set_string(&blk, 40, &newsval, true)?;
        tx2.commit()?;

        let mut tx3 = simpledb.new_tx()?;
        tx3.pin(&blk)?;
        println!("new value at location 80 = {}", tx3.get_i32(&blk, 80)?);
        println!("new value at location 40 = {}", tx3.get_string(&blk, 40)?);
        assert_eq!(2, tx3.get_i32(&blk, 80)?);
        assert_eq!("one!".to_string(), tx3.get_string(&blk, 40)?);
        tx3.set_i32(&blk, 80, 9999, true)?;
        println!(
            "pre-rollback value at location 80 = {}",
            tx3.get_i32(&blk, 80)?
        );
        assert_eq!(9999, tx3.get_i32(&blk, 80)?);
        tx3.rollback()?;

        let mut tx4 = simpledb.new_tx()?;
        tx4.pin(&blk)?;
        println!("post-rollback at location 80 = {}", tx4.get_i32(&blk, 80)?);
        assert_eq!(2, tx4.get_i32(&blk, 80)?);
        tx4.commit()?;

        Ok(())
    }

    #[test]
    fn exercise_3_17() -> Result<()> {
        if Path::new("_test/tx/exercise_3_17").exists() {
            fs::remove_dir_all("_test/tx/exercise_3_17")?;
        }

        let simpledb = SimpleDB::new_with("_test/tx/exercise_3_17", 400, 8);

        let mut tx1 = simpledb.new_tx()?;
        let blk = BlockId::new("testfile", 1);
        tx1.pin(&blk)?;
        // Don't log initial block values.
        tx1.set_i8(&blk, 10, 108, false)?;
        tx1.set_u8(&blk, 20, 225, false)?;
        tx1.set_i16(&blk, 30, 12345, false)?;
        tx1.set_u16(&blk, 40, 54321, false)?;
        tx1.set_i32(&blk, 50, 1234567890, false)?;
        tx1.set_u32(&blk, 60, 3141592653, false)?;
        tx1.set_bool(&blk, 70, true, false)?;
        tx1.set_bool(&blk, 80, false, false)?;
        tx1.set_date(&blk, 90, NaiveDate::from_ymd(2022, 6, 14), false)?;
        tx1.commit()?;

        let mut tx2 = simpledb.new_tx()?;
        tx2.pin(&blk)?;
        assert_eq!(108, tx2.get_i8(&blk, 10)?);
        assert_eq!(225, tx2.get_u8(&blk, 20)?);
        assert_eq!(12345, tx2.get_i16(&blk, 30)?);
        assert_eq!(54321, tx2.get_u16(&blk, 40)?);
        assert_eq!(1234567890, tx2.get_i32(&blk, 50)?);
        assert_eq!(3141592653, tx2.get_u32(&blk, 60)?);
        assert_eq!(true, tx2.get_bool(&blk, 70)?);
        assert_eq!(false, tx2.get_bool(&blk, 80)?);
        assert_eq!(NaiveDate::from_ymd(2022, 6, 14), tx2.get_date(&blk, 90)?);

        Ok(())
    }
}
