use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex, Once},
    usize,
};

use anyhow::Result;

use crate::{
    buffer::manager::BufferMgr,
    file::{block_id::BlockId, manager::FileMgr},
    log::manager::LogMgr,
};

use super::{
    bufferlist::BufferList, concurrency::manager::ConcurrencyMgr, recovery::manager::RecoveryMgr,
};

static END_OF_FILE: i32 = -1;

#[derive(Debug, Clone)]
pub struct Transaction {
    // static member (shared by all Transaction)
    next_tx_num: Arc<Mutex<i32>>,

    recovery_mgr: Option<Rc<RefCell<RecoveryMgr>>>,
    concur_mgr: ConcurrencyMgr,
    bm: Arc<Mutex<BufferMgr>>,
    fm: Arc<Mutex<FileMgr>>,
    txnum: i32,
    mybuffers: BufferList,
}

impl Transaction {
    pub fn new(fm: Arc<Mutex<FileMgr>>, lm: Arc<Mutex<LogMgr>>, bm: Arc<Mutex<BufferMgr>>) -> Self {
        static mut NEXT_TX_NUM: Option<Arc<Mutex<i32>>> = None;
        static ONCE: Once = Once::new();

        unsafe {
            ONCE.call_once(|| {
                let next_tx_num = Arc::new(Mutex::new(0));
                NEXT_TX_NUM = Some(next_tx_num);
            });

            let mut tran = Self {
                next_tx_num: NEXT_TX_NUM.clone().unwrap(),
                recovery_mgr: None, // dummy
                concur_mgr: ConcurrencyMgr::new(),
                bm: Arc::clone(&bm),
                fm,
                txnum: 0, // dummy
                mybuffers: BufferList::new(Arc::clone(&bm)),
            };

            // update txnum
            let next_tx_num = tran.next_tx_number();
            tran.txnum = next_tx_num;
            // update recovery_mgr field (cyclic reference)
            let tx = Rc::new(RefCell::new(tran.clone()));
            tran.recovery_mgr =
                Rc::new(RefCell::new(RecoveryMgr::new(tx, next_tx_num, lm, bm))).into();

            tran
        }
    }
    pub fn commit(&mut self) -> Result<()> {
        self.recovery_mgr.as_ref().unwrap().borrow_mut().commit()?;
        self.concur_mgr.release()?;
        self.mybuffers.unpin_all()?;
        println!("transaction {} committed", self.txnum);
        self.dump(self.txnum, "COMMITTED!!");

        Ok(())
    }
    pub fn rollback(&mut self) -> Result<()> {
        self.recovery_mgr
            .as_ref()
            .unwrap()
            .borrow_mut()
            .rollback()?;
        self.concur_mgr.release()?;
        self.mybuffers.unpin_all()?;
        println!("transaction {} rolled back", self.txnum);

        Ok(())
    }
    pub fn recover(&mut self) -> Result<()> {
        self.bm.lock().unwrap().flush_all(self.txnum)?;
        self.recovery_mgr.as_ref().unwrap().borrow_mut().recover()
    }
    pub fn pin(&mut self, blk: &BlockId) -> Result<()> {
        self.mybuffers.pin(blk)
    }
    pub fn unpin(&mut self, blk: &BlockId) -> Result<()> {
        self.mybuffers.unpin(blk)
    }
    pub fn get_i32(&mut self, blk: &BlockId, offset: i32) -> Result<i32> {
        self.dump(self.txnum, "BEFORE GET");
        self.concur_mgr.s_lock(self.txnum, blk)?;
        self.dump(self.txnum, "AFTER GET");
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        buff.contents().get_i32(offset as usize)
    }
    pub fn get_string(&mut self, blk: &BlockId, offset: i32) -> Result<String> {
        self.concur_mgr.s_lock(self.txnum, blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        buff.contents().get_string(offset as usize)
    }
    pub fn set_i32(&mut self, blk: &BlockId, offset: i32, val: i32, ok_to_log: bool) -> Result<()> {
        self.dump(self.txnum, "BEFORE SET");
        self.concur_mgr.x_lock(self.txnum, blk)?;
        self.dump(self.txnum, "AFTER SET");
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        let mut lsn: i32 = -1;
        if ok_to_log {
            let mut rm = self.recovery_mgr.as_ref().unwrap().borrow_mut();
            lsn = rm.set_i32(&mut buff, offset, val)?.try_into().unwrap();
        }
        let p = buff.contents();
        p.set_i32(offset as usize, val)?;
        buff.set_modified(self.txnum, lsn);

        Ok(())
    }
    pub fn set_string(
        &mut self,
        blk: &BlockId,
        offset: i32,
        val: &str,
        ok_to_log: bool,
    ) -> Result<()> {
        self.concur_mgr.x_lock(self.txnum, blk)?;
        let mut buff = self.mybuffers.get_bufer(blk).unwrap().lock().unwrap();
        let mut lsn: i32 = -1;
        if ok_to_log {
            let mut rm = self.recovery_mgr.as_ref().unwrap().borrow_mut();
            lsn = rm.set_string(&mut buff, offset, val)?.try_into().unwrap();
        }
        let p = buff.contents();
        p.set_string(offset as usize, val.to_string())?;
        buff.set_modified(self.txnum, lsn);

        Ok(())
    }
    pub fn size(&mut self, filename: &str) -> Result<i32> {
        let dummyblk = BlockId::new(filename, END_OF_FILE);
        self.concur_mgr.s_lock(self.txnum, &dummyblk)?;
        self.fm.lock().unwrap().length(filename)
    }
    pub fn append(&mut self, filename: &str) -> Result<BlockId> {
        let dummyblk = BlockId::new(filename, END_OF_FILE);
        self.concur_mgr.x_lock(self.txnum, &dummyblk)?;
        self.fm.lock().unwrap().append(filename)
    }
    pub fn block_size(&self) -> i32 {
        self.fm.lock().unwrap().block_size()
    }
    pub fn available_buffs(&self) -> usize {
        self.bm.lock().unwrap().available()
    }
    fn next_tx_number(&mut self) -> i32 {
        let mut next_tx_num = self.next_tx_num.lock().unwrap();
        *next_tx_num += 1;

        *next_tx_num
    }
    // for DEBUG
    pub fn tx_num(&self) -> i32 {
        self.txnum
    }
    // for DEBUG
    pub fn dump(&self, txnum: i32, msg: &str) {
        self.concur_mgr.dump(txnum, msg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::Result;
    use std::fs;
    use std::path::Path;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_txtest").exists() {
            fs::remove_dir_all("_txtest")?;
        }

        let fm = Arc::new(Mutex::new(FileMgr::new("_txtest", 400)?));
        let lm = Arc::new(Mutex::new(LogMgr::new(Arc::clone(&fm), "testfile")?));
        let bm = Arc::new(Mutex::new(BufferMgr::new(
            Arc::clone(&fm),
            Arc::clone(&lm),
            8,
        )));

        let mut tx1 = Transaction::new(Arc::clone(&fm), Arc::clone(&lm), Arc::clone(&bm));
        let blk = BlockId::new("testfile", 1);
        tx1.pin(&blk)?;
        // Don't log initial block values.
        tx1.set_i32(&blk, 80, 1, false)?;
        tx1.set_string(&blk, 40, "one", false)?;
        tx1.commit()?;

        let mut tx2 = Transaction::new(Arc::clone(&fm), Arc::clone(&lm), Arc::clone(&bm));
        tx2.pin(&blk)?;
        let ival = tx2.get_i32(&blk, 80)?;
        let sval = tx2.get_string(&blk, 40)?;
        println!("initial value at location 80 = {}", ival);
        println!("initial value at location 40 = {}", sval);
        let newival = ival + 1;
        let newsval = format!("{}!", sval);
        tx2.set_i32(&blk, 80, newival, true)?;
        tx2.set_string(&blk, 40, &newsval, true)?;
        tx2.commit()?;

        let mut tx3 = Transaction::new(Arc::clone(&fm), Arc::clone(&lm), Arc::clone(&bm));
        tx3.pin(&blk)?;
        println!("new value at location 80 = {}", tx3.get_i32(&blk, 80)?);
        println!("new value at location 40 = {}", tx3.get_string(&blk, 40)?);
        tx3.set_i32(&blk, 80, 9999, true)?;
        println!(
            "pre-rollback value at location 80 = {}",
            tx3.get_i32(&blk, 80)?
        );
        tx3.rollback()?;

        let mut tx4 = Transaction::new(Arc::clone(&fm), Arc::clone(&lm), Arc::clone(&bm));
        tx4.pin(&blk)?;
        println!("post-rollback at location 80 = {}", tx4.get_i32(&blk, 80)?);
        tx4.commit()?;

        Ok(())
    }
}
