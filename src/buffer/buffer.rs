use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use crate::{
    file::{block_id::BlockId, manager::FileMgr, page::Page},
    log::manager::LogMgr,
};

#[derive(Debug)]
enum BufferError {
    BlockNotFound,
}
impl std::error::Error for BufferError {}
impl fmt::Display for BufferError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &BufferError::BlockNotFound => {
                write!(f, "block not found")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Buffer {
    fm: Arc<Mutex<FileMgr>>,
    lm: Arc<Mutex<LogMgr>>,
    contents: Page,
    blk: Option<BlockId>,
    pins: u64,
    txnum: i32,
    lsn: i32,
}

impl Buffer {
    pub fn new(fm: Arc<Mutex<FileMgr>>, lm: Arc<Mutex<LogMgr>>) -> Self {
        let blksize = fm.lock().unwrap().block_size() as usize;
        let contents = Page::new_from_size(blksize);

        Self {
            fm,
            lm,
            contents,
            blk: None,
            pins: 0,
            txnum: -1,
            lsn: -1,
        }
    }
    pub fn contents(&mut self) -> &mut Page {
        &mut self.contents
    }
    pub fn block(&self) -> Option<&BlockId> {
        self.blk.as_ref()
    }
    pub fn set_modified(&mut self, txnum: i32, lsn: i32) {
        self.txnum = txnum;
        if lsn >= 0 {
            self.lsn = lsn;
        }
    }
    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }
    pub fn modifying_tx(&self) -> i32 {
        self.txnum
    }
    pub fn assign_to_block(&mut self, b: BlockId) -> Result<()> {
        self.flush()?;

        let mut fm = self.fm.lock().unwrap();

        fm.read(&b, &mut self.contents)?;
        self.blk = Some(b);
        self.pins = 0;

        Ok(())
    }
    pub fn flush(&mut self) -> Result<()> {
        let mut lm = self.lm.lock().unwrap();
        if self.txnum >= 0 {
            lm.flush(self.lsn)?;

            match self.blk.as_ref() {
                Some(blk) => {
                    let mut fm = self.fm.lock().unwrap();
                    fm.write(blk, &mut self.contents)?;
                    self.txnum = -1;
                }
                None => return Err(From::from(BufferError::BlockNotFound)),
            }
        }

        Ok(())
    }
    pub fn pin(&mut self) {
        self.pins += 1;
    }
    pub fn unpin(&mut self) {
        self.pins -= 1;
    }
    // my own extends
    pub fn is_modified(&self) -> bool {
        self.txnum != -1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::simpledb::SimpleDB;

    use std::fs;
    use std::path::Path;

    #[test]
    fn unit_test() {
        if Path::new("_test/buffertest").exists() {
            fs::remove_dir_all("_test/buffertest").expect("cleanup");
        }

        let simpledb = SimpleDB::new_with("_test/buffertest", 400, 3);

        let bm = simpledb.buffer_mgr();
        let mut bm = bm.lock().unwrap();

        let buff1 = bm.pin(&BlockId::new("testfile", 1)).unwrap();
        {
            let mut b1 = buff1.lock().unwrap();
            let p = b1.contents();
            let n = p.get_i32(80).unwrap(); // This modification will get written to disk.
            p.set_i32(80, n + 1).unwrap();
            b1.set_modified(1, 0);
            println!("The new value is  {}", n + 1);
        }
        bm.unpin(buff1).unwrap();

        // One of these pins will flush buff1 to disk:
        let buff2 = bm.pin(&BlockId::new("testfile", 2)).unwrap();
        let _buff3 = bm.pin(&BlockId::new("testfile", 3)).unwrap();
        let _buff4 = bm.pin(&BlockId::new("testfile", 4)).unwrap();

        bm.unpin(buff2).unwrap();
        let buff2 = bm.pin(&BlockId::new("testfile", 1)).unwrap();
        {
            let mut b2 = buff2.lock().unwrap();
            let p2 = b2.contents();
            p2.set_i32(80, 9999).unwrap(); // This modification won't get written to disk.
            b2.set_modified(1, 0);
        }
        bm.unpin(buff2).unwrap();
    }
}
