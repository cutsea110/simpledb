use anyhow::Result;
use core::fmt;
use std::cell::RefCell;
use std::mem;
use std::sync::{Arc, Mutex};

use crate::file::block_id::BlockId;
use crate::file::manager::FileMgr;
use crate::file::page::Page;

use super::iterator::LogIterator;

#[derive(Debug)]
enum LogMgrError {
    LogPageAccessFailed,
}

impl std::error::Error for LogMgrError {}
impl fmt::Display for LogMgrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LogMgrError::LogPageAccessFailed => write!(f, "log access failed"),
        }
    }
}

pub struct LogMgr {
    fm: Arc<RefCell<FileMgr>>,
    logfile: String,
    logpage: Page,
    currentblk: BlockId,
    // latest log sequence number
    latest_lsn: u64,
    // last saved log sequence number
    last_saved_lsn: u64,
    // for synchronized
    l: Arc<Mutex<()>>,
}

impl LogMgr {
    pub fn new(fm: Arc<RefCell<FileMgr>>, logfile: &str) -> Result<Self> {
        let mut logpage = Page::new_from_size(fm.borrow().block_size() as usize);
        let logsize = fm.borrow_mut().length(logfile)?;

        let logmgr;

        if logsize == 0 {
            let blk = fm.borrow_mut().append(logfile)?;
            logpage.set_i32(0, fm.borrow().block_size() as i32)?;
            fm.borrow_mut().write(&blk, &mut logpage)?;

            logmgr = Self {
                fm,
                logfile: logfile.to_string(),
                logpage,
                currentblk: blk,
                latest_lsn: 0,
                last_saved_lsn: 0,
                l: Arc::new(Mutex::default()),
            };
        } else {
            let newblk = BlockId::new(logfile, logsize - 1);
            fm.borrow_mut().read(&newblk, &mut logpage)?;

            logmgr = Self {
                fm,
                logfile: logfile.to_string(),
                logpage,
                currentblk: newblk,
                latest_lsn: 0,
                last_saved_lsn: 0,
                l: Arc::new(Mutex::default()),
            };
        }

        Ok(logmgr)
    }
    pub fn flush(&mut self, lsn: u64) -> Result<()> {
        if lsn > self.last_saved_lsn {
            self.flush_to_fm()?;
        }

        Ok(())
    }
    pub fn iterator(&mut self) -> Result<LogIterator> {
        self.flush_to_fm()?;
        let iter = LogIterator::new(Arc::clone(&self.fm), self.currentblk.clone())?;

        Ok(iter)
    }
    pub fn append(&mut self, logrec: &mut Vec<u8>) -> Result<u64> {
        if self.l.lock().is_ok() {
            let mut boundary = self.logpage.get_i32(0)?;
            let recsize = logrec.len() as i32;
            let int32_size = mem::size_of::<i32>() as i32;
            let bytes_needed = recsize + int32_size;

            if boundary - bytes_needed < int32_size {
                self.flush_to_fm()?;

                self.currentblk = self.append_new_block()?;
                boundary = self.logpage.get_i32(0)?;
            }

            let recpos = (boundary - bytes_needed) as usize;
            self.logpage.set_bytes(recpos, logrec)?;
            self.logpage.set_i32(0, recpos as i32)?;
            self.latest_lsn += 1;

            return Ok(self.last_saved_lsn);
        }

        Err(From::from(LogMgrError::LogPageAccessFailed))
    }
    fn append_new_block(&mut self) -> Result<BlockId> {
        let blk = self.fm.borrow_mut().append(self.logfile.as_str())?;
        self.logpage
            .set_i32(0, self.fm.borrow().block_size() as i32)?;
        self.fm.borrow_mut().write(&blk, &mut self.logpage)?;

        Ok(blk)
    }
    fn flush_to_fm(&mut self) -> Result<()> {
        self.fm
            .borrow_mut()
            .write(&self.currentblk, &mut self.logpage)?;
        self.last_saved_lsn = self.latest_lsn;

        Ok(())
    }
}
