use anyhow::Result;
use core::fmt;
use std::{
    mem,
    sync::{Arc, Mutex},
};

use super::iterator::LogIterator;
use crate::file::{block_id::BlockId, manager::FileMgr, page::Page};

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
    fm: Arc<Mutex<FileMgr>>,
    logfile: String,
    logpage: Page,
    currentblk: BlockId,
    // latest log sequence number
    latest_lsn: u64,
    // last saved log sequence number
    last_saved_lsn: u64,
}

impl LogMgr {
    pub fn new(fm: Arc<Mutex<FileMgr>>, logfile: &str) -> Result<Self> {
        let mut filemgr = fm.lock().unwrap();

        let mut logpage = Page::new_from_size(filemgr.block_size() as usize);
        let logsize = filemgr.length(logfile)? as i32;

        let logmgr;

        if logsize == 0 {
            let blk = filemgr.append(logfile)?;
            logpage.set_i32(0, filemgr.block_size() as i32)?;
            filemgr.write(&blk, &mut logpage)?;

            drop(filemgr); // release lock
            logmgr = Self {
                fm,
                logfile: logfile.to_string(),
                logpage,
                currentblk: blk,
                latest_lsn: 0,
                last_saved_lsn: 0,
            };
        } else {
            let newblk = BlockId::new(logfile, logsize - 1);
            filemgr.read(&newblk, &mut logpage)?;

            drop(filemgr); // release lock
            logmgr = Self {
                fm,
                logfile: logfile.to_string(),
                logpage,
                currentblk: newblk,
                latest_lsn: 0,
                last_saved_lsn: 0,
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
    // synchronized
    pub fn append(&mut self, logrec: &mut Vec<u8>) -> Result<u64> {
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
    fn append_new_block(&mut self) -> Result<BlockId> {
        let mut filemgr = self.fm.lock().unwrap();

        let blk = filemgr.append(self.logfile.as_str())?;
        self.logpage.set_i32(0, filemgr.block_size() as i32)?;
        filemgr.write(&blk, &mut self.logpage)?;

        Ok(blk)
    }
    fn flush_to_fm(&mut self) -> Result<()> {
        let mut filemgr = self.fm.lock().unwrap();

        filemgr.write(&self.currentblk, &mut self.logpage)?;
        self.last_saved_lsn = self.latest_lsn;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;
    use std::usize;

    #[test]
    fn unit_test() {
        fs::remove_dir_all("logtest").expect("cleanup");

        let fm = FileMgr::new("logtest", 400).expect("create FileMgr");
        let mut lm = LogMgr::new(Arc::new(Mutex::new(fm)), "testfile").expect("create LogMgr");
        create_records(&mut lm, 1, 35);
        print_log_records(&mut lm, "The log file now has these records:");
        create_records(&mut lm, 35, 70);
        lm.flush(65).expect("LogMgr flush");
        print_log_records(&mut lm, "The log file now has these records:");
    }
    fn print_log_records(lm: &mut LogMgr, msg: &str) {
        println!("{}", msg);
        let mut iter = lm.iterator().unwrap();
        while iter.has_next() {
            if let Some(rec) = iter.next() {
                let p = Page::new_from_bytes(rec.clone());
                let s = p.get_string(0).unwrap();
                let npos = Page::max_length(s.len());
                let val = p.get_i32(npos).unwrap();
                println!("[{}, {}]", s, val);
            }
            println!("");
        }
    }
    fn create_records(lm: &mut LogMgr, start: i32, end: i32) {
        println!("Creating records: ");
        for i in start..=end {
            let mut rec = create_log_record(format!("record: {}", i), i + 100);
            let lsn = lm.append(&mut rec).expect("LogMgr append");
            println!("{} ", format!("{}", lsn));
        }
        println!("");
    }
    fn create_log_record(s: String, n: i32) -> Vec<u8> {
        let npos = Page::max_length(s.len()) as i32;
        let size = npos + mem::size_of::<i32>() as i32;
        let mut p = Page::new_from_size(size as usize);
        p.set_string(0, s).expect("set string");
        p.set_i32(npos as usize, n).expect("set i32");
        p.contents().clone()
    }
}
