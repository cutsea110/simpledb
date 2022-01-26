use anyhow::Result;
use core::fmt;
use std::{cell::RefCell, sync::Arc};

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

pub struct Buffer {
    fm: Arc<RefCell<FileMgr>>,
    lm: Arc<RefCell<LogMgr>>,
    contents: Page,
    blk: Option<BlockId>,
    pins: u64,
    txnum: i32,
    lsn: i32,
}

impl Buffer {
    pub fn new(fm: Arc<RefCell<FileMgr>>, lm: Arc<RefCell<LogMgr>>) -> Self {
        let blksize = fm.borrow().block_size() as usize;
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
        self.fm.borrow_mut().read(&b, &mut self.contents)?;
        self.blk = Some(b);
        self.pins = 0;

        Ok(())
    }
    pub fn flush(&mut self) -> Result<()> {
        if self.txnum >= 0 {
            self.lm.borrow_mut().flush(self.lsn as u64)?;

            match self.blk.as_ref() {
                Some(blk) => {
                    self.fm.borrow_mut().write(blk, &mut self.contents)?;
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
}
