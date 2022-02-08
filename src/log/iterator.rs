use anyhow::Result;
use std::{
    mem,
    sync::{Arc, Mutex},
};

use crate::file::{block_id::BlockId, manager::FileMgr, page::Page};

pub struct LogIterator {
    fm: Arc<Mutex<FileMgr>>,
    blk: BlockId,
    p: Page,
    currentpos: u64,
    boundary: u64,
}

impl LogIterator {
    pub fn new(fm: Arc<Mutex<FileMgr>>, blk: BlockId) -> Result<Self> {
        let mut filemgr = fm.lock().unwrap();

        let mut p = Page::new_from_size(filemgr.block_size() as usize);

        // move to block
        filemgr.read(&blk, &mut p)?;
        let boundary = p.get_i32(0)? as u64;
        let currentpos = boundary;

        drop(filemgr);
        Ok(Self {
            fm,
            blk,
            p,
            currentpos,
            boundary,
        })
    }
    pub fn has_next(&self) -> bool {
        self.currentpos < self.fm.lock().unwrap().block_size() || self.blk.number() > 0
    }
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next() {
            return None;
        }

        let mut filemgr = self.fm.lock().unwrap();

        if self.currentpos == filemgr.block_size() {
            self.blk = BlockId::new(&self.blk.file_name(), self.blk.number() - 1);

            if filemgr.read(&self.blk, &mut self.p).is_err() {
                return None;
            }

            if let Ok(n) = self.p.get_i32(0) {
                self.boundary = n as u64;
                self.currentpos = self.boundary;
            } else {
                return None;
            }
        }

        if let Ok(rec) = self.p.get_bytes_vec(self.currentpos as usize) {
            let i32_size = mem::size_of::<i32>() as u64;
            self.currentpos += i32_size + rec.len() as u64;

            return Some(rec);
        }

        None
    }
}
