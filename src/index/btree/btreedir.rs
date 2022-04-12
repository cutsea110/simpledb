use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::{btpage::BTPage, direntry::DirEntry};
use crate::{
    file::block_id::BlockId, query::constant::Constant, record::layout::Layout,
    tx::transaction::Transaction,
};

#[derive(Debug, Clone)]
pub struct BTreeDir {
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    contents: BTPage,
    filename: String,
}

impl BTreeDir {
    pub fn new(tx: Arc<Mutex<Transaction>>, blk: BlockId, layout: Arc<Layout>) -> Result<Self> {
        let filename = blk.file_name();
        let contents = BTPage::new(Arc::clone(&tx), blk, Arc::clone(&layout))?;

        Ok(Self {
            tx,
            layout,
            contents,
            filename,
        })
    }
    pub fn close(&mut self) -> Result<()> {
        self.contents.close()
    }
    pub fn search(&mut self, searchkey: &Constant) -> Result<i32> {
        let mut childblk = self.find_child_block(searchkey)?;
        while self.contents.get_flag()? > 0 {
            self.contents.close()?;
            self.contents = BTPage::new(Arc::clone(&self.tx), childblk, Arc::clone(&self.layout))?;
            childblk = self.find_child_block(searchkey)?;
        }

        Ok(childblk.number())
    }
    pub fn make_new_root(&mut self, e: DirEntry) -> Result<()> {
        let firstval = self.contents.get_data_val(0)?;
        let level = self.contents.get_flag()?;
        let newblk = self.contents.split(0, level)?; // ie, transfer all the recs
        let oldroot = DirEntry::new(firstval, newblk.number());
        self.insert_entry(oldroot);
        self.insert_entry(e);
        self.contents.set_flag(level + 1)
    }
    pub fn insert(&mut self, e: DirEntry) -> Option<DirEntry> {
        if self.contents.get_flag().unwrap() == 0 {
            return self.insert_entry(e);
        }
        let childblk = self.find_child_block(e.data_val()).unwrap();
        let mut child =
            BTreeDir::new(Arc::clone(&self.tx), childblk, Arc::clone(&self.layout)).unwrap();
        let myentry = child.insert(e);
        child.close().unwrap();
        myentry.and_then(|myentry| self.insert_entry(myentry))
    }
    pub fn insert_entry(&mut self, e: DirEntry) -> Option<DirEntry> {
        let newslot = 1 + self.contents.find_slot_before(e.data_val()).unwrap();
        self.contents
            .insert_dir(newslot, e.data_val().clone(), e.block_number())
            .unwrap();
        if !self.contents.is_full() {
            return None;
        }
        // else page is full, so split it
        let level = self.contents.get_flag().unwrap();
        let splitpos = self.contents.get_num_recs().unwrap() / 2;
        let splitval = self.contents.get_data_val(splitpos).unwrap();
        let newblk = self.contents.split(splitpos, level).unwrap();
        Some(DirEntry::new(splitval, newblk.number()))
    }
    pub fn find_child_block(&self, searchkey: &Constant) -> Result<BlockId> {
        let mut slot = self.contents.find_slot_before(searchkey)?;
        if self.contents.get_data_val(slot + 1)? == *searchkey {
            slot += 1;
        }
        let blknum = self.contents.get_child_num(slot)?;
        Ok(BlockId::new(&self.filename, blknum))
    }
}
