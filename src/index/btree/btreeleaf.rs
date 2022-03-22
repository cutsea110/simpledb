use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{btpage::BTPage, direntry::DirEntry};
use crate::{
    file::block_id::BlockId,
    query::constant::Constant,
    record::{layout::Layout, rid::RID},
    tx::transaction::Transaction,
};

#[derive(Debug)]
pub enum BTreeLeafError {
    RIDNotFound(RID),
}

impl std::error::Error for BTreeLeafError {}
impl fmt::Display for BTreeLeafError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BTreeLeafError::RIDNotFound(rid) => {
                write!(f, "rid({}) not found", rid)
            }
        }
    }
}

pub struct BTreeLeaf {
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    searchkey: Constant,
    contents: BTPage,
    currentslot: i32,
    filename: String,
}

impl BTreeLeaf {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        blk: BlockId,
        layout: Arc<Layout>,
        searchkey: Constant,
    ) -> Result<Self> {
        let filename = blk.file_name();
        let contents = BTPage::new(Arc::clone(&tx), blk, Arc::clone(&layout))?;
        let currentslot = contents.find_slot_before(&searchkey)?;

        Ok(Self {
            tx,
            layout,
            searchkey,
            contents,
            currentslot,
            filename,
        })
    }
    pub fn close(&mut self) -> Result<()> {
        self.contents.close()
    }
    pub fn next(&mut self) -> bool {
        self.currentslot += 1;
        if self.currentslot >= self.contents.get_num_recs().unwrap() {
            self.try_overflow()
        } else if self.contents.get_data_val(self.currentslot).unwrap() == self.searchkey {
            true
        } else {
            self.try_overflow()
        }
    }
    pub fn get_data_rid(&self) -> Result<RID> {
        self.contents.get_data_rid(self.currentslot)
    }
    pub fn delete(&mut self, datarid: RID) -> Result<()> {
        while self.next() {
            if self.get_data_rid()? == datarid {
                self.contents.delete(self.currentslot)?;
                return Ok(());
            }
        }

        Err(From::from(BTreeLeafError::RIDNotFound(datarid)))
    }
    pub fn insert(&mut self, datarid: RID) -> Option<DirEntry> {
        if self.contents.get_flag().unwrap() >= 0
            && self.contents.get_data_val(0).unwrap() > self.searchkey
        {
            let firstval = self.contents.get_data_val(0).unwrap();
            let newblk = self
                .contents
                .split(0, self.contents.get_flag().unwrap())
                .unwrap();
            self.currentslot = 0;
            self.contents.set_flag(-1).unwrap();
            self.contents
                .insert_leaf(self.currentslot, self.searchkey.clone(), datarid)
                .unwrap();
            return Some(DirEntry::new(firstval, newblk.number()));
        }
        self.currentslot += 1;
        self.contents
            .insert_leaf(self.currentslot, self.searchkey.clone(), datarid)
            .unwrap();
        if !self.contents.is_full() {
            return None;
        }
        // else page is full, so split it
        let firstkey = self.contents.get_data_val(0).unwrap();
        let lastkey = self
            .contents
            .get_data_val(self.contents.get_num_recs().unwrap() - 1)
            .unwrap();
        if lastkey == firstkey {
            // create an overflow block to hold all but the first record
            let newblk = self
                .contents
                .split(1, self.contents.get_flag().unwrap())
                .unwrap();
            self.contents.set_flag(newblk.number()).unwrap();
            return None;
        } else {
            let mut splitpos = self.contents.get_num_recs().unwrap() / 2;
            let mut splitkey = self.contents.get_data_val(splitpos).unwrap();
            if splitkey == firstkey {
                // move right, looking for the next key
                while self.contents.get_data_val(splitpos).unwrap() == splitkey {
                    splitpos += 1;
                }
                splitkey = self.contents.get_data_val(splitpos).unwrap();
            } else {
                // move left, looking fir first entry having the key
                while self.contents.get_data_val(splitpos - 1).unwrap() == splitkey {
                    splitpos -= 1;
                }
            }
            let newblk = self.contents.split(splitpos, -1).unwrap();
            return Some(DirEntry::new(splitkey, newblk.number()));
        }
    }
    fn try_overflow(&mut self) -> bool {
        let firstkey = self.contents.get_data_val(0).unwrap();
        let flag = self.contents.get_flag().unwrap();
        if self.searchkey != firstkey || flag < 0 {
            return false;
        }
        self.contents.close().unwrap();
        let nextblk = BlockId::new(&self.filename, flag);
        self.contents =
            BTPage::new(Arc::clone(&self.tx), nextblk, Arc::clone(&self.layout)).unwrap();
        self.currentslot = 0;
        return true;
    }
}
