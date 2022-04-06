use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use crate::{
    file::block_id::BlockId,
    query::{constant::Constant, scan::Scan},
    record::schema::FieldType,
    record::{layout::Layout, recordpage::RecordPage},
    tx::transaction::Transaction,
};

#[derive(Debug)]
pub enum ChunkScanError {
    DowncastError,
}

impl std::error::Error for ChunkScanError {}
impl fmt::Display for ChunkScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ChunkScanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

pub struct ChunkScan {
    buffs: Vec<RecordPage>,
    tx: Arc<Mutex<Transaction>>,
    filename: String,
    layout: Arc<Layout>,
    startbnum: i32,
    endbnum: i32,
    currentbnum: i32,
    rp: Option<RecordPage>,
    currentslot: i32,
}

impl ChunkScan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        filename: String,
        layout: Arc<Layout>,
        startbnum: i32,
        endbnum: i32,
    ) -> Self {
        let mut buffs: Vec<RecordPage> = vec![];
        for i in startbnum..=endbnum {
            let blk = BlockId::new(&filename, i);
            let rp = RecordPage::new(Arc::clone(&tx), blk, Arc::clone(&layout)).unwrap();
            buffs.push(rp);
        }

        let mut scan = Self {
            buffs,
            tx,
            filename,
            layout,
            startbnum,
            endbnum,
            currentbnum: 0,
            rp: None,
            currentslot: 0,
        };
        scan.move_to_block(startbnum);

        scan
    }
    fn move_to_block(&mut self, blknum: i32) {
        self.currentbnum = blknum;
        self.rp = self
            .buffs
            .get((self.currentbnum - self.startbnum) as usize)
            .map(|rp| rp.clone());
        self.currentslot = -1;
    }
}

impl Scan for ChunkScan {
    fn before_first(&mut self) -> Result<()> {
        self.move_to_block(self.startbnum);

        Ok(())
    }
    fn next(&mut self) -> bool {
        let rp = self.rp.as_mut().unwrap();
        self.currentslot = rp.next_after(self.currentslot).unwrap();
        while self.currentslot < 0 {
            if self.currentbnum == self.endbnum {
                return false;
            }
            self.move_to_block(self.rp.as_ref().unwrap().block().number() + 1);
            let rp = self.rp.as_mut().unwrap();
            self.currentslot = rp.next_after(self.currentslot).unwrap();
        }

        true
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        self.rp.as_mut().unwrap().get_i32(self.currentslot, fldname)
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        let rp = self.rp.as_mut().unwrap();
        rp.get_string(self.currentslot, fldname)
    }
    fn get_val(&mut self, fldname: &str) -> Result<Constant> {
        match self.layout.schema().field_type(fldname) {
            FieldType::INTEGER => Ok(Constant::new_i32(self.get_i32(fldname)?)),
            FieldType::VARCHAR => Ok(Constant::new_string(self.get_string(fldname)?)),
        }
    }
    fn has_field(&self, fldname: &str) -> bool {
        self.layout.schema().has_field(fldname)
    }
    fn close(&mut self) -> Result<()> {
        for i in 0..self.buffs.len() {
            let blk = BlockId::new(&self.filename, self.startbnum + i as i32);
            self.tx.lock().unwrap().unpin(&blk)?;
        }

        Ok(())
    }
    fn to_update_scan(&mut self) -> Result<&mut dyn crate::query::updatescan::UpdateScan> {
        Err(From::from(ChunkScanError::DowncastError))
    }
    fn as_table_scan(&mut self) -> Result<&mut crate::record::tablescan::TableScan> {
        Err(From::from(ChunkScanError::DowncastError))
    }
    fn as_sort_scan(&mut self) -> Result<&mut crate::materialize::sortscan::SortScan> {
        Err(From::from(ChunkScanError::DowncastError))
    }
}
