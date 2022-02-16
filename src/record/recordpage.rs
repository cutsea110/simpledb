use anyhow::Result;
use core::fmt;

use super::{layout::Layout, schema::FieldType};
use crate::{file::block_id::BlockId, tx::transaction::Transaction};

type Flag = i32;
pub const EMPTY: Flag = 0;
pub const USED: Flag = 1;

#[derive(Debug)]
enum RecordPageError {
    NoEmptySlot,
}

impl std::error::Error for RecordPageError {}
impl fmt::Display for RecordPageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &RecordPageError::NoEmptySlot => {
                write!(f, "no empty slot")
            }
        }
    }
}

pub struct RecordPage {
    tx: Transaction,
    blk: BlockId,
    layout: Layout,
}

impl RecordPage {
    pub fn new(mut tx: Transaction, blk: BlockId, layout: Layout) -> Self {
        tx.pin(&blk).unwrap();

        Self { tx, blk, layout }
    }
    pub fn get_i32(&mut self, slot: i32, fldname: &str) -> Result<i32> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        self.tx.get_i32(&self.blk, fldpos as i32)
    }
    pub fn get_string(&mut self, slot: i32, fldname: &str) -> Result<String> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        self.tx.get_string(&self.blk, fldpos as i32)
    }
    pub fn set_i32(&mut self, slot: i32, fldname: &str, val: i32) -> Result<()> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        self.tx.set_i32(&self.blk, fldpos as i32, val, true)
    }
    pub fn set_string(&mut self, slot: i32, fldname: &str, val: String) -> Result<()> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        self.tx.set_string(&self.blk, fldpos as i32, &val, true)
    }
    pub fn delete(&mut self, slot: i32) -> Result<()> {
        self.set_flag(slot, EMPTY)
    }
    pub fn format(&mut self) -> Result<()> {
        let mut slot: i32 = 0;
        while self.is_valid_slot(slot) {
            self.tx
                .set_i32(&self.blk, self.offset(slot), EMPTY, false)?;
            let sch = self.layout.schema();
            for fldname in sch.fields().iter() {
                let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
                match sch.field_type(fldname) {
                    FieldType::INTEGER => {
                        self.tx.set_i32(&self.blk, fldpos, 0, false)?;
                    }
                    FieldType::VARCHAR => {
                        self.tx.set_string(&self.blk, fldpos, "", false)?;
                    }
                }
                slot += 1;
            }
        }
        Ok(())
    }
    pub fn next_after(&mut self, slot: i32) -> Option<i32> {
        self.search_after(slot, USED)
    }
    pub fn insert_after(&mut self, slot: i32) -> Result<i32> {
        if let Some(newslot) = self.search_after(slot, EMPTY) {
            if newslot >= 0 {
                self.set_flag(newslot, USED)?;
            }

            return Ok(newslot);
        }

        Err(From::from(RecordPageError::NoEmptySlot))
    }
    pub fn block(&self) -> &BlockId {
        &self.blk
    }
    fn set_flag(&mut self, slot: i32, flag: Flag) -> Result<()> {
        let offset = self.offset(slot) as i32;
        self.tx.set_i32(&self.blk, offset, flag as i32, true)
    }
    fn search_after(&mut self, mut slot: i32, flag: Flag) -> Option<i32> {
        slot += 1;
        while self.is_valid_slot(slot) {
            if self.tx.get_i32(&self.blk, self.offset(slot)).unwrap() as Flag == flag {
                return Some(slot);
            }
            slot += 1;
        }

        None
    }
    fn is_valid_slot(&self, slot: i32) -> bool {
        self.offset(slot) as i32 <= self.tx.block_size()
    }
    fn offset(&self, slot: i32) -> i32 {
        slot * self.layout.slot_size() as i32
    }
}
