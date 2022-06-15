use anyhow::Result;
use chrono::{Datelike, NaiveDate, Utc};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::sync::{Arc, Mutex};

use super::{layout::Layout, schema::FieldType};
use crate::{file::block_id::BlockId, tx::transaction::Transaction};

#[derive(FromPrimitive, Debug, Eq, PartialEq, Clone, Copy)]
pub enum SlotFlag {
    EMPTY = 0,
    USED = 1,
}

#[derive(Debug, Clone)]
pub struct RecordPage {
    tx: Arc<Mutex<Transaction>>,
    blk: BlockId,
    layout: Arc<Layout>,
}

impl RecordPage {
    pub fn new(tx: Arc<Mutex<Transaction>>, blk: BlockId, layout: Arc<Layout>) -> Result<Self> {
        tx.lock().unwrap().pin(&blk)?;

        Ok(Self { tx, blk, layout })
    }
    pub fn get_i8(&mut self, slot: i32, fldname: &str) -> Result<i8> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.get_i8(&self.blk, fldpos)
    }
    pub fn get_u8(&mut self, slot: i32, fldname: &str) -> Result<u8> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.get_u8(&self.blk, fldpos)
    }
    pub fn get_i16(&mut self, slot: i32, fldname: &str) -> Result<i16> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.get_i16(&self.blk, fldpos)
    }
    pub fn get_u16(&mut self, slot: i32, fldname: &str) -> Result<u16> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.get_u16(&self.blk, fldpos)
    }
    pub fn get_i32(&mut self, slot: i32, fldname: &str) -> Result<i32> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.get_i32(&self.blk, fldpos)
    }
    pub fn get_u32(&mut self, slot: i32, fldname: &str) -> Result<u32> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.get_u32(&self.blk, fldpos)
    }
    pub fn get_string(&mut self, slot: i32, fldname: &str) -> Result<String> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.get_string(&self.blk, fldpos)
    }
    pub fn get_bool(&mut self, slot: i32, fldname: &str) -> Result<bool> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.get_bool(&self.blk, fldpos)
    }
    pub fn get_date(&mut self, slot: i32, fldname: &str) -> Result<NaiveDate> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.get_date(&self.blk, fldpos)
    }
    pub fn set_i8(&mut self, slot: i32, fldname: &str, val: i8) -> Result<()> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.set_i8(&self.blk, fldpos as i32, val, true)
    }
    pub fn set_u8(&mut self, slot: i32, fldname: &str, val: u8) -> Result<()> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.set_u8(&self.blk, fldpos as i32, val, true)
    }
    pub fn set_i16(&mut self, slot: i32, fldname: &str, val: i16) -> Result<()> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.set_i16(&self.blk, fldpos as i32, val, true)
    }
    pub fn set_u16(&mut self, slot: i32, fldname: &str, val: u16) -> Result<()> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.set_u16(&self.blk, fldpos as i32, val, true)
    }
    pub fn set_i32(&mut self, slot: i32, fldname: &str, val: i32) -> Result<()> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.set_i32(&self.blk, fldpos as i32, val, true)
    }
    pub fn set_u32(&mut self, slot: i32, fldname: &str, val: u32) -> Result<()> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.set_u32(&self.blk, fldpos as i32, val, true)
    }
    pub fn set_string(&mut self, slot: i32, fldname: &str, val: String) -> Result<()> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.set_string(&self.blk, fldpos, &val, true)
    }
    pub fn set_bool(&mut self, slot: i32, fldname: &str, val: bool) -> Result<()> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.set_bool(&self.blk, fldpos as i32, val, true)
    }
    pub fn set_date(&mut self, slot: i32, fldname: &str, val: NaiveDate) -> Result<()> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.set_date(&self.blk, fldpos as i32, val, true)
    }
    pub fn delete(&mut self, slot: i32) -> Result<()> {
        self.set_flag(slot, SlotFlag::EMPTY)
    }
    pub fn format(&mut self) -> Result<()> {
        let mut slot: i32 = 0;
        while self.is_valid_slot(slot) {
            let mut tx = self.tx.lock().unwrap();

            tx.set_i32(&self.blk, self.offset(slot), SlotFlag::EMPTY as i32, false)?;
            let sch = self.layout.schema();
            for fldname in sch.fields() {
                let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
                match sch.field_type(fldname) {
                    FieldType::WORD => {
                        tx.set_i8(&self.blk, fldpos, 0, false)?;
                    }
                    FieldType::UWORD => {
                        tx.set_u8(&self.blk, fldpos, 0, false)?;
                    }
                    FieldType::SHORT => {
                        tx.set_i16(&self.blk, fldpos, 0, false)?;
                    }
                    FieldType::USHORT => {
                        tx.set_u16(&self.blk, fldpos, 0, false)?;
                    }
                    FieldType::INTEGER => {
                        tx.set_i32(&self.blk, fldpos, 0, false)?;
                    }
                    FieldType::UINTEGER => {
                        tx.set_u32(&self.blk, fldpos, 0, false)?;
                    }
                    FieldType::VARCHAR => {
                        tx.set_string(&self.blk, fldpos, "", false)?;
                    }
                    FieldType::BOOL => {
                        tx.set_bool(&self.blk, fldpos, false, false)?;
                    }
                    FieldType::DATE => {
                        let today = Utc::today();
                        tx.set_date(
                            &self.blk,
                            fldpos,
                            NaiveDate::from_ymd(today.year(), today.month(), today.day()),
                            false,
                        )?;
                    }
                }
            }
            slot += 1;
        }
        Ok(())
    }
    pub fn next_after(&mut self, slot: i32) -> Option<i32> {
        self.search_after(slot, SlotFlag::USED)
    }
    pub fn insert_after(&mut self, slot: i32) -> Option<i32> {
        if let Some(newslot) = self.search_after(slot, SlotFlag::EMPTY) {
            self.set_flag(newslot, SlotFlag::USED).unwrap();
            return Some(newslot);
        }

        None
    }
    pub fn block(&self) -> &BlockId {
        &self.blk
    }
    fn set_flag(&mut self, slot: i32, flag: SlotFlag) -> Result<()> {
        let offset = self.offset(slot);
        let mut tx = self.tx.lock().unwrap();

        tx.set_i32(&self.blk, offset, flag as i32, true)
    }
    fn search_after(&mut self, mut slot: i32, flag: SlotFlag) -> Option<i32> {
        slot += 1;
        while self.is_valid_slot(slot) {
            let mut tx = self.tx.lock().unwrap();
            let flg = tx.get_i32(&self.blk, self.offset(slot)).unwrap();
            if FromPrimitive::from_i32(flg) == Some(flag) {
                return Some(slot);
            }
            slot += 1;
        }

        None
    }
    fn is_valid_slot(&self, slot: i32) -> bool {
        self.offset(slot + 1) as i32 <= self.tx.lock().unwrap().block_size()
    }
    fn offset(&self, slot: i32) -> i32 {
        slot * self.layout.slot_size() as i32
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use rand::Rng;
    use std::{
        fs,
        path::Path,
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::{record::schema::Schema, server::simpledb::SimpleDB};

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/recordpage").exists() {
            fs::remove_dir_all("_test/recordpage")?;
        }

        let simpledb = SimpleDB::new_with("_test/recordpage", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        let mut sch = Schema::new();
        sch.add_i32_field("A");
        sch.add_string_field("B", 9);
        let layout = Arc::new(Layout::new(Arc::new(sch)));
        for fldname in layout.schema().fields() {
            let offset = layout.offset(fldname);
            println!("{} has offset {}", fldname, offset);
        }

        let blk = tx.lock().unwrap().append("testfile")?;
        tx.lock().unwrap().pin(&blk)?;
        let mut rp = RecordPage::new(Arc::clone(&tx), blk.clone(), layout)?;
        rp.format()?;

        println!("Filling the page with random records.");

        let mut rng = rand::thread_rng();
        let mut next_slot = rp.insert_after(-1);
        while let Some(slot) = next_slot {
            let n: i32 = rng.gen_range(1..50);
            rp.set_i32(slot, "A", n)?;
            rp.set_string(slot, "B", format!("rec{}", n))?;
            println!("inserting into slot {}: {{{}, rec{}}}", slot, n, n);

            next_slot = rp.insert_after(slot)
        }
        println!("Deleted these records with A-values < 25.");

        let mut count = 0;
        next_slot = rp.next_after(-1);
        while let Some(slot) = next_slot {
            let a = rp.get_i32(slot, "A")?;
            let b = rp.get_string(slot, "B")?;
            if a < 25 {
                count += 1;
                println!("slot {} : {{{}, {}}}", slot, a, b);
                rp.delete(slot)?;
            }

            next_slot = rp.next_after(slot);
        }
        println!("{} values under 25 were deleted.\n", count);
        println!("Here are the remaining records.");

        next_slot = rp.next_after(-1);
        while let Some(slot) = next_slot {
            let a = rp.get_i32(slot, "A")?;
            let b = rp.get_string(slot, "B")?;
            println!("slot {}: {{{}, {}}}", slot, a, b);

            next_slot = rp.next_after(slot);
        }

        tx.lock().unwrap().unpin(&blk)?;
        tx.lock().unwrap().commit()?;

        Ok(())
    }
}
