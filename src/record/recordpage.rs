use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::{layout::Layout, schema::FieldType};
use crate::{file::block_id::BlockId, tx::transaction::Transaction};

type Flag = i32;
pub const EMPTY: Flag = 0;
pub const USED: Flag = 1;

pub struct RecordPage {
    tx: Arc<Mutex<Transaction>>,
    blk: BlockId,
    layout: Layout,
}

impl RecordPage {
    pub fn new(tx: Arc<Mutex<Transaction>>, blk: BlockId, layout: Layout) -> Self {
        tx.lock().unwrap().pin(&blk).unwrap();

        Self { tx, blk, layout }
    }
    pub fn get_i32(&mut self, slot: i32, fldname: &str) -> Result<i32> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.get_i32(&self.blk, fldpos)
    }
    pub fn get_string(&mut self, slot: i32, fldname: &str) -> Result<String> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.get_string(&self.blk, fldpos)
    }
    pub fn set_i32(&mut self, slot: i32, fldname: &str, val: i32) -> Result<()> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.set_i32(&self.blk, fldpos as i32, val, true)
    }
    pub fn set_string(&mut self, slot: i32, fldname: &str, val: String) -> Result<()> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
        let mut tx = self.tx.lock().unwrap();
        tx.set_string(&self.blk, fldpos, &val, true)
    }
    pub fn delete(&mut self, slot: i32) -> Result<()> {
        self.set_flag(slot, EMPTY)
    }
    pub fn format(&mut self) -> Result<()> {
        let mut slot: i32 = 0;
        while self.is_valid_slot(slot) {
            let mut tx = self.tx.lock().unwrap();

            tx.set_i32(&self.blk, self.offset(slot), EMPTY, false)?;
            let sch = self.layout.schema();
            for fldname in sch.fields() {
                let fldpos = self.offset(slot) + self.layout.offset(fldname) as i32;
                match sch.field_type(fldname) {
                    FieldType::INTEGER => {
                        tx.set_i32(&self.blk, fldpos, 0, false)?;
                    }
                    FieldType::VARCHAR => {
                        tx.set_string(&self.blk, fldpos, "", false)?;
                    }
                }
            }
            slot += 1;
        }
        Ok(())
    }
    pub fn next_after(&mut self, slot: i32) -> Option<i32> {
        self.search_after(slot, USED)
    }
    pub fn insert_after(&mut self, slot: i32) -> Option<i32> {
        if let Some(newslot) = self.search_after(slot, EMPTY) {
            self.set_flag(newslot, USED).unwrap();
            return Some(newslot);
        }

        None
    }
    pub fn block(&self) -> &BlockId {
        &self.blk
    }
    fn set_flag(&mut self, slot: i32, flag: Flag) -> Result<()> {
        let offset = self.offset(slot);
        let mut tx = self.tx.lock().unwrap();

        tx.set_i32(&self.blk, offset, flag as i32, true)
    }
    fn search_after(&mut self, mut slot: i32, flag: Flag) -> Option<i32> {
        slot += 1;
        while self.is_valid_slot(slot) {
            let mut tx = self.tx.lock().unwrap();

            if tx.get_i32(&self.blk, self.offset(slot)).unwrap() as Flag == flag {
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
    use crate::{
        buffer::manager::BufferMgr, file::manager::FileMgr, log::manager::LogMgr,
        record::schema::Schema,
    };

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_recordpage").exists() {
            fs::remove_dir_all("_recordpage")?;
        }

        let next_tx_num = Arc::new(Mutex::new(0));
        let fm = Arc::new(Mutex::new(FileMgr::new("_recordpage", 400)?));
        let lm = Arc::new(Mutex::new(LogMgr::new(Arc::clone(&fm), "testfile")?));
        let bm = Arc::new(Mutex::new(BufferMgr::new(
            Arc::clone(&fm),
            Arc::clone(&lm),
            8,
        )));

        let tx = Arc::new(Mutex::new(Transaction::new(next_tx_num, fm, lm, bm)));
        let mut sch = Schema::new();
        sch.add_i32_field("A");
        sch.add_string_field("B", 9);
        let layout = Layout::new(sch);
        for fldname in layout.schema().fields() {
            let offset = layout.offset(fldname);
            println!("{} has offset {}", fldname, offset);
        }

        let blk = tx.lock().unwrap().append("testfile")?;
        tx.lock().unwrap().pin(&blk)?;
        let mut rp = RecordPage::new(Arc::clone(&tx), blk.clone(), layout);
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
        println!("Deleted these records with A-value < 25.");

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
