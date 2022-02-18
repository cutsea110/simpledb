use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{layout::Layout, recordpage::RecordPage, rid::RID, schema::FieldType};
use crate::{file::block_id::BlockId, query::constant::Constant, tx::transaction::Transaction};

#[derive(Debug)]
pub enum TableScanError {
    NoRecordPage,
}

impl std::error::Error for TableScanError {}
impl fmt::Display for TableScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableScanError::NoRecordPage => {
                write!(f, "no record page")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableScan {
    tx: Arc<Mutex<Transaction>>,
    layout: Layout,
    rp: Option<RecordPage>,
    filename: String,
    currentslot: i32,
}

impl TableScan {
    pub fn new(tx: Arc<Mutex<Transaction>>, tblname: &str, layout: Layout) -> Self {
        let filename = format!("{}.tbl", tblname);
        let mut scan = Self {
            tx,
            layout,
            rp: None, // dummy
            filename,
            currentslot: -1, // dummy
        };

        if scan.tx.lock().unwrap().size(&scan.filename).unwrap() == 0 {
            scan.move_to_new_block().unwrap();
        } else {
            scan.move_to_block(0).unwrap();
        }

        scan
    }
    // TODO: Methods that implement Scan trait
    pub fn close(&mut self) -> Result<()> {
        if self.rp.is_some() {
            self.tx
                .lock()
                .unwrap()
                .unpin(self.rp.as_ref().unwrap().block())?;
        }

        Ok(())
    }
    pub fn before_first(&mut self) -> Result<()> {
        self.move_to_block(0)
    }
    pub fn next(&mut self) -> bool {
        self.currentslot = self
            .rp
            .as_mut()
            .unwrap()
            .next_after(self.currentslot)
            .unwrap_or(-1);
        while self.currentslot < 0 {
            if self.at_last_block() {
                return false;
            }
            self.move_to_block(self.rp.as_ref().unwrap().block().number() + 1)
                .unwrap();
            self.currentslot = self
                .rp
                .as_mut()
                .unwrap()
                .next_after(self.currentslot)
                .unwrap_or(-1);
        }

        true
    }
    pub fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        self.rp.as_mut().unwrap().get_i32(self.currentslot, fldname)
    }
    pub fn get_string(&mut self, fldname: &str) -> Result<String> {
        self.rp
            .as_mut()
            .unwrap()
            .get_string(self.currentslot, fldname)
    }
    pub fn get_val(&mut self, fldname: &str) -> Constant {
        match self.layout.schema().field_type(fldname) {
            FieldType::INTEGER => {
                return Constant::new_i32(self.get_i32(fldname).unwrap_or(0));
            }
            FieldType::VARCHAR => {
                return Constant::new_string(self.get_string(fldname).unwrap_or("".to_string()));
            }
        }
    }
    pub fn has_field(&self, fldname: &str) -> bool {
        self.layout.schema().has_field(fldname)
    }
    pub fn set_i32(&mut self, fldname: &str, val: i32) -> Result<()> {
        self.rp
            .as_mut()
            .unwrap()
            .set_i32(self.currentslot, fldname, val)
    }
    pub fn set_string(&mut self, fldname: &str, val: String) -> Result<()> {
        self.rp
            .as_mut()
            .unwrap()
            .set_string(self.currentslot, fldname, val)
    }
    pub fn set_val(&mut self, fldname: &str, val: Constant) -> Result<()> {
        match self.layout.schema().field_type(fldname) {
            FieldType::INTEGER => {
                self.set_i32(fldname, val.as_i32().unwrap())?;
            }
            FieldType::VARCHAR => {
                self.set_string(fldname, val.as_string().unwrap().to_string())?;
            }
        }

        Ok(())
    }
    pub fn insert(&mut self) -> Result<()> {
        self.currentslot = self
            .rp
            .as_mut()
            .unwrap()
            .insert_after(self.currentslot)
            .unwrap_or(-1);
        while self.currentslot < 0 {
            if self.at_last_block() {
                self.move_to_new_block()?;
            } else {
                self.move_to_block(self.rp.as_ref().unwrap().block().number() + 1)?;
            }
            self.currentslot = self
                .rp
                .as_mut()
                .unwrap()
                .insert_after(self.currentslot)
                .unwrap_or(-1);
        }

        Ok(())
    }
    pub fn delete(&mut self) -> Result<()> {
        self.rp.as_mut().unwrap().delete(self.currentslot)
    }
    pub fn move_to_rid(&mut self, rid: RID) -> Result<()> {
        self.close()?;
        let blk = BlockId::new(&self.filename, rid.block_number());
        self.rp = RecordPage::new(Arc::clone(&self.tx), blk, self.layout.clone()).into();
        self.currentslot = rid.slot();

        Ok(())
    }
    pub fn get_rid(&self) -> RID {
        RID::new(self.rp.as_ref().unwrap().block().number(), self.currentslot)
    }
    fn move_to_block(&mut self, blknum: i32) -> Result<()> {
        self.close()?;
        let blk = BlockId::new(&self.filename, blknum);
        self.rp = RecordPage::new(Arc::clone(&self.tx), blk, self.layout.clone()).into();
        self.currentslot = -1;

        Ok(())
    }
    fn move_to_new_block(&mut self) -> Result<()> {
        self.close()?;
        let blk = self.tx.lock().unwrap().append(&self.filename)?;
        self.rp = RecordPage::new(Arc::clone(&self.tx), blk, self.layout.clone()).into();
        self.rp.as_mut().unwrap().format()?;
        self.currentslot = -1;

        Ok(())
    }
    fn at_last_block(&self) -> bool {
        self.rp.as_ref().unwrap().block().number()
            == self.tx.lock().unwrap().size(&self.filename).unwrap() - 1
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use rand::Rng;
    use std::{fs, path::Path};

    use super::*;
    use crate::{record::schema::Schema, server::simpledb::SimpleDB};

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_tablescan").exists() {
            fs::remove_dir_all("_tablescan")?;
        }

        let simpledb = SimpleDB::new("_tablescan", "simpledb.log", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()));
        let mut sch = Schema::new();
        sch.add_i32_field("A");
        sch.add_string_field("B", 9);
        let layout = Layout::new(sch);
        for fldname in layout.schema().fields() {
            let offset = layout.offset(fldname);
            println!("{} has offset {}", fldname, offset);
        }

        let mut ts = TableScan::new(Arc::clone(&tx), "T", layout);
        println!("Filling the table with 50 random records.");
        ts.before_first()?;
        let mut rng = rand::thread_rng();
        for _ in 0..50 {
            ts.insert()?;
            let n: i32 = rng.gen_range(1..50);
            ts.set_i32("A", n)?;
            ts.set_string("B", format!("rec{}", n))?;
            println!("inserting into slot {}: {{{}, rec{}}}", ts.get_rid(), n, n);
        }
        println!("Deleting records with A-values < 25.");
        let mut count = 0;
        ts.before_first()?;
        while ts.next() {
            let a = ts.get_i32("A")?;
            let b = ts.get_string("B")?;
            if a < 25 {
                count += 1;
                println!("slot {}: {{{}, {}}}", ts.get_rid(), a, b);
                ts.delete()?;
            }
        }
        println!("{} values under 25 where deleted.\n", count);
        println!("Here are the remaining records.");
        ts.before_first()?;
        while ts.next() {
            let a = ts.get_i32("A")?;
            let b = ts.get_string("B")?;
            println!("slot {}: {{{}, {}}}", ts.get_rid(), a, b);
        }
        ts.close()?;
        tx.lock().unwrap().commit()?;

        Ok(())
    }
}
