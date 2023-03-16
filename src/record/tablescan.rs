use anyhow::Result;
use chrono::NaiveDate;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{layout::Layout, recordpage::RecordPage, rid::RID, schema::FieldType};
use crate::{
    file::block_id::BlockId,
    materialize::sortscan::SortScan,
    query::{constant::Constant, scan::Scan, updatescan::UpdateScan},
    tx::transaction::Transaction,
};

#[derive(Debug)]
pub enum TableScanError {
    NoRecordPage,
    DowncastError,
}

impl std::error::Error for TableScanError {}
impl fmt::Display for TableScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableScanError::NoRecordPage => {
                write!(f, "no record page")
            }
            TableScanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableScan {
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    rp: Option<RecordPage>,
    filename: String,
    currentslot: i32,
}

impl Scan for TableScan {
    fn before_first(&mut self) -> Result<()> {
        self.move_to_block(0)
    }
    fn next(&mut self) -> bool {
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
    fn get_i16(&mut self, fldname: &str) -> Result<i16> {
        self.rp.as_mut().unwrap().get_i16(self.currentslot, fldname)
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        self.rp.as_mut().unwrap().get_i32(self.currentslot, fldname)
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        self.rp
            .as_mut()
            .unwrap()
            .get_string(self.currentslot, fldname)
    }
    fn get_bool(&mut self, fldname: &str) -> Result<bool> {
        self.rp
            .as_mut()
            .unwrap()
            .get_bool(self.currentslot, fldname)
    }
    fn get_date(&mut self, fldname: &str) -> Result<NaiveDate> {
        self.rp
            .as_mut()
            .unwrap()
            .get_date(self.currentslot, fldname)
    }
    fn get_val(&mut self, fldname: &str) -> Result<Constant> {
        return match self.layout.schema().field_type(fldname) {
            FieldType::SMALLINT => Ok(Constant::new_i16(self.get_i16(fldname).unwrap_or(0))),
            FieldType::INTEGER => Ok(Constant::new_i32(self.get_i32(fldname).unwrap_or(0))),
            FieldType::VARCHAR => Ok(Constant::new_string(
                self.get_string(fldname).unwrap_or("".to_string()),
            )),
            FieldType::BOOL => Ok(Constant::new_bool(
                self.get_bool(fldname).unwrap_or_default(),
            )),
            FieldType::DATE => Ok(Constant::new_date(
                self.get_date(fldname)
                    .unwrap_or(NaiveDate::from_ymd_opt(0, 1, 1).unwrap()), // NOTE: default 0000-01-01
            )),
        };
    }
    fn has_field(&self, fldname: &str) -> bool {
        self.layout.schema().has_field(fldname)
    }
    fn close(&mut self) -> Result<()> {
        if self.rp.is_some() {
            self.tx
                .lock()
                .unwrap()
                .unpin(self.rp.as_ref().unwrap().block())?;
        }

        Ok(())
    }
    // downcast
    fn to_update_scan(&mut self) -> Result<&mut dyn UpdateScan> {
        Ok(self)
    }
    fn as_table_scan(&mut self) -> Result<&mut TableScan> {
        Ok(self)
    }
    fn as_sort_scan(&mut self) -> Result<&mut SortScan> {
        Err(From::from(TableScanError::DowncastError))
    }
}

impl UpdateScan for TableScan {
    fn set_i16(&mut self, fldname: &str, val: i16) -> Result<()> {
        self.rp
            .as_mut()
            .unwrap()
            .set_i16(self.currentslot, fldname, val)
    }
    fn set_i32(&mut self, fldname: &str, val: i32) -> Result<()> {
        self.rp
            .as_mut()
            .unwrap()
            .set_i32(self.currentslot, fldname, val)
    }
    fn set_string(&mut self, fldname: &str, val: String) -> Result<()> {
        self.rp
            .as_mut()
            .unwrap()
            .set_string(self.currentslot, fldname, val)
    }
    fn set_bool(&mut self, fldname: &str, val: bool) -> Result<()> {
        self.rp
            .as_mut()
            .unwrap()
            .set_bool(self.currentslot, fldname, val)
    }
    fn set_date(&mut self, fldname: &str, val: NaiveDate) -> Result<()> {
        self.rp
            .as_mut()
            .unwrap()
            .set_date(self.currentslot, fldname, val)
    }
    fn set_val(&mut self, fldname: &str, val: Constant) -> Result<()> {
        match self.layout.schema().field_type(fldname) {
            FieldType::SMALLINT => {
                self.set_i16(fldname, val.as_i16().unwrap())?;
            }
            FieldType::INTEGER => {
                self.set_i32(fldname, val.as_i32().unwrap())?;
            }
            FieldType::VARCHAR => {
                self.set_string(fldname, val.as_string().unwrap().to_string())?;
            }
            FieldType::BOOL => {
                self.set_bool(fldname, val.as_bool().unwrap())?;
            }
            FieldType::DATE => {
                self.set_date(fldname, val.as_date().unwrap())?;
            }
        }

        Ok(())
    }
    fn insert(&mut self) -> Result<()> {
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
    fn delete(&mut self) -> Result<()> {
        self.rp.as_mut().unwrap().delete(self.currentslot)
    }
    fn move_to_rid(&mut self, rid: RID) -> Result<()> {
        self.close()?;
        let blk = BlockId::new(&self.filename, rid.block_number());
        self.rp = RecordPage::new(Arc::clone(&self.tx), blk, Arc::clone(&self.layout))?.into();
        self.currentslot = rid.slot();

        Ok(())
    }
    fn get_rid(&self) -> Result<RID> {
        let rid = RID::new(self.rp.as_ref().unwrap().block().number(), self.currentslot);

        Ok(rid)
    }
    // upcast
    fn to_scan(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        Ok(Arc::new(Mutex::new(self.clone())))
    }
}

impl TableScan {
    pub fn new(tx: Arc<Mutex<Transaction>>, tblname: &str, layout: Arc<Layout>) -> Result<Self> {
        let filename = format!("{}.tbl", tblname);
        let mut scan = Self {
            tx,
            layout,
            rp: None, // dummy
            filename,
            currentslot: -1, // dummy
        };

        if scan.tx.lock().unwrap().size(&scan.filename)? == 0 {
            scan.move_to_new_block()?;
        } else {
            scan.move_to_block(0)?;
        }

        Ok(scan)
    }

    fn move_to_block(&mut self, blknum: i32) -> Result<()> {
        self.close()?;
        let blk = BlockId::new(&self.filename, blknum);
        self.rp = RecordPage::new(Arc::clone(&self.tx), blk, Arc::clone(&self.layout))?.into();
        self.currentslot = -1;

        Ok(())
    }
    fn move_to_new_block(&mut self) -> Result<()> {
        self.close()?;
        let blk = self.tx.lock().unwrap().append(&self.filename)?;
        self.rp = RecordPage::new(Arc::clone(&self.tx), blk, Arc::clone(&self.layout))?.into();
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
        if Path::new("_test/tablescan").exists() {
            fs::remove_dir_all("_test/tablescan")?;
        }

        let simpledb = SimpleDB::new_with("_test/tablescan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        let mut sch = Schema::new();
        sch.add_i32_field("A");
        sch.add_string_field("B", 9);
        let layout = Arc::new(Layout::new(Arc::new(sch)));
        for fldname in layout.schema().fields() {
            let offset = layout.offset(fldname);
            println!("{} has offset {}", fldname, offset);
        }

        let mut ts = TableScan::new(Arc::clone(&tx), "T", layout)?;
        println!("Filling the table with 50 random records.");
        ts.before_first()?;
        let mut rng = rand::thread_rng();
        for _ in 0..50 {
            ts.insert()?;
            let n: i32 = rng.gen_range(1..50);
            ts.set_i32("A", n)?;
            ts.set_string("B", format!("rec{}", n))?;
            println!("inserting into slot {}: {{{}, rec{}}}", ts.get_rid()?, n, n);
        }
        println!("Deleting records with A-values < 25.");
        let mut count = 0;
        ts.before_first()?;
        while ts.next() {
            let a = ts.get_i32("A")?;
            let b = ts.get_string("B")?;
            if a < 25 {
                count += 1;
                println!("slot {}: {{{}, {}}}", ts.get_rid()?, a, b);
                ts.delete()?;
            }
        }
        println!("{} values under 25 where deleted.\n", count);
        println!("Here are the remaining records.");
        ts.before_first()?;
        while ts.next() {
            let a = ts.get_i32("A")?;
            let b = ts.get_string("B")?;
            println!("slot {}: {{{}, {}}}", ts.get_rid()?, a, b);
        }
        ts.close()?;
        tx.lock().unwrap().commit()?;

        Ok(())
    }
}
