use anyhow::Result;
use num_traits::FromPrimitive;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    usize,
};

use crate::{
    query::{scan::Scan, updatescan::UpdateScan},
    record::{layout::Layout, schema::Schema, tablescan::TableScan},
    tx::transaction::Transaction,
};

// table or field name
pub const MAX_NAME: usize = 16;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableMgr {
    tcat_layout: Arc<Layout>,
    fcat_layout: Arc<Layout>,
}

impl TableMgr {
    pub fn new(is_new: bool, tx: Arc<Mutex<Transaction>>) -> Result<Self> {
        let mut tcat_schema = Schema::new();
        tcat_schema.add_string_field("tblname", MAX_NAME);
        tcat_schema.add_i32_field("slotsize");
        let tcat_layout = Arc::new(Layout::new(Arc::new(tcat_schema)));
        let mut fcat_schema = Schema::new();
        fcat_schema.add_string_field("tblname", MAX_NAME);
        fcat_schema.add_string_field("fldname", MAX_NAME);
        fcat_schema.add_i32_field("type");
        fcat_schema.add_i32_field("length");
        fcat_schema.add_i32_field("offset");
        let fcat_layout = Arc::new(Layout::new(Arc::new(fcat_schema)));
        let mgr = Self {
            tcat_layout,
            fcat_layout,
        };

        if is_new {
            mgr.create_table("tblcat", mgr.tcat_layout.schema(), Arc::clone(&tx))?;
            mgr.create_table("fldcat", mgr.fcat_layout.schema(), Arc::clone(&tx))?;
        }

        Ok(mgr)
    }
    pub fn create_table(
        &self,
        tblname: &str,
        sch: Arc<Schema>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        let layout = Layout::new(sch);
        // insert one record into tblcat
        let mut tcat = TableScan::new(Arc::clone(&tx), "tblcat", Arc::clone(&self.tcat_layout))?;
        tcat.insert()?;
        tcat.set_string("tblname", tblname.to_string())?;
        tcat.set_i32("slotsize", layout.slot_size() as i32)?;
        tcat.close()?;
        // insert a record into fldcat for each field
        let mut fcat = TableScan::new(tx, "fldcat", Arc::clone(&self.fcat_layout))?;
        for fldname in layout.schema().fields() {
            fcat.insert()?;
            fcat.set_string("tblname", tblname.to_string())?;
            fcat.set_string("fldname", fldname.to_string())?;
            fcat.set_i32("type", layout.schema().field_type(fldname) as i32)?;
            fcat.set_i32("length", layout.schema().length(fldname) as i32)?;
            fcat.set_i32("offset", layout.offset(fldname) as i32)?;
        }
        fcat.close()?;

        Ok(())
    }
    pub fn get_layout(&self, tblname: &str, tx: Arc<Mutex<Transaction>>) -> Result<Arc<Layout>> {
        let mut size = -1;
        let mut tcat = TableScan::new(Arc::clone(&tx), "tblcat", Arc::clone(&self.tcat_layout))?;
        while tcat.next() {
            if tcat.get_string("tblname")? == tblname {
                size = tcat.get_i32("slotsize")?;
                break;
            }
        }
        tcat.close()?;

        let mut sch = Schema::new();
        let mut offsets = HashMap::new();
        let mut fcat = TableScan::new(tx, "fldcat", Arc::clone(&self.fcat_layout))?;
        while fcat.next() {
            if fcat.get_string("tblname")? == tblname {
                let fldname = fcat.get_string("fldname")?;
                let fldtype = FromPrimitive::from_i32(fcat.get_i32("type")?).unwrap();
                let fldlen = fcat.get_i32("length")? as usize;
                let offset = fcat.get_i32("offset")? as usize;
                offsets.insert(fldname.clone(), offset);
                sch.add_field(&fldname, fldtype, fldlen);
            }
        }
        fcat.close()?;

        let layout = Arc::new(Layout::new_with(Arc::new(sch), offsets, size as usize));
        Ok(layout)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::{
        fs,
        path::Path,
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::record::schema::FieldType;
    use crate::server::simpledb::SimpleDB;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/tblmgrtest").exists() {
            fs::remove_dir_all("_test/tblmgrtest")?;
        }

        let simpledb = SimpleDB::new_with("_test/tblmgrtest", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        let tm = TableMgr::new(true, Arc::clone(&tx))?;

        let mut sch = Schema::new();
        sch.add_i32_field("A");
        sch.add_string_field("B", 9);
        tm.create_table("MyTable", Arc::new(sch), Arc::clone(&tx))?;

        let layout = tm.get_layout("MyTable", Arc::clone(&tx))?;
        let size = layout.slot_size();
        let sch2 = layout.schema();
        println!("MyTable has slot size {}", size);
        println!("Its fields are:");
        for fldname in sch2.fields() {
            let fld_type = match sch2.field_type(fldname) {
                FieldType::WORD => "int8".to_string(),
                FieldType::UWORD => "uint8".to_string(),
                FieldType::SHORT => "int16".to_string(),
                FieldType::USHORT => "uint16".to_string(),
                FieldType::INTEGER => "int32".to_string(),
                FieldType::UINTEGER => "uint32".to_string(),
                FieldType::VARCHAR => {
                    let strlen = sch2.length(fldname);
                    format!("varchar({})", strlen)
                }
                FieldType::BOOL => "bool".to_string(),
                FieldType::DATE => "date".to_string(),
            };
            println!("{}: {}", fldname, fld_type);
        }
        tx.lock().unwrap().commit()?;

        Ok(())
    }

    #[test]
    fn catalog_test() -> Result<()> {
        if Path::new("_test/catalogtest").exists() {
            fs::remove_dir_all("_test/catalogtest")?;
        }

        let simpledb = SimpleDB::new_with("_test/catalogtest", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        let tm = TableMgr::new(true, Arc::clone(&tx))?;

        let mut sch = Schema::new();
        sch.add_i32_field("A");
        sch.add_string_field("B", 9);
        tm.create_table("MyTable", Arc::new(sch), Arc::clone(&tx))?;

        println!("All tables and their lengths:");
        let layout = tm.get_layout("tblcat", Arc::clone(&tx))?;
        let mut ts = TableScan::new(Arc::clone(&tx), "tblcat", layout)?;
        while ts.next() {
            let tname = ts.get_string("tblname")?;
            let size = ts.get_i32("slotsize")?;
            println!("{} {}", tname, size);
        }
        ts.close()?;

        println!("All fields and their offsets:");
        let layout = tm.get_layout("fldcat", Arc::clone(&tx))?;
        let mut ts = TableScan::new(Arc::clone(&tx), "fldcat", layout)?;
        while ts.next() {
            let tname = ts.get_string("tblname")?;
            let fname = ts.get_string("fldname")?;
            let offset = ts.get_i32("offset")?;
            println!("{} {} {}", tname, fname, offset);
        }
        ts.close()?;

        Ok(())
    }
}
