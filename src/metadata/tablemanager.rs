use anyhow::Result;
use core::panic;
use num_traits::FromPrimitive;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    usize,
};

use crate::{
    record::{layout::Layout, schema::Schema, tablescan::TableScan},
    tx::transaction::Transaction,
};

// table or field name
const MAX_NAME: i32 = 16;

#[derive(Debug, Clone)]
pub struct TableMgr {
    tcat_layout: Layout,
    fcat_layout: Layout,
}

impl TableMgr {
    pub fn new(is_new: bool, tx: Arc<Mutex<Transaction>>) -> Self {
        panic!("TODO")
    }
    pub fn create_table(
        &mut self,
        tblname: &str,
        sch: Schema,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        let layout = Layout::new(sch);
        // insert one record into tblcat
        let mut tcat = TableScan::new(Arc::clone(&tx), "tblcat", self.tcat_layout.clone());
        tcat.insert()?;
        tcat.set_string("tblname", tblname.to_string())?;
        tcat.set_i32("slotsize", layout.slot_size() as i32)?;
        tcat.close()?;
        // insert a record into fldcat for each field
        let mut fcat = TableScan::new(tx, "fldcat", self.fcat_layout.clone());
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
    pub fn get_layout(&self, tblname: &str, tx: Arc<Mutex<Transaction>>) -> Result<Layout> {
        let mut size = -1;
        let mut tcat = TableScan::new(Arc::clone(&tx), "tblcat", self.tcat_layout.clone());
        while tcat.next() {
            if tcat.get_string("tblname")? == tblname {
                size = tcat.get_i32("slotsize")?;
                break;
            }
        }
        tcat.close()?;

        let mut sch = Schema::new();
        let mut offsets = HashMap::<String, usize>::new();
        let mut fcat = TableScan::new(tx, "fldcat", self.fcat_layout.clone());
        while fcat.next() {
            if fcat.get_string("tblname")? == tblname {
                let fldname = fcat.get_string("fldname")?;
                let fldtype = FromPrimitive::from_i32(fcat.get_i32("type")?).unwrap();
                let fldlen = fcat.get_i32("length")? as usize;
                let offset = fcat.get_i32("offset")? as usize;
                offsets.insert("fldname".to_string(), offset);
                sch.add_field(&fldname, fldtype, fldlen);
            }
        }
        fcat.close()?;

        Ok(Layout::new_with(sch, offsets, size as usize))
    }
}
