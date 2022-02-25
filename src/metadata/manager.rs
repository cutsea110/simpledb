use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    record::{layout::Layout, schema::Schema},
    tx::transaction::Transaction,
};

use super::{
    indexmanager::{IndexInfo, IndexMgr},
    statmanager::{StatInfo, StatMgr},
    tablemanager::TableMgr,
    viewmanager::ViewMgr,
};

#[derive(Debug, Clone)]
pub struct MetadataMgr {
    tblmgr: TableMgr,
    viewmgr: ViewMgr,
    statmgr: StatMgr,
    idxmgr: IndexMgr,
}

impl MetadataMgr {
    pub fn new(isnew: bool, tx: Arc<Mutex<Transaction>>) -> Result<Self> {
        let tblmgr = TableMgr::new(isnew, Arc::clone(&tx))?;
        let viewmgr = ViewMgr::new(isnew, tblmgr.clone(), Arc::clone(&tx))?;
        let statmgr = StatMgr::new(tblmgr.clone(), Arc::clone(&tx))?;
        let idxmgr = IndexMgr::new(isnew, tblmgr.clone(), statmgr.clone(), Arc::clone(&tx))?;

        Ok(Self {
            tblmgr,
            viewmgr,
            statmgr,
            idxmgr,
        })
    }
    pub fn create_table(
        &self,
        tblname: &str,
        sch: Arc<Schema>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        self.tblmgr.create_table(tblname, sch, tx)
    }
    pub fn get_layout(&self, tblname: &str, tx: Arc<Mutex<Transaction>>) -> Result<Arc<Layout>> {
        self.tblmgr.get_layout(tblname, tx)
    }
    pub fn create_view(
        &self,
        viewname: &str,
        viewdef: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        self.viewmgr.create_view(viewname, viewdef, tx)
    }
    pub fn get_view_def(&self, viewname: &str, tx: Arc<Mutex<Transaction>>) -> Result<String> {
        self.viewmgr.get_view_def(viewname, tx)
    }
    pub fn create_index(
        &self,
        idxname: &str,
        tblname: &str,
        fldname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        self.idxmgr.create_index(idxname, tblname, fldname, tx)
    }
    pub fn get_index_info(
        &mut self,
        tblname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<HashMap<String, IndexInfo>> {
        self.idxmgr.get_index_info(tblname, tx)
    }
    pub fn get_stat_info(
        &mut self,
        tblname: &str,
        layout: Arc<Layout>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatInfo> {
        self.statmgr.get_stat_info(tblname, layout, tx)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use rand::Rng;
    use std::{fs, path::Path};

    use super::*;
    use crate::query::updatescan::UpdateScan;
    use crate::record::schema::FieldType;
    use crate::record::tablescan::TableScan;
    use crate::server::simpledb::SimpleDB;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/metadatamgrtest").exists() {
            fs::remove_dir_all("_test/metadatamgrtest")?;
        }

        let simpledb = SimpleDB::new_with("_test/metadatamgrtest", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        let mut mdm = MetadataMgr::new(true, Arc::clone(&tx))?;

        let mut sch = Schema::new();
        sch.add_i32_field("A");
        sch.add_string_field("B", 9);

        // Part 1: Table Metadata
        mdm.create_table("MyTable", Arc::new(sch), Arc::clone(&tx))?;
        let layout = mdm.get_layout("MyTable", Arc::clone(&tx))?;
        let size = layout.slot_size();
        let sch2 = layout.schema();
        println!("MyTable has slot size {}", size);
        println!("Its fields are:");
        for fldname in sch2.fields() {
            let fld_type = match sch2.field_type(fldname) {
                FieldType::INTEGER => "int".to_string(),
                FieldType::VARCHAR => {
                    let strlen = sch2.length(fldname);
                    format!("varchar({})", strlen)
                }
            };
            println!("{}: {}", fldname, fld_type);
        }

        // Part 2: Statistics Metadata
        let mut ts = TableScan::new(Arc::clone(&tx), "MyTable", Arc::clone(&layout))?;
        let mut rng = rand::thread_rng();
        for _ in 0..50 {
            ts.insert()?;
            let n = rng.gen_range(1..50);
            ts.set_i32("A", n)?;
            ts.set_string("B", format!("rec{}", n))?;
        }
        let si = mdm.get_stat_info("MyTable", Arc::clone(&layout), Arc::clone(&tx))?;
        println!("B(MyTable) = {}", si.blocks_accessed());
        println!("R(MyTable) = {}", si.records_output());
        println!("V(MyTable,A) = {}", si.distinct_values("A"));
        println!("V(MyTable,B) = {}", si.distinct_values("B"));

        // Part 3: View Metadata
        let viewdef = "select B from MyTable where A = 1";
        mdm.create_view("viewA", viewdef, Arc::clone(&tx))?;
        let v = mdm.get_view_def("viewA", Arc::clone(&tx))?;
        println!("View def = {}", v);

        // Part 4: Index Metadata
        mdm.create_index("indexA", "MyTable", "A", Arc::clone(&tx))?;
        mdm.create_index("indexB", "MyTable", "B", Arc::clone(&tx))?;
        let idxmap = mdm.get_index_info("MyTable", Arc::clone(&tx))?;
        if let Some(ii) = idxmap.get("A") {
            println!("B(indexA) = {}", ii.blocks_accessed());
            println!("R(indexA) = {}", ii.records_output());
            println!("V(indexA,A) = {}", ii.distinct_values("A"));
            println!("V(indexA,B) = {}", ii.distinct_values("B"));
        }
        if let Some(ii) = idxmap.get("B") {
            println!("B(indexB) = {}", ii.blocks_accessed());
            println!("R(indexB) = {}", ii.records_output());
            println!("V(indexB,A) = {}", ii.distinct_values("A"));
            println!("V(indexB,B) = {}", ii.distinct_values("B"));
        }
        tx.lock().unwrap().commit()?;

        Ok(())
    }
}
