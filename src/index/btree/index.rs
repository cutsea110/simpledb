use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::{btreedir::BTreeDir, btreeleaf::BTreeLeaf};
use crate::{
    file::block_id::BlockId,
    index::{btree::btpage::BTPage, Index},
    query::constant::Constant,
    record::{
        layout::Layout,
        rid::RID,
        schema::{FieldType, Schema},
    },
    tx::transaction::Transaction,
};

pub struct BTreeIndex {
    tx: Arc<Mutex<Transaction>>,
    dir_layout: Arc<Layout>,
    leaf_layout: Arc<Layout>,
    leaftbl: String,
    leaf: Option<BTreeLeaf>,
    rootblk: BlockId,
}

impl BTreeIndex {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        idxname: &str,
        leaf_layout: Arc<Layout>,
    ) -> Result<Self> {
        // deal with the leaves
        let leaftbl = format!("{}leaf", idxname);
        if tx.lock().unwrap().size(&leaftbl)? == 0 {
            let blk = tx.lock().unwrap().append(&leaftbl)?;
            let mut node = BTPage::new(Arc::clone(&tx), blk.clone(), Arc::clone(&leaf_layout))?;
            node.format(&blk, -1)?;
        }

        // deal with the directory
        let mut dirsch = Schema::new();
        dirsch.add("block", leaf_layout.schema());
        dirsch.add("dataval", leaf_layout.schema());
        let dirtbl = format!("{}dir", idxname);
        let dir_layout = Arc::new(Layout::new(Arc::new(dirsch.clone())));
        let rootblk = BlockId::new(&dirtbl, 0);
        if tx.lock().unwrap().size(&dirtbl)? == 0 {
            // create new root block
            tx.lock().unwrap().append(&dirtbl)?;
            let mut node = BTPage::new(Arc::clone(&tx), rootblk.clone(), Arc::clone(&dir_layout))?;
            node.format(&rootblk, 0)?;
            // insert initial directory entry
            let fldtype = dirsch.field_type("dataval");
            let minval = match fldtype {
                FieldType::INTEGER => Constant::new_i32(i32::MIN),
                FieldType::VARCHAR => Constant::new_string("".to_string()),
            };
            node.insert_dir(0, minval, 0)?;
            node.close()?;
        }

        Ok(Self {
            tx,
            dir_layout,
            leaf_layout,
            leaftbl,
            leaf: None,
            rootblk,
        })
    }
    pub fn search_cost(numblocks: i32, rpb: i32) -> i32 {
        1 + ((numblocks as f32).ln() / (rpb as f32).ln()) as i32
    }
}

impl Index for BTreeIndex {
    fn before_first(&mut self, searchkey: Constant) -> Result<()> {
        self.close()?;
        let mut root = BTreeDir::new(
            Arc::clone(&self.tx),
            self.rootblk.clone(),
            Arc::clone(&self.dir_layout),
        )?;
        let blknum = root.search(&searchkey)?;
        root.close()?;
        let leafblk = BlockId::new(&self.leaftbl, blknum);
        self.leaf = BTreeLeaf::new(
            Arc::clone(&self.tx),
            leafblk,
            Arc::clone(&self.leaf_layout),
            searchkey,
        )
        .ok();

        Ok(())
    }
    fn next(&mut self) -> bool {
        self.leaf.as_mut().unwrap().next()
    }
    fn get_data_rid(&mut self) -> Result<RID> {
        self.leaf.as_mut().unwrap().get_data_rid()
    }
    fn insert(&mut self, dataval: Constant, datarid: RID) -> Result<()> {
        self.before_first(dataval)?;
        let dirent = self.leaf.as_mut().unwrap().insert(datarid);
        self.leaf.as_mut().unwrap().close()?;
        match dirent {
            None => Ok(()),
            Some(e) => {
                let mut root = BTreeDir::new(
                    Arc::clone(&self.tx),
                    self.rootblk.clone(),
                    Arc::clone(&self.dir_layout),
                )?;
                let dirent2 = root.insert(e);
                if let Some(e2) = dirent2 {
                    root.make_new_root(e2)?;
                }
                root.close()
            }
        }
    }
    fn delete(&mut self, dataval: Constant, datarid: RID) -> Result<()> {
        self.before_first(dataval)?;
        self.leaf.as_mut().unwrap().delete(datarid)?;
        self.leaf.as_mut().unwrap().close()
    }
    fn close(&mut self) -> Result<()> {
        if let Some(leaf) = self.leaf.as_mut() {
            leaf.close()?;
        }

        Ok(())
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

    use crate::{
        plan::{plan::Plan, tableplan::TablePlan},
        query::{constant::Constant, scan::Scan, updatescan::UpdateScan},
        record::schema::Schema,
        server::simpledb::SimpleDB,
    };

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/index").exists() {
            fs::remove_dir_all("_test/index")?;
        }

        let db = SimpleDB::new("_test/index")?;
        let tx = Arc::new(Mutex::new(db.new_tx()?));
        let mdm = db.metadata_mgr().unwrap();

        // Create student table
        let mut sch = Schema::new();
        sch.add_i32_field("sid");
        sch.add_string_field("sname", 10);
        sch.add_i32_field("grad_year");
        sch.add_i32_field("major_id");
        mdm.lock()
            .unwrap()
            .create_table("student", Arc::new(sch), Arc::clone(&tx))?;

        // Create index for major_id on student
        mdm.lock()
            .unwrap()
            .create_index("idx_major_id", "student", "major_id", Arc::clone(&tx))?;

        // Open an scan on the data table
        let studentplan = TablePlan::new("student", Arc::clone(&tx), Arc::clone(&mdm))?;
        let studentscan = studentplan.open()?;

        // Open the index on MajorId
        let indexes = mdm
            .lock()
            .unwrap()
            .get_index_info("student", Arc::clone(&tx))?;
        let ii = indexes.get("major_id").unwrap();
        let idx = ii.open();

        // Initialize data
        if let Ok(ts) = studentscan.lock().unwrap().as_table_scan() {
            ts.before_first()?;

            ts.insert()?;
            ts.set_i32("sid", 1)?;
            ts.set_string("sname", "joe".to_string())?;
            ts.set_i32("grad_year", 2020)?;
            ts.set_i32("major_id", 10)?;
            idx.lock()
                .unwrap()
                .insert(Constant::I32(10), ts.get_rid()?)?;

            ts.insert()?;
            ts.set_i32("sid", 2)?;
            ts.set_string("sname", "amy".to_string())?;
            ts.set_i32("grad_year", 2021)?;
            ts.set_i32("major_id", 20)?;
            idx.lock()
                .unwrap()
                .insert(Constant::I32(20), ts.get_rid()?)?;

            ts.insert()?;
            ts.set_i32("sid", 3)?;
            ts.set_string("sname", "max".to_string())?;
            ts.set_i32("grad_year", 2022)?;
            ts.set_i32("major_id", 30)?;
            idx.lock()
                .unwrap()
                .insert(Constant::I32(30), ts.get_rid()?)?;

            ts.insert()?;
            ts.set_i32("sid", 4)?;
            ts.set_string("sname", "lee".to_string())?;
            ts.set_i32("grad_year", 2020)?;
            ts.set_i32("major_id", 20)?;
            idx.lock()
                .unwrap()
                .insert(Constant::I32(20), ts.get_rid()?)?;
        }

        // Retrieve all index records having a dataval of 20.
        idx.lock().unwrap().before_first(Constant::I32(20))?;
        while idx.lock().unwrap().next() {
            // Use the datarid to go to the corresponding STUDENT record.
            let datarid = idx.lock().unwrap().get_data_rid()?;
            studentscan
                .lock()
                .unwrap()
                .to_update_scan()?
                .move_to_rid(datarid)?;
            println!("{}", studentscan.lock().unwrap().get_string("sname")?);
        }

        // Close the index and the data table
        idx.lock().unwrap().close()?;
        studentscan.lock().unwrap().close()?;
        tx.lock().unwrap().commit()?;

        Ok(())
    }
}
