use anyhow::Result;
use chrono::NaiveDate;
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

#[derive(Debug, Clone)]
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
                FieldType::SMALLINT => Constant::new_i16(i16::MIN),
                FieldType::INTEGER => Constant::new_i32(i32::MIN),
                FieldType::VARCHAR => Constant::new_string("".to_string()),
                FieldType::BOOL => Constant::new_bool(false),
                FieldType::DATE => Constant::new_date(NaiveDate::from_ymd(0, 1, 1)), // NOTE: default 0000-01-01
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
