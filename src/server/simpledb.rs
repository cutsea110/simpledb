use anyhow::Result;
use core::fmt;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::manager::BufferMgr,
    file::manager::FileMgr,
    index::planner::indexupdateplanner::IndexUpdatePlanner,
    log::manager::LogMgr,
    metadata::{indexmanager::IndexInfo, manager::MetadataMgr},
    plan::{
        basicqueryplanner::BasicQueryPlanner, planner::Planner, queryplanner::QueryPlanner,
        updateplanner::UpdatePlanner,
    },
    record::schema::Schema,
    tx::{concurrency::locktable::LockTable, transaction::Transaction},
};

#[derive(Debug)]
pub enum SimpleDBError {
    NoPlanner,
    NoTableSchema,
    NoViewDefinition,
    NoIndexInfo,
}

impl std::error::Error for SimpleDBError {}
impl fmt::Display for SimpleDBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SimpleDBError::NoPlanner => {
                write!(f, "no planner")
            }
            SimpleDBError::NoTableSchema => {
                write!(f, "no table schema")
            }
            SimpleDBError::NoViewDefinition => {
                write!(f, "no view definition")
            }
            SimpleDBError::NoIndexInfo => {
                write!(f, "no index info")
            }
        }
    }
}

pub const LOG_FILE: &str = "simpledb.log";
pub const BLOCK_SIZE: i32 = 400;
pub const BUFFER_SIZE: usize = 8;

pub struct SimpleDB {
    // configure
    db_directory: String,
    blocksize: i32,
    numbuffs: usize,

    // base for static members
    next_tx_num: Arc<Mutex<i32>>,
    locktbl: Arc<Mutex<LockTable>>,

    // managers
    fm: Arc<Mutex<FileMgr>>,
    lm: Arc<Mutex<LogMgr>>,
    bm: Arc<Mutex<BufferMgr>>,
    mdm: Option<Arc<Mutex<MetadataMgr>>>,
    qp: Option<Arc<Mutex<dyn QueryPlanner>>>,
    up: Option<Arc<Mutex<dyn UpdatePlanner>>>,
}

impl SimpleDB {
    pub fn new(db_directory: &str) -> Result<Self> {
        let mut db = SimpleDB::new_with(db_directory, BLOCK_SIZE, BUFFER_SIZE);
        let tx = Arc::new(Mutex::new(db.new_tx()?));
        let isnew = db.file_mgr().lock().unwrap().is_new();
        if isnew {
            println!("creating new database");
        } else {
            println!("recovering existing database");
            tx.lock().unwrap().recover()?;
        }
        let meta = MetadataMgr::new(isnew, Arc::clone(&tx))?;
        db.mdm = Some(Arc::new(Mutex::new(meta)));
        // let next_table_num = Arc::new(Mutex::new(0));
        // let qp = HeuristicQueryPlanner::new(next_table_num, Arc::clone(&db.mdm.as_ref().unwrap()));
        let qp = BasicQueryPlanner::new(Arc::clone(&db.mdm.as_ref().unwrap()));
        db.qp = Some(Arc::new(Mutex::new(qp)));
        let up = IndexUpdatePlanner::new(Arc::clone(&db.mdm.as_ref().unwrap()));
        db.up = Some(Arc::new(Mutex::new(up)));

        tx.lock().unwrap().commit()?;

        Ok(db)
    }
    pub fn new_with(db_directory: &str, blocksize: i32, numbuffs: usize) -> Self {
        let next_tx_num = Arc::new(Mutex::new(0));
        let locktbl = Arc::new(Mutex::new(LockTable::new()));
        let fm = Arc::new(Mutex::new(
            FileMgr::new(&db_directory.clone(), blocksize).unwrap(),
        ));
        let lm = Arc::new(Mutex::new(LogMgr::new(Arc::clone(&fm), LOG_FILE).unwrap()));
        let bm = Arc::new(Mutex::new(BufferMgr::new(
            Arc::clone(&fm),
            Arc::clone(&lm),
            numbuffs,
        )));

        Self {
            db_directory: db_directory.to_string(),
            blocksize,
            numbuffs,
            next_tx_num,
            locktbl,
            fm,
            lm,
            bm,
            mdm: None,
            qp: None,
            up: None,
        }
    }
    pub fn file_mgr(&self) -> Arc<Mutex<FileMgr>> {
        Arc::clone(&self.fm)
    }
    pub fn log_mgr(&self) -> Arc<Mutex<LogMgr>> {
        Arc::clone(&self.lm)
    }
    pub fn buffer_mgr(&self) -> Arc<Mutex<BufferMgr>> {
        Arc::clone(&self.bm)
    }
    pub fn metadata_mgr(&self) -> Option<Arc<Mutex<MetadataMgr>>> {
        self.mdm.as_ref().map(|md| Arc::clone(md))
    }
    pub fn new_tx(&self) -> Result<Transaction> {
        Transaction::new(
            Arc::clone(&self.next_tx_num),
            Arc::clone(&self.locktbl),
            Arc::clone(&self.fm),
            Arc::clone(&self.lm),
            Arc::clone(&self.bm),
        )
    }
    pub fn planner(&self) -> Result<Planner> {
        if let Some(qp) = self.qp.as_ref() {
            if let Some(up) = self.up.as_ref() {
                return Ok(Planner::new(Arc::clone(qp), Arc::clone(up)));
            }
        }
        Err(From::from(SimpleDBError::NoPlanner))
    }
    // my own extend
    pub fn get_table_schema(
        &self,
        tblname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<Schema>> {
        if let Ok(layout) = self
            .mdm
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .get_layout(tblname, tx)
        {
            return Ok(layout.schema());
        }

        Err(From::from(SimpleDBError::NoTableSchema))
    }
    // my own extend
    pub fn get_view_definitoin(
        &self,
        viewname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(String, String)> {
        if let Ok(viewdef) = self
            .mdm
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .get_view_def(viewname, tx)
        {
            return Ok((viewname.to_string(), viewdef));
        }

        Err(From::from(SimpleDBError::NoViewDefinition))
    }
    // my own extend
    pub fn get_index_info(
        &self,
        tblname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<HashMap<String, IndexInfo>> {
        self.mdm
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .get_index_info(tblname, tx)
            .or_else(|_| Err(From::from(SimpleDBError::NoIndexInfo)))
    }
    // my own extend
    pub fn db_dir(&self) -> &str {
        &self.db_directory
    }
    // my own extend
    pub fn block_size(&self) -> i32 {
        self.blocksize
    }
    // my own extend
    pub fn buffer_nums(&self) -> usize {
        self.numbuffs
    }
}
