use anyhow::Result;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
};

use super::{connection::EmbeddedConnection, resultsetmetadata::EmbeddedResultSetMetaData};
use crate::{
    plan::plan::Plan,
    query::scan::Scan,
    rdbc::{
        connectionadapter::ConnectionAdapter,
        resultsetadapter::{ResultSetAdapter, ResultSetError},
        resultsetmetadataadapter::ResultSetMetaDataAdapter,
    },
    record::schema::Schema,
};

pub struct EmbeddedResultSet {
    s: Arc<Mutex<dyn Scan>>,
    sch: Arc<Schema>,
    conn: Rc<RefCell<EmbeddedConnection>>,
}

impl EmbeddedResultSet {
    pub fn new(plan: Arc<dyn Plan>, conn: Rc<RefCell<EmbeddedConnection>>) -> Result<Self> {
        if let Ok(s) = plan.open() {
            let sch = plan.schema();
            return Ok(Self { s, sch, conn });
        }

        Err(From::from(ResultSetError::ScanFailed))
    }
}

impl ResultSetAdapter for EmbeddedResultSet {
    fn next(&self) -> bool {
        self.s.lock().unwrap().next()
    }
    fn get_i32(&self, fldname: &str) -> Result<i32> {
        self.s.lock().unwrap().get_i32(fldname)
    }
    fn get_string(&self, fldname: &str) -> Result<String> {
        self.s.lock().unwrap().get_string(fldname)
    }
    fn get_meta_data(&self) -> Result<Rc<RefCell<dyn ResultSetMetaDataAdapter>>> {
        Ok(Rc::new(RefCell::new(EmbeddedResultSetMetaData::new(
            Arc::clone(&self.sch),
        ))))
    }
    fn close(&self) -> Result<()> {
        self.s.lock().unwrap().close()?;
        self.conn.borrow_mut().close()
    }
}
