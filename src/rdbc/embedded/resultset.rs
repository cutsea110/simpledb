use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::{connection::EmbeddedConnection, metadata::EmbeddedMetaData};
use crate::{
    plan::plan::Plan,
    query::scan::Scan,
    rdbc::{
        connectionadapter::ConnectionAdapter,
        resultsetadapter::{ResultSetAdapter, ResultSetError},
    },
    record::schema::Schema,
};

pub struct EmbeddedResultSet<'a> {
    s: Arc<Mutex<dyn Scan>>,
    sch: Arc<Schema>,
    conn: &'a mut EmbeddedConnection,
}

impl<'a> EmbeddedResultSet<'a> {
    pub fn new(plan: Arc<dyn Plan>, conn: &'a mut EmbeddedConnection) -> Result<Self> {
        if let Ok(s) = plan.open() {
            let sch = plan.schema();
            return Ok(Self { s, sch, conn });
        }

        Err(From::from(ResultSetError::ScanFailed))
    }
}

impl<'a> ResultSetAdapter for EmbeddedResultSet<'a> {
    type Meta = EmbeddedMetaData;
    type Next = bool;

    fn next(&self) -> Self::Next {
        self.s.lock().unwrap().next()
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        self.s.lock().unwrap().get_i32(fldname).or_else(|_| {
            self.conn.rollback().and_then(|_| {
                Err(From::from(ResultSetError::UnknownField(
                    fldname.to_string(),
                )))
            })
        })
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        self.s.lock().unwrap().get_string(fldname).or_else(|_| {
            self.conn.rollback().and_then(|_| {
                Err(From::from(ResultSetError::UnknownField(
                    fldname.to_string(),
                )))
            })
        })
    }
    fn get_meta_data(&self) -> Result<Self::Meta> {
        Ok(EmbeddedMetaData::new(Arc::clone(&self.sch)))
    }
    fn close(&mut self) -> Result<()> {
        match self.s.lock().unwrap().close() {
            Ok(_) => self.conn.close(),
            Err(_) => Err(From::from(ResultSetError::CloseFailed)),
        }
    }
}
