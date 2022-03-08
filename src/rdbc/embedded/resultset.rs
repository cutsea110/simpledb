use rdbc::{Error, Result, ResultSet, ResultSetMetaData};
use std::{
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{
    plan::plan::Plan,
    query::scan::Scan,
    rdbc::{connectionadapter::ConnectionAdapter, resultsetadapter::ResultSetAdapter},
    record::schema::Schema,
};

use super::{connection::EmbeddedConnection, resultsetmetadata::EmbeddedResultSetMetaData};

pub struct EmbeddedResultSet<'a> {
    s: Arc<Mutex<dyn Scan>>,
    sch: Arc<Schema>,
    conn: &'a mut EmbeddedConnection,
}

impl<'a> EmbeddedResultSet<'a> {
    pub fn new(plan: Arc<dyn Plan>, conn: &'a mut EmbeddedConnection) -> Self {
        let s = plan.open().unwrap();
        let sch = plan.schema();

        Self { s, sch, conn }
    }
    fn get_field_name(&self, i: u64) -> Result<&str> {
        if let Some(fldname) = self.sch.fields().get(i as usize) {
            return Ok(fldname);
        }

        Err(From::from(Error::General(
            "failed to access field index".to_string(),
        )))
    }
}

impl<'a> ResultSet for EmbeddedResultSet<'a> {
    fn meta_data(&self) -> Result<Rc<dyn ResultSetMetaData>> {
        Ok(Rc::new(EmbeddedResultSetMetaData::new(Arc::clone(
            &self.sch,
        ))))
    }
    fn next(&mut self) -> bool {
        self.s.lock().unwrap().next()
    }
    fn get_i8(&self, i: u64) -> Result<Option<i8>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn get_i16(&self, i: u64) -> Result<Option<i16>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn get_i32(&self, i: u64) -> Result<Option<i32>> {
        let fldname = self.get_field_name(i)?;
        if let Ok(ival) = self.s.lock().unwrap().get_i32(fldname) {
            return Ok(Some(ival));
        }

        Err(From::from(Error::General(
            "failed to get i32 value".to_string(),
        )))
    }
    fn get_i64(&self, i: u64) -> Result<Option<i64>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn get_f32(&self, i: u64) -> Result<Option<f32>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn get_f64(&self, i: u64) -> Result<Option<f64>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
    fn get_string(&self, i: u64) -> Result<Option<String>> {
        let fldname = self.get_field_name(i)?;
        if let Ok(sval) = self.s.lock().unwrap().get_string(fldname) {
            return Ok(Some(sval));
        }

        Err(From::from(Error::General(
            "failed to get string value".to_string(),
        )))
    }
    fn get_bytes(&self, i: u64) -> Result<Option<Vec<u8>>> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
}

impl<'a> ResultSetAdapter for EmbeddedResultSet<'a> {
    fn close(&mut self) -> Result<()> {
        if let Ok(_) = self.s.lock().unwrap().close() {
            return self.conn.commit();
        }

        Err(From::from(Error::General("failed to close".to_string())))
    }
}
