use anyhow::Result;
use chrono::NaiveDate;
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
    type Int8Value = i8;
    type UInt8Value = u8;
    type Int16Value = i16;
    type UInt16Value = u16;
    type Int32Value = i32;
    type UInt32Value = u32;
    type StringValue = String;
    type BoolValue = bool;
    type DateValue = NaiveDate;
    type Res = ();

    fn next(&self) -> Self::Next {
        self.s.lock().unwrap().next()
    }
    fn get_i8(&mut self, fldname: &str) -> Result<Self::Int8Value> {
        self.s.lock().unwrap().get_i8(fldname).or_else(|_| {
            self.conn.rollback().and_then(|_| {
                Err(From::from(ResultSetError::UnknownField(
                    fldname.to_string(),
                )))
            })
        })
    }
    fn get_u8(&mut self, fldname: &str) -> Result<Self::UInt8Value> {
        self.s.lock().unwrap().get_u8(fldname).or_else(|_| {
            self.conn.rollback().and_then(|_| {
                Err(From::from(ResultSetError::UnknownField(
                    fldname.to_string(),
                )))
            })
        })
    }
    fn get_i16(&mut self, fldname: &str) -> Result<Self::Int16Value> {
        self.s.lock().unwrap().get_i16(fldname).or_else(|_| {
            self.conn.rollback().and_then(|_| {
                Err(From::from(ResultSetError::UnknownField(
                    fldname.to_string(),
                )))
            })
        })
    }
    fn get_u16(&mut self, fldname: &str) -> Result<Self::UInt16Value> {
        self.s.lock().unwrap().get_u16(fldname).or_else(|_| {
            self.conn.rollback().and_then(|_| {
                Err(From::from(ResultSetError::UnknownField(
                    fldname.to_string(),
                )))
            })
        })
    }
    fn get_i32(&mut self, fldname: &str) -> Result<Self::Int32Value> {
        self.s.lock().unwrap().get_i32(fldname).or_else(|_| {
            self.conn.rollback().and_then(|_| {
                Err(From::from(ResultSetError::UnknownField(
                    fldname.to_string(),
                )))
            })
        })
    }
    fn get_u32(&mut self, fldname: &str) -> Result<Self::UInt32Value> {
        self.s.lock().unwrap().get_u32(fldname).or_else(|_| {
            self.conn.rollback().and_then(|_| {
                Err(From::from(ResultSetError::UnknownField(
                    fldname.to_string(),
                )))
            })
        })
    }
    fn get_string(&mut self, fldname: &str) -> Result<Self::StringValue> {
        self.s.lock().unwrap().get_string(fldname).or_else(|_| {
            self.conn.rollback().and_then(|_| {
                Err(From::from(ResultSetError::UnknownField(
                    fldname.to_string(),
                )))
            })
        })
    }
    fn get_bool(&mut self, fldname: &str) -> Result<Self::BoolValue> {
        self.s.lock().unwrap().get_bool(fldname).or_else(|_| {
            self.conn.rollback().and_then(|_| {
                Err(From::from(ResultSetError::UnknownField(
                    fldname.to_string(),
                )))
            })
        })
    }
    fn get_date(&mut self, fldname: &str) -> Result<Self::DateValue> {
        self.s.lock().unwrap().get_date(fldname).or_else(|_| {
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
    fn close(&mut self) -> Result<Self::Res> {
        match self.s.lock().unwrap().close() {
            Ok(_) => self.conn.close(),
            Err(_) => Err(From::from(ResultSetError::CloseFailed)),
        }
    }
}
