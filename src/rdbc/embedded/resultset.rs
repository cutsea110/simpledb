use anyhow::Result;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
};

use super::connection::EmbeddedConnection;
use crate::{
    query::scan::Scan,
    rdbc::{
        resultsetadapter::ResultSetAdapter, resultsetmetadataadapter::ResultSetMetaDataAdapter,
    },
    record::schema::Schema,
};

pub struct EmbeddedResultSet<'a> {
    s: Arc<Mutex<dyn Scan>>,
    sch: Arc<Schema>,
    conn: &'a mut EmbeddedConnection,
}

impl<'a> ResultSetAdapter for EmbeddedResultSet<'a> {
    fn next(&self) -> Result<bool> {
        panic!("TODO")
    }
    fn get_i32(&self, fldname: &str) -> Result<i32> {
        panic!("TODO")
    }
    fn get_string(&self, fldname: &str) -> Result<String> {
        panic!("TODO")
    }
    fn get_meta_data(&self) -> Result<Rc<RefCell<dyn ResultSetMetaDataAdapter>>> {
        panic!("TODO")
    }
}
