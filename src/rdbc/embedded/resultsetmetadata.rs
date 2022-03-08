use anyhow::Result;
use std::{cmp::max, sync::Arc};

use crate::{
    rdbc::resultsetmetadataadapter::{DataType, ResultSetMetaDataAdapter},
    record::schema::{FieldType, Schema},
};

pub struct EmbeddedResultSetMetaData {
    sch: Arc<Schema>,
}

impl EmbeddedResultSetMetaData {
    pub fn new(sch: Arc<Schema>) -> Self {
        Self { sch }
    }
}

impl ResultSetMetaDataAdapter for EmbeddedResultSetMetaData {
    fn get_column_count(&self) -> usize {
        panic!("TODO")
    }
    fn get_column_type(&self, fldname: &str) -> Result<DataType> {
        panic!("TODO")
    }
    fn get_column_display_size(&self, fldname: &str) -> Result<usize> {
        panic!("TODO")
    }
}
