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
        self.sch.fields().len()
    }
    fn get_column_type(&self, fldname: &str) -> DataType {
        match self.sch.field_type(fldname) {
            FieldType::INTEGER => DataType::Int32,
            FieldType::VARCHAR => DataType::Varchar,
        }
    }
    fn get_column_display_size(&self, fldname: &str) -> usize {
        let fldlength = match self.sch.field_type(fldname) {
            FieldType::INTEGER => 6,
            FieldType::VARCHAR => self.sch.length(fldname),
        };

        max(fldname.len(), fldlength) + 1
    }
}
