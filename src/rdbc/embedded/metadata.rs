use std::{cmp::max, sync::Arc};

use crate::{
    rdbc::resultsetmetadataadapter::{DataType, ResultSetMetaDataAdapter},
    record::schema::{FieldType, Schema},
};

pub struct EmbeddedMetaData {
    sch: Arc<Schema>,
}

impl EmbeddedMetaData {
    pub fn new(sch: Arc<Schema>) -> Self {
        Self { sch }
    }
}

impl ResultSetMetaDataAdapter for EmbeddedMetaData {
    fn get_column_count(&self) -> usize {
        self.sch.fields().len()
    }
    fn get_column_name(&self, column: usize) -> Option<&String> {
        self.sch.fields().get(column)
    }
    fn get_column_type(&self, column: usize) -> Option<DataType> {
        if let Some(fldname) = self.get_column_name(column) {
            match self.sch.field_type(fldname) {
                FieldType::INTEGER => {
                    return Some(DataType::Int32);
                }
                FieldType::VARCHAR => {
                    return Some(DataType::Varchar);
                }
            }
        }

        None
    }
    fn get_column_display_size(&self, column: usize) -> Option<usize> {
        if let Some(fldname) = self.get_column_name(column) {
            let fldlength = match self.sch.field_type(fldname) {
                FieldType::INTEGER => 6,
                FieldType::VARCHAR => self.sch.length(fldname),
            };

            return Some(max(fldname.len(), fldlength) + 1);
        }

        None
    }
}
