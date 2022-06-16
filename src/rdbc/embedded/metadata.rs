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
            return match self.sch.field_type(fldname) {
                FieldType::SMALLINT => Some(DataType::Int16),
                FieldType::INTEGER => Some(DataType::Int32),
                FieldType::VARCHAR => Some(DataType::Varchar),
                FieldType::BOOL => Some(DataType::Bool),
                FieldType::DATE => Some(DataType::Date),
            };
        }

        None
    }
    fn get_column_display_size(&self, column: usize) -> Option<usize> {
        if let Some(fldname) = self.get_column_name(column) {
            let fldlength = match self.sch.field_type(fldname) {
                FieldType::SMALLINT => 6, // WANTFIX
                FieldType::INTEGER => 6,  // WANTFIX
                FieldType::VARCHAR => self.sch.length(fldname),
                FieldType::BOOL => 5,  // length of false
                FieldType::DATE => 10, // length of YYYY-MM-DD
            };

            return Some(max(fldname.len(), fldlength) + 1);
        }

        None
    }
}
