use std::{cmp::max, sync::Arc};

use rdbc::{DataType, Error, Result, ResultSetMetaData};

use crate::{
    rdbc::resultsetmetadataadapter::ResultSetMetaDataAdapter,
    record::schema::{FieldType, Schema},
};

pub struct EmbeddedResultSetMetaData {
    sch: Arc<Schema>,
}

impl EmbeddedResultSetMetaData {
    pub fn new(sch: Arc<Schema>) -> Self {
        Self { sch }
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

impl ResultSetMetaData for EmbeddedResultSetMetaData {
    fn num_columns(&self) -> u64 {
        self.sch.fields().len() as u64
    }
    fn column_name(&self, i: u64) -> String {
        if let Ok(fldname) = self.get_field_name(i) {
            return fldname.to_string();
        }

        // want to fail for this case :(
        "".to_string()
    }
    fn column_type(&self, i: u64) -> DataType {
        if let Ok(fldname) = self.get_field_name(i) {
            match self.sch.field_type(fldname) {
                FieldType::INTEGER => {
                    return DataType::Integer;
                }
                FieldType::VARCHAR => {
                    return DataType::Char;
                }
            }
        }

        // want to fail for this case :(
        DataType::Integer
    }
}

impl ResultSetMetaDataAdapter for EmbeddedResultSetMetaData {
    fn get_column_display_size(&self, column: i32) -> Result<usize> {
        if let Ok(fldname) = self.get_field_name(column as u64) {
            let fldtype = self.sch.field_type(fldname);
            let fldlength = match fldtype {
                FieldType::INTEGER => 6,
                FieldType::VARCHAR => self.sch.length(fldname),
            };

            return Ok(max(fldname.len(), fldlength + 1));
        }

        Err(From::from(Error::General(
            "failed to access field index".to_string(),
        )))
    }
}
