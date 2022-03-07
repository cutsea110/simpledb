use rdbc::{DataType, Error, Result, ResultSetMetaData};

use crate::rdbc::resultsetmetadataadapter::ResultSetMetaDataAdapter;

pub struct EmbeddedResultSetMetaData {}

impl ResultSetMetaData for EmbeddedResultSetMetaData {
    fn num_columns(&self) -> u64 {
        0
    }
    fn column_name(&self, i: u64) -> String {
        "TODO".to_string()
    }
    fn column_type(&self, i: u64) -> DataType {
        DataType::Integer
    }
}

impl ResultSetMetaDataAdapter for EmbeddedResultSetMetaData {
    fn get_column_display_size(&self, column: i32) -> Result<usize> {
        Err(From::from(Error::General("not implemented".to_string())))
    }
}
