use rdbc::{DataType, ResultSetMetaData};

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
