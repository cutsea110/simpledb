use rdbc::{Result, ResultSetMetaData};

pub trait ResultSetMetaDataAdapter: ResultSetMetaData {
    fn get_column_display_size(&self, column: i32) -> Result<usize>;
}
