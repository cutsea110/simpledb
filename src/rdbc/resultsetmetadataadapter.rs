use anyhow::Result;

pub enum DataType {
    Int32,
    Varchar,
}

pub trait ResultSetMetaDataAdapter {
    fn get_column_count(&self) -> usize;
    fn get_column_type(&self, fldname: &str) -> Result<DataType>;
    fn get_column_display_size(&self, fldname: &str) -> Result<usize>;
}
