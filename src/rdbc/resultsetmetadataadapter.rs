pub enum DataType {
    Int16,
    Int32,
    Varchar,
    Bool,
    Date,
}

pub trait ResultSetMetaDataAdapter {
    fn get_column_count(&self) -> usize;
    fn get_column_name(&self, column: usize) -> Option<&String>;
    fn get_column_type(&self, column: usize) -> Option<DataType>;
    fn get_column_display_size(&self, column: usize) -> Option<usize>;
}
