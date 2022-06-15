pub enum DataType {
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
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
