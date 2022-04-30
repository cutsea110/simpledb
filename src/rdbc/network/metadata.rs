use crate::rdbc::resultsetmetadataadapter::ResultSetMetaDataAdapter;

pub struct NetworkResultSetMetaData {
    // TODO
}

impl ResultSetMetaDataAdapter for NetworkResultSetMetaData {
    fn get_column_count(&self) -> usize {
        panic!("TODO")
    }
    fn get_column_name(&self, column: usize) -> Option<&String> {
        panic!("TODO")
    }
    fn get_column_type(
        &self,
        column: usize,
    ) -> Option<crate::rdbc::resultsetmetadataadapter::DataType> {
        panic!("TODO")
    }
    fn get_column_display_size(&self, column: usize) -> Option<usize> {
        panic!("TODO")
    }
}
