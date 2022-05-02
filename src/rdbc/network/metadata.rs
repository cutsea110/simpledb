use crate::rdbc::resultsetmetadataadapter::{DataType, ResultSetMetaDataAdapter};
use crate::remote_capnp::remote_result_set;

pub struct NetworkResultSetMetaData {
    // TODO
}

impl<'a> From<remote_result_set::meta_data::Reader<'a>> for NetworkResultSetMetaData {
    fn from(_: remote_result_set::meta_data::Reader) -> Self {
        panic!("TODO")
    }
}

impl ResultSetMetaDataAdapter for NetworkResultSetMetaData {
    fn get_column_count(&self) -> usize {
        panic!("TODO")
    }
    fn get_column_name(&self, column: usize) -> Option<&String> {
        panic!("TODO")
    }
    fn get_column_type(&self, column: usize) -> Option<DataType> {
        panic!("TODO")
    }
    fn get_column_display_size(&self, column: usize) -> Option<usize> {
        panic!("TODO")
    }
}
