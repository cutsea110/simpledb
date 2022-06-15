use log::{debug, trace};
use std::cmp::max;
use std::sync::Arc;

use crate::{
    rdbc::{
        model::{FieldType, Schema},
        resultsetmetadataadapter::{DataType, ResultSetMetaDataAdapter},
    },
    remote_capnp::remote_meta_data,
};

pub struct NetworkResultSetMetaData {
    client: remote_meta_data::Client,
    sch: Arc<Schema>, // TODO なくす
}
impl NetworkResultSetMetaData {
    pub fn new(client: remote_meta_data::Client) -> Self {
        Self {
            client,
            sch: Arc::new(Schema::new()),
        }
    }
    // This interface is not smart. Any idea?
    pub async fn load_schema(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        trace!("load schema");
        let request = self.client.get_schema_request();
        let sch = request.send().promise.await?;
        self.sch = Arc::new(Schema::from(sch.get()?.get_sch()?));
        debug!("loaded");

        Ok(())
    }
}

impl ResultSetMetaDataAdapter for NetworkResultSetMetaData {
    fn get_column_count(&self) -> usize {
        self.sch.fields.len()
    }
    fn get_column_name(&self, column: usize) -> Option<&String> {
        self.sch.fields.get(column)
    }
    fn get_column_type(&self, column: usize) -> Option<DataType> {
        if let Some(fldname) = self.get_column_name(column) {
            match self.sch.field_type(fldname) {
                FieldType::WORD => return Some(DataType::Int8),
                FieldType::UWORD => return Some(DataType::UInt8),
                FieldType::SHORT => return Some(DataType::Int16),
                FieldType::USHORT => return Some(DataType::UInt16),
                FieldType::INTEGER => return Some(DataType::Int32),
                FieldType::UINTEGER => return Some(DataType::UInt32),
                FieldType::VARCHAR => return Some(DataType::Varchar),
                FieldType::BOOL => return Some(DataType::Bool),
                FieldType::DATE => return Some(DataType::Date),
            }
        }

        None
    }
    fn get_column_display_size(&self, column: usize) -> Option<usize> {
        if let Some(fldname) = self.get_column_name(column) {
            let fldlength = match self.sch.field_type(fldname) {
                FieldType::WORD => 3,
                FieldType::UWORD => 4,
                FieldType::SHORT => 6,    // WANTFIX
                FieldType::USHORT => 6,   // WANTFIX
                FieldType::INTEGER => 6,  // WANTFIX
                FieldType::UINTEGER => 6, // WANTFIX
                FieldType::VARCHAR => self.sch.length(fldname),
                FieldType::BOOL => 5,  // length of false
                FieldType::DATE => 10, // length of YYYY-MM-DD
            };

            return Some(max(fldname.len(), fldlength) + 1);
        }

        None
    }
}
