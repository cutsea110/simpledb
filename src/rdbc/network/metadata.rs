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
                FieldType::INTEGER => return Some(DataType::Int32),
                FieldType::VARCHAR => return Some(DataType::Varchar),
            }
        }

        None
    }
    fn get_column_display_size(&self, column: usize) -> Option<usize> {
        if let Some(fldname) = self.get_column_name(column) {
            let fldlength = match self.sch.field_type(fldname) {
                FieldType::INTEGER => 6,
                FieldType::VARCHAR => self.sch.length(fldname),
            };

            return Some(max(fldname.len(), fldlength) + 1);
        }

        None
    }
}
