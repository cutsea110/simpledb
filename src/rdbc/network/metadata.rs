use std::cmp::max;

use crate::rdbc::{
    model::{FieldType, Schema},
    resultsetmetadataadapter::{DataType, ResultSetMetaDataAdapter},
};

pub struct NetworkResultSetMetaData {
    schema: Schema,
}

impl NetworkResultSetMetaData {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }

    pub fn column_count(&self) -> usize {
        self.schema.fields.len()
    }
}

impl ResultSetMetaDataAdapter for NetworkResultSetMetaData {
    fn get_column_count(&self) -> usize {
        self.column_count()
    }

    fn get_column_name(&self, column: usize) -> Option<&String> {
        self.schema.fields.get(column)
    }

    fn get_column_type(&self, column: usize) -> Option<DataType> {
        let field = self.get_column_name(column)?;
        Some(match self.schema.field_type(field) {
            FieldType::SMALLINT => DataType::Int16,
            FieldType::INTEGER => DataType::Int32,
            FieldType::VARCHAR => DataType::Varchar,
            FieldType::BOOL => DataType::Bool,
            FieldType::DATE => DataType::Date,
        })
    }

    fn get_column_display_size(&self, column: usize) -> Option<usize> {
        let field = self.get_column_name(column)?;
        let field_length = match self.schema.field_type(field) {
            FieldType::SMALLINT => 6,
            FieldType::INTEGER => 11,
            FieldType::VARCHAR => self.schema.length(field),
            FieldType::BOOL => 5,
            FieldType::DATE => 10,
        };
        Some(max(field.len(), field_length) + 1)
    }
}
