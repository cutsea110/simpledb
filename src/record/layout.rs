use std::{collections::HashMap, mem, sync::Arc};

use super::schema::{FieldType, Schema};
use crate::file::page::Page;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Layout {
    schema: Arc<Schema>,
    offsets: HashMap<String, usize>,
    slotsize: usize,
}

impl Layout {
    pub fn new(schema: Arc<Schema>) -> Self {
        let mut offsets = HashMap::new();
        let mut pos = mem::size_of::<i32>(); // space for the empty/inuse flag
        for fldname in schema.fields() {
            offsets.insert(fldname.to_string(), pos);
            pos += lengthin_bytes(&schema, fldname.to_string())
        }

        Self {
            schema,
            offsets,
            slotsize: pos,
        }
    }

    pub fn new_with(schema: Arc<Schema>, offsets: HashMap<String, usize>, slotsize: usize) -> Self {
        Self {
            schema,
            offsets,
            slotsize,
        }
    }
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
    pub fn offset(&self, fldname: &str) -> usize {
        *self.offsets.get(fldname).unwrap()
    }
    pub fn slot_size(&self) -> usize {
        self.slotsize
    }
}

fn lengthin_bytes(schema: &Schema, fldname: String) -> usize {
    let fldtype = schema.field_type(&fldname);
    match fldtype {
        FieldType::SMALLINT => mem::size_of::<i16>(),
        FieldType::INTEGER => mem::size_of::<i32>(),
        FieldType::VARCHAR => Page::max_length(schema.length(&fldname)),
        FieldType::BOOL => mem::size_of::<bool>(),
        FieldType::DATE => mem::size_of::<u32>(), // NOTE: u16(year) + u8(month) + u8(day)
    }
}
