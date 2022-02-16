use std::{collections::HashMap, mem};

use crate::file::page::Page;

use super::schema::{FieldType, Schema};

pub struct Layout {
    schema: Schema,
    offsets: HashMap<String, usize>,
    slotsize: usize,
}

impl Layout {
    pub fn new(schema: Schema) -> Self {
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

    pub fn new_with(schema: Schema, offsets: HashMap<String, usize>, slotsize: usize) -> Self {
        Self {
            schema,
            offsets,
            slotsize,
        }
    }
    pub fn schema(&self) -> &Schema {
        &self.schema
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
        FieldType::INTEGER => mem::size_of::<i32>(),
        FieldType::VARCHAR => Page::max_length(schema.length(&fldname)),
    }
}
