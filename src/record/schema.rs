use std::collections::HashMap;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Schema {
    fields: Vec<String>,
    info: HashMap<String, FieldInfo>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            fields: vec![],
            info: HashMap::new(),
        }
    }
    pub fn add_field(&mut self, fldname: &str, fld_type: FieldType, length: usize) {
        self.fields.push(fldname.to_string());
        self.info
            .insert(fldname.to_string(), FieldInfo::new(fld_type, length));
    }
    pub fn add_i32_field(&mut self, fldname: &str) {
        self.add_field(fldname, FieldType::INTEGER, 0)
    }
    pub fn add_string_field(&mut self, fldname: &str, length: usize) {
        self.add_field(fldname, FieldType::VARCHAR, length)
    }
    pub fn add(&mut self, fldname: &str, sch: &Schema) {
        let fld_type = sch.field_type(fldname.clone());
        let length = sch.length(fldname.clone());
        self.add_field(fldname, fld_type, length)
    }
    pub fn add_all(&mut self, sch: &Schema) {
        for fldname in sch.fields() {
            self.add(fldname, sch)
        }
    }
    pub fn fields(&self) -> &Vec<String> {
        &self.fields
    }
    pub fn has_field(&self, fldname: &str) -> bool {
        self.fields.contains(&fldname.to_string())
    }
    pub fn field_type(&self, fldname: &str) -> FieldType {
        self.info.get(fldname).unwrap().fld_type
    }
    pub fn length(&self, fldname: &str) -> usize {
        self.info.get(fldname).unwrap().length
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FieldType {
    INTEGER = 4,
    VARCHAR = 12,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct FieldInfo {
    fld_type: FieldType,
    length: usize,
}

impl FieldInfo {
    fn new(fld_type: FieldType, length: usize) -> Self {
        Self { fld_type, length }
    }
}
