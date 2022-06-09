use itertools::Itertools;
use std::collections::HashMap;

use crate::{metadata, record, remote_capnp};

pub struct Schema {
    pub fields: Vec<String>,
    info: HashMap<String, FieldInfo>,
}
impl Schema {
    pub fn new() -> Self {
        Self {
            fields: vec![],
            info: HashMap::new(),
        }
    }
    pub fn field_type(&self, fldname: &str) -> FieldType {
        self.info.get(fldname).unwrap().fld_type
    }
    pub fn length(&self, fldname: &str) -> usize {
        self.info.get(fldname).unwrap().length
    }
    pub fn add_field(&mut self, fldname: &str) {
        self.fields.push(fldname.to_string());
    }
    pub fn add_info(&mut self, fldname: &str, info: FieldInfo) {
        self.info.insert(fldname.to_string(), info);
    }
    pub fn fields(&self) -> &Vec<String> {
        &self.fields
    }
    pub fn info(&self) -> &HashMap<String, FieldInfo> {
        &self.info
    }
}

impl<'a> From<remote_capnp::schema::Reader<'a>> for Schema {
    fn from(sch: remote_capnp::schema::Reader<'a>) -> Self {
        let fields = sch
            .get_fields()
            .unwrap()
            .into_iter()
            .map(|s| s.unwrap().to_string())
            .collect_vec();
        let mut info = HashMap::new();
        for kv in sch.get_info().unwrap().get_entries().unwrap().into_iter() {
            let key = kv.get_key().unwrap().to_string();
            let fi = FieldInfo::from(kv.get_value().unwrap());
            info.insert(key, fi);
        }
        Self { fields, info }
    }
}
impl From<Schema> for record::schema::Schema {
    fn from(sch: Schema) -> Self {
        let mut result = Self::new();
        for (fldname, FieldInfo { fld_type, length }) in sch.info.into_iter() {
            match fld_type {
                FieldType::INTEGER => result.add_i32_field(&fldname),
                FieldType::VARCHAR => result.add_string_field(&fldname, length),
            }
        }
        result
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct FieldInfo {
    fld_type: FieldType,
    length: usize,
}
impl FieldInfo {
    pub fn new_int32() -> Self {
        Self {
            fld_type: FieldType::INTEGER,
            length: 0,
        }
    }
    pub fn new_string(length: usize) -> Self {
        Self {
            fld_type: FieldType::VARCHAR,
            length,
        }
    }
}
impl<'a> From<remote_capnp::field_info::Reader<'a>> for FieldInfo {
    fn from(fi: remote_capnp::field_info::Reader<'a>) -> Self {
        Self {
            fld_type: fi.get_type().unwrap().into(),
            length: fi.get_length() as usize,
        }
    }
}
impl From<FieldInfo> for record::schema::FieldInfo {
    fn from(fi: FieldInfo) -> Self {
        Self {
            fld_type: record::schema::FieldType::from(fi.fld_type),
            length: fi.length,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum FieldType {
    INTEGER,
    VARCHAR,
}
impl<'a> From<remote_capnp::FieldType> for FieldType {
    fn from(ft: remote_capnp::FieldType) -> Self {
        match ft {
            remote_capnp::FieldType::Integer => Self::INTEGER,
            remote_capnp::FieldType::Varchar => Self::VARCHAR,
        }
    }
}
impl From<FieldType> for record::schema::FieldType {
    fn from(ft: FieldType) -> Self {
        match ft {
            FieldType::INTEGER => Self::INTEGER,
            FieldType::VARCHAR => Self::VARCHAR,
        }
    }
}

pub struct IndexInfo {
    fldname: String,
    idxname: String,
}
impl IndexInfo {
    pub fn new(fldname: &str, idxname: &str) -> Self {
        Self {
            fldname: fldname.to_string(),
            idxname: idxname.to_string(),
        }
    }
    pub fn field_name(&self) -> &str {
        &self.fldname
    }
    pub fn index_name(&self) -> &str {
        &self.idxname
    }
}

impl From<metadata::indexmanager::IndexInfo> for IndexInfo {
    fn from(ii: metadata::indexmanager::IndexInfo) -> Self {
        Self {
            fldname: ii.field_name().to_string(),
            idxname: ii.index_name().to_string(),
        }
    }
}
