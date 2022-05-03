use std::collections::HashMap;

use itertools::Itertools;

use crate::rdbc::resultsetmetadataadapter::{DataType, ResultSetMetaDataAdapter};
use crate::record;
use crate::remote_capnp::{self, remote_result_set};

struct Schema {
    fields: Vec<String>,
    info: HashMap<String, FieldInfo>,
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

struct FieldInfo {
    fld_type: FieldType,
    length: usize,
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

enum FieldType {
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

pub struct NetworkResultSetMetaData {
    sch: Schema,
}

impl<'a> From<remote_result_set::meta_data::Reader<'a>> for NetworkResultSetMetaData {
    fn from(meta: remote_result_set::meta_data::Reader) -> Self {
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
