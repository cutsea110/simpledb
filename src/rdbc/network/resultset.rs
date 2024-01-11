use anyhow::Result;
use chrono::NaiveDate;
use log::trace;
use std::collections::HashMap;

use super::{connection::ResponseImpl, metadata::NetworkResultSetMetaData};
use crate::{
    rdbc::{
        resultsetadapter::ResultSetAdapter, resultsetmetadataadapter::ResultSetMetaDataAdapter,
    },
    remote_capnp::{bool_box, date_box, int16_box, int32_box, remote_result_set, string_box},
};

pub struct NextImpl {
    client: bool_box::Client,
}
impl NextImpl {
    pub fn new(client: bool_box::Client) -> Self {
        Self { client }
    }
    pub async fn has_next(&self) -> Result<bool> {
        let reply = self.client.read_request().send().promise.await?;
        Ok(reply.get()?.get_val())
    }
}

pub struct Int16ValueImpl {
    client: int16_box::Client,
}
impl Int16ValueImpl {
    pub fn new(client: int16_box::Client) -> Self {
        Self { client }
    }
    pub async fn get_value(&self) -> Result<i16> {
        let reply = self.client.read_request().send().promise.await?;
        Ok(reply.get()?.get_val())
    }
}

pub struct Int32ValueImpl {
    client: int32_box::Client,
}
impl Int32ValueImpl {
    pub fn new(client: int32_box::Client) -> Self {
        Self { client }
    }
    pub async fn get_value(&self) -> Result<i32> {
        let reply = self.client.read_request().send().promise.await?;
        Ok(reply.get()?.get_val())
    }
}

pub struct StringValueImpl {
    client: string_box::Client,
}
impl StringValueImpl {
    pub fn new(client: string_box::Client) -> Self {
        Self { client }
    }
    pub async fn get_value(&self) -> Result<String> {
        let reply = self.client.read_request().send().promise.await?;
        Ok(reply.get()?.get_val()?.to_string().unwrap())
    }
}

pub struct BoolValueImpl {
    client: bool_box::Client,
}
impl BoolValueImpl {
    pub fn new(client: bool_box::Client) -> Self {
        Self { client }
    }
    pub async fn get_value(&self) -> Result<bool> {
        let reply = self.client.read_request().send().promise.await?;
        Ok(reply.get()?.get_val())
    }
}

pub struct DateValueImpl {
    client: date_box::Client,
}
impl DateValueImpl {
    pub fn new(client: date_box::Client) -> Self {
        Self { client }
    }
    pub async fn get_value(&self) -> Result<NaiveDate> {
        let reply = self.client.read_request().send().promise.await?;
        let val = reply.get()?.get_val()?;
        let year = val.get_year() as i32;
        let month = val.get_month() as u32;
        let day = val.get_day() as u32;

        Ok(NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Value {
    Int16(i16),
    Int32(i32),
    String(String),
    Bool(bool),
    Date(NaiveDate),
}

pub struct NetworkResultSet {
    resultset: remote_result_set::Client,
}
impl NetworkResultSet {
    pub fn new(resultset: remote_result_set::Client) -> Self {
        Self { resultset }
    }
    pub async fn get_row<'a, 'b: 'a>(
        &'a self,
        metadata: &'b NetworkResultSetMetaData,
    ) -> Result<HashMap<&'a str, Value>, Box<dyn std::error::Error>> {
        trace!("get_row");
        let request = self.resultset.get_row_request();
        let reply = request.send().promise.await?;
        let entry = Self::to_hashmap(reply.get()?.get_row()?);

        let mut result = HashMap::new();
        for i in 0..metadata.get_column_count() {
            let fldname = metadata
                .get_column_name(i)
                .expect("get column name")
                .as_str();
            match entry.get(fldname) {
                Some(Value::Int16(v)) => {
                    result.insert(fldname, Value::Int16(*v));
                }
                Some(Value::Int32(v)) => {
                    result.insert(fldname, Value::Int32(*v));
                }
                Some(Value::String(s)) => {
                    result.insert(fldname, Value::String(s.clone()));
                }
                Some(Value::Bool(v)) => {
                    result.insert(fldname, Value::Bool(*v));
                }
                Some(Value::Date(v)) => {
                    result.insert(fldname, Value::Date(*v));
                }
                None => {
                    panic!("field missing: {}", fldname);
                }
            }
        }

        Ok(result)
    }
    pub async fn get_rows<'a, 'b: 'a>(
        &'a self,
        limit: u32,
        metadata: &'b NetworkResultSetMetaData,
    ) -> Result<Vec<HashMap<&'a str, Value>>, Box<dyn std::error::Error>> {
        trace!("get_rows limit: {}", limit);
        let mut request = self.resultset.get_rows_request();
        request.get().set_limit(limit);
        let reply = request.send().promise.await?;
        let rows = reply.get()?.get_rows()?;
        let count = reply.get()?.get_count();

        let mut results = vec![];
        trace!("get_rows has {} rows", rows.len());

        for i in 0..count {
            let row = rows.get(i as u32);
            let entry = Self::to_hashmap(row);
            let mut result = HashMap::new();
            for i in 0..metadata.get_column_count() {
                let fldname = metadata
                    .get_column_name(i)
                    .expect("get column name")
                    .as_str();
                match entry.get(fldname) {
                    Some(Value::Int16(v)) => {
                        result.insert(fldname, Value::Int16(*v));
                    }
                    Some(Value::Int32(v)) => {
                        result.insert(fldname, Value::Int32(*v));
                    }
                    Some(Value::String(s)) => {
                        result.insert(fldname, Value::String(s.clone()));
                    }
                    Some(Value::Bool(v)) => {
                        result.insert(fldname, Value::Bool(*v));
                    }
                    Some(Value::Date(v)) => {
                        result.insert(fldname, Value::Date(*v));
                    }
                    None => {
                        panic!("field missing: {} at index {}", fldname, i);
                    }
                }
            }

            results.push(result);
        }

        Ok(results)
    }

    fn to_hashmap(row: remote_result_set::row::Reader) -> HashMap<&str, Value> {
        let entries = row
            .get_map()
            .expect("get row map")
            .get_entries()
            .expect("get entries");
        let mut result = HashMap::new();
        for kv in entries.into_iter() {
            let key = kv.get_key().expect("get key").to_str().unwrap();
            let val = match kv.get_value().unwrap().which().expect("match value type") {
                remote_result_set::value::Int16(v) => Value::Int16(v),
                remote_result_set::value::Int32(v) => Value::Int32(v),
                remote_result_set::value::String(s) => {
                    Value::String(s.unwrap().to_string().unwrap())
                }
                remote_result_set::value::Bool(v) => Value::Bool(v),
                remote_result_set::value::Date(v) => {
                    let v = v.unwrap();
                    let year = v.get_year() as i32;
                    let month = v.get_month() as u32;
                    let day = v.get_day() as u32;
                    Value::Date(NaiveDate::from_ymd_opt(year, month, day).unwrap())
                }
            };
            result.insert(key, val);
        }

        result
    }
}

impl ResultSetAdapter for NetworkResultSet {
    type Meta = NetworkResultSetMetaData;
    type Next = NextImpl;
    type Int16Value = Int16ValueImpl;
    type Int32Value = Int32ValueImpl;
    type StringValue = StringValueImpl;
    type BoolValue = BoolValueImpl;
    type DateValue = DateValueImpl;
    type Res = ResponseImpl;

    fn next(&self) -> Self::Next {
        let exists = self.resultset.next_request().send().pipeline.get_val();

        Self::Next::new(exists)
    }
    fn get_i16(&mut self, fldname: &str) -> Result<Self::Int16Value> {
        let mut request = self.resultset.get_int16_request();
        request.get().set_fldname(fldname.into());
        let val = request.send().pipeline.get_val();

        Ok(Self::Int16Value::new(val))
    }
    fn get_i32(&mut self, fldname: &str) -> Result<Self::Int32Value> {
        let mut request = self.resultset.get_int32_request();
        request.get().set_fldname(fldname.into());
        let val = request.send().pipeline.get_val();

        Ok(Self::Int32Value::new(val))
    }
    fn get_string(&mut self, fldname: &str) -> Result<Self::StringValue> {
        let mut request = self.resultset.get_string_request();
        request.get().set_fldname(fldname.into());
        let val = request.send().pipeline.get_val();

        Ok(Self::StringValue::new(val))
    }
    fn get_bool(&mut self, fldname: &str) -> Result<Self::BoolValue> {
        let mut request = self.resultset.get_bool_request();
        request.get().set_fldname(fldname.into());
        let val = request.send().pipeline.get_val();

        Ok(Self::BoolValue::new(val))
    }
    fn get_date(&mut self, fldname: &str) -> Result<Self::DateValue> {
        let mut request = self.resultset.get_date_request();
        request.get().set_fldname(fldname.into());
        let val = request.send().pipeline.get_val();

        Ok(Self::DateValue::new(val))
    }
    fn get_meta_data(&self) -> Result<Self::Meta> {
        let request = self.resultset.get_metadata_request();
        let meta = request.send().pipeline.get_metadata();

        Ok(Self::Meta::new(meta))
    }
    fn close(&mut self) -> Result<Self::Res> {
        let request = self.resultset.close_request();
        let res = request.send().pipeline.get_res();

        Ok(ResponseImpl::new(res))
    }
}
