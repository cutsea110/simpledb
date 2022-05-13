use std::collections::HashMap;

use anyhow::Result;

use super::metadata::NetworkResultSetMetaData;
use crate::{
    rdbc::{
        resultsetadapter::ResultSetAdapter, resultsetmetadataadapter::ResultSetMetaDataAdapter,
    },
    remote_capnp,
};
use remote_capnp::{next, remote_result_set};

pub struct NextImpl {
    client: next::Client,
}
impl NextImpl {
    pub fn new(client: next::Client) -> Self {
        Self { client }
    }
    pub async fn has_next(&self) -> Result<bool> {
        let reply = self.client.read_request().send().promise.await?;
        Ok(reply.get()?.get_exists())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Value {
    Int32(i32),
    String(String),
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
        let request = self.resultset.get_row_request();
        let reply = request.send().promise.await?;
        let entry = to_hashmap(reply.get()?.get_row()?);

        let mut result = HashMap::new();
        for i in 0..metadata.get_column_count() {
            let fldname = metadata
                .get_column_name(i)
                .expect("get column name")
                .as_str();
            match entry.get(fldname) {
                Some(Value::Int32(v)) => {
                    result.insert(fldname, Value::Int32(*v));
                }
                Some(Value::String(s)) => {
                    result.insert(fldname, Value::String(s.clone()));
                }
                None => {
                    panic!("field missing");
                }
            }
        }

        Ok(result)
    }
    pub async fn get_meta(&self) -> Result<NetworkResultSetMetaData, Box<dyn std::error::Error>> {
        let request = self.resultset.get_metadata_request();
        let reply = request.send().promise.await?;
        let meta = reply.get()?.get_metadata()?;

        Ok(NetworkResultSetMetaData::from(meta))
    }
}

fn to_hashmap(row: remote_result_set::row::Reader) -> HashMap<&str, Value> {
    let entries = row.get_map().unwrap().get_entries().unwrap(); // TODO
    let mut result = HashMap::new();
    for kv in entries.into_iter() {
        let key = kv.get_key().unwrap(); // TODO
        let val = match kv.get_value().unwrap().which().unwrap() {
            remote_result_set::value::Int32(v) => Value::Int32(v),
            remote_result_set::value::String(s) => Value::String(s.unwrap().to_string()),
        };
        result.insert(key, val);
    }

    result
}

impl ResultSetAdapter for NetworkResultSet {
    type Meta = NetworkResultSetMetaData;
    type Next = NextImpl;

    fn next(&self) -> Self::Next {
        let exists = self.resultset.next_request().send().pipeline.get_exists();

        Self::Next::new(exists)
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        panic!("TODO")
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        panic!("TODO")
    }
    fn get_meta_data(&self) -> Result<Self::Meta> {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
}
