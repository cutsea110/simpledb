use anyhow::Result;
use core::fmt;
use std::collections::HashMap;

use super::metadata::NetworkResultSetMetaData;
use crate::{rdbc::resultsetadapter::ResultSetAdapter, remote_capnp};
use remote_capnp::remote_result_set;

#[derive(Debug)]
pub enum NetworkResultSetError {
    NoRecord,
    NoField(String),
}

impl std::error::Error for NetworkResultSetError {}
impl fmt::Display for NetworkResultSetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NetworkResultSetError::NoRecord => {
                write!(f, "no record")
            }
            NetworkResultSetError::NoField(fldname) => {
                write!(f, "no field: {}", fldname)
            }
        }
    }
}

pub enum Value {
    Int32(i32),
    String(String),
}

pub struct NetworkResultSet {
    client: remote_result_set::Client,
    record: Option<HashMap<String, Value>>,
}
impl NetworkResultSet {
    pub fn new(client: remote_result_set::Client) -> Self {
        Self {
            client,
            record: None,
        }
    }
    // if you call this, you have to check next method return true.
    pub fn get_next_record(&mut self) -> Result<HashMap<String, Value>> {
        let rt = tokio::runtime::Runtime::new()?;
        let mut map = HashMap::new();
        rt.block_on(async {
            let request = self.client.get_next_record_request();
            let reply = request.send().promise.await.unwrap(); // TODO
            let record = reply.get().unwrap().get_record().unwrap(); // TODO

            let entries = record.get_map().unwrap().get_entries().unwrap(); // TODO
            for kv in entries.into_iter() {
                let key = kv.get_key().unwrap().to_string(); // TODO
                let val = kv.get_value().unwrap(); // TODO
                let val = match val.which().unwrap() {
                    remote_result_set::value::Int32(v) => Value::Int32(v),
                    remote_result_set::value::String(s) => Value::String(s.unwrap().to_string()),
                };
                map.insert(key, val);
            }
        });

        Ok(map)
    }
}

impl ResultSetAdapter for NetworkResultSet {
    type Meta = NetworkResultSetMetaData;

    fn next(&self) -> bool {
        let rt = tokio::runtime::Runtime::new().unwrap(); // TODO
        rt.block_on(async {
            let request = self.client.next_request();
            let exists = request.send().promise.await.unwrap(); // TODO
            exists.get().unwrap().get_exists()
        })
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        if let Some(map) = self.record.as_ref() {
            if let Some(Value::Int32(v)) = map.get(fldname) {
                return Ok(*v);
            }

            return Err(From::from(NetworkResultSetError::NoField(
                fldname.to_string(),
            )));
        }

        Err(From::from(NetworkResultSetError::NoRecord))
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        if let Some(map) = self.record.as_ref() {
            if let Some(Value::String(s)) = map.get(fldname) {
                return Ok(s.clone());
            }

            return Err(From::from(NetworkResultSetError::NoField(
                fldname.to_string(),
            )));
        }

        Err(From::from(NetworkResultSetError::NoRecord))
    }
    fn get_meta_data(&self) -> Result<Self::Meta> {
        let rt = tokio::runtime::Runtime::new()?;
        let meta = rt.block_on(async {
            let request = self.client.get_metadata_request();
            let reply = request.send().promise.await.unwrap(); // TODO
            let meta = reply.get().unwrap().get_metadata().unwrap();
            panic!("TODO")
        });

        Ok(meta)
    }
    fn close(&mut self) -> Result<()> {
        self.record = None;
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let request = self.client.close_request();
            request.send().promise.await.unwrap(); // TODO
        });

        Ok(())
    }
}
