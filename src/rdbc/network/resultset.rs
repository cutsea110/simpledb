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

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
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

            NetworkResultSetMetaData::from(meta)
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
