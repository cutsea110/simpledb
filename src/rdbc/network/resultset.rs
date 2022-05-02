use anyhow::Result;
use core::fmt;
use std::{cell::RefCell, collections::HashMap};

use super::metadata::NetworkResultSetMetaData;
use crate::{rdbc::resultsetadapter::ResultSetAdapter, remote_capnp};
use remote_capnp::remote_result_set;

#[derive(Debug)]
enum ResultSetError {
    FieldNotFound(String),
}

impl std::error::Error for ResultSetError {}
impl fmt::Display for ResultSetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ResultSetError::FieldNotFound(s) => {
                write!(f, "field {} not found", s)
            }
        }
    }
}

enum Value {
    Int32(i32),
    String(String),
}

pub struct NetworkResultSet {
    client: remote_result_set::Client,
    current_index: RefCell<i32>,
    count: i32,
    records: Vec<HashMap<String, Value>>,
}
impl NetworkResultSet {
    pub fn new(client: remote_result_set::Client) -> Self {
        Self {
            client,
            count: -1,
            records: vec![],

            current_index: RefCell::new(-1),
        }
    }
    pub fn get_records_all(&mut self) -> Result<i32> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let request = self.client.get_records_all_request();
            let reply = request.send().promise.await.unwrap(); // TODO
            let results = reply.get().unwrap().get_results().unwrap(); // TODO
            let records = results.get_records().unwrap(); // TODO

            self.count = results.get_count();
            self.records.clear();
            for r in records.into_iter() {
                let mut map = HashMap::new();
                let entries = r.get_map().unwrap().get_entries().unwrap(); // TODO
                for kv in entries.into_iter() {
                    let key = kv.get_key().unwrap().to_string(); // TODO
                    let val = kv.get_value().unwrap(); // TODO
                    let val = match val.which().unwrap() {
                        remote_result_set::value::Int32(v) => Value::Int32(v),
                        remote_result_set::value::String(s) => {
                            Value::String(s.unwrap().to_string())
                        }
                    };
                    map.insert(key, val);
                }
                self.records.push(map);
            }
            self.current_index = RefCell::new(-1);
        });

        Ok(self.count)
    }
}

impl ResultSetAdapter for NetworkResultSet {
    type Meta = NetworkResultSetMetaData;

    fn next(&self) -> bool {
        *self.current_index.borrow_mut() += 1;
        self.count != -1 && *self.current_index.borrow() < self.count
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        let idx = *self.current_index.borrow();
        if let Some(m) = self.records.get(idx as usize) {
            if let Some(Value::Int32(v)) = m.get(fldname) {
                return Ok(*v);
            }
        }

        Err(From::from(ResultSetError::FieldNotFound(
            fldname.to_string(),
        )))
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        let idx = *self.current_index.borrow();
        if let Some(m) = self.records.get(idx as usize) {
            if let Some(Value::String(s)) = m.get(fldname) {
                return Ok(s.clone());
            }
        }

        Err(From::from(ResultSetError::FieldNotFound(
            fldname.to_string(),
        )))
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
        self.count = -1;
        *self.current_index.borrow_mut() = -1;
        self.records.clear();
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let request = self.client.close_request();
            request.send().promise.await.unwrap(); // TODO
        });

        Ok(())
    }
}
