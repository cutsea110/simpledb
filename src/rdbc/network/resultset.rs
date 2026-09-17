use anyhow::{anyhow, Result};
use chrono::NaiveDate;
use log::trace;

use super::metadata::NetworkResultSetMetaData;
use crate::{rdbc::model::Schema, remote_capnp::remote_result_set};

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
    metadata: NetworkResultSetMetaData,
}

impl NetworkResultSet {
    pub fn new(resultset: remote_result_set::Client, schema: Schema) -> Self {
        Self {
            resultset,
            metadata: NetworkResultSetMetaData::new(schema),
        }
    }

    pub fn metadata(&self) -> &NetworkResultSetMetaData {
        &self.metadata
    }

    pub async fn get_rows(&self, limit: u16) -> Result<Vec<Vec<Value>>> {
        trace!("get_rows limit: {}", limit);
        let mut request = self.resultset.get_rows_request();
        request.get().set_limit(limit);
        let response = request.send().promise.await?;
        let response = response.get()?;
        let rows = response.get_rows()?;

        let mut result = Vec::with_capacity(rows.len() as usize);
        for row in rows {
            let values = row.get_values()?;
            if values.len() as usize != self.metadata.column_count() {
                return Err(anyhow!(
                    "row has {} values but schema has {} columns",
                    values.len(),
                    self.metadata.column_count()
                ));
            }

            let mut decoded = Vec::with_capacity(values.len() as usize);
            for value in values {
                let value = match value.which()? {
                    remote_result_set::value::Int16(value) => Value::Int16(value),
                    remote_result_set::value::Int32(value) => Value::Int32(value),
                    remote_result_set::value::String(value) => Value::String(value?.to_string()?),
                    remote_result_set::value::Bool(value) => Value::Bool(value),
                    remote_result_set::value::Date(value) => {
                        let value = value?;
                        let date = NaiveDate::from_ymd_opt(
                            value.get_year() as i32,
                            value.get_month() as u32,
                            value.get_day() as u32,
                        )
                        .ok_or_else(|| anyhow!("server returned an invalid date"))?;
                        Value::Date(date)
                    }
                };
                decoded.push(value);
            }
            result.push(decoded);
        }

        Ok(result)
    }

    pub async fn close(&mut self) -> Result<i32> {
        let response = self.resultset.close_request().send().promise.await?;
        Ok(response.get()?.get_tx())
    }
}
