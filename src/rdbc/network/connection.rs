use anyhow::Result;
use std::{collections::HashMap, sync::Arc, usize};

use super::{metadata::IndexInfo, statement::NetworkStatement};
use crate::{
    rdbc::connectionadapter::ConnectionAdapter,
    record::schema::{FieldType, Schema},
    remote_capnp::{self, remote_connection, void_box},
};

pub struct NetworkConnection {
    conn: remote_connection::Client,
}
impl NetworkConnection {
    pub fn new(conn: remote_connection::Client) -> Self {
        Self { conn }
    }
    pub async fn commit(&mut self) -> Result<i32> {
        let request = self.conn.commit_request();
        let reply = request.send().pipeline.get_tx();
        let request = reply.read_request();
        let reply = request.send().promise.await?;
        let tx_num = reply.get()?.get_tx_num();

        Ok(tx_num)
    }
    pub async fn rollback(&mut self) -> Result<i32> {
        let request = self.conn.rollback_request();
        let reply = request.send().pipeline.get_tx();
        let request = reply.read_request();
        let reply = request.send().promise.await?;
        let tx_num = reply.get()?.get_tx_num();

        Ok(tx_num)
    }
    pub async fn get_table_schema(&self, tblname: &str) -> Result<Arc<Schema>> {
        let mut schema = Schema::new();

        let mut request = self.conn.get_table_schema_request();
        request.get().set_tblname(tblname.into());
        let reply = request.send().promise.await?;
        let sch = reply.get()?.get_sch()?;

        let mut map = HashMap::new();
        let entries = sch.get_info()?.get_entries()?;
        for i in 0..entries.len() {
            let entry = entries.get(i as u32);
            let fldname = entry.get_key()?;
            let val = entry.get_value()?;
            match val.get_type()? {
                remote_capnp::FieldType::Integer => {
                    map.insert(fldname, (FieldType::INTEGER, val.get_length()));
                }
                remote_capnp::FieldType::Varchar => {
                    map.insert(fldname, (FieldType::VARCHAR, val.get_length()));
                }
            }
        }
        let fields = sch.get_fields()?;
        for i in 0..fields.len() {
            let fldname = fields.get(i as u32)?;
            if let Some((t, l)) = map.get(fldname) {
                schema.add_field(fldname, t.clone(), *l as usize);
            }
        }

        Ok(Arc::new(schema))
    }
    pub async fn get_view_definition(&self, viewname: &str) -> Result<(String, String)> {
        let mut request = self.conn.get_view_definition_request();
        request.get().set_viewname(viewname.into());
        let reply = request.send().promise.await?;
        let viewdef = reply.get()?.get_vwdef()?;

        Ok((
            viewdef.reborrow().get_vwname()?.to_string(),
            viewdef.reborrow().get_vwdef()?.to_string(),
        ))
    }
    pub async fn get_index_info(&self, tblname: &str) -> Result<HashMap<String, IndexInfo>> {
        let mut map = HashMap::new();

        let mut request = self.conn.get_index_info_request();
        request.get().set_tblname(tblname.into());
        let reply = request.send().promise.await?;
        let ii = reply.get()?.get_ii()?;
        let entries = ii.get_entries()?;
        for i in 0..entries.len() {
            let val = entries.get(i as u32).get_value()?;
            let fldname = val.get_fldname()?;
            let idxname = val.get_idxname()?;
            let info = IndexInfo::new(fldname, idxname);
            map.insert(fldname.to_string(), info);
        }

        Ok(map)
    }
}

pub struct ResponseImpl {
    client: void_box::Client,
}
impl ResponseImpl {
    pub fn new(client: void_box::Client) -> Self {
        Self { client }
    }
    pub async fn response(&self) -> Result<()> {
        let request = self.client.read_request();
        request.send().promise.await?.get()?.get_void();

        Ok(())
    }
}

impl<'a> ConnectionAdapter<'a> for NetworkConnection {
    type Stmt = NetworkStatement;
    type Res = ResponseImpl;

    fn create_statement(&'a mut self, sql: &str) -> Result<Self::Stmt> {
        let mut request = self.conn.create_statement_request();
        request.get().set_sql(sql.into());
        let stmt = request.send().pipeline.get_stmt();

        Ok(Self::Stmt::new(stmt))
    }
    fn close(&mut self) -> Result<Self::Res> {
        let request = self.conn.close_request();
        let res = request.send().pipeline.get_res();

        Ok(ResponseImpl::new(res))
    }
}
