use anyhow::Result;
use std::{collections::HashMap, sync::Arc};

use super::statement::NetworkStatement;
use crate::{
    rdbc::model::IndexInfo,
    record::schema::{FieldType, Schema},
    remote_capnp::{self, remote_connection},
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
        let reply = request.send().promise.await?;
        let tx_num = reply.get()?.get_tx();

        Ok(tx_num)
    }
    pub async fn rollback(&mut self) -> Result<i32> {
        let request = self.conn.rollback_request();
        let reply = request.send().promise.await?;
        let tx_num = reply.get()?.get_tx();

        Ok(tx_num)
    }
    pub async fn create_statement(&mut self, sql: &str) -> Result<NetworkStatement> {
        let mut request = self.conn.create_statement_request();
        request.get().set_sql(sql);
        let response = request.send().promise.await?;
        Ok(NetworkStatement::new(response.get()?.get_stmt()?))
    }
    pub async fn close(&mut self) -> Result<i32> {
        let response = self.conn.close_request().send().promise.await?;
        Ok(response.get()?.get_tx())
    }
    pub async fn get_table_schema(&self, tblname: &str) -> Result<Arc<Schema>> {
        let mut schema = Schema::new();

        let mut request = self.conn.get_table_schema_request();
        request.get().set_tblname(tblname);
        let reply = request.send().promise.await?;
        let sch = reply.get()?.get_sch()?;

        for column in sch.get_columns()? {
            let fldname = column.get_name()?.to_str()?;
            let field_type = match column.get_type()? {
                remote_capnp::FieldType::SmallInt => FieldType::SMALLINT,
                remote_capnp::FieldType::Integer => FieldType::INTEGER,
                remote_capnp::FieldType::Varchar => FieldType::VARCHAR,
                remote_capnp::FieldType::Bool => FieldType::BOOL,
                remote_capnp::FieldType::Date => FieldType::DATE,
            };
            schema.add_field(fldname, field_type, column.get_length() as usize);
        }

        Ok(Arc::new(schema))
    }
    pub async fn get_view_definition(&self, viewname: &str) -> Result<(String, String)> {
        let mut request = self.conn.get_view_definition_request();
        request.get().set_viewname(viewname);
        let reply = request.send().promise.await?;
        let viewdef = reply.get()?.get_vwdef()?;

        Ok((
            viewdef.reborrow().get_vwname()?.to_string().unwrap(),
            viewdef.reborrow().get_vwdef()?.to_string().unwrap(),
        ))
    }
    pub async fn get_index_info(&self, tblname: &str) -> Result<HashMap<String, IndexInfo>> {
        let mut map = HashMap::new();

        let mut request = self.conn.get_index_info_request();
        request.get().set_tblname(tblname);
        let reply = request.send().promise.await?;
        let indexes = reply.get()?.get_indexes()?;
        for index in indexes {
            let fldname = index.get_fldname()?.to_str()?;
            let idxname = index.get_idxname()?.to_str()?;
            let info = IndexInfo::new(fldname, idxname);
            map.insert(fldname.to_string(), info);
        }

        Ok(map)
    }

    // extends for statistics by exercise 3.15
    pub async fn numbers_of_read_written_blocks(&self) -> Result<(u32, u32)> {
        // for statistics
        let request = self.conn.nums_of_read_written_blocks_request();
        let reply = request.send().promise.await?;
        let r = reply.get()?.get_r();
        let w = reply.get()?.get_w();

        Ok((r, w))
    }
    // extends for statistics by exercise 4.18
    pub async fn numbers_of_total_pinned_unpinned(&self) -> Result<(u32, u32)> {
        // for statistics
        let request = self.conn.nums_of_total_pinned_unpinned_request();
        let reply = request.send().promise.await?;
        let pinned = reply.get()?.get_pinned();
        let unpinned = reply.get()?.get_unpinned();

        Ok((pinned, unpinned))
    }
    // extends for statistics by exercise 4.18
    pub async fn buffer_cache_hit_assigned(&self) -> Result<(u32, u32)> {
        let request = self.conn.buffer_cache_hit_assigned_request();
        let reply = request.send().promise.await?;
        let hit = reply.get()?.get_hit();
        let assigned = reply.get()?.get_assigned();

        Ok((hit, assigned))
    }
}
