use anyhow::Result;

use super::statement::NetworkStatement;
use crate::{rdbc::connectionadapter::ConnectionAdapter, remote_capnp};
use remote_capnp::remote_connection;

pub struct NetworkConnection {
    client: remote_connection::Client,
}
impl NetworkConnection {
    pub fn new(client: remote_connection::Client) -> Self {
        Self { client }
    }
    pub fn commit(&mut self) -> Result<()> {
        let rt = tokio::runtime::Runtime::new().unwrap(); // TODO
        rt.block_on(async {
            let request = self.client.commit_request();
            request.send().promise.await.unwrap(); // TODO
        });

        Ok(())
    }
    pub fn rollback(&mut self) -> Result<()> {
        let rt = tokio::runtime::Runtime::new().unwrap(); // TODO
        rt.block_on(async {
            let request = self.client.rollback_request();
            request.send().promise.await.unwrap(); // TODO
        });

        Ok(())
    }
}

impl<'a> ConnectionAdapter<'a> for NetworkConnection {
    type Stmt = NetworkStatement;

    fn create_statement(&'a mut self, sql: &str) -> Result<Self::Stmt> {
        let rt = tokio::runtime::Runtime::new().unwrap(); // TODO
        let stmt = rt.block_on(async {
            let mut request = self.client.create_statement_request();
            request
                .get()
                .set_sql(::capnp::text::new_reader(sql.as_bytes()).unwrap());
            request.send().pipeline.get_stmt()
        });

        Ok(NetworkStatement::new(stmt))
    }
    fn close(&mut self) -> Result<()> {
        let rt = tokio::runtime::Runtime::new().unwrap(); // TODO
        rt.block_on(async {
            let request = self.client.close_request();
            request.send().promise.await.unwrap(); // TODO
        });

        Ok(())
    }
}
