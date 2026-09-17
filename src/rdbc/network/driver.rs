use anyhow::Result;

use super::connection::NetworkConnection;
use crate::remote_capnp;

pub struct NetworkDriver {
    driver: remote_capnp::remote_driver::Client,
}

impl NetworkDriver {
    pub async fn new(driver: remote_capnp::remote_driver::Client) -> Self {
        Self { driver }
    }
    pub async fn get_server_version(&self) -> Result<(i32, i32)> {
        let request = self.driver.get_version_request();
        let reply = request.send().promise.await?;
        let ver = reply.get()?.get_ver()?;

        Ok((ver.get_major_ver(), ver.get_minor_ver()))
    }
    pub async fn connect(&self, dbname: &str) -> Result<NetworkConnection> {
        let mut request = self.driver.connect_request();
        request.get().set_dbname(dbname);
        let response = request.send().promise.await?;
        Ok(NetworkConnection::new(response.get()?.get_conn()?))
    }
}
