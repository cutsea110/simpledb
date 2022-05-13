use anyhow::Result;

use super::connection::NetworkConnection;
use crate::{rdbc::driveradapter::DriverAdapter, remote_capnp};

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
}

impl<'a> DriverAdapter<'a> for NetworkDriver {
    type Con = NetworkConnection;

    fn connect(&self, dbname: &str) -> Result<Self::Con> {
        let mut request = self.driver.connect_request();
        request.get().set_dbname(dbname.into());
        let conn = request.send().pipeline.get_conn();

        Ok(Self::Con::new(conn))
    }
    fn get_major_version(&self) -> i32 {
        0
    }
    fn get_minor_version(&self) -> i32 {
        1
    }
}
