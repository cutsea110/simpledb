use std::net::SocketAddr;

use anyhow::Result;
use capnp_rpc::{
    rpc_twoparty_capnp::{self, Side},
    twoparty, RpcSystem,
};
use futures::AsyncReadExt;

use super::connection::NetworkConnection;
use crate::{rdbc::driveradapter::DriverAdapter, remote_capnp};

pub struct NetworkDriver {
    driver: remote_capnp::remote_driver::Client,
    rpc_system: RpcSystem<Side>,
}

impl NetworkDriver {
    pub async fn new(addr: &SocketAddr) -> Self {
        let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        stream.set_nodelay(true).expect("set nodelay");
        let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
        let rpc_network = Box::new(twoparty::VatNetwork::new(
            reader,
            writer,
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        ));
        let mut rpc_system = RpcSystem::new(rpc_network, None);
        let driver: remote_capnp::remote_driver::Client =
            rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

        Self { driver, rpc_system }
    }
    pub async fn get_server_version(&self) -> Result<(i32, i32)> {
        let request = self.driver.get_version_request();
        let reply = request.send().promise.await?;
        let ver = reply.get()?.get_ver()?;

        Ok((ver.get_major_ver(), ver.get_minor_ver()))
    }
    pub fn rpc_system(&self) -> &RpcSystem<Side> {
        &self.rpc_system
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
