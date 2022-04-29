use capnp::capability::Promise;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt};
use log::{info, trace};
use std::net::{SocketAddr, ToSocketAddrs};

use env_logger::Env;

#[macro_use]
extern crate capnp_rpc;

pub mod remote_capnp {
    include!(concat!(
        env!("OUT_DIR"),
        "/src/rdbc/network/remote_capnp.rs"
    ));
}
use remote_capnp::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();
    let addr = "127.0.0.1:4000"
        .to_socket_addrs()?
        .next()
        .expect("could not parse address");

    tokio::task::LocalSet::new().run_until(try_main(addr)).await
}

async fn try_main(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    trace!("start server");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let driver_impl = RemoteDriverImpl::new();
    let driver: remote_driver::Client = capnp_rpc::new_client(driver_impl);

    loop {
        trace!("listening...");
        let (stream, _) = listener.accept().await?;
        info!("accepted");
        stream.set_nodelay(true)?;
        let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
        let rpc_network = Box::new(twoparty::VatNetwork::new(
            reader,
            writer,
            rpc_twoparty_capnp::Side::Server,
            Default::default(),
        ));
        let rpc_system = RpcSystem::new(rpc_network, Some(driver.clone().client));

        tokio::task::spawn_local(Box::pin(rpc_system.map(|_| ())));
    }
}

///////////////////////////
pub struct RemoteDriverImpl;
impl RemoteDriverImpl {
    pub fn new() -> Self {
        Self
    }
}
impl remote_capnp::remote_driver::Server for RemoteDriverImpl {
    fn connect(
        &mut self,
        params: remote_driver::ConnectParams,
        mut results: remote_driver::ConnectResults,
    ) -> Promise<(), capnp::Error> {
        let conn_str = pry!(pry!(params.get()).get_conn_string());
        info!("receive conn_str: {}", conn_str);
        let conn: remote_connection::Client = capnp_rpc::new_client(RemoteConnectionImpl::new());
        results.get().set_conn(conn);
        Promise::ok(())
    }
}

pub struct RemoteConnectionImpl {
    id: u32,
}
impl RemoteConnectionImpl {
    pub fn new() -> Self {
        Self { id: 0 }
    }
}
impl remote_capnp::remote_connection::Server for RemoteConnectionImpl {
    fn connection_id(
        &mut self,
        _: remote_connection::ConnectionIdParams,
        mut results: remote_connection::ConnectionIdResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_conn_id(self.id);
        Promise::ok(())
    }
}
