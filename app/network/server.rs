#[macro_use]
extern crate capnp_rpc;
extern crate simpledb;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use env_logger::Env;
use futures::{AsyncReadExt, FutureExt};
use log::trace;
use std::{
    error::Error,
    net::{SocketAddr, ToSocketAddrs},
};

use simpledb::{remote_capnp::remote_driver, server::remote::RemoteDriverImpl};

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();

    // TODO: from argument
    // rdbc:simpledb://127.0.0.1:1099/dbname
    let addr = "127.0.0.1:1099"
        .to_socket_addrs()?
        .next()
        .expect("could not parse address");
    let dbname = "demo";

    tokio::task::LocalSet::new()
        .run_until(try_main(addr, dbname))
        .await
}

async fn try_main(addr: SocketAddr, dbname: &str) -> Result<(), Box<dyn Error>> {
    trace!("start server");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let driver_impl = RemoteDriverImpl::new();
    let driver_client: remote_driver::Client = capnp_rpc::new_client(driver_impl);

    loop {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
        let rpc_network = Box::new(twoparty::VatNetwork::new(
            reader,
            writer,
            rpc_twoparty_capnp::Side::Server,
            Default::default(),
        ));

        let rpc_system = RpcSystem::new(rpc_network, Some(driver_client.clone().client));

        tokio::task::spawn_local(Box::pin(rpc_system.map(|_| ())));
    }
}
