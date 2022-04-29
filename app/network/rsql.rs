use capnp::capability::Promise;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncBufReadExt, AsyncReadExt, FutureExt};
use log::trace;
use std::{
    error::Error,
    net::{SocketAddr, ToSocketAddrs},
};

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
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = "127.0.0.1:4000"
        .to_socket_addrs()?
        .next()
        .expect("could not parse address");
    let conn_str = "rdbc://username:password@simpledb/demo"; // demo is sample database name

    tokio::task::LocalSet::new()
        .run_until(try_main(addr, conn_str))
        .await
}

async fn try_main(addr: SocketAddr, conn_str: &str) -> Result<(), Box<dyn Error>> {
    trace!("start");
    let stream = tokio::net::TcpStream::connect(&addr).await?;
    stream.set_nodelay(true)?;
    let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
    let rpc_network = Box::new(twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));
    let mut rpc_system = RpcSystem::new(rpc_network, None);
    let driver: remote_driver::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

    tokio::task::spawn_local(Box::pin(rpc_system.map(|_| ())));

    {
        let mut request = driver.connect_request();
        request.get().set_conn_string(conn_str);
        let reply = request.send().promise.await?;
        let request = reply.get()?.get_conn()?.connection_id_request();
        let conn_id = request.send().promise.await?.get()?.get_conn_id();
        println!("received: {}", conn_id);
    }

    Ok(())
}
