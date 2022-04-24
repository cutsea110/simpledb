use capnp::capability::Promise;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt};
use std::{error::Error, net::ToSocketAddrs};

use crate::test_capnp::ping;

struct PingImpl;

impl ping::Server for PingImpl {
    fn ping(
        &mut self,
        params: ping::PingParams,
        mut results: ping::PingResults,
    ) -> Promise<(), capnp::Error> {
        let request = params.get().unwrap().get_request().unwrap();
        let name = request.get_name().unwrap();
        let message = format!("Hello, {}", name);

        println!("received: {}", message);
        results.get().init_reply().set_message(&message);

        Promise::ok(())
    }
}

pub async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = ::std::env::args().collect();
    if args.len() != 3 {
        println!("usage: {} server ADDRESS[:PORT]", args[0]);
    }

    let addr = args[2]
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("could not parse address");

    tokio::task::LocalSet::new()
        .run_until(async move {
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            let ping_client: ping::Client = capnp_rpc::new_client(PingImpl);

            loop {
                let (stream, _) = listener.accept().await?;
                stream.set_nodelay(true)?;
                let (reader, writer) =
                    tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
                let network = twoparty::VatNetwork::new(
                    reader,
                    writer,
                    rpc_twoparty_capnp::Side::Server,
                    Default::default(),
                );

                let rpc_system =
                    RpcSystem::new(Box::new(network), Some(ping_client.clone().client));
                tokio::task::spawn_local(Box::pin(rpc_system.map(|_| ())));
            }
        })
        .await
}
