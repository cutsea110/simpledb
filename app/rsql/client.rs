use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt};
use std::{error::Error, net::ToSocketAddrs};

use crate::test_capnp::ping;

pub async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = ::std::env::args().collect();
    if args.len() != 4 {
        println!("usage: {} client HOST:PORT MESSAGE", args[0]);
        return Ok(());
    }

    let addr = args[2]
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("could not parse address");
    let msg = args[3].to_string();

    tokio::task::LocalSet::new()
        .run_until(async move {
            let stream = tokio::net::TcpStream::connect(&addr).await?;
            stream.set_nodelay(true)?;
            let (reader, writer) =
                tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
            let rpc_network = Box::new(twoparty::VatNetwork::new(
                reader,
                writer,
                rpc_twoparty_capnp::Side::Client,
                Default::default(),
            ));
            let mut rpc_system = RpcSystem::new(rpc_network, None);
            let ping_client: ping::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

            tokio::task::spawn_local(Box::pin(rpc_system.map(|_| ())));

            let mut request = ping_client.ping_request();
            request.get().init_request().set_name(&msg);

            let reply = request.send().promise.await.unwrap();

            println!(
                "received: {}",
                reply
                    .get()
                    .unwrap()
                    .get_reply()
                    .unwrap()
                    .get_message()
                    .unwrap()
            );

            Ok(())
        })
        .await
}
