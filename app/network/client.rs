use std::net::{SocketAddr, ToSocketAddrs};

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt};
use simpledb::{
    rdbc::network::resultset::Value,
    remote_capnp::{remote_driver, remote_result_set},
};

#[macro_use]
extern crate capnp_rpc;
extern crate simpledb;

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:1099"
        .to_socket_addrs()?
        .next()
        .expect("could not parse address");

    tokio::task::LocalSet::new().run_until(try_main(addr)).await
}

async fn try_main(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
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

    // TODO
    {
        let mut conn_request = driver.connect_request();
        conn_request
            .get()
            .set_dbname(::capnp::text::new_reader("demo".as_bytes())?);
        let conn = conn_request.send().promise.await?.get()?.get_conn()?;
        let mut stmt_request = conn.create_statement_request();
        stmt_request.get().set_sql(::capnp::text::new_reader(
            "SELECT sid, sname FROM student".as_bytes(),
        )?);
        let stmt = stmt_request.send().promise.await?.get()?.get_stmt()?;
        let query_request = stmt.execute_query_request();
        let result = query_request.send().promise.await?.get()?.get_result()?;
        loop {
            let next_request = result.next_request();
            if !next_request.send().promise.await?.get()?.get_exists() {
                break;
            }
            let request = result.get_next_record_request();
            let reply = request.send().promise.await?;
            let record = reply.get()?.get_record()?;
            let entries = record.get_map()?.get_entries()?;
            for kv in entries.into_iter() {
                let key = kv.get_key()?.to_string();
                let val = kv.get_value()?;
                let val = match val.which()? {
                    remote_result_set::value::Int32(v) => Value::Int32(v),
                    remote_result_set::value::String(s) => Value::String(s?.to_string()),
                };
                println!("{} = {:?}", key, val)
            }
        }
    }

    Ok(())
}
