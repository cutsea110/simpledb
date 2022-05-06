use std::net::{SocketAddr, ToSocketAddrs};

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt};
use simpledb::{
    rdbc::{
        network::{metadata::NetworkResultSetMetaData, resultset::Value},
        resultsetmetadataadapter::{self, ResultSetMetaDataAdapter},
    },
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
        let ver_req = driver.get_version_request();
        let ver = ver_req.send().promise.await?;
        let major_ver = ver.get()?.get_ver()?.get_major_ver();
        let minor_ver = ver.get()?.get_ver()?.get_minor_ver();
        println!("simpledb server version {}.{}", major_ver, minor_ver);

        let mut conn_request = driver.connect_request();
        conn_request
            .get()
            .set_dbname(::capnp::text::new_reader("demo".as_bytes())?);
        let conn = conn_request.send().promise.await?.get()?.get_conn()?;
        let mut stmt_request = conn.create_statement_request();
        stmt_request.get().set_sql(::capnp::text::new_reader(
            "SELECT sid, sname, dname FROM student, dept WHERE did = major_id".as_bytes(),
        )?);
        let stmt = stmt_request.send().promise.await?.get()?.get_stmt()?;
        let query_request = stmt.execute_query_request();
        let result = query_request.send().promise.await?.get()?.get_result()?;

        let meta_request = result.get_metadata_request();
        let meta_reply = meta_request.send().promise.await?;
        let meta = meta_reply.get()?.get_metadata()?;

        let metadata = NetworkResultSetMetaData::from(meta);

        loop {
            let next_request = result.next_request();
            if !next_request.send().promise.await?.get()?.get_exists() {
                break;
            }

            for i in 0..metadata.get_column_count() {
                let fldname = metadata
                    .get_column_name(i)
                    .expect("get column name")
                    .as_str();
                let w = metadata
                    .get_column_display_size(i)
                    .expect("get column display size");
                match metadata.get_column_type(i).expect("get column type") {
                    resultsetmetadataadapter::DataType::Int32 => {
                        let mut req = result.get_int32_request();
                        req.get().set_fldname(fldname.into());
                        let val = req.send().promise.await?;
                        print!("{:width$} ", val.get()?.get_val(), width = w)
                    }
                    resultsetmetadataadapter::DataType::Varchar => {
                        let mut req = result.get_string_request();
                        req.get().set_fldname(fldname.into());
                        let val = req.send().promise.await?;
                        print!("{:width$} ", val.get()?.get_val()?, width = w)
                    }
                }
            }
            println!();
        }
    }

    Ok(())
}
