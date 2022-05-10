use std::{
    collections::HashMap,
    net::{SocketAddr, ToSocketAddrs},
};

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt};
use simpledb::{
    rdbc::{
        network::{metadata::NetworkResultSetMetaData, resultset::Value},
        resultsetmetadataadapter::{self, ResultSetMetaDataAdapter},
    },
    record::schema::FieldType,
    remote_capnp::{self, remote_driver, remote_result_set},
};

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

async fn get_server_version(
    driver: &remote_driver::Client,
) -> Result<(i32, i32), Box<dyn std::error::Error>> {
    let request = driver.get_version_request();
    let ver = request.send().promise.await?;
    let major_ver = ver.get()?.get_ver()?.get_major_ver();
    let minor_ver = ver.get()?.get_ver()?.get_minor_ver();

    Ok((major_ver, minor_ver))
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

    // Query sample
    {
        if let Ok((major_ver, minor_ver)) = get_server_version(&driver).await {
            println!("simpledb server version {}.{}\n", major_ver, minor_ver);
        }

        let mut conn_request = driver.connect_request();
        conn_request
            .get()
            .set_dbname(::capnp::text::new_reader("demo".as_bytes())?);
        let conn = conn_request.send().pipeline.get_conn();

        // table schema
        let mut req = conn.get_table_schema_request();
        req.get().set_tblname("student".into());
        let reply = req.send().promise.await?;
        let tblsch = reply.get()?.get_sch()?;
        let fields = tblsch.get_fields()?;
        let info = tblsch.get_info()?;
        let entries = info.get_entries()?;
        let mut map = HashMap::new();
        for i in 0..entries.len() {
            let entry = entries.get(i as u32);
            let fldname = entry.get_key()?;
            let val = entry.get_value()?;
            match val.get_type()? {
                remote_capnp::FieldType::Integer => {
                    map.insert(fldname.to_string(), (FieldType::INTEGER, 0));
                }
                remote_capnp::FieldType::Varchar => {
                    map.insert(fldname.to_string(), (FieldType::VARCHAR, val.get_length()));
                }
            }
        }
        for i in 0..fields.len() {
            let fldname = fields.get(i as u32)?;
            if let Some((t, n)) = map.get(fldname) {
                match t {
                    FieldType::INTEGER => {
                        println!("{:10} {:10}", fldname, "INT32");
                    }
                    FieldType::VARCHAR => {
                        println!("{:10} {:10}", fldname, format!("VARCHAR({})", n));
                    }
                }
            }
        }
        println!();
        // index info
        let mut req = conn.get_index_info_request();
        req.get().set_tblname("student".into());
        let reply = req.send().promise.await?;
        let ii = reply.get()?.get_ii()?;
        let entries = ii.get_entries()?;
        for i in 0..entries.len() {
            let entry = entries.get(i as u32);
            let val = entry.get_value()?;
            let fldname = val.get_fldname()?;
            let idxname = val.get_idxname()?;
            println!("{:20} {:10}", idxname, fldname)
        }
        println!();

        // view definition
        let mut view_request = conn.get_view_definition_request();
        view_request.get().set_viewname("einstein".into());
        let reply = view_request.send().promise.await?;
        let viewdef = reply.get()?.get_vwdef()?;
        let vwname = viewdef.reborrow().get_vwname()?;
        let vwdef = viewdef.reborrow().get_vwdef()?;
        println!("view name: {}", vwname);
        println!("view def:  {}", vwdef);
        println!();

        let mut cmd_request = conn.create_statement_request();
        cmd_request.get().set_sql(::capnp::text::new_reader(
            "UPDATE student SET grad_year=2020 WHERE grad_year=2024".as_bytes(),
        )?);
        let stmt = cmd_request.send().pipeline.get_stmt();
        let update_request = stmt.execute_update_request();
        let affected = update_request.send().promise.await?.get()?.get_affected();
        println!("Affected: {} rows", affected);

        // let commit_request = conn.commit_request();
        // commit_request.send().promise.await?;

        let mut stmt_request = conn.create_statement_request();
        stmt_request.get().set_sql(
            "SELECT sid, sname, dname, grad_year FROM student, dept WHERE did = major_id".into(),
        );
        let stmt = stmt_request.send().pipeline.get_stmt();
        let query_request = stmt.execute_query_request();
        let result = query_request.send().pipeline.get_result();

        let meta_request = result.get_metadata_request();
        let meta_reply = meta_request.send().promise.await?;
        let meta = meta_reply.get()?.get_metadata()?;
        let metadata = NetworkResultSetMetaData::from(meta);

        for i in 0..metadata.get_column_count() {
            let fldname = metadata
                .get_column_name(i)
                .expect("get column name")
                .as_str();
            let w = metadata
                .get_column_display_size(i)
                .expect("get column display size");
            print!("{:width$} ", fldname, width = w);
        }
        println!();
        for i in 0..metadata.get_column_count() {
            let w = metadata
                .get_column_display_size(i)
                .expect("get column display size");
            print!("{:-<width$}", "", width = w + 1);
        }
        println!();

        while result
            .next_request()
            .send()
            .promise
            .await?
            .get()?
            .get_exists()
        {
            let row_request = result.get_row_request();
            let row_reply = row_request.send().promise.await?;
            let row = row_reply.get()?.get_row()?;
            let entry = to_hashmap(row);

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
                        if let Some(Value::Int32(v)) = entry.get(fldname) {
                            print!("{:width$} ", v, width = w);
                        }
                    }
                    resultsetmetadataadapter::DataType::Varchar => {
                        if let Some(Value::String(s)) = entry.get(fldname) {
                            print!("{:width$} ", s, width = w);
                        }
                    }
                }
            }
            println!();
        }

        let rollback_request = conn.rollback_request();
        rollback_request.send().promise.await?;
    }

    Ok(())
}

fn to_hashmap(row: remote_result_set::row::Reader) -> HashMap<&str, Value> {
    let entries = row.get_map().unwrap().get_entries().unwrap(); // TODO
    let mut result = HashMap::new();
    for kv in entries.into_iter() {
        let key = kv.get_key().unwrap(); // TODO
        let val = match kv.get_value().unwrap().which().unwrap() {
            remote_result_set::value::Int32(v) => Value::Int32(v),
            remote_result_set::value::String(s) => Value::String(s.unwrap().to_string()),
        };

        result.insert(key, val);
    }

    result
}
