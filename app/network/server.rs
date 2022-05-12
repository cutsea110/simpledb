extern crate capnp_rpc;
extern crate simpledb;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use env_logger::Env;
use futures::{AsyncReadExt, FutureExt};
use log::info;
use std::{
    collections::HashMap,
    error::Error,
    net::{SocketAddr, ToSocketAddrs},
    sync::{Arc, Mutex},
};

use simpledb::{
    remote_capnp::remote_driver,
    server::{remote::RemoteDriverImpl, simpledb::SimpleDB},
};

const DB_DIR: &str = "data";

pub struct ServerImpl {
    dbs: HashMap<String, Arc<Mutex<SimpleDB>>>,
}
impl ServerImpl {
    pub fn new() -> Self {
        Self {
            dbs: HashMap::new(),
        }
    }
}
impl simpledb::server::remote::Server for ServerImpl {
    fn get_database(&mut self, dbname: &str) -> Arc<Mutex<SimpleDB>> {
        if !self.dbs.contains_key(dbname) {
            let db_path = format!("{}/{}", DB_DIR, dbname);
            let db = SimpleDB::new(&db_path).expect("new database");
            self.dbs
                .insert(dbname.to_string(), Arc::new(Mutex::new(db)));
        }

        Arc::clone(self.dbs.get(dbname).expect("get database"))
    }
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();

    // TODO: from argument
    // rdbc:simpledb://127.0.0.1:1099/dbname
    let addr = "127.0.0.1:1099"
        .to_socket_addrs()?
        .next()
        .expect("could not parse address");

    tokio::task::LocalSet::new().run_until(try_main(addr)).await
}

async fn try_main(addr: SocketAddr) -> Result<(), Box<dyn Error>> {
    info!("start server");
    let srv = ServerImpl::new();

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let driver_impl = RemoteDriverImpl::new(Arc::new(Mutex::new(srv)));
    let driver_client: remote_driver::Client = capnp_rpc::new_client(driver_impl);

    loop {
        info!("listening...");
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

        let rpc_system = RpcSystem::new(rpc_network, Some(driver_client.clone().client));

        tokio::task::spawn_local(Box::pin(rpc_system.map(|_| ())));
    }
}
