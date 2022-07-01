extern crate capnp_rpc;
extern crate simpledb;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use env_logger::Env;
use futures::{AsyncReadExt, FutureExt};
use log::{debug, info};
use std::{
    collections::HashMap,
    error::Error,
    net::{SocketAddr, ToSocketAddrs},
    sync::{Arc, Mutex},
};
use structopt::{clap, StructOpt};

use simpledb::{
    remote_capnp::remote_driver,
    server::{
        config::{self, SimpleDBConfig},
        remote::RemoteDriverImpl,
        simpledb::SimpleDB,
    },
};

const DB_DIR: &str = "data";

#[derive(Debug, StructOpt)]
#[structopt(setting(clap::AppSettings::ColoredHelp))]
struct Opt {
    #[structopt(short = "h", long = "host", default_value("127.0.0.1"))]
    host: String,

    #[structopt(short = "p", long = "port", default_value("1099"))]
    port: u16,

    #[structopt(long = "conf.block-size", default_value("400"))]
    block_size: i32,

    #[structopt(long = "conf.buffer-size", default_value("8"))]
    buffer_size: usize,

    #[structopt(long = "conf.buffer-manager", default_value("lru"))]
    buffer_manager: config::BufferMgr,

    #[structopt(long = "conf.query-planner", default_value("heuristic"))]
    query_planner: config::QueryPlanner,
}

#[derive(Debug, Clone)]
struct Config {
    pub addr: SocketAddr,

    block_size: i32,
    buffer_size: usize,
    buffer_manager: config::BufferMgr,
    query_planner: config::QueryPlanner,
}

impl Config {
    pub fn new(opt: Opt) -> Self {
        let addr = format!("{}:{}", opt.host, opt.port)
            .to_socket_addrs()
            .unwrap()
            .next()
            .expect("could not parse address");

        Self {
            addr,

            block_size: opt.block_size,
            buffer_size: opt.buffer_size,
            buffer_manager: opt.buffer_manager,
            query_planner: opt.query_planner,
        }
    }
}

pub struct ServerImpl {
    cfg: SimpleDBConfig,
    dbs: HashMap<String, Arc<Mutex<SimpleDB>>>,
}
impl ServerImpl {
    pub fn new(cfg: SimpleDBConfig) -> Self {
        Self {
            cfg,
            dbs: HashMap::new(),
        }
    }
}
impl simpledb::server::remote::Server for ServerImpl {
    fn get_database(&mut self, dbname: &str) -> Arc<Mutex<SimpleDB>> {
        if !self.dbs.contains_key(dbname) {
            let db_path = format!("{}/{}", DB_DIR, dbname);
            let db = SimpleDB::build_from(self.cfg.clone())(&db_path).expect("new database");
            self.dbs
                .insert(dbname.to_string(), Arc::new(Mutex::new(db)));
        }

        Arc::clone(self.dbs.get(dbname).expect("get database"))
    }
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();

    let opt = Opt::from_args();
    debug!("Opt: {:?}", opt);
    let cfg = Config::new(opt);
    debug!("Cofngi: {:?}", cfg);

    tokio::task::LocalSet::new().run_until(try_main(cfg)).await
}

async fn try_main(cfg: Config) -> Result<(), Box<dyn Error>> {
    info!("start server");
    info!("    _            _        _ _    ");
    info!(" __(_)_ __  _ __| |___ __| | |__ ");
    info!("(_-< | '  \\| '_ \\ / -_) _` | '_ \\");
    info!("/__/_|_|_|_| .__/_\\___\\__,_|_.__/");
    info!("           |_|                   ");
    info!("");

    let srv = ServerImpl::new(SimpleDBConfig {
        block_size: cfg.block_size,
        num_of_buffers: cfg.buffer_size,
        buffer_manager: cfg.buffer_manager,
        query_planner: cfg.query_planner,
    });

    let listener = tokio::net::TcpListener::bind(&cfg.addr).await?;
    let driver_impl = RemoteDriverImpl::new(Arc::new(Mutex::new(srv)));
    let driver_client: remote_driver::Client = capnp_rpc::new_client(driver_impl);

    loop {
        info!("listening...{}", &cfg.addr);
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
