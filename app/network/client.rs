pub use capnp_rpc;
pub use simpledb;

use anyhow::Result;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use core::fmt;
use env_logger::Env;
use futures::{AsyncReadExt, FutureExt};
use log::debug;
use std::{
    io::{stdout, Write},
    net::{SocketAddr, ToSocketAddrs},
    process,
};
use structopt::{clap, StructOpt};

use simpledb::{
    rdbc::{
        connectionadapter::ConnectionAdapter,
        driveradapter::DriverAdapter,
        network::{connection::NetworkConnection, driver::NetworkDriver},
    },
    remote_capnp::remote_driver,
};

pub mod execquery;
pub mod explainplan;
pub mod metacmd;
pub mod tableschema;
pub mod updatecmd;
pub mod viewdef;

#[derive(Debug, Clone)]
pub enum ClientError {
    Remote(String),
}
impl std::error::Error for ClientError {}
impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ClientError::Remote(msg) => write!(f, "remote server failed: {}", msg),
        }
    }
}

const VERSION: &str = "0.1.0";

#[derive(Debug, StructOpt)]
#[structopt(setting(clap::AppSettings::ColoredHelp))]
struct Opt {
    #[structopt(short = "h", long = "host", default_value("127.0.0.1"))]
    host: String,

    #[structopt(short = "p", long = "port", default_value("1099"))]
    port: u16,

    #[structopt(short = "d", long = "name", default_value("demo"))]
    dbname: String,

    #[structopt(short = "V", long = "version")]
    version: bool,
}

#[derive(Debug, Clone)]
struct Config {
    addr: SocketAddr,
    dbname: String,
    version: bool,
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
            dbname: opt.dbname,
            version: opt.version,
        }
    }
}

fn read_query(cfg: &Config) -> Result<String> {
    print!("{}({})> ", &cfg.addr, &cfg.dbname);
    stdout().flush().expect("require input");

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input)
}

async fn exec(conn: &mut NetworkConnection, qry: &str) {
    if qry.starts_with(":") {
        metacmd::exec_meta_cmd(conn, qry).await;
        return;
    }

    let mut stmt = conn.create_statement(&qry).expect("create statement");
    let words: Vec<&str> = qry.split_whitespace().collect();
    if !words.is_empty() {
        let cmd = words[0].trim().to_ascii_lowercase();
        if &cmd == "select" {
            execquery::exec_query(&mut stmt).await;
            println!();
        } else {
            updatecmd::exec_update_cmd(&mut stmt).await;
            println!();
        }
    }
}

async fn try_main<'a>(cfg: Config) -> Result<(), Box<dyn std::error::Error>> {
    let stream = tokio::net::TcpStream::connect(&cfg.addr).await?;
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

    let driver = NetworkDriver::new(driver).await;
    if let Ok((major_ver, minor_ver)) = driver.get_server_version().await {
        println!("simpledb server version {}.{}\n", major_ver, minor_ver);
    }
    let mut conn = driver.connect(&cfg.dbname).unwrap_or_else(|_| {
        println!("couldn't connect database.");
        process::exit(1);
    });

    while let Ok(qry) = read_query(&cfg) {
        exec(&mut conn, &qry.trim()).await;
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();

    let opt = Opt::from_args();
    debug!("Opt: {:?}", opt);
    let cfg = Config::new(opt);
    debug!("Config: {:?}", cfg);

    if cfg.version {
        println!("rSQL version {}", VERSION);
        process::exit(0);
    }

    tokio::task::LocalSet::new().run_until(try_main(cfg)).await
}
