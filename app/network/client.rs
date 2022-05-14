extern crate capnp_rpc;
extern crate simpledb;

use anyhow::Result;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use env_logger::Env;
use futures::{AsyncReadExt, FutureExt};
use itertools::Itertools;
use log::debug;
use std::{
    io::{stdout, Write},
    net::{SocketAddr, ToSocketAddrs},
    process,
};
use structopt::StructOpt;

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
pub mod tableschema;
pub mod updatecmd;
pub mod viewdef;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short = "h", long = "host", default_value("127.0.0.1"))]
    host: String,

    #[structopt(short = "p", long = "port", default_value("1099"))]
    port: u16,

    #[structopt(short = "d", long = "name")]
    dbname: String,
}

#[derive(Debug, Clone)]
struct Config {
    pub addr: SocketAddr,
    pub dbname: String,
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
        }
    }
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();

    let opt = Opt::from_args();
    debug!("Opt: {:?}", opt);
    let cfg = Config::new(opt);
    debug!("Config: {:?}", cfg);

    tokio::task::LocalSet::new().run_until(try_main(cfg)).await
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

fn read_query(cfg: &Config) -> Result<String> {
    print!("{}({})> ", &cfg.addr, &cfg.dbname);
    stdout().flush().expect("require input");

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input)
}

fn print_help_meta_cmd() {
    println!(":h, :help                       Show this help");
    println!(":q, :quit, :exit                Quit the program");
    println!(":t, :table   <table_name>       Show table schema");
    println!(":v, :view    <view_name>        Show view definition");
    println!(":e, :explain <sql>              Explain plan");
}

async fn exec_meta_cmd(conn: &mut NetworkConnection, qry: &str) {
    let tokens: Vec<&str> = qry.trim().split_whitespace().collect_vec();
    let cmd = tokens[0].to_ascii_lowercase();
    let args = &tokens[1..];
    match cmd.as_str() {
        ":h" | ":help" => {
            print_help_meta_cmd();
        }
        ":q" | ":quit" | ":exit" => {
            conn.close().unwrap().response().await.expect("close");
            println!("disconnected");
            process::exit(0);
        }
        ":t" | ":table" => {
            if args.is_empty() {
                println!("table name is required.");
                return;
            }
            let tblname = args[0];
            if let Ok(sch) = conn.get_table_schema(tblname).await {
                let idx_info = conn.get_index_info(tblname).await.unwrap_or_default();
                tableschema::print_table_schema(tblname, sch, idx_info);
            }
            return;
        }
        ":v" | ":view" => {
            if args.is_empty() {
                println!("view name is required.");
                return;
            }
            let viewname = args[0];
            if let Ok((viewname, viewdef)) = conn.get_view_definition(viewname).await {
                viewdef::print_view_definition(&viewname, &viewdef);
            }
            return;
        }
        ":e" | ":explain" => {
            if args.is_empty() {
                println!("SQL is required.");
                return;
            }
            let sql = qry[tokens[0].len()..].trim();
            let mut stmt = conn.create_statement(sql).expect("create statement");
            let words: Vec<&str> = sql.split_whitespace().collect();
            if !words.is_empty() {
                let cmd = words[0].trim().to_ascii_lowercase();
                if &cmd == "select" {
                    if let Ok(plan_repr) = stmt.explain_plan().await {
                        explainplan::print_explain_plan(plan_repr);
                        return;
                    }
                }
            }
            println!("expect query(not command).");
        }
        cmd => {
            println!("Unknown command: {}", cmd);
        }
    }
}

async fn exec(conn: &mut NetworkConnection, qry: &str) {
    if qry.starts_with(":") {
        exec_meta_cmd(conn, qry).await;
        return;
    }

    let mut stmt = conn.create_statement(&qry).expect("create statement");
    let words: Vec<&str> = qry.split_whitespace().collect();
    if !words.is_empty() {
        let cmd = words[0].trim().to_ascii_lowercase();
        if &cmd == "select" {
            execquery::exec_query(&mut stmt).await;
        } else {
            updatecmd::exec_update_cmd(&mut stmt).await;
        }
    }
}
