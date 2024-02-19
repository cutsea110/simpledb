use anyhow::Result;
use env_logger::Env;
use log::info;
use std::{
    io::{stdout, Write},
    path::Path,
    process,
};
use structopt::{clap, StructOpt};

use simpledb::{
    rdbc::{
        connectionadapter::ConnectionAdapter,
        driveradapter::DriverAdapter,
        embedded::{connection::EmbeddedConnection, driver::EmbeddedDriver},
    },
    server::config::{self, SimpleDBConfig},
};

pub mod execquery;
pub mod explainplan;
pub mod metacmd;
pub mod tableschema;
pub mod updatecmd;
pub mod viewdef;

const DB_DIR: &str = "data";
const VERSION: &str = "0.1.0";

#[derive(Debug, StructOpt)]
#[structopt(setting(clap::AppSettings::ColoredHelp), rename_all = "kebab-case")]
struct Opt {
    #[structopt(short = "d", long = "name", default_value("demo"))]
    dbname: String,

    #[structopt(short = "V", long)]
    version: bool,

    #[structopt(long, default_value("400"))]
    block_size: i32,

    #[structopt(long, default_value("8"))]
    buffer_size: usize,

    #[structopt(long,
		default_value("LRU"),
		possible_values = &config::BufferMgr::variants(),
		case_insensitive = true)]
    buffer_manager: config::BufferMgr,

    #[structopt(long,
		default_value("Heuristic"),
		possible_values = &config::QueryPlanner::variants(),
		case_insensitive = true)]
    query_planner: config::QueryPlanner,
}

#[derive(Debug, Clone)]
struct Config {
    dbname: String,
    dbpath: String,
    version: bool,

    block_size: i32,
    buffer_size: usize,
    buffer_manager: config::BufferMgr,
    query_planner: config::QueryPlanner,
}

impl Config {
    pub fn new(opt: Opt) -> Self {
        Self {
            dbname: opt.dbname.clone(),
            dbpath: format!("{}/{}", DB_DIR, &opt.dbname),
            version: opt.version,

            block_size: opt.block_size,
            buffer_size: opt.buffer_size,
            buffer_manager: opt.buffer_manager,
            query_planner: opt.query_planner,
        }
    }
}

fn confirm_new_db(dbname: &str) {
    print!("create new '{}'? [Yes/no]> ", dbname);
    stdout().flush().expect("confirm");

    let mut yes_no = String::new();
    std::io::stdin().read_line(&mut yes_no).ok();
    let ans = yes_no.trim().to_ascii_lowercase();
    if ans != "y" && ans != "yes" {
        println!("terminates the process.");
        process::exit(0);
    }
}

fn read_query() -> Result<String> {
    print!("SQL> ");
    stdout().flush().expect("require input");

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input)
}

fn exec(conn: &mut EmbeddedConnection, qry: &str) {
    if qry.starts_with(":") {
        metacmd::exec_meta_cmd(conn, qry);
        return;
    }

    let mut stmt = conn.create_statement(&qry).expect("create statement");
    let words: Vec<&str> = qry.split_whitespace().collect();
    if !words.is_empty() {
        let cmd = words[0].trim().to_ascii_lowercase();
        if &cmd == "select" {
            execquery::exec_query(&mut stmt);
            println!();
        } else {
            updatecmd::exec_update_cmd(&mut stmt);
            println!();
        }
    }
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();

    let opt = Opt::from_args();
    let cfg = Config::new(opt);

    if cfg.version {
        println!("eSQL version {}", VERSION);
        process::exit(0);
    }

    if !Path::new(&cfg.dbpath).exists() {
        confirm_new_db(&cfg.dbname);
    }
    let db_config = SimpleDBConfig {
        block_size: cfg.block_size,
        num_of_buffers: cfg.buffer_size,
        buffer_manager: cfg.buffer_manager,
        query_planner: cfg.query_planner,
    };
    info!("database config:");
    info!("      block size: {}", db_config.block_size);
    info!("   num of buffer: {}", db_config.num_of_buffers);
    info!("  buffer manager: {:?}", db_config.buffer_manager);
    info!("   query planner: {:?}", db_config.query_planner);
    let drvr = EmbeddedDriver::new(db_config);
    let mut conn = drvr.connect(&cfg.dbpath).unwrap_or_else(|_| {
        println!("couldn't connect database.");
        process::exit(1);
    });

    while let Ok(qry) = read_query() {
        exec(&mut conn, &qry.trim());
    }
}
