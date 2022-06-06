use anyhow::Result;
use env_logger::{self, Env};
use metacmd::exec_meta_cmd;
use std::{
    io::{stdout, Write},
    path::Path,
    process,
};
use structopt::{clap, StructOpt};

use simpledb::rdbc::{
    connectionadapter::ConnectionAdapter,
    driveradapter::DriverAdapter,
    embedded::{connection::EmbeddedConnection, driver::EmbeddedDriver},
};

use execquery::exec_query;
use updatecmd::exec_update_cmd;

pub mod execquery;
pub mod explainplan;
pub mod metacmd;
pub mod tableschema;
pub mod updatecmd;
pub mod viewdef;

const DB_DIR: &str = "data";
const VERSION: &str = "0.1.0";

#[derive(Debug, StructOpt)]
#[structopt(setting(clap::AppSettings::ColoredHelp))]
struct Opt {
    #[structopt(short = "d", long = "name", default_value("demo"))]
    dbname: String,

    #[structopt(short = "V", long = "version")]
    version: bool,
}

#[derive(Debug, Clone)]
struct Config {
    dbname: String,
    dbpath: String,
    version: bool,
}

impl Config {
    pub fn new(opt: Opt) -> Self {
        Self {
            dbname: opt.dbname.clone(),
            dbpath: format!("{}/{}", DB_DIR, &opt.dbname),
            version: opt.version,
        }
    }
}

fn read_query() -> Result<String> {
    print!("SQL> ");
    stdout().flush().expect("require input");

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input)
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

fn exec(conn: &mut EmbeddedConnection, qry: &str) {
    if qry.starts_with(":") {
        exec_meta_cmd(conn, qry);
        return;
    }

    let mut stmt = conn.create_statement(&qry).expect("create statement");
    let words: Vec<&str> = qry.split_whitespace().collect();
    if !words.is_empty() {
        let cmd = words[0].trim().to_ascii_lowercase();
        if &cmd == "select" {
            exec_query(&mut stmt);
            println!();
        } else {
            exec_update_cmd(&mut stmt);
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

    let drvr = EmbeddedDriver::new();
    let mut conn = drvr.connect(&cfg.dbpath).unwrap_or_else(|_| {
        println!("couldn't connect database.");
        process::exit(1);
    });

    while let Ok(qry) = read_query() {
        exec(&mut conn, &qry.trim());
    }
}
