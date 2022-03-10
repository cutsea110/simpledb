use anyhow::Result;
use std::{
    env,
    io::{stdout, Write},
    path::Path,
    process,
};

use getopts::Options;
use simpledb::rdbc::{
    connectionadapter::ConnectionAdapter,
    driveradapter::DriverAdapter,
    embedded::{
        connection::EmbeddedConnection, driver::EmbeddedDriver, resultset::EmbeddedResultSet,
    },
    resultsetadapter::ResultSetAdapter,
    resultsetmetadataadapter::{DataType, ResultSetMetaDataAdapter},
    statementadapter::StatementAdapter,
};

#[derive(Debug)]
struct Args {
    dbname: String,
}

fn print_usage(program: &str, opts: &Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
    stdout().flush().expect("display usage");
    process::exit(0);
}

fn parse_args() -> Args {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("d", "dbname", "set database name", "DBNAME");
    opts.optflag("h", "help", "print this help menu");

    let matches = opts
        .parse(&args[1..])
        .unwrap_or_else(|f| panic!("{}", f.to_string()));
    if matches.opt_present("h") {
        print_usage(&program, &opts);
    }

    Args {
        dbname: matches.opt_str("d").expect("require dbname"),
    }
}

fn read_query(conn: &EmbeddedConnection) -> Result<String> {
    print!("SQL {}> ", conn.get_transaction().lock().unwrap().tx_num());
    stdout().flush().expect("require input");

    let mut input = String::new();
    std::io::stdin().read_line(&mut input).ok();
    Ok(input)
}

fn print_result_set(mut results: EmbeddedResultSet) -> Result<()> {
    // resultset metadata
    let meta = results.get_meta_data()?;
    // print header
    for i in 0..meta.get_column_count() {
        let name = meta.get_column_name(i).expect("get column name");
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        print!("{:width$} ", name, width = w);
    }
    println!("");
    // separater
    for i in 0..meta.get_column_count() {
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        print!("{:-<width$}", "", width = w + 1);
    }
    println!("");
    // scan record
    let mut c = 0;
    while results.next() {
        c += 1;
        for i in 0..meta.get_column_count() {
            let fldname = meta.get_column_name(i).expect("get column name");
            let w = meta
                .get_column_display_size(i)
                .expect("get column display size");
            match meta.get_column_type(i).expect("get column type") {
                DataType::Int32 => {
                    print!("{:width$} ", results.get_i32(fldname)?, width = w);
                }
                DataType::Varchar => {
                    print!("{:width$} ", results.get_string(fldname)?, width = w);
                }
            }
        }
        println!("");
    }
    println!("({} Rows)", c);

    Ok(())
}

fn main() {
    let args = parse_args();
    let dbpath = format!("db/{}", args.dbname);

    if !Path::new(&dbpath).exists() {
        print!("create new '{}'? [Yes/no]> ", args.dbname);
        stdout().flush().expect("confirm");

        let mut yes_no = String::new();
        std::io::stdin().read_line(&mut yes_no).ok();
        let ans = yes_no.trim().to_ascii_lowercase();
        if ans != "y" && ans != "yes" {
            println!("terminates the process.");
            process::exit(0);
        }
    }

    let drvr = EmbeddedDriver::new();
    if let Ok(mut conn) = drvr.connect(&dbpath) {
        while let Ok(qry) = read_query(&conn) {
            if qry.trim().to_ascii_lowercase() == ":q" {
                conn.close().expect("close");
                println!("disconnected.");
                process::exit(0);
            }
            let mut stmt = conn.create(&qry).expect("create statement");
            let words: Vec<&str> = qry.split_whitespace().collect();
            if words[0].trim().to_ascii_lowercase() == "select" {
                if let Ok(result) = stmt.execute_query() {
                    print_result_set(result).expect("print result set");
                } else {
                    println!("invalid query: {}", qry);
                }
            } else {
                if let Ok(affected) = stmt.execute_update() {
                    println!("Affected {}", affected);
                } else {
                    println!("invalid command: {}", qry);
                }
            }
        }
    }

    println!("couldn't connect database.");
    process::exit(1);
}
