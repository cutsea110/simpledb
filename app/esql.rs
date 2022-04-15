use anyhow::Result;
use getopts::Options;
use itertools::Itertools;
use std::{
    cell::RefCell,
    collections::HashMap,
    env,
    io::{stdout, Write},
    path::Path,
    process,
    rc::Rc,
    sync::Arc,
    time::Instant,
};

use simpledb::{
    metadata::indexmanager::IndexInfo,
    rdbc::{
        connectionadapter::ConnectionAdapter,
        driveradapter::DriverAdapter,
        embedded::{
            connection::EmbeddedConnection, driver::EmbeddedDriver, metadata::EmbeddedMetaData,
            planrepr::EmbeddedPlanRepr, resultset::EmbeddedResultSet, statement::EmbeddedStatement,
        },
        planrepradapter::PlanReprAdapter,
        resultsetadapter::ResultSetAdapter,
        resultsetmetadataadapter::{DataType, ResultSetMetaDataAdapter},
        statementadapter::StatementAdapter,
    },
    record::schema::FieldType,
    record::schema::Schema,
    repr::planrepr::{Operation, PlanRepr},
};

const DB_DIR: &str = "data";
const VERSION: &str = "0.1.0";

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

fn print_version(program: &str, _opts: &Options) {
    let brief = format!("{} {}", program, VERSION);
    println!("{}", &brief);
    process::exit(0);
}

fn parse_args() -> Args {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("d", "dbname", "set database name", "DBNAME");
    opts.optflag("h", "help", "print this help menu");
    opts.optflag("v", "version", "print version");

    let matches = opts
        .parse(&args[1..])
        .unwrap_or_else(|f| panic!("{}", f.to_string()));
    if matches.opt_present("h") {
        print_usage(&program, &opts);
    }

    if matches.opt_present("v") {
        print_version(&program, &opts);
    }

    Args {
        dbname: matches.opt_str("d").expect("require dbname"),
    }
}

fn read_query() -> Result<String> {
    print!("SQL> ");
    stdout().flush().expect("require input");

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input)
}

fn print_record(results: &mut EmbeddedResultSet, meta: &EmbeddedMetaData) -> Result<()> {
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
    println!();

    Ok(())
}

fn print_result_set(mut results: EmbeddedResultSet) -> Result<i32> {
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
    println!();
    // separater
    for i in 0..meta.get_column_count() {
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        print!("{:-<width$}", "", width = w + 1);
    }
    println!();
    // scan record
    let mut c = 0;
    while results.next() {
        c += 1;
        print_record(&mut results, &meta)?;
    }

    // unpin!
    results.close()?;

    Ok(c)
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

fn print_table_schema(tblname: &str, schema: Arc<Schema>, idx_info: HashMap<String, IndexInfo>) {
    println!(
        " * table: {} has {} fields.\n",
        tblname,
        schema.fields().len()
    );

    println!(" #   name             type");
    println!("--------------------------------------");
    for (i, fldname) in schema.fields().iter().enumerate() {
        let fldtyp = match schema.field_type(fldname) {
            FieldType::INTEGER => "int32".to_string(),
            FieldType::VARCHAR => format!("varchar({})", schema.length(fldname)),
        };
        println!("{:>4} {:16} {:16}", i + 1, fldname, fldtyp);
    }
    println!();

    if !idx_info.is_empty() {
        println!(" * indexes on {}\n", tblname);

        println!(" #   name             field");
        println!("--------------------------------------");
        for (i, (_, ii)) in idx_info.iter().enumerate() {
            println!("{:>4} {:16} {:16}", i + 1, ii.index_name(), ii.field_name());
        }
        println!();
    }
}

fn print_view_definition(viewname: &str, viewdef: &str) {
    println!("view name: {}", viewname);
    println!("view def:\n > {}", viewdef);
    println!();
}

fn print_help_meta_cmd() {
    println!(":h, :help                       Show this help");
    println!(":q, :quit, :exit                Quit the program");
    println!(":t, :table   <table_name>       Show table schema");
    println!(":v, :view    <view_name>        Show view definition");
    println!(":e, :explain <sql>              Explain plan");
}

fn format_operation(op: Operation) -> String {
    match op {
        Operation::IndexJoinScan {
            idxname,
            idxfldname,
            joinfld,
        } => format!("INDEX JOIN SCAN BY {} = {}", idxfldname, joinfld),
        Operation::IndexSelectScan {
            idxname,
            idxfldname,
            val,
        } => format!("INDEX SELECT SCAN BY {} = {}", idxfldname, val),
        Operation::GroupByScan { fields, aggfns } => format!("GROUP BY",),
        Operation::Materialize => format!("MATERIALIZE"),
        Operation::MergeJoinScan { fldname1, fldname2 } => {
            format!("MERGE JOIN SCAN BY {} = {}", fldname1, fldname2)
        }
        Operation::SortScan { compflds } => format!("SORT SCAN"),
        Operation::MultibufferProductScan => format!("MULTIBUFFER PRODUCT SCAN"),
        Operation::ProductScan => format!("PRODUCT SCAN"),
        Operation::ProjectScan => format!("PROJECT SCAN"),
        Operation::SelectScan { pred } => format!("SELECT SCAN"),
        Operation::TableScan { tblname } => format!("TABLE SCAN"),
    }
}

fn format_name(op: Operation) -> String {
    match op {
        Operation::IndexJoinScan {
            idxname,
            idxfldname,
            joinfld,
        } => format!("{}", idxname),
        Operation::IndexSelectScan {
            idxname,
            idxfldname,
            val,
        } => format!("{}", idxname),
        Operation::GroupByScan { fields, aggfns } => format!(""),
        Operation::Materialize => format!(""),
        Operation::MergeJoinScan { fldname1, fldname2 } => format!(""),
        Operation::SortScan { compflds } => format!(""),
        Operation::MultibufferProductScan => format!(""),
        Operation::ProductScan => format!(""),
        Operation::ProjectScan => format!(""),
        Operation::SelectScan { pred } => format!(""),
        Operation::TableScan { tblname } => format!("{}", tblname),
    }
}

fn print_explain_plan(epr: EmbeddedPlanRepr) {
    fn print_pr(pr: Arc<dyn PlanRepr>, n: Rc<RefCell<i32>>, depth: usize) {
        let raw_op_str = format_operation(pr.operation());
        let mut op_str = format!("{:width$}{}", "", raw_op_str, width = depth * 2);
        if op_str.len() > 60 {
            op_str = format!("{}...", &op_str[0..57]);
        }
        println!(
            "{:>3} {:<60}{:<20}{:>8}{:>8}",
            n.borrow(),
            op_str,
            format_name(pr.operation()),
            pr.reads(),
            pr.writes(),
        );
        *n.borrow_mut() += 1;

        for sub_pr in pr.sub_plan_reprs() {
            print_pr(sub_pr, Rc::clone(&n), depth + 1);
        }
    }

    let row_num = Rc::new(RefCell::new(1));
    let pr = epr.repr();
    println!(
        "{:<3} {:<60}{:<20}{:>8}{:>8}",
        "#", "Operation", "Name", "Reads", "Writes"
    );
    println!("{}", "-".repeat(100));
    print_pr(pr, row_num, 0);
}

fn exec_meta_cmd(conn: &mut EmbeddedConnection, qry: &str) {
    let tokens: Vec<&str> = qry.trim().split_whitespace().collect_vec();
    let cmd = tokens[0].to_ascii_lowercase();
    let args = &tokens[1..];
    match cmd.as_str() {
        ":h" | ":help" => {
            print_help_meta_cmd();
        }
        ":q" | ":quit" | ":exit" => {
            conn.close().expect("close");
            println!("disconnected.");
            process::exit(0);
        }
        ":t" | ":table" => {
            if args.is_empty() {
                println!("table name is required.");
                return;
            }
            let tblname = args[0];
            if let Ok(sch) = conn.get_table_schema(tblname) {
                let idx_info = conn.get_index_info(tblname).unwrap_or_default();
                print_table_schema(tblname, sch, idx_info);
            }
            return;
        }
        ":v" | ":view" => {
            if args.is_empty() {
                println!("view name is required.");
                return;
            }
            let viewname = args[0];
            if let Ok((viewname, viewdef)) = conn.get_view_definition(viewname) {
                print_view_definition(&viewname, &viewdef);
            }
        }
        ":e" | ":explain" => {
            if args.is_empty() {
                println!("SQL is required.");
                return;
            }
            let sql = &qry[tokens[0].len()..].trim();
            println!("EXPLAIN PLAN FOR: {}", sql);
            let mut stmt = conn.create(sql).expect("create statement");
            let words: Vec<&str> = sql.split_whitespace().collect();
            if !words.is_empty() {
                let cmd = words[0].trim().to_ascii_lowercase();
                if &cmd == "select" {
                    if let Ok(plan_repr) = stmt.explain_plan() {
                        print_explain_plan(plan_repr);
                    }
                } else {
                    println!("expect query(not command).");
                }
            }
        }
        cmd => {
            println!("Unknown command: {}", cmd)
        }
    }
}

fn exec_query<'a>(stmt: &'a mut EmbeddedStatement<'a>) {
    let qry = stmt.sql().to_string();
    let start = Instant::now();
    match stmt.execute_query() {
        Err(_) => println!("invalid query: {}", qry),
        Ok(result) => {
            let cnt = print_result_set(result).expect("print result set");
            let end = start.elapsed();
            println!(
                "Rows {} ({}.{:03}s)",
                cnt,
                end.as_secs(),
                end.subsec_nanos() / 1_000_000
            );
        }
    }
}

fn exec_update_cmd<'a>(stmt: &'a mut EmbeddedStatement<'a>) {
    let qry = stmt.sql().to_string();
    let start = Instant::now();
    match stmt.execute_update() {
        Err(_) => println!("invalid command: {}", qry),
        Ok(affected) => {
            let end = start.elapsed();
            println!(
                "Affected {} ({}.{:03}s)",
                affected,
                end.as_secs(),
                end.subsec_nanos() / 1_000_000
            );
        }
    }
}

fn exec(conn: &mut EmbeddedConnection, qry: &str) {
    if qry.trim().to_ascii_lowercase().starts_with(":") {
        exec_meta_cmd(conn, qry);
        return;
    }

    let mut stmt = conn.create(&qry).expect("create statement");
    let words: Vec<&str> = qry.split_whitespace().collect();
    if !words.is_empty() {
        let cmd = words[0].trim().to_ascii_lowercase();
        if &cmd == "select" {
            exec_query(&mut stmt);
        } else {
            exec_update_cmd(&mut stmt);
        }
    }
}

fn main() {
    let args = parse_args();
    let dbpath = format!("{}/{}", DB_DIR, args.dbname);

    if !Path::new(&dbpath).exists() {
        confirm_new_db(&args.dbname);
    }

    let drvr = EmbeddedDriver::new();
    let mut conn = drvr.connect(&dbpath).unwrap_or_else(|_| {
        println!("couldn't connect database.");
        process::exit(1);
    });

    while let Ok(qry) = read_query() {
        exec(&mut conn, &qry);
    }
}
