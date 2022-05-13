use anyhow::Result;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt};
use itertools::Itertools;
use std::{
    cell::RefCell,
    collections::HashMap,
    io::{stdout, Write},
    net::{SocketAddr, ToSocketAddrs},
    process,
    rc::Rc,
    sync::Arc,
    time::Instant,
};

use simpledb::{
    rdbc::{
        connectionadapter::ConnectionAdapter,
        driveradapter::DriverAdapter,
        network::{
            connection::NetworkConnection,
            driver::NetworkDriver,
            metadata::{IndexInfo, NetworkResultSetMetaData},
            planrepr::NetworkPlanRepr,
            resultset::NetworkResultSet,
            statement::NetworkStatement,
        },
        resultsetadapter::ResultSetAdapter,
        resultsetmetadataadapter::{DataType, ResultSetMetaDataAdapter},
        statementadapter::StatementAdapter,
    },
    record::schema::{FieldType, Schema},
    remote_capnp::remote_driver,
    repr::planrepr::{Operation, PlanRepr},
};

extern crate capnp_rpc;
extern crate simpledb;

fn format_operation(op: Operation) -> String {
    match op {
        Operation::IndexJoinScan {
            idxname: _,
            idxfldname,
            joinfld,
        } => format!("INDEX JOIN SCAN BY {} = {}", idxfldname, joinfld),
        Operation::IndexSelectScan {
            idxname: _,
            idxfldname,
            val,
        } => format!("INDEX SELECT SCAN BY {} = {}", idxfldname, val),
        Operation::GroupByScan {
            fields: _,
            aggfns: _,
        } => format!("GROUP BY",),
        Operation::Materialize => format!("MATERIALIZE"),
        Operation::MergeJoinScan { fldname1, fldname2 } => {
            format!("MERGE JOIN SCAN BY {} = {}", fldname1, fldname2)
        }
        Operation::SortScan { compflds } => format!("SORT SCAN BY ({})", compflds.iter().join(",")),
        Operation::MultibufferProductScan => format!("MULTIBUFFER PRODUCT SCAN"),
        Operation::ProductScan => format!("PRODUCT SCAN"),
        Operation::ProjectScan => format!("PROJECT SCAN"),
        Operation::SelectScan { pred: _ } => format!("SELECT SCAN"),
        Operation::TableScan { tblname: _ } => format!("TABLE SCAN"),
    }
}

fn format_name(op: Operation) -> String {
    match op {
        Operation::IndexJoinScan {
            idxname,
            idxfldname: _,
            joinfld: _,
        } => format!("{}", idxname),
        Operation::IndexSelectScan {
            idxname,
            idxfldname: _,
            val: _,
        } => format!("{}", idxname),
        Operation::GroupByScan {
            fields: _,
            aggfns: _,
        } => format!(""),
        Operation::Materialize => format!(""),
        Operation::MergeJoinScan {
            fldname1: _,
            fldname2: _,
        } => format!(""),
        Operation::SortScan { compflds: _ } => format!(""),
        Operation::MultibufferProductScan => format!(""),
        Operation::ProductScan => format!(""),
        Operation::ProjectScan => format!(""),
        Operation::SelectScan { pred: _ } => format!(""),
        Operation::TableScan { tblname } => format!("{}", tblname),
    }
}

pub fn print_explain_plan(epr: NetworkPlanRepr) {
    const MAX_OP_WIDTH: usize = 60;

    fn print_pr(pr: Arc<dyn PlanRepr>, n: Rc<RefCell<i32>>, depth: usize) {
        let raw_op_str = format_operation(pr.operation());
        let mut indented_op_str = format!("{:width$}{}", "", raw_op_str, width = depth * 2);
        if indented_op_str.len() > MAX_OP_WIDTH {
            // 3 is length of "..."
            indented_op_str = format!("{}...", &indented_op_str[0..MAX_OP_WIDTH - 3]);
        }
        println!(
            "{:>2} {:<width$} {:<20} {:>8} {:>8}",
            n.borrow(),
            indented_op_str,
            format_name(pr.operation()),
            pr.reads(),
            pr.writes(),
            width = MAX_OP_WIDTH,
        );
        *n.borrow_mut() += 1;

        for sub_pr in pr.sub_plan_reprs() {
            print_pr(sub_pr, Rc::clone(&n), depth + 1);
        }
    }

    let row_num = Rc::new(RefCell::new(1));
    let pr = epr.repr();
    println!(
        "{:<2} {:<width$} {:<20} {:>8} {:>8}",
        "#",
        "Operation",
        "Name",
        "Reads",
        "Writes",
        width = MAX_OP_WIDTH
    );
    println!("{:-<width$}", "", width = 102);
    print_pr(pr, row_num, 0);
}

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

    let driver = NetworkDriver::new(driver).await;
    if let Ok((major_ver, minor_ver)) = driver.get_server_version().await {
        println!("simpledb server version {}.{}\n", major_ver, minor_ver);
    }
    let mut conn = driver.connect("demo").unwrap_or_else(|_| {
        println!("couldn't connect database.");
        process::exit(1);
    });

    while let Ok(qry) = read_query() {
        exec(&mut conn, &qry.trim()).await;
    }

    Ok(())
}

fn read_query() -> Result<String> {
    print!("SQL> ");
    stdout().flush().expect("require input");

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input)
}

async fn print_record(
    results: &mut NetworkResultSet,
    meta: &NetworkResultSetMetaData,
) -> Result<()> {
    for i in 0..meta.get_column_count() {
        let fldname = meta.get_column_name(i).expect("get column name");
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        match meta.get_column_type(i).expect("get column type") {
            DataType::Int32 => {
                print!(
                    "{:width$} ",
                    results.get_i32(fldname)?.get_value().await?,
                    width = w
                );
            }
            DataType::Varchar => {
                print!(
                    "{:width$} ",
                    results.get_string(fldname)?.get_value().await?,
                    width = w
                );
            }
        }
    }
    println!();

    Ok(())
}

async fn print_result_set(mut results: NetworkResultSet) -> Result<i32> {
    // resultset metadata
    let mut meta = results.get_meta_data()?;
    meta.load_schema().await.expect("load schema");
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
    while results.next().has_next().await? {
        c += 1;
        print_record(&mut results, &meta).await?;
    }

    // unpin!
    results.close()?.response().await.expect("close");

    Ok(c)
}

async fn exec_query(stmt: &mut NetworkStatement) {
    let start = Instant::now();
    match stmt.execute_query() {
        Err(_) => println!("invalid query"),
        Ok(result) => {
            let cnt = print_result_set(result).await.expect("print result set");
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

async fn exec_update_cmd(stmt: &mut NetworkStatement) {
    let start = Instant::now();
    match stmt.execute_update().unwrap().affected().await {
        Err(_) => println!("invalid command"),
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

fn print_help_meta_cmd() {
    println!(":h, :help                       Show this help");
    println!(":q, :quit, :exit                Quit the program");
    println!(":t, :table   <table_name>       Show table schema");
    println!(":v, :view    <view_name>        Show view definition");
    println!(":e, :explain <sql>              Explain plan");
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
            if let Ok((viewname, viewdef)) = conn.get_view_definition(viewname).await {
                print_view_definition(&viewname, &viewdef);
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
                        print_explain_plan(plan_repr);
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
            exec_query(&mut stmt).await;
        } else {
            exec_update_cmd(&mut stmt).await;
        }
    }
}
