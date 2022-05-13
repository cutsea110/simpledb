use std::{
    cell::RefCell,
    net::{SocketAddr, ToSocketAddrs},
    rc::Rc,
    sync::Arc,
};

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt};
use itertools::Itertools;
use simpledb::{
    rdbc::{
        connectionadapter::ConnectionAdapter,
        driveradapter::DriverAdapter,
        network::{driver::NetworkDriver, planrepr::NetworkPlanRepr, resultset::Value},
        resultsetadapter::ResultSetAdapter,
        resultsetmetadataadapter::ResultSetMetaDataAdapter,
        statementadapter::StatementAdapter,
    },
    remote_capnp,
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
    let driver: remote_capnp::remote_driver::Client =
        rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
    tokio::task::spawn_local(Box::pin(rpc_system.map(|_| ())));

    // Query sample
    {
        let driver = NetworkDriver::new(driver).await;

        if let Ok((major_ver, minor_ver)) = driver.get_server_version().await {
            println!("simpledb server version {}.{}\n", major_ver, minor_ver);
        }

        let mut conn = driver.connect("demo")?;

        // table schema
        let schema = conn.get_table_schema("student").await?;
        for fldname in schema.fields() {
            match schema.field_type(fldname.as_str()) {
                simpledb::record::schema::FieldType::INTEGER => {
                    println!("{:10} {:10}", fldname, "INT32");
                }
                simpledb::record::schema::FieldType::VARCHAR => {
                    println!(
                        "{:10} {:10}",
                        fldname,
                        format!("VARCHAR({})", schema.length(fldname))
                    );
                }
            }
        }
        println!();

        // index info
        let index_info = conn.get_index_info("student").await?;
        for (_, ii) in index_info.into_iter() {
            println!("{:20} {:10}", ii.index_name(), ii.field_name());
        }
        println!();

        // view definition
        let (vwname, vwdef) = conn.get_view_definition("einstein").await?;
        println!("view name: {}", vwname);
        println!("view def:  {}", vwdef);
        println!();

        let mut stmt =
            conn.create_statement("UPDATE student SET grad_year=2020 WHERE grad_year=2024")?;

        let affected = stmt.execute_update()?.affected().await?;
        println!("Affected: {} rows", affected);

        // let commit_request = conn.commit_request();
        // commit_request.send().promise.await?;

        let mut stmt = conn.create_statement(
            "SELECT sid, sname, dname, grad_year FROM student, dept WHERE did = major_id",
        )?;
        let plan = stmt.explain_plan().await?;
        print_explain_plan(plan);
        println!();

        let result_set = stmt.execute_query()?;

        let metadata = result_set.get_meta().await?;

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

        while result_set.next().has_next().await? {
            let entry = result_set.get_row(&metadata).await?;
            for i in 0..metadata.get_column_count() {
                let fldname = metadata
                    .get_column_name(i)
                    .expect("get column name")
                    .as_str();
                let w = metadata
                    .get_column_display_size(i)
                    .expect("get column display size");
                match entry.get(fldname) {
                    Some(Value::Int32(v)) => print!("{:width$} ", v, width = w),
                    Some(Value::String(s)) => print!("{:width$} ", s, width = w),
                    None => panic!("field missing"),
                }
            }
            println!();
        }

        // let rollback_request = conn.rollback_request();
        // rollback_request.send().promise.await?;
    }

    Ok(())
}
