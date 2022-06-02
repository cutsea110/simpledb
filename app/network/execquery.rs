use anyhow::Result;
use std::time::Instant;

use simpledb::rdbc::{
    network::{
        metadata::NetworkResultSetMetaData, resultset::NetworkResultSet,
        statement::NetworkStatement,
    },
    resultsetadapter::ResultSetAdapter,
    resultsetmetadataadapter::ResultSetMetaDataAdapter,
    statementadapter::StatementAdapter,
};

pub async fn exec_query(stmt: &mut NetworkStatement) {
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
            println!();
        }
    }
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

async fn print_record(
    results: &mut NetworkResultSet,
    meta: &NetworkResultSetMetaData,
) -> Result<()> {
    let row = results.get_row(&meta).await.expect("get row");
    for i in 0..meta.get_column_count() {
        let fldname = meta.get_column_name(i).expect("get column name");
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        match row.get(fldname.as_str()) {
            Some(simpledb::rdbc::network::resultset::Value::Int32(v)) => {
                print!("{:width$} ", v.clone(), width = w);
            }
            Some(simpledb::rdbc::network::resultset::Value::String(s)) => {
                print!("{:width$} ", s, width = w);
            }
            None => {
                panic!("shouldn't be here!")
            }
        }
    }
    println!();

    Ok(())
}
