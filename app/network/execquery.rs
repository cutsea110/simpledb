use anyhow::Result;
use std::{
    io::{stdout, BufWriter, Write},
    time::Instant,
};

use simpledb::rdbc::{
    network::{
        metadata::NetworkResultSetMetaData, resultset::NetworkResultSet,
        statement::NetworkStatement,
    },
    resultsetadapter::ResultSetAdapter,
    resultsetmetadataadapter::{DataType, ResultSetMetaDataAdapter},
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
        }
    }
}

async fn print_result_set(mut results: NetworkResultSet) -> Result<i32> {
    let out = stdout();
    let mut out = BufWriter::new(out.lock());
    let mut s = String::new();

    // resultset metadata
    let mut meta = results.get_meta_data()?;
    meta.load_schema().await.expect("load schema");
    // print header
    for i in 0..meta.get_column_count() {
        let name = meta.get_column_name(i).expect("get column name");
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        s += &format!("{:width$} ", name, width = w);
    }
    s += "\n";
    // separater
    for i in 0..meta.get_column_count() {
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        s += &format!("{:-<width$}", "", width = w + 1);
    }
    s += "\n";
    // scan record
    let mut c = 0;
    while results.next().has_next().await? {
        c += 1;
        s += format_record(&mut results, &meta).await?.as_str();
    }
    out.write_all(s.as_bytes()).expect("print");

    // unpin!
    results.close()?.response().await.expect("close");

    Ok(c)
}

async fn format_record(
    results: &mut NetworkResultSet,
    meta: &NetworkResultSetMetaData,
) -> Result<String> {
    let mut s = String::new();

    for i in 0..meta.get_column_count() {
        let fldname = meta.get_column_name(i).expect("get column name");
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        match meta.get_column_type(i).expect("get column type") {
            DataType::Int32 => {
                s += &format!(
                    "{:width$} ",
                    results.get_i32(fldname)?.get_value().await?,
                    width = w
                );
            }
            DataType::Varchar => {
                s += &format!(
                    "{:width$} ",
                    results.get_string(fldname)?.get_value().await?,
                    width = w
                );
            }
        }
    }
    s += "\n";

    Ok(s)
}
