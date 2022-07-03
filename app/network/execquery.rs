use anyhow::Result;
use log::{info, warn};
use std::{collections::HashMap, time::Instant};

use simpledb::rdbc::{
    network::{
        metadata::NetworkResultSetMetaData, resultset, resultset::NetworkResultSet,
        statement::NetworkStatement,
    },
    resultsetadapter::ResultSetAdapter,
    resultsetmetadataadapter::ResultSetMetaDataAdapter,
    statementadapter::StatementAdapter,
};

use crate::ClientError;

const MAX_ROWS: u32 = 80;

fn print_record(row: HashMap<&str, resultset::Value>, meta: &NetworkResultSetMetaData) {
    for i in 0..meta.get_column_count() {
        let fldname = meta.get_column_name(i).expect("get column name");
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        match row.get(fldname.as_str()).expect("get field value") {
            resultset::Value::Int16(v) => {
                print!("{:width$} ", v.clone(), width = w);
            }
            resultset::Value::Int32(v) => {
                print!("{:width$} ", v.clone(), width = w);
            }
            resultset::Value::String(s) => {
                print!("{:width$} ", s, width = w);
            }
            resultset::Value::Bool(v) => {
                print!("{:width$} ", v.clone(), width = w);
            }
            resultset::Value::Date(v) => {
                print!("{:width$} ", v.clone(), width = w);
            }
        }
    }
    println!();
}

async fn print_result_set(mut results: NetworkResultSet) -> Result<(i32, i32)> {
    // resultset metadata
    let mut meta = results.get_meta_data()?;
    if let Err(e) = meta.load_schema().await {
        return Err(From::from(ClientError::Remote(format!("{}", e))));
    }

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
    let mut total_count = 0;
    loop {
        let rows = results.get_rows(MAX_ROWS, &meta).await.expect("get rows");
        let c = rows.len();
        for row in rows {
            print_record(row, &meta);
        }
        total_count += c as i32;

        if c < MAX_ROWS as usize {
            break;
        }
    }
    // unpin!
    let tx_num = results.close()?.response().await.expect("close");

    Ok((total_count, tx_num))
}

pub async fn exec_query(stmt: &mut NetworkStatement) {
    let start = Instant::now();
    match stmt.execute_query() {
        Err(_) => println!("invalid query"),
        Ok(result) => match print_result_set(result).await {
            Ok((cnt, tx_num)) => {
                let end = start.elapsed();
                println!(
                    "Rows {} ({}.{:03}s)",
                    cnt,
                    end.as_secs(),
                    end.subsec_nanos() / 1_000_000
                );
                println!("transaction {} committed", tx_num);
                info!(
                    "elapsed time(secs): {}.{:03}",
                    end.as_secs(),
                    end.subsec_nanos() / 1_000_000
                );
            }
            Err(e) => {
                warn!("failed to exec query: {}", e);
                return;
            }
        },
    }
}
