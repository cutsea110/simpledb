use anyhow::{anyhow, Result};
use log::{info, warn};
use std::time::Instant;

use simpledb::rdbc::{
    network::{
        metadata::NetworkResultSetMetaData, resultset, resultset::NetworkResultSet,
        statement::NetworkStatement,
    },
    resultsetmetadataadapter::ResultSetMetaDataAdapter,
};

const MAX_ROWS: u16 = 80;

fn print_record(row: &[resultset::Value], meta: &NetworkResultSetMetaData) {
    for (i, value) in row.iter().enumerate() {
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        match value {
            resultset::Value::Int16(v) => {
                print!("{:width$} ", v, width = w);
            }
            resultset::Value::Int32(v) => {
                print!("{:width$} ", v, width = w);
            }
            resultset::Value::String(s) => {
                print!("{:width$} ", s, width = w);
            }
            resultset::Value::Bool(v) => {
                print!("{:width$} ", v, width = w);
            }
            resultset::Value::Date(v) => {
                print!("{:width$} ", v, width = w);
            }
        }
    }
    println!();
}

async fn print_result_set(results: NetworkResultSet) -> Result<(i32, i32)> {
    let meta = results.metadata();

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
        let rows = match results.get_rows(MAX_ROWS).await {
            Ok(rows) => rows,
            Err(read_error) => {
                return match results.close().await {
                    Ok(_) => Err(read_error),
                    Err(close_error) => Err(anyhow!(
                        "{}; result-set cleanup also failed: {}",
                        read_error,
                        close_error
                    )),
                };
            }
        };
        let c = rows.len();
        for row in rows {
            print_record(&row, meta);
        }
        total_count += c as i32;

        if c < MAX_ROWS as usize {
            break;
        }
    }
    // unpin!
    let tx_num = results.close().await?;

    Ok((total_count, tx_num))
}

pub async fn exec_query(stmt: &mut NetworkStatement) {
    let start = Instant::now();
    match stmt.execute_query().await {
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
