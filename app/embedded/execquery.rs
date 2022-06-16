use anyhow::Result;
use std::time::Instant;

use simpledb::rdbc::{
    embedded::{
        metadata::EmbeddedMetaData, resultset::EmbeddedResultSet, statement::EmbeddedStatement,
    },
    resultsetadapter::ResultSetAdapter,
    resultsetmetadataadapter::DataType,
    resultsetmetadataadapter::ResultSetMetaDataAdapter,
    statementadapter::StatementAdapter,
};

fn print_record(results: &mut EmbeddedResultSet, meta: &EmbeddedMetaData) -> Result<()> {
    for i in 0..meta.get_column_count() {
        let fldname = meta.get_column_name(i).expect("get column name");
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        match meta.get_column_type(i).expect("get column type") {
            DataType::Int16 => {
                print!("{:width$} ", results.get_i16(fldname)?, width = w);
            }
            DataType::Int32 => {
                print!("{:width$} ", results.get_i32(fldname)?, width = w);
            }
            DataType::Varchar => {
                print!("{:width$} ", results.get_string(fldname)?, width = w);
            }
            DataType::Bool => {
                print!("{:width$} ", results.get_bool(fldname)?, width = w);
            }
            DataType::Date => {
                print!("{:width$} ", results.get_date(fldname)?, width = w);
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

pub fn exec_query<'a>(stmt: &'a mut EmbeddedStatement<'a>) {
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
