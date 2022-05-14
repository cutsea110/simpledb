use anyhow::Result;
use simpledb::rdbc::{
    embedded::{
        metadata::EmbeddedMetaData, resultset::EmbeddedResultSet, statement::EmbeddedStatement,
    },
    resultsetadapter::ResultSetAdapter,
    resultsetmetadataadapter::DataType,
    resultsetmetadataadapter::ResultSetMetaDataAdapter,
    statementadapter::StatementAdapter,
};
use std::{
    io::{stdout, BufWriter, Write},
    time::Instant,
};

fn format_record(results: &mut EmbeddedResultSet, meta: &EmbeddedMetaData) -> Result<String> {
    let mut s = String::new();

    for i in 0..meta.get_column_count() {
        let fldname = meta.get_column_name(i).expect("get column name");
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        match meta.get_column_type(i).expect("get column type") {
            DataType::Int32 => {
                s += &format!("{:width$} ", results.get_i32(fldname)?, width = w);
            }
            DataType::Varchar => {
                s += &format!("{:width$} ", results.get_string(fldname)?, width = w);
            }
        }
    }
    s += "\n";

    Ok(s)
}

fn print_result_set(mut results: EmbeddedResultSet) -> Result<i32> {
    let out = stdout();
    let mut out = BufWriter::new(out.lock());
    let mut s = String::new();

    // resultset metadata
    let meta = results.get_meta_data()?;
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
    while results.next() {
        c += 1;
        s += format_record(&mut results, &meta)?.as_str();
    }
    out.write_all(s.as_bytes()).expect("print");

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
