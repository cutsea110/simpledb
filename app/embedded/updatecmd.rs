use std::time::Instant;

use simpledb::rdbc::{embedded::statement::EmbeddedStatement, statementadapter::StatementAdapter};

pub fn exec_update_cmd<'a>(stmt: &'a mut EmbeddedStatement<'a>) {
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
