use std::time::Instant;

use simpledb::rdbc::{network::statement::NetworkStatement, statementadapter::StatementAdapter};

pub async fn exec_update_cmd(stmt: &mut NetworkStatement) {
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
