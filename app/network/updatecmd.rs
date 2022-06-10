use std::time::Instant;

use simpledb::rdbc::{network::statement::NetworkStatement, statementadapter::StatementAdapter};

pub async fn exec_update_cmd(stmt: &mut NetworkStatement) {
    let start = Instant::now();
    let res = stmt.execute_update().unwrap();
    match res.affected().await {
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
    match res.committed_tx().await {
        Err(_) => println!("invalid command"),
        Ok(tx_num) => println!("transaction {} committed", tx_num),
    }
}
