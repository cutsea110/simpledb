use std::time::Instant;

use log::info;
use simpledb::rdbc::network::statement::NetworkStatement;

pub async fn exec_update_cmd(stmt: &mut NetworkStatement) {
    let start = Instant::now();
    match stmt.execute_update().await {
        Err(_) => println!("invalid command"),
        Ok((affected, tx_num)) => {
            let end = start.elapsed();
            println!(
                "Affected {} ({}.{:03}s)",
                affected,
                end.as_secs(),
                end.subsec_nanos() / 1_000_000
            );
            info!(
                "elapsed time(secs): {}.{:03}",
                end.as_secs(),
                end.subsec_nanos() / 1_000_000
            );
            println!("transaction {} committed", tx_num);
        }
    }
}
