use anyhow::Result;

use super::connection::EmbeddedConnection;
use crate::{
    rdbc::driveradapter::{DriverAdapter, DriverError},
    server::{
        config::{self, SimpleDBConfig},
        simpledb::SimpleDB,
    },
};

pub struct EmbeddedDriver {
    cfg: SimpleDBConfig,
}

impl EmbeddedDriver {
    pub fn new() -> Self {
        Self {
            cfg: SimpleDBConfig {
                block_size: config::BLOCK_SIZE,
                num_of_buffers: config::BUFFER_SIZE,
                buffer_manager: config::BufferMgr::LRU,
                query_planner: config::QueryPlanner::Heuristic,
            },
        }
    }
}

impl DriverAdapter<'_> for EmbeddedDriver {
    type Con = EmbeddedConnection;

    fn connect(&self, dbname: &str) -> Result<Self::Con> {
        if let Ok(db) = SimpleDB::build_from(self.cfg.clone())(dbname) {
            return Ok(EmbeddedConnection::new(db));
        }

        Err(From::from(DriverError::ConnectFailed))
    }
    fn get_major_version(&self) -> i32 {
        0
    }
    fn get_minor_version(&self) -> i32 {
        1
    }
}
