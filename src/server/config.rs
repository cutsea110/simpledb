use structopt::clap::arg_enum;

pub const BLOCK_SIZE: i32 = 400;
pub const BUFFER_SIZE: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimpleDBConfig {
    // configurable parameters
    //
    // File Manager
    pub block_size: i32,
    // Buffer Manager
    pub num_of_buffers: usize,
    pub buffer_manager: BufferMgr,
    // query planner
    pub query_planner: QueryPlanner,
}

arg_enum! {
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum BufferMgr {
        Naive,
        NaiveUp,
        NaiveBis,
        NaiveBisUp,
        FIFO,
        FIFOTs,
        FIFOUp,
        LRU,
        LRUUp,
        Clock,
        ClockUp,
    }
}

arg_enum! {
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum QueryPlanner {
        Basic,
        Heuristic,
    }
}
