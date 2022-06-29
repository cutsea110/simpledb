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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BufferMgr {
    Naive,
    NaiveBis,
    FIFO,
    LRU,
    Clock,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QueryPlanner {
    Basic,
    Heuristic,
}
