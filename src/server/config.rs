#[derive(Debug, Clone)]
pub struct Config {
    // configurable parameters
    //
    // Buffer Manager
    buffer_manager: BufferMgr,
    // query planner
    query_planner: QueryPlanner,
}

#[derive(Debug, Clone)]
enum BufferMgr {
    Naive,
    NaiveBis,
    FIFO,
    LRU,
    Clock,
}

#[derive(Debug, Clone)]
enum QueryPlanner {
    Basic,
    Heuristic,
}
