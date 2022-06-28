#[derive(Debug, Clone)]
pub struct Config {
    // configurable parameters
    //
    // Buffer Manager
    buffer_manager: BufferMgr,
}

#[derive(Debug, Clone)]
enum BufferMgr {
    Naive,
    NaiveBis,
    FIFO,
    LRU,
    Clock,
}
