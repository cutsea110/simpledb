use core::fmt;
use std::str::FromStr;

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BufferMgr {
    Naive,
    NaiveBis,
    FIFO,
    LRU,
    Clock,
}

#[derive(Debug)]
pub enum ParseError {
    UnknownBufferMgr(String),
    UnknownQueryPlanner(String),
}
impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::UnknownBufferMgr(s) => write!(f, "unknown buffer manager: {}", s),
            Self::UnknownQueryPlanner(s) => write!(f, "unknown query planner: {}", s),
        }
    }
}
impl std::error::Error for ParseError {}

impl FromStr for BufferMgr {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "naive" => Ok(BufferMgr::Naive),
            "naivebis" => Ok(BufferMgr::NaiveBis),
            "fifo" => Ok(BufferMgr::FIFO),
            "lru" => Ok(BufferMgr::LRU),
            "clock" => Ok(BufferMgr::Clock),
            _ => Err(From::from(ParseError::UnknownBufferMgr(s.to_string()))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QueryPlanner {
    Basic,
    Heuristic,
}

impl FromStr for QueryPlanner {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "basic" => Ok(QueryPlanner::Basic),
            "heuristic" => Ok(QueryPlanner::Heuristic),
            _ => Err(From::from(ParseError::UnknownQueryPlanner(s.to_string()))),
        }
    }
}
