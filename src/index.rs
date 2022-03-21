use core::fmt;

use anyhow::Result;

use crate::{query::constant::Constant, record::rid::RID};

pub mod btree;
pub mod hash;

#[derive(Debug)]
pub enum IndexError {
    NoTableScan,
}

impl std::error::Error for IndexError {}
impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IndexError::NoTableScan => {
                write!(f, "no table scan")
            }
        }
    }
}

pub trait Index {
    fn before_first(&mut self, searchkey: Constant) -> Result<()>;
    fn next(&mut self) -> bool;
    fn get_data_rid(&mut self) -> Result<RID>;
    fn insert(&mut self, dataval: Constant, datarid: RID) -> Result<()>;
    fn delete(&mut self, dataval: Constant, datarid: RID) -> Result<()>;
    fn close(&mut self) -> Result<()>;
}
