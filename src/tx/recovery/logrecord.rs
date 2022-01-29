use anyhow::Result;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use crate::file::page::Page;

mod checkpoint_record;
mod commit_record;
mod rollback_record;
mod set_i32_record;
mod set_string_record;
mod start_record;

#[derive(FromPrimitive, Debug, Eq, PartialEq, Clone, Copy)]
pub enum TxType {
    CHECKPOINT = 0,
    START = 1,
    COMMIT = 2,
    ROLLBACK = 3,
    SETI32 = 4,
    SETSTRING = 5,
}

pub trait LogRecord {
    fn op(&self) -> TxType;
    fn tx_number(&self) -> i32;
}

impl dyn LogRecord {
    pub fn create_log_record(bytes: Vec<u8>) -> Result<Box<Self>> {
        let p = Page::new_from_bytes(bytes);
        let tx_type = FromPrimitive::from_i32(p.get_i32(0)?);

        match tx_type {
            Some(TxType::CHECKPOINT) => Ok(Box::new(checkpoint_record::CheckpointRecord::new()?)),
            Some(TxType::START) => Ok(Box::new(start_record::StartRecord::new(p)?)),
            Some(TxType::COMMIT) => Ok(Box::new(commit_record::CommitRecord::new(p)?)),
            Some(TxType::ROLLBACK) => Ok(Box::new(rollback_record::RollbackRecord::new(p)?)),
            Some(TxType::SETI32) => Ok(Box::new(set_i32_record::SetI32Record::new(p)?)),
            Some(TxType::SETSTRING) => Ok(Box::new(set_string_record::SetStringRecord::new(p)?)),
            None => panic!("Unsupported TxType found."),
        }
    }
}
