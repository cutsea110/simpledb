use anyhow::Result;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use crate::file::page::Page;

#[derive(FromPrimitive)]
pub enum TxType {
    CHECKPOINT = 0,
    START = 1,
    COMMIT = 2,
    ROLLBACK = 3,
    SETI32 = 4,
    SETSTRING = 5,
}

pub enum LogRecord {
    SetCheckpoint(SetCheckpointRecord),
    SetStart(SetStartRecord),
    SetCommit(SetCommitRecord),
    SetRollback(SetRollbackRecord),
    SetI32(SetI32Record),
    SetString(SetStringRecord),
}

impl LogRecord {
    pub fn create_log_record(bytes: Vec<u8>) -> Result<Self> {
        let p = Page::new_from_bytes(bytes);
        let tx_type: i32 = p.get_i32(0)?;

        match FromPrimitive::from_i32(tx_type) {
            Some(TxType::CHECKPOINT) => Ok(LogRecord::SetCheckpoint(SetCheckpointRecord {})),
            Some(TxType::START) => Ok(LogRecord::SetStart(SetStartRecord {})),
            Some(TxType::COMMIT) => Ok(LogRecord::SetCommit(SetCommitRecord {})),
            Some(TxType::ROLLBACK) => Ok(LogRecord::SetRollback(SetRollbackRecord {})),
            Some(TxType::SETI32) => Ok(LogRecord::SetI32(SetI32Record {})),
            Some(TxType::SETSTRING) => Ok(LogRecord::SetString(SetStringRecord {})),
            None => panic!("TODO"),
        }
    }
}

pub struct SetCheckpointRecord {}

pub struct SetStartRecord {}

pub struct SetCommitRecord {}

pub struct SetRollbackRecord {}

pub struct SetI32Record {}

pub struct SetStringRecord {}
