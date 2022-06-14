use anyhow::Result;
use core::fmt;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::sync::{Arc, Mutex};

use crate::{file::page::Page, tx::transaction::Transaction};

pub mod checkpoint_record;
pub mod commit_record;
pub mod rollback_record;
pub mod set_bool_record;
pub mod set_date_record;
pub mod set_i16_record;
pub mod set_i32_record;
pub mod set_i8_record;
pub mod set_string_record;
pub mod set_u16_record;
pub mod set_u32_record;
pub mod set_u8_record;
pub mod start_record;

#[derive(Debug)]
enum LogRecordError {
    UnknownRecord,
}

impl std::error::Error for LogRecordError {}
impl fmt::Display for LogRecordError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &LogRecordError::UnknownRecord => {
                write!(f, "unknown log record")
            }
        }
    }
}

#[derive(FromPrimitive, Debug, Eq, PartialEq, Clone, Copy)]
pub enum TxType {
    CHECKPOINT = 0,
    START = 1,
    COMMIT = 2,
    ROLLBACK = 3,
    SETI8 = 4,
    SETU8 = 5,
    SETI16 = 6,
    SETU16 = 7,
    SETI32 = 8,
    SETU32 = 9,
    SETSTRING = 10,
    SETBOOL = 11,
    SETDATE = 12,
}

pub trait LogRecord {
    fn op(&self) -> TxType;
    fn tx_number(&self) -> i32;
    fn undo(&mut self, tx: Arc<Mutex<Transaction>>) -> Result<()>;
}

pub fn create_log_record(bytes: Vec<u8>) -> Result<Box<dyn LogRecord>> {
    let p = Page::new_from_bytes(bytes);
    let tx_type = FromPrimitive::from_i32(p.get_i32(0)?);

    match tx_type {
        Some(TxType::CHECKPOINT) => Ok(Box::new(checkpoint_record::CheckpointRecord::new()?)),
        Some(TxType::START) => Ok(Box::new(start_record::StartRecord::new(p)?)),
        Some(TxType::COMMIT) => Ok(Box::new(commit_record::CommitRecord::new(p)?)),
        Some(TxType::ROLLBACK) => Ok(Box::new(rollback_record::RollbackRecord::new(p)?)),
        Some(TxType::SETI8) => Ok(Box::new(set_i8_record::SetI8Record::new(p)?)),
        Some(TxType::SETU8) => Ok(Box::new(set_u8_record::SetU8Record::new(p)?)),
        Some(TxType::SETI16) => Ok(Box::new(set_i16_record::SetI16Record::new(p)?)),
        Some(TxType::SETU16) => Ok(Box::new(set_u16_record::SetU16Record::new(p)?)),
        Some(TxType::SETI32) => Ok(Box::new(set_i32_record::SetI32Record::new(p)?)),
        Some(TxType::SETU32) => Ok(Box::new(set_u32_record::SetU32Record::new(p)?)),
        Some(TxType::SETSTRING) => Ok(Box::new(set_string_record::SetStringRecord::new(p)?)),
        Some(TxType::SETBOOL) => Ok(Box::new(set_bool_record::SetBoolRecord::new(p)?)),
        Some(TxType::SETDATE) => Ok(Box::new(set_date_record::SetDateRecord::new(p)?)),
        None => Err(From::from(LogRecordError::UnknownRecord)),
    }
}
