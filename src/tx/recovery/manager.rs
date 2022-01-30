use anyhow::Result;
use core::fmt;
use std::{cell::RefCell, sync::Arc};

use crate::{
    buffer::{buffer::Buffer, manager::BufferMgr},
    log::manager::LogMgr,
    tx::recovery::logrecord::start_record::StartRecord,
    tx::transaction::Transaction,
};

use super::logrecord::{
    self, checkpoint_record::CheckpointRecord, commit_record::CommitRecord,
    rollback_record::RollbackRecord, set_i32_record::SetI32Record,
    set_string_record::SetStringRecord, TxType,
};

#[derive(Debug)]
enum RecoveryMgrError {
    BufferFailed(String),
}

impl std::error::Error for RecoveryMgrError {}
impl fmt::Display for RecoveryMgrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::BufferFailed(s) => {
                write!(f, "buffer failed: {}", s)
            }
        }
    }
}

pub struct RecoveryMgr {
    lm: Arc<RefCell<LogMgr>>,
    bm: Arc<RefCell<BufferMgr>>,
    tx: Transaction,
    txnum: i32,
}

impl RecoveryMgr {
    pub fn new(
        tx: Transaction,
        txnum: i32,
        lm: Arc<RefCell<LogMgr>>,
        bm: Arc<RefCell<BufferMgr>>,
    ) -> Result<Self> {
        StartRecord::write_to_log(Arc::clone(&lm), txnum)?;

        Ok(Self { lm, bm, tx, txnum })
    }
    pub fn commit(&mut self) -> Result<()> {
        self.bm.borrow_mut().flush_all(self.txnum)?;
        let lsn = CommitRecord::write_to_log(Arc::clone(&self.lm), self.txnum)?;
        self.lm.borrow_mut().flush(lsn)
    }
    pub fn rollback(&mut self) -> Result<()> {
        self.do_rollback()?;
        self.bm.borrow_mut().flush_all(self.txnum)?;
        let lsn = RollbackRecord::write_to_log(Arc::clone(&self.lm), self.txnum)?;
        self.lm.borrow_mut().flush(lsn)
    }
    pub fn recover(&mut self) -> Result<()> {
        self.do_recover()?;
        self.bm.borrow_mut().flush_all(self.txnum)?;
        let lsn = CheckpointRecord::write_to_log(Arc::clone(&self.lm))?;
        self.lm.borrow_mut().flush(lsn)
    }
    pub fn set_i32(&mut self, buff: &mut Buffer, offset: i32, _new_val: i32) -> Result<u64> {
        let old_val = buff.contents().get_i32(offset as usize)?;
        if let Some(blk) = buff.block() {
            return SetI32Record::write_to_log(
                Arc::clone(&self.lm),
                self.txnum,
                blk,
                offset,
                old_val,
            );
        }

        Err(From::from(RecoveryMgrError::BufferFailed(
            "set_i32".to_string(),
        )))
    }
    pub fn set_string(&mut self, buff: &mut Buffer, offset: i32, _new_val: &str) -> Result<u64> {
        let old_val = buff.contents().get_string(offset as usize)?;
        if let Some(blk) = buff.block() {
            return SetStringRecord::write_to_log(
                Arc::clone(&self.lm),
                self.txnum,
                blk,
                offset,
                old_val,
            );
        }

        Err(From::from(RecoveryMgrError::BufferFailed(
            "set_string".to_string(),
        )))
    }
    fn do_rollback(&mut self) -> Result<()> {
        let mut iter = self.lm.borrow_mut().iterator()?;
        while let Some(bytes) = iter.next() {
            let mut rec = logrecord::create_log_record(bytes)?;
            if rec.tx_number() == self.txnum {
                if rec.op() == TxType::START {
                    return Ok(());
                }

                rec.undo(&mut self.tx)?;
            }
        }

        Ok(())
    }
    fn do_recover(&mut self) -> Result<()> {
        let mut finished_txs = vec![];
        let mut iter = self.lm.borrow_mut().iterator()?;
        while let Some(bytes) = iter.next() {
            let mut rec = logrecord::create_log_record(bytes)?;
            match rec.op() {
                TxType::CHECKPOINT => return Ok(()),
                TxType::COMMIT | TxType::ROLLBACK => {
                    finished_txs.push(rec.tx_number());
                }
                _ => {
                    if !finished_txs.contains(&rec.tx_number()) {
                        rec.undo(&mut self.tx)?;
                    }
                }
            }
        }

        Ok(())
    }
}
