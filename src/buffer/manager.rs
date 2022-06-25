use anyhow::Result;
use core::fmt;
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use super::buffer::Buffer;
use crate::file::block_id::BlockId;

// implements of BufferMgr trait
pub mod naive;

const MAX_TIME: i64 = 10_000; // 10 seconds

#[derive(Debug)]
enum BufferMgrError {
    BufferAbort,
}

impl std::error::Error for BufferMgrError {}
impl fmt::Display for BufferMgrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BufferMgrError::BufferAbort => {
                write!(f, "buffer abort")
            }
        }
    }
}

pub trait BufferMgr {
    fn available(&self) -> usize;
    fn flush_all(&mut self, txnum: i32) -> Result<()>;
    fn unpin(&mut self, buff: Arc<Mutex<Buffer>>) -> Result<()>;
    fn pin(&mut self, blk: &BlockId) -> Result<Arc<Mutex<Buffer>>>;
}

impl Debug for dyn BufferMgr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BufferMgr")
    }
}

#[cfg(test)]
mod tests {
    use crate::server::simpledb::SimpleDB;

    use super::*;

    use anyhow::Result;
    use std::fs;
    use std::path::Path;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/buffermgrtest").exists() {
            fs::remove_dir_all("_test/buffermgrtest")?;
        }

        let simpledb = SimpleDB::new_with("_test/buffermgrtest", 400, 3);

        let bm = simpledb.buffer_mgr();
        let mut bm = bm.lock().unwrap();

        let mut buff: Vec<Option<Arc<Mutex<Buffer>>>> = vec![None; 6];
        buff[0] = bm.pin(&BlockId::new("testfile", 0))?.into();
        buff[1] = bm.pin(&BlockId::new("testfile", 1))?.into();
        buff[2] = bm.pin(&BlockId::new("testfile", 2))?.into();
        bm.unpin(Arc::clone(&buff[1].clone().unwrap()))?;
        buff[1] = None;

        buff[3] = bm.pin(&BlockId::new("testfile", 0))?.into();
        buff[4] = bm.pin(&BlockId::new("testfile", 1))?.into();
        println!("Available buffers: {:?}", bm.available());

        println!("Attempting to pin block 3...");
        if let Ok(_) = bm.pin(&BlockId::new("testfile", 3)) {
            // couldn't come here!
            println!("Succeed!");
        } else {
            println!("Failed!");
        }
        bm.unpin(Arc::clone(&buff[2].clone().unwrap()))?;
        buff[2] = None;
        buff[5] = bm.pin(&BlockId::new("testfile", 3))?.into(); // now this works

        println!("Final buffer Allocation:");
        for i in 0..buff.len() {
            if let Some(b) = buff[i].clone() {
                println!(
                    "buff[{:?}] pinned to block {:?}",
                    i,
                    b.lock().unwrap().block()
                );
            }
        }

        Ok(())
    }
}
