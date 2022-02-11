use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::{buffer::Buffer, manager::BufferMgr},
    file::block_id::BlockId,
};

#[derive(Debug, Clone)]
pub struct BufferList {
    buffers: HashMap<BlockId, Arc<Mutex<Buffer>>>,
    pins: Vec<BlockId>,
    bm: Arc<Mutex<BufferMgr>>,
}

impl BufferList {
    pub fn new(bm: Arc<Mutex<BufferMgr>>) -> Self {
        Self {
            buffers: HashMap::new(),
            pins: vec![],
            bm,
        }
    }
    pub fn get_bufer(&mut self, blk: &BlockId) -> Option<&Arc<Mutex<Buffer>>> {
        self.buffers.get(blk)
    }
    pub fn pin(&mut self, blk: &BlockId) -> Result<()> {
        let buff = self.bm.lock().unwrap().pin(blk)?;
        self.buffers.insert(blk.clone(), buff);
        self.pins.push(blk.clone());

        Ok(())
    }
    pub fn unpin(&mut self, blk: &BlockId) -> Result<()> {
        if let Some(buff) = self.buffers.get(blk) {
            self.bm.lock().unwrap().unpin(buff.clone())?;
            self.pins.retain(|x| x == blk);
            if self.pins.contains(blk) {
                self.buffers.remove(blk);
            }
        }

        Ok(())
    }
    pub fn unpin_all(&mut self) -> Result<()> {
        for blk in self.pins.iter() {
            if let Some(buff) = self.buffers.get(blk) {
                self.bm.lock().unwrap().unpin(buff.clone())?;
            }
        }
        self.buffers.clear();
        self.pins.clear();

        Ok(())
    }
}
