use anyhow::Result;
use std::{cell::RefCell, collections::HashMap, ops::Deref, sync::Arc};

use crate::{
    buffer::{buffer::Buffer, manager::BufferMgr},
    file::block_id::BlockId,
};

pub struct BufferList {
    buffers: HashMap<BlockId, Arc<RefCell<Buffer>>>,
    pins: Vec<BlockId>,
    bm: BufferMgr,
}

impl BufferList {
    pub fn new(bm: BufferMgr) -> Self {
        Self {
            buffers: HashMap::new(),
            pins: vec![],
            bm,
        }
    }
    fn get_bufer(&mut self, blk: &BlockId) -> Option<&mut Arc<RefCell<Buffer>>> {
        self.buffers.get_mut(blk)
    }
    fn pin(&mut self, blk: BlockId) -> Result<()> {
        let buff = self.bm.pin(&blk)?;
        self.buffers.insert(blk.clone(), buff);
        self.pins.push(blk);

        Ok(())
    }
    fn unpin(&mut self, blk: &BlockId) -> Result<()> {
        if let Some(buff) = self.buffers.get(blk) {
            self.bm.unpin(buff.clone())?;
            self.pins.retain(|x| x == blk);
            if self.pins.contains(blk) {
                self.buffers.remove(blk);
            }
        }
        Ok(())
    }
}
