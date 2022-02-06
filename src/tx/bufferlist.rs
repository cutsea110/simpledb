use std::collections::HashMap;

use crate::{
    buffer::{buffer::Buffer, manager::BufferMgr},
    file::block_id::BlockId,
};

pub struct BufferList {
    buffers: HashMap<BlockId, Buffer>,
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
    fn get_bufer(&self, blk: &BlockId) -> Option<&Buffer> {
        self.buffers.get(blk)
    }
}
