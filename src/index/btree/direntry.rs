use crate::query::constant::Constant;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DirEntry {
    dataval: Constant,
    blknum: i32,
}

impl DirEntry {
    pub fn new(dataval: Constant, blknum: i32) -> Self {
        Self { dataval, blknum }
    }
    pub fn data_val(&self) -> &Constant {
        &self.dataval
    }
    pub fn block_number(&self) -> i32 {
        self.blknum
    }
}
