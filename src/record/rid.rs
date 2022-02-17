use core::fmt;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RID {
    blknum: i32,
    slot: i32,
}

impl fmt::Display for RID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{}, {}]", self.blknum, self.slot)
    }
}

impl RID {
    pub fn new(blknum: i32, slot: i32) -> Self {
        Self { blknum, slot }
    }
    pub fn block_number(&self) -> i32 {
        self.blknum
    }
    pub fn slot(&self) -> i32 {
        self.slot
    }
}
