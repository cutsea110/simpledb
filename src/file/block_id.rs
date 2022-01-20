use core::fmt;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BlockId {
    filename: String,
    blknum: u64,
}

impl BlockId {
    pub fn new(filename: &str, blknum: u64) -> Self {
        Self {
            filename: filename.to_string(),
            blknum,
        }
    }

    pub fn file_name(&self) -> String {
        self.filename.clone()
    }

    pub fn number(&self) -> u64 {
        self.blknum
    }
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[file {}, block {}]", self.filename, self.blknum)
    }
}
