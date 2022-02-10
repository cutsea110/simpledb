use core::fmt;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BlockId {
    filename: String,
    blknum: i32,
}

impl BlockId {
    pub fn new(filename: &str, blknum: i32) -> Self {
        Self {
            filename: filename.to_string(),
            blknum,
        }
    }

    pub fn file_name(&self) -> String {
        self.filename.clone()
    }

    pub fn number(&self) -> i32 {
        self.blknum
    }
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[file {}, block {}]", self.filename, self.blknum)
    }
}
