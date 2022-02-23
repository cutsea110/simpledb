use anyhow::Result;
use core::fmt;

#[derive(Debug)]
pub enum ConstantError {
    TypeError,
}

impl std::error::Error for ConstantError {}
impl fmt::Display for ConstantError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::TypeError => {
                write!(f, "type error")
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Constant {
    I32(i32),
    String(String),
}

impl fmt::Display for Constant {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Constant::I32(ival) => write!(f, "{}", ival),
            Constant::String(sval) => write!(f, "{}", sval),
        }
    }
}

impl Constant {
    pub fn new_i32(ival: i32) -> Self {
        Constant::I32(ival)
    }
    pub fn new_string(sval: String) -> Self {
        Constant::String(sval)
    }
    pub fn as_i32(&self) -> Result<i32> {
        match self {
            Constant::I32(ival) => Ok(*ival),
            Constant::String(_) => Err(From::from(ConstantError::TypeError)),
        }
    }
    pub fn as_string(&self) -> Result<&str> {
        match self {
            Constant::I32(_) => Err(From::from(ConstantError::TypeError)),
            Constant::String(sval) => Ok(sval),
        }
    }
}
