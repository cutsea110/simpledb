use anyhow::Result;
use chrono::NaiveDate;
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
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    String(String),
    Bool(bool),
    Date(NaiveDate),
}

impl fmt::Display for Constant {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Constant::I8(ival) => write!(f, "{}", ival),
            Constant::U8(ival) => write!(f, "{}", ival),
            Constant::I16(ival) => write!(f, "{}", ival),
            Constant::U16(ival) => write!(f, "{}", ival),
            Constant::I32(ival) => write!(f, "{}", ival),
            Constant::U32(ival) => write!(f, "{}", ival),
            Constant::String(sval) => write!(f, "'{}'", sval),
            Constant::Bool(bval) => write!(f, "{}", bval),
            Constant::Date(dval) => write!(f, "{}", dval.format("%Y-%m-%d")),
        }
    }
}

impl Constant {
    pub fn new_i8(ival: i8) -> Self {
        Constant::I8(ival)
    }
    pub fn new_u8(ival: u8) -> Self {
        Constant::U8(ival)
    }
    pub fn new_i16(ival: i16) -> Self {
        Constant::I16(ival)
    }
    pub fn new_u16(ival: u16) -> Self {
        Constant::U16(ival)
    }
    pub fn new_i32(ival: i32) -> Self {
        Constant::I32(ival)
    }
    pub fn new_u32(ival: u32) -> Self {
        Constant::U32(ival)
    }
    pub fn new_string(sval: String) -> Self {
        Constant::String(sval)
    }
    pub fn new_bool(bval: bool) -> Self {
        Constant::Bool(bval)
    }
    pub fn new_date(dval: NaiveDate) -> Self {
        Constant::Date(dval)
    }
    pub fn as_i8(&self) -> Result<i8> {
        match self {
            Constant::I8(ival) => Ok(*ival),
            _ => Err(From::from(ConstantError::TypeError)),
        }
    }
    pub fn as_u8(&self) -> Result<u8> {
        match self {
            Constant::U8(ival) => Ok(*ival),
            _ => Err(From::from(ConstantError::TypeError)),
        }
    }
    pub fn as_i16(&self) -> Result<i16> {
        match self {
            Constant::I16(ival) => Ok(*ival),
            _ => Err(From::from(ConstantError::TypeError)),
        }
    }
    pub fn as_u16(&self) -> Result<u16> {
        match self {
            Constant::U16(ival) => Ok(*ival),
            _ => Err(From::from(ConstantError::TypeError)),
        }
    }
    pub fn as_i32(&self) -> Result<i32> {
        match self {
            Constant::I32(ival) => Ok(*ival),
            _ => Err(From::from(ConstantError::TypeError)),
        }
    }
    pub fn as_u32(&self) -> Result<u32> {
        match self {
            Constant::U32(ival) => Ok(*ival),
            _ => Err(From::from(ConstantError::TypeError)),
        }
    }
    pub fn as_string(&self) -> Result<&str> {
        match self {
            Constant::String(sval) => Ok(sval),
            _ => Err(From::from(ConstantError::TypeError)),
        }
    }
    pub fn as_bool(&self) -> Result<bool> {
        match self {
            Constant::Bool(bval) => Ok(*bval),
            _ => Err(From::from(ConstantError::TypeError)),
        }
    }
    pub fn as_date(&self) -> Result<NaiveDate> {
        match self {
            Constant::Date(dval) => Ok(*dval),
            _ => Err(From::from(ConstantError::TypeError)),
        }
    }
}
