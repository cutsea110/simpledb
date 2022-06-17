use anyhow::Result;
use chrono::NaiveDate;
use core::fmt;
use log::debug;

use crate::record::schema::FieldType;

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

#[derive(Debug, Clone, Ord, PartialOrd, Hash)]
pub enum Constant {
    I16(i16),
    I32(i32),
    String(String),
    Bool(bool),
    Date(NaiveDate),
}
impl PartialEq for Constant {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Constant::I16(l) => match other {
                Constant::I16(r) => *l == *r,
                Constant::I32(r) => *l as i32 == *r,
                _ => false,
            },
            Constant::I32(l) => match other {
                Constant::I16(r) => *l == *r as i32,
                Constant::I32(r) => *l == *r,
                _ => false,
            },
            Constant::String(l) => match other {
                Constant::String(r) => *l == *r,
                Constant::Date(r) => *l == *r.format("%Y-%m-%d").to_string(),
                _ => false,
            },
            Constant::Bool(l) => match other {
                Constant::Bool(r) => *l == *r,
                _ => false,
            },
            Constant::Date(l) => match other {
                Constant::String(r) => *l.format("%Y-%m-%d").to_string() == *r,
                Constant::Date(r) => *l == *r,
                _ => false,
            },
        }
    }
}
impl Eq for Constant {}

impl fmt::Display for Constant {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Constant::I16(ival) => write!(f, "{}", ival),
            Constant::I32(ival) => write!(f, "{}", ival),
            Constant::String(sval) => write!(f, "'{}'", sval),
            Constant::Bool(bval) => write!(f, "{}", bval),
            Constant::Date(dval) => write!(f, "{}", dval.format("%Y-%m-%d")),
        }
    }
}

impl Constant {
    pub fn new_i16(ival: i16) -> Self {
        Constant::I16(ival)
    }
    pub fn new_i32(ival: i32) -> Self {
        Constant::I32(ival)
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
    pub fn as_i16(&self) -> Result<i16> {
        match self {
            Constant::I16(ival) => Ok(*ival),
            Constant::I32(ival) => {
                debug!("try to convert from i32 to i16: {}", *ival);
                i16::try_from(*ival).map_err(|_| From::from(ConstantError::TypeError))
            }
            _ => Err(From::from(ConstantError::TypeError)),
        }
    }
    pub fn as_i32(&self) -> Result<i32> {
        match self {
            Constant::I16(ival) => {
                debug!("convert from i16 to i32: {}", *ival);
                Ok(*ival as i32)
            }
            Constant::I32(ival) => Ok(*ival),
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
            Constant::String(sval) => {
                debug!("try to convert from string to date: {}", sval);
                NaiveDate::parse_from_str(sval, "%Y-%m-%d")
                    .map_err(|_| From::from(ConstantError::TypeError))
            }
            Constant::Date(dval) => Ok(*dval),
            _ => Err(From::from(ConstantError::TypeError)),
        }
    }
    // extends by exercise 3.17
    pub fn as_field_type(&self, fldtype: FieldType) -> Result<Self> {
        match fldtype {
            FieldType::SMALLINT => self.as_i16().map(|x| Constant::I16(x)),
            FieldType::INTEGER => self.as_i32().map(|x| Constant::I32(x)),
            FieldType::VARCHAR => self.as_string().map(|x| Constant::String(x.to_string())),
            FieldType::BOOL => self.as_bool().map(|x| Constant::Bool(x)),
            FieldType::DATE => self.as_date().map(|x| Constant::Date(x)),
        }
    }
}
