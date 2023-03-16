use anyhow::Result;
use chrono::{Datelike, NaiveDate};
use core::fmt;
use itertools::izip;
use std::mem;

#[derive(Debug)]
enum PageError {
    BufferSizeExceeded,
}

impl std::error::Error for PageError {}
impl fmt::Display for PageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PageError::BufferSizeExceeded => write!(f, "buffer size exceeded"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Page {
    bb: Vec<u8>,
}

impl Page {
    pub fn new_from_bytes(b: Vec<u8>) -> Self {
        Self { bb: b }
    }
    pub fn new_from_size(blocksize: usize) -> Self {
        Self {
            bb: vec![0u8; blocksize],
        }
    }
    // extends by exercise 3.17
    pub fn get_u8(&self, offset: usize) -> Result<u8> {
        let u8_size = mem::size_of::<u8>();

        if offset + u8_size - 1 < self.bb.len() {
            let bytes = &self.bb[offset..offset + u8_size];
            Ok(u8::from_be_bytes((*bytes).try_into()?))
        } else {
            Err(From::from(PageError::BufferSizeExceeded))
        }
    }
    // extends by exercise 3.17
    pub fn set_u8(&mut self, offset: usize, n: u8) -> Result<usize> {
        let bytes = n.to_be_bytes();

        if offset + bytes.len() - 1 < self.bb.len() {
            for (b, added) in izip!(&mut self.bb[offset..offset + bytes.len()], &bytes) {
                *b = *added;
            }

            Ok(offset + bytes.len())
        } else {
            Err(From::from(PageError::BufferSizeExceeded))
        }
    }
    // extends by exercise 3.17
    pub fn get_i16(&self, offset: usize) -> Result<i16> {
        let i16_size = mem::size_of::<i16>();

        if offset + i16_size - 1 < self.bb.len() {
            let bytes = &self.bb[offset..offset + i16_size];
            Ok(i16::from_be_bytes((*bytes).try_into()?))
        } else {
            Err(From::from(PageError::BufferSizeExceeded))
        }
    }
    // extends by exercise 3.17
    pub fn set_i16(&mut self, offset: usize, n: i16) -> Result<usize> {
        let bytes = n.to_be_bytes();

        if offset + bytes.len() - 1 < self.bb.len() {
            for (b, added) in izip!(&mut self.bb[offset..offset + bytes.len()], &bytes) {
                *b = *added;
            }

            Ok(offset + bytes.len())
        } else {
            Err(From::from(PageError::BufferSizeExceeded))
        }
    }
    pub fn get_i32(&self, offset: usize) -> Result<i32> {
        let i32_size = mem::size_of::<i32>();

        if offset + i32_size - 1 < self.bb.len() {
            let bytes = &self.bb[offset..offset + i32_size];
            Ok(i32::from_be_bytes((*bytes).try_into()?))
        } else {
            Err(From::from(PageError::BufferSizeExceeded))
        }
    }
    pub fn set_i32(&mut self, offset: usize, n: i32) -> Result<usize> {
        let bytes = n.to_be_bytes();

        if offset + bytes.len() - 1 < self.bb.len() {
            for (b, added) in izip!(&mut self.bb[offset..offset + bytes.len()], &bytes) {
                *b = *added;
            }

            Ok(offset + bytes.len())
        } else {
            Err(From::from(PageError::BufferSizeExceeded))
        }
    }
    // extends by exercise 3.17
    pub fn get_u32(&self, offset: usize) -> Result<u32> {
        let u32_size = mem::size_of::<u32>();

        if offset + u32_size - 1 < self.bb.len() {
            let bytes = &self.bb[offset..offset + u32_size];
            Ok(u32::from_be_bytes((*bytes).try_into()?))
        } else {
            Err(From::from(PageError::BufferSizeExceeded))
        }
    }
    // extends by exercise 3.17
    pub fn set_u32(&mut self, offset: usize, n: u32) -> Result<usize> {
        let bytes = n.to_be_bytes();

        if offset + bytes.len() - 1 < self.bb.len() {
            for (b, added) in izip!(&mut self.bb[offset..offset + bytes.len()], &bytes) {
                *b = *added;
            }

            Ok(offset + bytes.len())
        } else {
            Err(From::from(PageError::BufferSizeExceeded))
        }
    }
    pub fn get_bytes(&self, offset: usize) -> Result<&[u8]> {
        let len = self.get_i32(offset)? as usize;
        let new_offset = offset + mem::size_of::<i32>();

        if new_offset + len - 1 < self.bb.len() {
            Ok(&self.bb[new_offset..new_offset + len])
        } else {
            Err(From::from(PageError::BufferSizeExceeded))
        }
    }
    pub fn set_bytes(&mut self, offset: usize, b: &[u8]) -> Result<usize> {
        if offset + mem::size_of::<i32>() + b.len() - 1 < self.bb.len() {
            let new_offset = self.set_i32(offset, b.len() as i32)?;
            for (p, added) in izip!(&mut self.bb[new_offset..new_offset + b.len()], b) {
                *p = *added
            }

            Ok(new_offset + b.len())
        } else {
            Err(From::from(PageError::BufferSizeExceeded))
        }
    }
    pub fn get_string(&self, offset: usize) -> Result<String> {
        let bytes = self.get_bytes(offset)?;
        let s = String::from_utf8(bytes.to_vec())?;

        Ok(s)
    }
    pub fn set_string(&mut self, offset: usize, s: String) -> Result<usize> {
        self.set_bytes(offset, s.as_bytes())
    }
    pub fn max_length(strlen: usize) -> usize {
        mem::size_of::<i32>() + (strlen * mem::size_of::<u8>())
    }
    pub fn contents(&mut self) -> &mut Vec<u8> {
        &mut self.bb
    }
    pub(crate) fn get_bytes_vec(&self, offset: usize) -> Result<Vec<u8>> {
        let len = self.get_i32(offset)? as usize;
        let new_offset = offset + mem::size_of::<i32>();

        if new_offset + len - 1 < self.bb.len() {
            Ok(self.bb[new_offset..new_offset + len].try_into()?)
        } else {
            Err(From::from(PageError::BufferSizeExceeded))
        }
    }
    // extends by exercise 3.17
    // internal representation of bool
    // t: 1
    // f: 0
    pub fn get_bool(&self, offset: usize) -> Result<bool> {
        self.get_u8(offset).map(|n| n != 0)
    }
    // extends by exercise 3.17
    // internal representation of bool
    // t: 1
    // f: 0
    pub fn set_bool(&mut self, offset: usize, b: bool) -> Result<usize> {
        self.set_u8(offset, if b { 1 } else { 0 })
    }
    // extends by exercise 3.17
    // internal representation of date
    // yyyy: u16
    // mm: u8
    // dd: u8
    pub fn get_date(&self, offset: usize) -> Result<NaiveDate> {
        self.get_u32(offset).map(|ymd| {
            let y = ymd >> 16;
            let m = (ymd >> 8) & 255;
            let d = ymd & 255;
            NaiveDate::from_ymd_opt(y as i32, m, d).unwrap()
        })
    }
    // extends by exercise 3.17
    // internal representation of date
    // yyyy: u16
    // mm: u8
    // dd: u8
    pub fn set_date(&mut self, offset: usize, d: NaiveDate) -> Result<usize> {
        let ymd = (((d.year() as u32) << 8) + d.month() << 8) + d.day();
        self.set_u32(offset, ymd)
    }
}
