use anyhow::Result;
use chrono::NaiveDate;
use std::sync::{Arc, Mutex};

use super::{constant::Constant, scan::Scan};
use crate::record::rid::RID;

pub trait UpdateScan: Scan {
    fn set_i8(&mut self, fldname: &str, val: i8) -> Result<()>;
    fn set_u8(&mut self, fldname: &str, val: u8) -> Result<()>;
    fn set_i16(&mut self, fldname: &str, val: i16) -> Result<()>;
    fn set_u16(&mut self, fldname: &str, val: u16) -> Result<()>;
    fn set_i32(&mut self, fldname: &str, val: i32) -> Result<()>;
    fn set_u32(&mut self, fldname: &str, val: u32) -> Result<()>;
    fn set_string(&mut self, fldname: &str, val: String) -> Result<()>;
    fn set_bool(&mut self, fldname: &str, val: bool) -> Result<()>;
    fn set_date(&mut self, fldname: &str, val: NaiveDate) -> Result<()>;
    fn set_val(&mut self, fldname: &str, val: Constant) -> Result<()>;
    fn insert(&mut self) -> Result<()>;
    fn delete(&mut self) -> Result<()>;
    fn get_rid(&self) -> Result<RID>;
    fn move_to_rid(&mut self, rid: RID) -> Result<()>;

    fn to_scan(&self) -> Result<Arc<Mutex<dyn Scan>>>;
}
