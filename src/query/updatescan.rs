use std::sync::{Arc, Mutex};

use anyhow::Result;

use super::{constant::Constant, scan::Scan};
use crate::record::rid::RID;

pub trait UpdateScan: Scan {
    fn set_i32(&mut self, fldname: &str, val: i32) -> Result<()>;
    fn set_string(&mut self, fldname: &str, val: String) -> Result<()>;
    fn set_val(&mut self, fldname: &str, val: Constant) -> Result<()>;
    fn insert(&mut self) -> Result<()>;
    fn delete(&mut self) -> Result<()>;
    fn get_rid(&self) -> Result<RID>;
    fn move_to_rid(&mut self, rid: RID) -> Result<()>;

    fn to_scan(&self) -> Result<Arc<Mutex<dyn Scan>>>;
}
