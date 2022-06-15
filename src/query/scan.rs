use anyhow::Result;
use chrono::NaiveDate;

use super::{constant::Constant, updatescan::UpdateScan};
use crate::{materialize::sortscan::SortScan, record::tablescan::TableScan};

pub trait Scan {
    fn before_first(&mut self) -> Result<()>;
    fn next(&mut self) -> bool;
    fn get_i8(&mut self, fldname: &str) -> Result<i8>;
    fn get_u8(&mut self, fldname: &str) -> Result<u8>;
    fn get_i16(&mut self, fldname: &str) -> Result<i16>;
    fn get_u16(&mut self, fldname: &str) -> Result<u16>;
    fn get_i32(&mut self, fldname: &str) -> Result<i32>;
    fn get_u32(&mut self, fldname: &str) -> Result<u32>;
    fn get_string(&mut self, fldname: &str) -> Result<String>;
    fn get_bool(&mut self, fldname: &str) -> Result<bool>;
    fn get_date(&mut self, fldname: &str) -> Result<NaiveDate>;
    fn get_val(&mut self, fldname: &str) -> Result<Constant>;
    fn has_field(&self, fldname: &str) -> bool;
    fn close(&mut self) -> Result<()>;

    fn to_update_scan(&mut self) -> Result<&mut dyn UpdateScan>;
    fn as_table_scan(&mut self) -> Result<&mut TableScan>;
    fn as_sort_scan(&mut self) -> Result<&mut SortScan>;
}
