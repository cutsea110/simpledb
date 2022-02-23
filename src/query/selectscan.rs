use anyhow::Result;
use std::sync::Arc;

use super::{predicate::Predicate, scan::Scan, updatescan::UpdateScan};
use crate::{query::constant::Constant, record::rid::RID};

pub struct SelectScan {
    s: Arc<dyn Scan>,
    pred: Predicate,
}

impl Scan for SelectScan {
    fn before_first(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn next(&mut self) -> bool {
        panic!("TODO")
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        panic!("TODO")
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        panic!("TODO")
    }
    fn get_val(&mut self, fldname: &str) -> Constant {
        panic!("TODO")
    }
    fn has_field(&self, fldname: &str) -> bool {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
}

impl UpdateScan for SelectScan {
    fn set_i32(&mut self, fldname: &str, val: i32) -> Result<()> {
        panic!("TODO")
    }
    fn set_string(&mut self, fldname: &str, val: String) -> Result<()> {
        panic!("TODO")
    }
    fn set_val(&mut self, fldname: &str, val: Constant) -> Result<()> {
        panic!("TODO")
    }
    fn insert(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn delete(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn get_rid(&self) -> RID {
        panic!("TODO")
    }
    fn move_to_rid(&mut self, rid: RID) -> Result<()> {
        panic!("TODO")
    }
}

impl SelectScan {
    pub fn new(s: Arc<dyn Scan>, pred: Predicate) -> Self {
        Self { s, pred }
    }
}
