use anyhow::Result;
use std::alloc::Layout;

use super::{recordpage::RecordPage, rid::RID};
use crate::{query::constant::Constant, tx::transaction::Transaction};

pub struct TableScan {
    tx: Transaction,
    layout: Layout,
    rp: RecordPage,
    filename: String,
    currentslot: i32,
}

impl TableScan {
    pub fn new(tx: Transaction, tblname: String, layout: Layout) -> Self {
        panic!("TODO")
    }
    // TODO: Methods that implement Scan trait
    pub fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn before_first(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn next(&self) -> bool {
        panic!("TODO")
    }
    pub fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        panic!("TODO")
    }
    pub fn get_string(&mut self, fldname: &str) -> Result<String> {
        panic!("TODO")
    }
    pub fn get_val(&self, fldname: &str) -> Constant {
        panic!("TODO")
    }
    pub fn has_field(&self, fldname: &str) -> bool {
        panic!("TODO")
    }
    pub fn set_i32(&mut self, fldname: &str, val: i32) -> Result<()> {
        panic!("TODO")
    }
    pub fn set_string(&mut self, fldname: &str, val: String) -> Result<()> {
        panic!("TODO")
    }
    pub fn set_val(&mut self, fldname: &str, val: Constant) -> Result<()> {
        panic!("TODO")
    }
    pub fn insert(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn delete(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn move_to_ird(&mut self, ird: RID) -> Result<()> {
        panic!("TODO")
    }
    pub fn get_rid(&self) -> RID {
        panic!("TODO")
    }
    fn move_to_block(&mut self, blknum: i32) -> Result<()> {
        panic!("TODO")
    }
    fn move_to_new_block(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn at_last_block(&self) -> bool {
        panic!("TODO")
    }
}
