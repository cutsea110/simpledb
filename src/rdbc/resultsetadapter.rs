use anyhow::Result;
use core::fmt;
use std::{cell::RefCell, rc::Rc};

use super::resultsetmetadataadapter::ResultSetMetaDataAdapter;

#[derive(Debug)]
pub enum ResultSetError {}

impl std::error::Error for ResultSetError {}
impl fmt::Display for ResultSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("TODO")
    }
}

pub trait ResultSetAdapter {
    fn next(&self) -> Result<bool>;
    fn get_i32(&self, fldname: &str) -> Result<i32>;
    fn get_string(&self, fldname: &str) -> Result<String>;
    fn get_meta_data(&self) -> Result<Rc<RefCell<dyn ResultSetMetaDataAdapter>>>;
}
