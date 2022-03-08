use anyhow::Result;
use core::fmt;
use std::{cell::RefCell, rc::Rc};

use super::connectionadapter::ConnectionAdapter;

#[derive(Debug)]
pub enum DriverError {
    ConnectFailed,
}

impl std::error::Error for DriverError {}
impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DriverError::ConnectFailed => {
                write!(f, "failed to connect database")
            }
        }
    }
}

pub trait DriverAdapter {
    fn connect(&self, url: &str) -> Result<Rc<RefCell<dyn ConnectionAdapter>>>;
    fn get_major_version(&self) -> i32;
    fn get_minor_version(&self) -> i32;
}
