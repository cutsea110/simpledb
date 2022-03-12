use anyhow::Result;
use core::fmt;
use serde::{Deserialize, Serialize};

use super::connection::RemoteConnection;
use crate::rdbc::driveradapter::DriverAdapter;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteDriver {}

impl fmt::Display for RemoteDriver {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl RemoteDriver {
    pub fn new() -> Self {
        Self {}
    }
}

impl<'a> DriverAdapter<'a> for RemoteDriver {
    type Con = RemoteConnection;

    fn connect(&self, url: &str) -> Result<Self::Con> {
        panic!("TODO")
    }
    fn get_major_version(&self) -> i32 {
        0
    }
    fn get_minor_version(&self) -> i32 {
        1
    }
}
