use anyhow::Result;

use super::connection::NetworkConnection;
use crate::rdbc::driveradapter::DriverAdapter;

pub struct NetworkDriver {
    // TODO
}
impl NetworkDriver {
    pub fn new() -> Self {
        panic!("TODO")
    }
}
impl<'a> DriverAdapter<'a> for NetworkDriver {
    type Con = NetworkConnection;

    fn connect(&self, url: &str) -> Result<Self::Con> {
        panic!("TODO")
    }
    fn get_major_version(&self) -> i32 {
        panic!("TODO")
    }
    fn get_minor_version(&self) -> i32 {
        panic!("TODO")
    }
}
