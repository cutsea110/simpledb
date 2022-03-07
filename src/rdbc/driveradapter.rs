use rdbc::Driver;

pub trait DriverAdapter: Driver {
    fn get_major_version(&self) -> i32;
    fn get_minor_version(&self) -> i32;
    // TODO: get_property_info
    // TODO: rdbc_compliant
    // TODO: get_parent_logger
}
