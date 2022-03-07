use rdbc::{Result, ResultSet};

pub trait ResultSetAdapter: ResultSet {
    fn close(&mut self) -> Result<()>;
}
