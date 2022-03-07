use rdbc::{Result, Statement};

pub trait StatementAdapter: Statement {
    fn close(&mut self) -> Result<()>;
}
