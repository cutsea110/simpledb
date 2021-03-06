use anyhow::Result;
use core::fmt;

use super::statementadapter::StatementAdapter;

#[derive(Debug)]
pub enum ConnectionError {
    CreateStatementFailed,
    StartNewTransactionFailed,
    CommitFailed,
    RollbackFailed,
    CloseFailed,
}

impl std::error::Error for ConnectionError {}
impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConnectionError::CreateStatementFailed => {
                write!(f, "failed to create statement")
            }
            ConnectionError::StartNewTransactionFailed => {
                write!(f, "failed to start new transaction")
            }
            ConnectionError::CommitFailed => {
                write!(f, "failed to commit")
            }
            ConnectionError::RollbackFailed => {
                write!(f, "failed to rollback")
            }
            ConnectionError::CloseFailed => {
                write!(f, "failed to close")
            }
        }
    }
}

pub trait ConnectionAdapter<'a> {
    type Stmt: StatementAdapter<'a>;
    type Res;

    fn create_statement(&'a mut self, sql: &str) -> Result<Self::Stmt>;
    fn close(&mut self) -> Result<Self::Res>;
}
