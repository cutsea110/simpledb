use super::{ddl::DDL, dml::DML};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SQL {
    DDL(DDL),
    DML(DML),
}
