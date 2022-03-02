use super::{
    createindexdata::CreateIndexData, createtabledata::CreateTableData,
    createviewdata::CreateViewData,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DDL {
    Table(CreateTableData),
    View(CreateViewData),
    Index(CreateIndexData),
}
