use super::{
    deletedata::DeleteData, insertdata::InsertData, modifydata::ModifyData, querydata::QueryData,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DML {
    Query(QueryData),
    Insert(InsertData),
    Delete(DeleteData),
    Modify(ModifyData),
}
