use std::sync::{Arc, Mutex};

use crate::{
    parser::{
        createindexdata::CreateIndexData, createtabledata::CreateTableData,
        createviewdata::CreateViewData, deletedata::DeleteData, insertdata::InsertData,
        modifydata::ModifyData,
    },
    tx::transaction::Transaction,
};

pub trait UpdatePlanner {
    fn execute_insert(data: InsertData, tx: Arc<Mutex<Transaction>>) -> i32;
    fn execute_delete(data: DeleteData, tx: Arc<Mutex<Transaction>>) -> i32;
    fn execute_modify(data: ModifyData, tx: Arc<Mutex<Transaction>>) -> i32;
    fn execute_create_table(data: CreateTableData, tx: Arc<Mutex<Transaction>>) -> i32;
    fn execute_create_view(data: CreateViewData, tx: Arc<Mutex<Transaction>>) -> i32;
    fn execute_create_index(data: CreateIndexData, tx: Arc<Mutex<Transaction>>) -> i32;
}
