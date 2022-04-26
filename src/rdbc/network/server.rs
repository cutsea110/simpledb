use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use capnp::capability::Promise;

use crate::metadata::indexmanager::IndexInfo;
use crate::rdbc::{
    connectionadapter, driveradapter, planrepradapter, resultsetadapter, resultsetmetadataadapter,
    statementadapter,
};
use crate::record::schema::Schema;
use crate::remote_capnp::{
    remote_connection, remote_driver, remote_meta_data, remote_plan_repr, remote_result_set,
    remote_statement,
};
use crate::repr::planrepr::PlanRepr;
use crate::tx::transaction::Transaction;

pub struct RemoteDriverImpl {
    // TODO
}

impl<'a> driveradapter::DriverAdapter<'a> for RemoteDriverImpl {
    type Con = RemoteConnectionImpl;

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

impl remote_driver::Server for RemoteDriverImpl {
    fn connect(
        &mut self,
        _: remote_driver::ConnectParams,
        _: remote_driver::ConnectResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_version(
        &mut self,
        _: remote_driver::GetVersionParams,
        _: remote_driver::GetVersionResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
}

pub struct RemoteConnectionImpl {
    // TODO
}

impl<'a> connectionadapter::ConnectionAdapter<'a> for RemoteConnectionImpl {
    type Stmt = RemoteStatementImpl;

    fn create(&'a mut self, sql: &str) -> Result<Self::Stmt> {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn commit(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn rollback(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn get_transaction(&self) -> Arc<Mutex<Transaction>> {
        panic!("TODO")
    }
    fn get_table_schema(&self, tblname: &str) -> Result<Arc<Schema>> {
        panic!("TODO")
    }
    fn get_view_definition(&self, viewname: &str) -> Result<(String, String)> {
        panic!("TODO")
    }
    fn get_index_info(&self, tblname: &str) -> Result<HashMap<String, IndexInfo>> {
        panic!("TODO")
    }
}

impl remote_connection::Server for RemoteConnectionImpl {
    fn create(
        &mut self,
        _: remote_connection::CreateParams,
        _: remote_connection::CreateResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn close(
        &mut self,
        _: remote_connection::CloseParams,
        _: remote_connection::CloseResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn commit(
        &mut self,
        _: remote_connection::CommitParams,
        _: remote_connection::CommitResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn rollback(
        &mut self,
        _: remote_connection::RollbackParams,
        _: remote_connection::RollbackResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_transaction(
        &mut self,
        _: remote_connection::GetTransactionParams,
        _: remote_connection::GetTransactionResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_table_schema(
        &mut self,
        _: remote_connection::GetTableSchemaParams,
        _: remote_connection::GetTableSchemaResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_view_definition(
        &mut self,
        _: remote_connection::GetViewDefinitionParams,
        _: remote_connection::GetViewDefinitionResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_index_info(
        &mut self,
        _: remote_connection::GetIndexInfoParams,
        _: remote_connection::GetIndexInfoResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
}

pub struct RemoteStatementImpl {
    // TODO
}

impl<'a> statementadapter::StatementAdapter<'a> for RemoteStatementImpl {
    type Set = RemoteResultSetImpl;
    type PlanRepr = RemotePlanReprImpl;

    fn execute_query(&'a mut self) -> Result<Self::Set> {
        panic!("TODO")
    }
    fn execute_update(&mut self) -> Result<i32> {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn explain_plan(&mut self) -> Result<Self::PlanRepr> {
        panic!("TODO")
    }
}

impl remote_statement::Server for RemoteStatementImpl {
    fn execute_query(
        &mut self,
        _: remote_statement::ExecuteQueryParams,
        _: remote_statement::ExecuteQueryResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn execute_update(
        &mut self,
        _: remote_statement::ExecuteUpdateParams,
        _: remote_statement::ExecuteUpdateResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn close(
        &mut self,
        _: remote_statement::CloseParams,
        _: remote_statement::CloseResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn explain_plan(
        &mut self,
        _: remote_statement::ExplainPlanParams,
        _: remote_statement::ExplainPlanResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
}

pub struct RemoteResultSetImpl {
    // TODO
}

impl resultsetadapter::ResultSetAdapter for RemoteResultSetImpl {
    type Meta = RemoteMetaDataImpl;

    fn next(&self) -> bool {
        panic!("TODO")
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        panic!("TODO")
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        panic!("TODO")
    }
    fn get_meta_data(&self) -> Result<Self::Meta> {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
}

impl remote_result_set::Server for RemoteResultSetImpl {
    fn next(
        &mut self,
        _: remote_result_set::NextParams,
        _: remote_result_set::NextResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn close(
        &mut self,
        _: remote_result_set::CloseParams,
        _: remote_result_set::CloseResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_i32(
        &mut self,
        _: remote_result_set::GetI32Params,
        _: remote_result_set::GetI32Results,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_string(
        &mut self,
        _: remote_result_set::GetStringParams,
        _: remote_result_set::GetStringResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_meta_data(
        &mut self,
        _: remote_result_set::GetMetaDataParams,
        _: remote_result_set::GetMetaDataResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
}

pub struct RemoteMetaDataImpl {
    // TODO
}

impl resultsetmetadataadapter::ResultSetMetaDataAdapter for RemoteMetaDataImpl {
    fn get_column_count(&self) -> usize {
        panic!("TODO")
    }
    fn get_column_name(&self, column: usize) -> Option<&String> {
        panic!("TODO")
    }
    fn get_column_type(&self, column: usize) -> Option<resultsetmetadataadapter::DataType> {
        panic!("TODO")
    }
    fn get_column_display_size(&self, column: usize) -> Option<usize> {
        panic!("TODO")
    }
}

impl remote_meta_data::Server for RemoteMetaDataImpl {
    fn get_column_count(
        &mut self,
        _: remote_meta_data::GetColumnCountParams,
        _: remote_meta_data::GetColumnCountResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_column_name(
        &mut self,
        _: remote_meta_data::GetColumnNameParams,
        _: remote_meta_data::GetColumnNameResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_column_type(
        &mut self,
        _: remote_meta_data::GetColumnTypeParams,
        _: remote_meta_data::GetColumnTypeResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_column_display_size(
        &mut self,
        _: remote_meta_data::GetColumnDisplaySizeParams,
        _: remote_meta_data::GetColumnDisplaySizeResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
}

pub struct RemotePlanReprImpl {
    // TODO
}

impl planrepradapter::PlanReprAdapter for RemotePlanReprImpl {
    fn repr(&self) -> Arc<dyn PlanRepr> {
        panic!("TODO")
    }
}

impl remote_plan_repr::Server for RemotePlanReprImpl {
    fn operation(
        &mut self,
        _: remote_plan_repr::OperationParams,
        _: remote_plan_repr::OperationResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn reads(
        &mut self,
        _: remote_plan_repr::ReadsParams,
        _: remote_plan_repr::ReadsResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn writes(
        &mut self,
        _: remote_plan_repr::WritesParams,
        _: remote_plan_repr::WritesResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn sub_plan_reprs(
        &mut self,
        _: remote_plan_repr::SubPlanReprsParams,
        _: remote_plan_repr::SubPlanReprsResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
}
