use crate::remote_capnp::{self, remote_driver};
use capnp::capability::Promise;

pub struct RemoteDriverImpl;

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
        mut results: remote_driver::GetVersionResults,
    ) -> Promise<(), capnp::Error> {
        results.get().init_ver().set_major_ver(0);
        results.get().init_ver().set_minor_ver(1);

        Promise::ok(())
    }
}

pub struct RemoteConnectionImpl;

impl remote_capnp::remote_connection::Server for RemoteConnectionImpl {
    fn create(
        &mut self,
        _: remote_capnp::remote_connection::CreateParams,
        _: remote_capnp::remote_connection::CreateResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn close(
        &mut self,
        _: remote_capnp::remote_connection::CloseParams,
        _: remote_capnp::remote_connection::CloseResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn commit(
        &mut self,
        _: remote_capnp::remote_connection::CommitParams,
        _: remote_capnp::remote_connection::CommitResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn rollback(
        &mut self,
        _: remote_capnp::remote_connection::RollbackParams,
        _: remote_capnp::remote_connection::RollbackResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_table_schema(
        &mut self,
        _: remote_capnp::remote_connection::GetTableSchemaParams,
        _: remote_capnp::remote_connection::GetTableSchemaResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_view_definition(
        &mut self,
        _: remote_capnp::remote_connection::GetViewDefinitionParams,
        _: remote_capnp::remote_connection::GetViewDefinitionResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_index_info(
        &mut self,
        _: remote_capnp::remote_connection::GetIndexInfoParams,
        _: remote_capnp::remote_connection::GetIndexInfoResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
}

pub struct RemoteStatementImpl;

impl remote_capnp::remote_statement::Server for RemoteStatementImpl {
    fn execute_query(
        &mut self,
        _: remote_capnp::remote_statement::ExecuteQueryParams,
        _: remote_capnp::remote_statement::ExecuteQueryResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn execute_update(
        &mut self,
        _: remote_capnp::remote_statement::ExecuteUpdateParams,
        _: remote_capnp::remote_statement::ExecuteUpdateResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn close(
        &mut self,
        _: remote_capnp::remote_statement::CloseParams,
        _: remote_capnp::remote_statement::CloseResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn explain_plan(
        &mut self,
        _: remote_capnp::remote_statement::ExplainPlanParams,
        _: remote_capnp::remote_statement::ExplainPlanResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
}

pub struct RemoteResultSetImpl;

impl remote_capnp::remote_result_set::Server for RemoteResultSetImpl {
    fn next(
        &mut self,
        _: remote_capnp::remote_result_set::NextParams,
        _: remote_capnp::remote_result_set::NextResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn close(
        &mut self,
        _: remote_capnp::remote_result_set::CloseParams,
        _: remote_capnp::remote_result_set::CloseResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_metadata(
        &mut self,
        _: remote_capnp::remote_result_set::GetMetadataParams,
        _: remote_capnp::remote_result_set::GetMetadataResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
    fn get_next_record(
        &mut self,
        _: remote_capnp::remote_result_set::GetNextRecordParams,
        _: remote_capnp::remote_result_set::GetNextRecordResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
}
