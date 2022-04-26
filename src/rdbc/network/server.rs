use capnp::capability::Promise;

use crate::remote_capnp::{
    remote_connection, remote_driver, remote_meta_data, remote_result_set, remote_statement,
};

pub struct RemoteDriverImpl {
    // TODO
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
