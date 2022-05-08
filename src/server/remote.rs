use capnp::capability::Promise;
use capnp_rpc::pry;
use core::panic;
use log::{debug, info, trace};
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
};

use super::simpledb::SimpleDB;
use crate::{
    plan::{plan::Plan, planner::Planner},
    query::scan::Scan,
    record::schema::{FieldType, Schema},
    remote_capnp::{self, remote_connection, remote_driver, remote_result_set, remote_statement},
    tx::transaction::Transaction,
};

const MAJOR_VERSION: i32 = 0;
const MINOR_VERSION: i32 = 1;

pub trait Server {
    fn get_database(&mut self, dbname: &str) -> Arc<Mutex<SimpleDB>>;
}

pub struct RemoteDriverImpl {
    major_ver: i32,
    minor_ver: i32,
    server: Arc<Mutex<dyn Server>>,
}

impl RemoteDriverImpl {
    pub fn new(srv: Arc<Mutex<dyn Server>>) -> Self {
        Self {
            major_ver: MAJOR_VERSION,
            minor_ver: MINOR_VERSION,
            server: srv,
        }
    }
}

impl remote_driver::Server for RemoteDriverImpl {
    fn connect(
        &mut self,
        params: remote_driver::ConnectParams,
        mut results: remote_driver::ConnectResults,
    ) -> Promise<(), capnp::Error> {
        trace!("connecting");
        let dbname = pry!(pry!(params.get()).get_dbname());
        info!("connect db: {}", dbname);
        let db = self.server.lock().unwrap().get_database(dbname);
        let conn: remote_connection::Client = capnp_rpc::new_client(RemoteConnectionImpl::new(db));
        results.get().set_conn(conn);
        trace!("connected");

        Promise::ok(())
    }
    fn get_version(
        &mut self,
        _: remote_driver::GetVersionParams,
        mut results: remote_driver::GetVersionResults,
    ) -> Promise<(), capnp::Error> {
        trace!("get version");
        results.get().init_ver().set_major_ver(self.major_ver);
        results.get().init_ver().set_minor_ver(self.minor_ver);
        info!("version: {}.{}", self.major_ver, self.minor_ver);

        Promise::ok(())
    }
}

pub struct ConnectionInternal {
    db: Arc<Mutex<SimpleDB>>,
    current_tx: Arc<Mutex<Transaction>>,
}

pub struct RemoteConnectionImpl {
    conn: Rc<RefCell<ConnectionInternal>>,
}

impl RemoteConnectionImpl {
    pub fn new(db: Arc<Mutex<SimpleDB>>) -> Self {
        let tx = db.lock().unwrap().new_tx().expect("new transaction");
        trace!("tx: {}", tx.tx_num());
        let conn = ConnectionInternal {
            db,
            current_tx: Arc::new(Mutex::new(tx)),
        };

        Self {
            conn: Rc::new(RefCell::new(conn)),
        }
    }
}

impl remote_capnp::remote_connection::Server for RemoteConnectionImpl {
    fn create_statement(
        &mut self,
        params: remote_connection::CreateStatementParams,
        mut results: remote_connection::CreateStatementResults,
    ) -> Promise<(), capnp::Error> {
        trace!("create statement");
        let sql = pry!(pry!(params.get()).get_sql());
        info!("SQL: {}", sql);
        let planner = self
            .conn
            .borrow()
            .db
            .lock()
            .unwrap()
            .planner()
            .expect("planner");
        let stmt: remote_statement::Client = capnp_rpc::new_client(RemoteStatementImpl::new(
            sql,
            planner,
            Arc::clone(&self.conn.borrow().current_tx),
            Rc::clone(&self.conn),
        ));
        results.get().set_stmt(stmt);

        Promise::ok(())
    }
    fn close(
        &mut self,
        _: remote_capnp::remote_connection::CloseParams,
        _: remote_capnp::remote_connection::CloseResults,
    ) -> Promise<(), capnp::Error> {
        let tx_num = self.conn.borrow().current_tx.lock().unwrap().tx_num();
        trace!("close tx: {}", tx_num);
        self.conn
            .borrow()
            .current_tx
            .lock()
            .unwrap()
            .commit()
            .expect("commit transaction");
        debug!("committed");
        // TODO: close

        Promise::ok(())
    }
    fn commit(
        &mut self,
        _: remote_capnp::remote_connection::CommitParams,
        _: remote_capnp::remote_connection::CommitResults,
    ) -> Promise<(), capnp::Error> {
        let tx_num = self.conn.borrow().current_tx.lock().unwrap().tx_num();
        trace!("commit tx: {}", tx_num);
        self.conn
            .borrow()
            .current_tx
            .lock()
            .unwrap()
            .commit()
            .expect("commit transaction");
        debug!("committed");
        let new_tx = self
            .conn
            .borrow()
            .db
            .lock()
            .unwrap()
            .new_tx()
            .expect("new transaction");
        trace!("start new tx: {}", new_tx.tx_num());
        self.conn.borrow_mut().current_tx = Arc::new(Mutex::new(new_tx));

        Promise::ok(())
    }
    fn rollback(
        &mut self,
        _: remote_capnp::remote_connection::RollbackParams,
        _: remote_capnp::remote_connection::RollbackResults,
    ) -> Promise<(), capnp::Error> {
        let tx_num = self.conn.borrow().current_tx.lock().unwrap().tx_num();
        trace!("rollback tx: {}", tx_num);
        self.conn
            .borrow()
            .current_tx
            .lock()
            .unwrap()
            .rollback()
            .expect("rollback transaction");
        debug!("rollbacked");
        let new_tx = self
            .conn
            .borrow()
            .db
            .lock()
            .unwrap()
            .new_tx()
            .expect("new transaction");
        trace!("start new tx: {}", new_tx.tx_num());
        self.conn.borrow_mut().current_tx = Arc::new(Mutex::new(new_tx));

        Promise::ok(())
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

pub struct RemoteStatementImpl {
    sql: String,
    planner: Planner,
    current_tx: Arc<Mutex<Transaction>>,
    conn: Rc<RefCell<ConnectionInternal>>,
}
impl RemoteStatementImpl {
    pub fn new(
        sql: &str,
        planner: Planner,
        tx: Arc<Mutex<Transaction>>,
        conn: Rc<RefCell<ConnectionInternal>>,
    ) -> Self {
        Self {
            sql: sql.to_string(),
            planner,
            current_tx: tx,
            conn,
        }
    }
}

impl remote_capnp::remote_statement::Server for RemoteStatementImpl {
    fn execute_query(
        &mut self,
        _: remote_capnp::remote_statement::ExecuteQueryParams,
        mut results: remote_capnp::remote_statement::ExecuteQueryResults,
    ) -> Promise<(), capnp::Error> {
        trace!("execute query: {}", self.sql);
        let plan = self
            .planner
            .create_query_plan(&self.sql, Arc::clone(&self.current_tx))
            .expect("create query plan");
        trace!("planned");
        let resultset: remote_result_set::Client =
            capnp_rpc::new_client(RemoteResultSetImpl::new(plan));
        results.get().set_result(resultset);

        Promise::ok(())
    }
    fn execute_update(
        &mut self,
        _: remote_capnp::remote_statement::ExecuteUpdateParams,
        mut results: remote_capnp::remote_statement::ExecuteUpdateResults,
    ) -> Promise<(), capnp::Error> {
        trace!("execute update: {}", self.sql);
        let affected = self
            .planner
            .execute_update(&self.sql, Arc::clone(&self.current_tx))
            .expect("execute update");
        results.get().set_affected(affected);

        Promise::ok(())
    }
    fn close(
        &mut self,
        _: remote_capnp::remote_statement::CloseParams,
        _: remote_capnp::remote_statement::CloseResults,
    ) -> Promise<(), capnp::Error> {
        let tx_num = self.conn.borrow().current_tx.lock().unwrap().tx_num();
        trace!("close tx: {}", tx_num);
        if let Ok(mut tx) = self.conn.borrow().current_tx.lock() {
            tx.commit().expect("commit transaction");
            return Promise::ok(());
        }

        Promise::err(::capnp::Error::failed(format!(
            "failed to close tx: {}",
            tx_num
        )))
    }
    fn explain_plan(
        &mut self,
        _: remote_capnp::remote_statement::ExplainPlanParams,
        _: remote_capnp::remote_statement::ExplainPlanResults,
    ) -> Promise<(), capnp::Error> {
        panic!("TODO")
    }
}

pub struct RemoteResultSetImpl {
    scan: Arc<Mutex<dyn Scan>>,
    sch: Arc<Schema>,
}
impl RemoteResultSetImpl {
    pub fn new(plan: Arc<dyn Plan>) -> Self {
        let scan = plan.open().expect("open plan");
        let sch = plan.schema();
        Self { scan, sch }
    }
}

impl remote_capnp::remote_result_set::Server for RemoteResultSetImpl {
    fn next(
        &mut self,
        _: remote_capnp::remote_result_set::NextParams,
        mut results: remote_capnp::remote_result_set::NextResults,
    ) -> Promise<(), capnp::Error> {
        let has_next = self.scan.lock().unwrap().next();
        trace!("next: {}", has_next);
        results.get().set_exists(has_next);

        Promise::ok(())
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
        mut results: remote_capnp::remote_result_set::GetMetadataResults,
    ) -> Promise<(), capnp::Error> {
        trace!("get metadata");
        let meta = results.get().init_metadata();
        let mut schema = meta.init_schema();
        let mut fields = schema
            .reborrow()
            .init_fields(self.sch.fields().len() as u32);
        for i in 0..self.sch.fields().len() {
            let fldname = self.sch.fields()[i].as_bytes();
            fields.set(i as u32, ::capnp::text::new_reader(fldname).unwrap());
        }
        let mut info = schema.reborrow().init_info();
        let mut entries = info
            .reborrow()
            .init_entries(self.sch.info().keys().len() as u32);
        for (i, (k, fi)) in self.sch.info().into_iter().enumerate() {
            let fldname = k.as_bytes();
            entries
                .reborrow()
                .get(i as u32)
                .set_key(::capnp::text::new_reader(fldname).unwrap())
                .unwrap();
            let mut val = entries.reborrow().get(i as u32).init_value();
            val.reborrow().set_length(fi.length as i32);
            let t = match fi.fld_type {
                FieldType::INTEGER => remote_capnp::FieldType::Integer,
                FieldType::VARCHAR => remote_capnp::FieldType::Varchar,
            };
            val.reborrow().set_type(t);
        }

        Promise::ok(())
    }
    fn get_row(
        &mut self,
        _: remote_result_set::GetRowParams,
        mut results: remote_result_set::GetRowResults,
    ) -> Promise<(), capnp::Error> {
        let row = results.get().init_row();
        let mut map = row.init_map();
        let mut entries = map.reborrow().init_entries(self.sch.fields().len() as u32);
        for (i, (k, fi)) in self.sch.info().into_iter().enumerate() {
            let fldname = k.as_bytes();
            entries
                .reborrow()
                .get(i as u32)
                .set_key(::capnp::text::new_reader(fldname).unwrap())
                .unwrap();
            let mut val = entries.reborrow().get(i as u32).init_value();
            match fi.fld_type {
                FieldType::INTEGER => {
                    if let Ok(v) = self.scan.lock().unwrap().get_i32(k) {
                        val.reborrow().set_int32(v);
                    }
                }
                FieldType::VARCHAR => {
                    if let Ok(s) = self.scan.lock().unwrap().get_string(k) {
                        val.reborrow()
                            .set_string(::capnp::text::new_reader(s.as_bytes()).unwrap());
                    }
                }
            }
        }

        Promise::ok(())
    }
    fn get_int32(
        &mut self,
        params: remote_result_set::GetInt32Params,
        mut results: remote_result_set::GetInt32Results,
    ) -> Promise<(), capnp::Error> {
        let fldname = pry!(pry!(params.get()).get_fldname());
        debug!("get int32 value: {}", fldname);
        let val = self
            .scan
            .lock()
            .unwrap()
            .get_i32(fldname)
            .expect("get int32");
        results.get().set_val(val);

        Promise::ok(())
    }
    fn get_string(
        &mut self,
        params: remote_result_set::GetStringParams,
        mut results: remote_result_set::GetStringResults,
    ) -> Promise<(), capnp::Error> {
        let fldname = pry!(pry!(params.get()).get_fldname());
        debug!("get string value: {}", fldname);
        let val = self
            .scan
            .lock()
            .unwrap()
            .get_string(fldname)
            .expect("get string");
        results
            .get()
            .set_val(::capnp::text::new_reader(val.as_bytes()).unwrap()); // TODO

        Promise::ok(())
    }
}
