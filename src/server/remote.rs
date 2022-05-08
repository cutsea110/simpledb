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
impl ConnectionInternal {
    pub fn close(&mut self) -> anyhow::Result<()> {
        let tx_num = self.current_tx.lock().unwrap().tx_num();
        trace!("close tx: {}", tx_num);
        self.current_tx.lock().unwrap().commit()
        // TODO: close
    }
    pub fn commit(&mut self) -> anyhow::Result<()> {
        let tx_num = self.current_tx.lock().unwrap().tx_num();
        trace!("commit tx: {}", tx_num);
        self.current_tx.lock().unwrap().commit()
    }
    pub fn rollback(&mut self) -> anyhow::Result<()> {
        let tx_num = self.current_tx.lock().unwrap().tx_num();
        trace!("rollback tx: {}", tx_num);
        self.current_tx.lock().unwrap().rollback()
    }
    pub fn renew_tx(&mut self) -> anyhow::Result<()> {
        let new_tx = self.db.lock().unwrap().new_tx()?;
        trace!("start new tx: {}", new_tx.tx_num());
        self.current_tx = Arc::new(Mutex::new(new_tx));

        Ok(())
    }
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

fn set_schema(sch: Arc<Schema>, schema: &mut remote_capnp::schema::Builder) {
    let mut fields = schema.reborrow().init_fields(sch.fields().len() as u32);
    for i in 0..sch.fields().len() {
        let fldname = sch.fields()[i].as_bytes();
        fields.set(i as u32, ::capnp::text::new_reader(fldname).unwrap());
    }
    let mut info = schema.reborrow().init_info();
    let mut entries = info.reborrow().init_entries(sch.info().keys().len() as u32);
    for (i, (k, fi)) in sch.info().into_iter().enumerate() {
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
        trace!("close");
        self.conn.borrow_mut().close().expect("close");

        Promise::ok(())
    }
    fn commit(
        &mut self,
        _: remote_capnp::remote_connection::CommitParams,
        _: remote_capnp::remote_connection::CommitResults,
    ) -> Promise<(), capnp::Error> {
        trace!("commit");
        self.conn.borrow_mut().commit().expect("commit");
        self.conn.borrow_mut().renew_tx().expect("start new tx");

        Promise::ok(())
    }
    fn rollback(
        &mut self,
        _: remote_capnp::remote_connection::RollbackParams,
        _: remote_capnp::remote_connection::RollbackResults,
    ) -> Promise<(), capnp::Error> {
        trace!("rollback");
        self.conn.borrow_mut().rollback().expect("rollback");
        self.conn.borrow_mut().renew_tx().expect("start new tx");

        Promise::ok(())
    }
    fn get_table_schema(
        &mut self,
        params: remote_capnp::remote_connection::GetTableSchemaParams,
        mut results: remote_capnp::remote_connection::GetTableSchemaResults,
    ) -> Promise<(), capnp::Error> {
        trace!("get table schema");
        let tblname = params.get().unwrap().get_tblname().expect("get table name");
        let sch = self
            .conn
            .borrow()
            .db
            .lock()
            .unwrap()
            .get_table_schema(tblname, Arc::clone(&self.conn.borrow().current_tx))
            .expect("table schema");
        let mut schema = results.get().init_sch();
        set_schema(sch, &mut schema);

        Promise::ok(())
    }
    fn get_view_definition(
        &mut self,
        params: remote_capnp::remote_connection::GetViewDefinitionParams,
        mut results: remote_capnp::remote_connection::GetViewDefinitionResults,
    ) -> Promise<(), capnp::Error> {
        let viewname = params.get().unwrap().get_viewname().expect("get view name");
        panic!("TODO")
    }
    fn get_index_info(
        &mut self,
        params: remote_capnp::remote_connection::GetIndexInfoParams,
        mut results: remote_capnp::remote_connection::GetIndexInfoResults,
    ) -> Promise<(), capnp::Error> {
        let tblname = params.get().unwrap().get_tblname().expect("get table name");
        panic!("TODO")
    }
}

pub struct RemoteStatementImpl {
    sql: String,
    planner: Planner,
    conn: Rc<RefCell<ConnectionInternal>>,
}
impl RemoteStatementImpl {
    pub fn new(sql: &str, planner: Planner, conn: Rc<RefCell<ConnectionInternal>>) -> Self {
        Self {
            sql: sql.to_string(),
            planner,
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
            .create_query_plan(&self.sql, Arc::clone(&self.conn.borrow().current_tx))
            .expect("create query plan");
        trace!("planned");
        let resultset: remote_result_set::Client =
            capnp_rpc::new_client(RemoteResultSetImpl::new(plan, Rc::clone(&self.conn)));
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
            .execute_update(&self.sql, Arc::clone(&self.conn.borrow().current_tx))
            .expect("execute update");
        results.get().set_affected(affected);

        Promise::ok(())
    }
    fn close(
        &mut self,
        _: remote_capnp::remote_statement::CloseParams,
        _: remote_capnp::remote_statement::CloseResults,
    ) -> Promise<(), capnp::Error> {
        self.conn.borrow_mut().close().expect("close");

        Promise::ok(())
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
    conn: Rc<RefCell<ConnectionInternal>>,
}
impl RemoteResultSetImpl {
    pub fn new(plan: Arc<dyn Plan>, conn: Rc<RefCell<ConnectionInternal>>) -> Self {
        let scan = plan.open().expect("open plan");
        let sch = plan.schema();
        Self { scan, sch, conn }
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
        trace!("close");
        self.conn.borrow_mut().close().expect("close");

        Promise::ok(())
    }
    fn get_metadata(
        &mut self,
        _: remote_capnp::remote_result_set::GetMetadataParams,
        mut results: remote_capnp::remote_result_set::GetMetadataResults,
    ) -> Promise<(), capnp::Error> {
        trace!("get metadata");
        let mut schema = results.get().init_metadata().init_schema();
        set_schema(Arc::clone(&self.sch), &mut schema);

        Promise::ok(())
    }
    fn get_row(
        &mut self,
        _: remote_result_set::GetRowParams,
        mut results: remote_result_set::GetRowResults,
    ) -> Promise<(), capnp::Error> {
        trace!("get row");
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
