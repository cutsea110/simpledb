use capnp::capability::Promise;
use capnp_rpc::pry;
use chrono::Datelike;
use log::{info, trace};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    sync::{Arc, Mutex},
};

use super::simpledb::SimpleDB;
use crate::{
    plan::{plan::Plan, planner::Planner},
    query::{constant::Constant, expression::Expression, scan::Scan},
    record::schema::{FieldType, Schema},
    remote_capnp::{
        self, remote_connection, remote_driver, remote_result_set, remote_statement, schema,
    },
    repr,
    repr::planrepr::PlanRepr,
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
        self: Rc<RemoteDriverImpl>,
        params: remote_driver::ConnectParams,
        mut results: remote_driver::ConnectResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("connecting");
        let dbname = match pry!(pry!(params.get()).get_dbname()).to_str() {
            Ok(dbname) => dbname,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        info!("connect db: {}", dbname);
        let db = self.server.lock().unwrap().get_database(dbname);
        let conn = match RemoteConnectionImpl::new(db) {
            Ok(conn) => conn,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        let conn: remote_connection::Client = capnp_rpc::new_client(conn);
        results.get().set_conn(conn);
        trace!("connected");

        Promise::ok(())
    }
    fn get_version(
        self: Rc<RemoteDriverImpl>,
        _: remote_driver::GetVersionParams,
        mut results: remote_driver::GetVersionResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("get version");
        let mut ver = results.get().init_ver();
        ver.set_major_ver(self.major_ver);
        ver.set_minor_ver(self.minor_ver);
        info!("version: {}.{}", self.major_ver, self.minor_ver);

        Promise::ok(())
    }
}

pub struct ConnectionInternal {
    db: Arc<Mutex<SimpleDB>>,
    state: ConnectionState,
}

enum ConnectionState {
    Active(Arc<Mutex<Transaction>>),
    Failed(String),
}
impl ConnectionInternal {
    pub fn close(&mut self) -> anyhow::Result<()> {
        self.dump_statistics();

        // Essential body
        let tx = self.current_tx()?;
        let tx_num = tx.lock().unwrap().tx_num();
        trace!("close tx: {}", tx_num);
        let commit_result = tx.lock().unwrap().commit();
        if let Err(e) = commit_result {
            return Err(self.fail(format!("failed to commit transaction {}: {}", tx_num, e)));
        }
        self.renew_tx()
    }
    fn rollback(&mut self) -> anyhow::Result<()> {
        self.dump_statistics();

        // Essential body
        let tx = self.current_tx()?;
        let tx_num = tx.lock().unwrap().tx_num();
        trace!("rollback tx: {}", tx_num);
        let rollback_result = tx.lock().unwrap().rollback();
        match rollback_result {
            Ok(()) => Ok(()),
            Err(e) => Err(self.fail(format!("failed to roll back transaction {}: {}", tx_num, e))),
        }
    }
    pub fn rollback_and_renew(&mut self) -> anyhow::Result<()> {
        self.rollback()?;
        self.renew_tx()
    }
    fn renew_tx(&mut self) -> anyhow::Result<()> {
        let new_tx_result = { self.db.lock().unwrap().new_tx() };
        match new_tx_result {
            Ok(new_tx) => {
                trace!("start new tx: {}", new_tx.tx_num());
                self.state = ConnectionState::Active(Arc::new(Mutex::new(new_tx)));
                Ok(())
            }
            Err(e) => Err(self.fail(format!("failed to start a new transaction: {}", e))),
        }
    }
    fn current_tx(&self) -> anyhow::Result<Arc<Mutex<Transaction>>> {
        match &self.state {
            ConnectionState::Active(tx) => Ok(Arc::clone(tx)),
            ConnectionState::Failed(reason) => {
                Err(anyhow::anyhow!("connection is unusable: {}", reason))
            }
        }
    }
    fn ensure_active(&self) -> anyhow::Result<()> {
        self.current_tx().map(|_| ())
    }
    fn ensure_current_tx(&self, expected_tx_num: i32) -> anyhow::Result<()> {
        let current_tx_num = self.current_tx_num()?;
        if current_tx_num != expected_tx_num {
            return Err(anyhow::anyhow!(
                "result set belongs to transaction {}, but the current transaction is {}",
                expected_tx_num,
                current_tx_num
            ));
        }

        Ok(())
    }
    fn fail(&mut self, reason: String) -> anyhow::Error {
        self.state = ConnectionState::Failed(reason.clone());
        anyhow::anyhow!(reason)
    }
    // my own extends
    pub fn current_tx_num(&self) -> anyhow::Result<i32> {
        Ok(self.current_tx()?.lock().unwrap().tx_num())
    }

    fn dump_statistics(&self) {
        let (r, w) = self.numbers_of_read_written_blocks();
        info!("numbers of read/written blocks: {}/{}", r, w);
        let available = self.numbers_of_available_buffers();
        info!("numbers of available buffers: {}", available);
        let (pinned, unpinned) = self.numbers_of_total_pinned_unpinned();
        info!(
            "numbers of pinned/unpinned buffers: {}/{}",
            pinned, unpinned
        );
        let (hit, assigned) = self.buffer_cache_hit_assigned();
        let ratio = (hit as f32 / assigned as f32) * 100.0;
        info!(
            "buffer cache hit/assigned(ratio): {}/{}({:.3}%)",
            hit, assigned, ratio
        );
    }

    // extends for statistics by exercise 3.15
    fn numbers_of_read_written_blocks(&self) -> (u32, u32) {
        self.db
            .lock()
            .unwrap()
            .file_mgr()
            .lock()
            .unwrap()
            .nums_of_read_written_blocks()
    }
    // extends for statistics by exercise 4.18
    fn numbers_of_available_buffers(&self) -> usize {
        self.db
            .lock()
            .unwrap()
            .buffer_mgr()
            .lock()
            .unwrap()
            .available()
    }
    fn numbers_of_total_pinned_unpinned(&self) -> (u32, u32) {
        self.db
            .lock()
            .unwrap()
            .buffer_mgr()
            .lock()
            .unwrap()
            .nums_total_pinned_unpinned()
    }
    fn buffer_cache_hit_assigned(&self) -> (u32, u32) {
        self.db
            .lock()
            .unwrap()
            .buffer_mgr()
            .lock()
            .unwrap()
            .buffer_cache_hit_assigned()
    }
}

fn rpc_error_after_rollback(
    conn: &Rc<RefCell<ConnectionInternal>>,
    operation_error: impl std::fmt::Display,
) -> capnp::Error {
    let operation_error = operation_error.to_string();
    match conn.borrow_mut().rollback_and_renew() {
        Ok(()) => capnp::Error::failed(operation_error),
        Err(cleanup_error) => capnp::Error::failed(format!(
            "{}; transaction cleanup failed: {}",
            operation_error, cleanup_error
        )),
    }
}

pub struct RemoteConnectionImpl {
    conn: Rc<RefCell<ConnectionInternal>>,
}

impl RemoteConnectionImpl {
    pub fn new(db: Arc<Mutex<SimpleDB>>) -> anyhow::Result<Self> {
        let tx = db.lock().unwrap().new_tx()?;
        trace!("tx: {}", tx.tx_num());
        let conn = ConnectionInternal {
            db,
            state: ConnectionState::Active(Arc::new(Mutex::new(tx))),
        };

        Ok(Self {
            conn: Rc::new(RefCell::new(conn)),
        })
    }
}

fn set_schema(schema: Arc<Schema>, sch: &mut schema::Builder) {
    let mut columns = sch.reborrow().init_columns(schema.fields().len() as u32);
    for (i, fldname) in schema.fields().iter().enumerate() {
        let mut column = columns.reborrow().get(i as u32);
        column.set_name(fldname);
        column.set_length(schema.length(fldname) as u32);
        let t = match schema.field_type(fldname) {
            FieldType::SMALLINT => remote_capnp::FieldType::SmallInt,
            FieldType::INTEGER => remote_capnp::FieldType::Integer,
            FieldType::VARCHAR => remote_capnp::FieldType::Varchar,
            FieldType::BOOL => remote_capnp::FieldType::Bool,
            FieldType::DATE => remote_capnp::FieldType::Date,
        };
        column.set_type(t);
    }
}
fn set_constant(cnst: &Constant, c: &mut remote_statement::constant::Builder) {
    match cnst {
        Constant::I16(v) => {
            c.set_int16(*v);
        }
        Constant::I32(v) => {
            c.set_int32(*v);
        }
        Constant::String(s) => {
            c.set_string(s.as_str());
        }
        Constant::Bool(b) => {
            c.set_bool(*b);
        }
        Constant::Date(d) => {
            let mut dt = c.reborrow().init_date();
            dt.set_year(d.year() as i16);
            dt.set_month(d.month() as u8);
            dt.set_day(d.day() as u8);
        }
    }
}
fn set_expression(expr: &Expression, e: &mut remote_statement::expression::Builder) {
    match expr {
        Expression::Fldname(f) => {
            e.reborrow().set_fldname(f.as_str());
        }
        Expression::Val(c) => {
            let mut v = e.reborrow().init_val();
            set_constant(c, &mut v);
        }
    }
}

fn set_operation(
    operation: repr::planrepr::Operation,
    ope: &mut remote_statement::plan_repr::operation::Builder,
) {
    let op = ope.reborrow();

    match operation {
        repr::planrepr::Operation::IndexJoinScan {
            idxname,
            idxfldname,
            joinfld,
        } => {
            let mut op = op.init_index_join_scan();
            op.set_idxname(idxname.as_str());
            op.set_idxfldname(idxfldname.as_str());
            op.set_joinfld(joinfld.as_str());
        }
        repr::planrepr::Operation::IndexSelectScan {
            idxname,
            idxfldname,
            val,
        } => {
            let mut op = op.init_index_select_scan();
            op.set_idxname(idxname.as_str());
            op.set_idxfldname(idxfldname.as_str());
            let mut v = op.init_val();
            set_constant(&val, &mut v);
        }
        repr::planrepr::Operation::GroupByScan { fields, aggfns } => {
            let mut op = op.init_group_by_scan();
            let mut flds = op.reborrow().init_fields(fields.len() as u32);
            for (i, f) in fields.into_iter().enumerate() {
                flds.set(i as u32, f.as_str());
            }
            let mut fns = op.reborrow().init_aggfns(aggfns.len() as u32);
            for (i, (f, c)) in aggfns.into_iter().enumerate() {
                let mut aggregation = fns.reborrow().get(i as u32);
                aggregation.set_field(f.as_str());
                let mut v = aggregation.init_value();
                set_constant(&c, &mut v);
            }
        }
        repr::planrepr::Operation::Materialize => {
            op.init_materialize();
        }
        repr::planrepr::Operation::MergeJoinScan { fldname1, fldname2 } => {
            let mut op = op.init_merge_join_scan();
            op.set_fldname1(fldname1.as_str());
            op.set_fldname2(fldname2.as_str());
        }
        repr::planrepr::Operation::SortScan { compflds } => {
            let op = op.init_sort_scan();
            let mut flds = op.init_compflds(compflds.len() as u32);
            for (i, f) in compflds.into_iter().enumerate() {
                flds.set(i as u32, f.as_str());
            }
        }
        repr::planrepr::Operation::MultibufferProductScan => {
            op.init_multibuffer_product_scan();
        }
        repr::planrepr::Operation::ProductScan => {
            op.init_product_scan();
        }
        repr::planrepr::Operation::ProjectScan => {
            op.init_project_scan();
        }
        repr::planrepr::Operation::SelectScan { pred } => {
            let op = op.init_select_scan();
            let p = op.init_pred();
            let mut ts = p.init_terms(pred.terms().len() as u32);
            for (i, term) in pred.terms().into_iter().enumerate() {
                let mut t = ts.reborrow().get(i as u32);
                let mut lhs = t.reborrow().init_lhs();
                set_expression(term.lhs(), &mut lhs);
                let mut rhs = t.reborrow().init_rhs();
                set_expression(term.rhs(), &mut rhs);
            }
        }
        repr::planrepr::Operation::TableScan { tblname } => {
            op.init_table_scan().set_tblname(tblname.as_str());
        }
    }
}

fn set_plan_repr(planrepr: Arc<dyn PlanRepr>, pr: &mut remote_statement::plan_repr::Builder) {
    let mut op = pr.reborrow().init_operation();
    set_operation(planrepr.operation(), &mut op);
    pr.set_reads(planrepr.reads());
    pr.set_writes(planrepr.writes());
    let mut subs = pr
        .reborrow()
        .init_sub_plan_reprs(planrepr.sub_plan_reprs().len() as u32);
    for (i, repr) in planrepr.sub_plan_reprs().into_iter().enumerate() {
        let mut r = subs.reborrow().get(i as u32);
        set_plan_repr(repr, &mut r);
    }
}

impl remote_connection::Server for RemoteConnectionImpl {
    fn create_statement(
        self: Rc<RemoteConnectionImpl>,
        params: remote_connection::CreateStatementParams,
        mut results: remote_connection::CreateStatementResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("create statement");
        if let Err(e) = self.conn.borrow().ensure_active() {
            return Promise::err(capnp::Error::failed(e.to_string()));
        }
        let sql = match pry!(pry!(params.get()).get_sql()).to_str() {
            Ok(sql) => sql,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        info!("SQL: {}", sql);
        let db = Arc::clone(&self.conn.borrow().db);
        let planner = match db.lock().unwrap().planner() {
            Ok(planner) => planner,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        let stmt: remote_statement::Client = capnp_rpc::new_client(RemoteStatementImpl::new(
            sql,
            planner,
            Rc::clone(&self.conn),
        ));
        results.get().set_stmt(stmt);

        Promise::ok(())
    }
    fn close(
        self: Rc<RemoteConnectionImpl>,
        _: remote_connection::CloseParams,
        mut results: remote_connection::CloseResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("close");
        let tx_num = match self.conn.borrow().current_tx_num() {
            Ok(tx_num) => tx_num,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        if let Err(e) = self.conn.borrow_mut().close() {
            return Promise::err(capnp::Error::failed(e.to_string()));
        }
        results.get().set_tx(tx_num);

        Promise::ok(())
    }
    fn commit(
        self: Rc<RemoteConnectionImpl>,
        _: remote_connection::CommitParams,
        mut results: remote_connection::CommitResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        let tx_num = match self.conn.borrow().current_tx_num() {
            Ok(tx_num) => tx_num,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        trace!("commit tx: {}", tx_num);

        if let Err(e) = self.conn.borrow_mut().close() {
            return Promise::err(capnp::Error::failed(e.to_string()));
        }

        results.get().set_tx(tx_num);

        Promise::ok(())
    }
    fn rollback(
        self: Rc<RemoteConnectionImpl>,
        _: remote_connection::RollbackParams,
        mut results: remote_connection::RollbackResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        let tx_num = match self.conn.borrow().current_tx_num() {
            Ok(tx_num) => tx_num,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        trace!("rollback tx: {}", tx_num);
        if let Err(e) = self.conn.borrow_mut().rollback_and_renew() {
            return Promise::err(capnp::Error::failed(e.to_string()));
        }

        results.get().set_tx(tx_num);

        Promise::ok(())
    }
    fn get_table_schema(
        self: Rc<RemoteConnectionImpl>,
        params: remote_connection::GetTableSchemaParams,
        mut results: remote_connection::GetTableSchemaResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("get table schema");
        let tblname = match pry!(pry!(params.get()).get_tblname()).to_str() {
            Ok(tblname) => tblname,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        let tx = match self.conn.borrow().current_tx() {
            Ok(tx) => tx,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        let db = Arc::clone(&self.conn.borrow().db);
        let schema_result = db.lock().unwrap().get_table_schema(tblname, tx);
        let schema = match schema_result {
            Ok(schema) => schema,
            Err(e) => return Promise::err(rpc_error_after_rollback(&self.conn, e)),
        };
        let mut sch = results.get().init_sch();
        set_schema(schema, &mut sch);

        Promise::ok(())
    }
    fn get_view_definition(
        self: Rc<RemoteConnectionImpl>,
        params: remote_connection::GetViewDefinitionParams,
        mut results: remote_connection::GetViewDefinitionResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("get view definition");
        let viewname = match pry!(pry!(params.get()).get_viewname()).to_str() {
            Ok(viewname) => viewname,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        let tx = match self.conn.borrow().current_tx() {
            Ok(tx) => tx,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        let db = Arc::clone(&self.conn.borrow().db);
        let view_result = db.lock().unwrap().get_view_definitoin(viewname, tx);
        let (_, def) = match view_result {
            Ok(view) => view,
            Err(e) => return Promise::err(rpc_error_after_rollback(&self.conn, e)),
        };
        let mut viewdef = results.get().init_vwdef();
        viewdef.set_vwname(viewname);
        viewdef.set_vwdef(def.as_str());

        Promise::ok(())
    }
    fn get_index_info(
        self: Rc<RemoteConnectionImpl>,
        params: remote_connection::GetIndexInfoParams,
        mut results: remote_connection::GetIndexInfoResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("get index info");
        let tblname = match pry!(pry!(params.get()).get_tblname()).to_str() {
            Ok(tblname) => tblname,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        let tx = match self.conn.borrow().current_tx() {
            Ok(tx) => tx,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        let db = Arc::clone(&self.conn.borrow().db);
        let index_result = db.lock().unwrap().get_index_info(tblname, tx);
        let indexinfo = match index_result {
            Ok(indexinfo) => indexinfo,
            Err(e) => return Promise::err(rpc_error_after_rollback(&self.conn, e)),
        };
        let mut indexes = results.get().init_indexes(indexinfo.len() as u32);
        for (i, (_, ii)) in indexinfo.into_iter().enumerate() {
            let idxname = ii.index_name();
            let fldname = ii.field_name();
            let mut index = indexes.reborrow().get(i as u32);
            index.set_idxname(idxname);
            index.set_fldname(fldname);
        }

        Promise::ok(())
    }

    // extends for statistics by exercise 3.15
    fn nums_of_read_written_blocks(
        self: Rc<RemoteConnectionImpl>,
        _: remote_connection::NumsOfReadWrittenBlocksParams,
        mut results: remote_connection::NumsOfReadWrittenBlocksResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("nums of read/written blocks");
        if let Err(e) = self.conn.borrow().ensure_active() {
            return Promise::err(capnp::Error::failed(e.to_string()));
        }
        let (r, w) = self.conn.borrow().numbers_of_read_written_blocks();
        results.get().set_r(r);
        results.get().set_w(w);

        Promise::ok(())
    }
    // extends for statistics by exercise 4.18
    fn nums_of_total_pinned_unpinned(
        self: Rc<RemoteConnectionImpl>,
        _: remote_connection::NumsOfTotalPinnedUnpinnedParams,
        mut results: remote_connection::NumsOfTotalPinnedUnpinnedResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("nums of total pinned/unpinned buffers");
        if let Err(e) = self.conn.borrow().ensure_active() {
            return Promise::err(capnp::Error::failed(e.to_string()));
        }
        let (pinned, unpinned) = self.conn.borrow().numbers_of_total_pinned_unpinned();
        results.get().set_pinned(pinned);
        results.get().set_unpinned(unpinned);

        Promise::ok(())
    }
    // extends for statistics by exercise 4.18
    fn buffer_cache_hit_assigned(
        self: Rc<RemoteConnectionImpl>,
        _: remote_connection::BufferCacheHitAssignedParams,
        mut results: remote_connection::BufferCacheHitAssignedResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("buffer cache hit/assigned");
        if let Err(e) = self.conn.borrow().ensure_active() {
            return Promise::err(capnp::Error::failed(e.to_string()));
        }
        let (hit, assigned) = self.conn.borrow().buffer_cache_hit_assigned();
        results.get().set_hit(hit);
        results.get().set_assigned(assigned);

        Promise::ok(())
    }
}

pub struct RemoteStatementImpl {
    sql: String,
    planner: RefCell<Planner>,
    conn: Rc<RefCell<ConnectionInternal>>,
}
impl RemoteStatementImpl {
    pub fn new(sql: &str, planner: Planner, conn: Rc<RefCell<ConnectionInternal>>) -> Self {
        Self {
            sql: sql.to_string(),
            planner: planner.into(),
            conn,
        }
    }
}

impl remote_statement::Server for RemoteStatementImpl {
    fn execute_query(
        self: Rc<RemoteStatementImpl>,
        _: remote_statement::ExecuteQueryParams,
        mut results: remote_statement::ExecuteQueryResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("execute query: {}", self.sql);
        let tx = match self.conn.borrow().current_tx() {
            Ok(tx) => tx,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        let plan_result = self.planner.borrow_mut().create_query_plan(&self.sql, tx);
        match plan_result {
            Ok(plan) => {
                trace!("planned");
                let schema = plan.schema();
                let resultset = match RemoteResultSetImpl::new(plan, Rc::clone(&self.conn)) {
                    Ok(resultset) => resultset,
                    Err(e) => return Promise::err(rpc_error_after_rollback(&self.conn, e)),
                };
                let resultset: remote_result_set::Client = capnp_rpc::new_client(resultset);
                let mut result = results.get();
                result.set_result(resultset);
                set_schema(schema, &mut result.init_schema());

                return Promise::ok(());
            }
            Err(e) => {
                return Promise::err(rpc_error_after_rollback(
                    &self.conn,
                    format!("failed to create query plan: {}", e),
                ));
            }
        }
    }
    fn execute_update(
        self: Rc<RemoteStatementImpl>,
        _: remote_statement::ExecuteUpdateParams,
        mut results: remote_statement::ExecuteUpdateResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("execute update: {}", self.sql);
        let tx = match self.conn.borrow().current_tx() {
            Ok(tx) => tx,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        let update_result = self.planner.borrow_mut().execute_update(&self.sql, tx);
        let affected = match update_result {
            Ok(affected) => affected,
            Err(e) => return Promise::err(rpc_error_after_rollback(&self.conn, e)),
        };
        let tx_num = match self.conn.borrow().current_tx_num() {
            Ok(tx_num) => tx_num,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        if let Err(e) = self.conn.borrow_mut().close() {
            return Promise::err(capnp::Error::failed(e.to_string()));
        }
        let mut result = results.get();
        result.set_affected(affected);
        result.set_committed_tx(tx_num);

        Promise::ok(())
    }
    fn close(
        self: Rc<RemoteStatementImpl>,
        _: remote_statement::CloseParams,
        mut results: remote_statement::CloseResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("close");
        let tx_num = match self.conn.borrow().current_tx_num() {
            Ok(tx_num) => tx_num,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        if let Err(e) = self.conn.borrow_mut().close() {
            return Promise::err(capnp::Error::failed(e.to_string()));
        }
        results.get().set_tx(tx_num);

        Promise::ok(())
    }
    fn explain_plan(
        self: Rc<RemoteStatementImpl>,
        _: remote_statement::ExplainPlanParams,
        mut results: remote_statement::ExplainPlanResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("explain plan");
        let tx = match self.conn.borrow().current_tx() {
            Ok(tx) => tx,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        let plan_result = self.planner.borrow_mut().create_query_plan(&self.sql, tx);
        let planrepr = match plan_result {
            Ok(plan) => plan.repr(),
            Err(e) => {
                return Promise::err(rpc_error_after_rollback(
                    &self.conn,
                    format!("failed to create query plan: {}", e),
                ));
            }
        };

        let mut pr = results.get().init_planrepr();
        set_plan_repr(planrepr, &mut pr);

        Promise::ok(())
    }
}

pub struct RemoteResultSetImpl {
    scan: Arc<Mutex<dyn Scan>>,
    sch: Arc<Schema>,
    conn: Rc<RefCell<ConnectionInternal>>,
    tx_num: i32,
    state: Cell<ResultSetState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResultSetState {
    Open,
    Aborted,
    Closed,
    Failed,
}
impl RemoteResultSetImpl {
    pub fn new(plan: Arc<dyn Plan>, conn: Rc<RefCell<ConnectionInternal>>) -> anyhow::Result<Self> {
        let tx_num = conn.borrow().current_tx_num()?;
        let scan = plan.open()?;
        let sch = plan.schema();
        Ok(Self {
            scan,
            sch,
            conn,
            tx_num,
            state: Cell::new(ResultSetState::Open),
        })
    }

    fn read_rows(&self, limit: u16) -> anyhow::Result<Vec<Vec<Constant>>> {
        let mut batch = Vec::with_capacity(limit as usize);
        let mut scan = self.scan.lock().unwrap();
        for _ in 0..limit {
            if !scan.next() {
                break;
            }
            let mut row = Vec::with_capacity(self.sch.fields().len());
            for field in self.sch.fields() {
                let value = match self.sch.field_type(field) {
                    FieldType::SMALLINT => scan.get_i16(field).map(Constant::I16),
                    FieldType::INTEGER => scan.get_i32(field).map(Constant::I32),
                    FieldType::VARCHAR => scan.get_string(field).map(Constant::String),
                    FieldType::BOOL => scan.get_bool(field).map(Constant::Bool),
                    FieldType::DATE => scan.get_date(field).map(Constant::Date),
                }?;
                row.push(value);
            }
            batch.push(row);
        }

        Ok(batch)
    }

    fn abort_after_read_error(&self, read_error: anyhow::Error) -> capnp::Error {
        self.state.set(ResultSetState::Aborted);
        let scan_result = self.scan.lock().unwrap().close();
        let rollback_result = self.conn.borrow_mut().rollback_and_renew();

        let mut message = read_error.to_string();
        if let Err(e) = scan_result {
            message.push_str(&format!("; scan cleanup also failed: {}", e));
        }
        if let Err(e) = rollback_result {
            message.push_str(&format!("; transaction rollback also failed: {}", e));
        }
        capnp::Error::failed(message)
    }

    fn reject_stale(&self, stale_error: anyhow::Error) -> capnp::Error {
        self.state.set(ResultSetState::Aborted);
        match self.scan.lock().unwrap().close() {
            Ok(()) => capnp::Error::failed(stale_error.to_string()),
            Err(scan_error) => capnp::Error::failed(format!(
                "{}; scan cleanup also failed: {}",
                stale_error, scan_error
            )),
        }
    }
}

impl remote_result_set::Server for RemoteResultSetImpl {
    fn close(
        self: Rc<RemoteResultSetImpl>,
        _: remote_result_set::CloseParams,
        mut results: remote_result_set::CloseResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("close");
        match self.state.get() {
            ResultSetState::Aborted | ResultSetState::Closed => {
                self.state.set(ResultSetState::Closed);
                results.get().set_tx(self.tx_num);
                return Promise::ok(());
            }
            ResultSetState::Failed => {
                return Promise::err(capnp::Error::failed(
                    "result-set cleanup previously failed".to_string(),
                ));
            }
            ResultSetState::Open => {}
        }
        let scan_result = self.scan.lock().unwrap().close();
        let ownership_result = { self.conn.borrow().ensure_current_tx(self.tx_num) };
        let connection_result = match ownership_result {
            Ok(()) => self.conn.borrow_mut().close(),
            Err(e) => Err(e),
        };
        match (scan_result, connection_result) {
            (Ok(()), Ok(())) => self.state.set(ResultSetState::Closed),
            (Err(e), Ok(())) | (Ok(()), Err(e)) => {
                self.state.set(ResultSetState::Failed);
                return Promise::err(capnp::Error::failed(e.to_string()));
            }
            (Err(scan_error), Err(connection_error)) => {
                self.state.set(ResultSetState::Failed);
                return Promise::err(capnp::Error::failed(format!(
                    "failed to close scan: {}; connection cleanup also failed: {}",
                    scan_error, connection_error
                )));
            }
        }
        results.get().set_tx(self.tx_num);

        Promise::ok(())
    }
    fn get_rows(
        self: Rc<RemoteResultSetImpl>,
        params: remote_result_set::GetRowsParams,
        mut results: remote_result_set::GetRowsResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        const MAX_ROWS_PER_BATCH: u16 = 1024;

        if self.state.get() != ResultSetState::Open {
            return Promise::err(capnp::Error::failed("result set is closed".to_string()));
        }
        if let Err(e) = self.conn.borrow().ensure_current_tx(self.tx_num) {
            return Promise::err(self.reject_stale(e));
        }
        let limit = pry!(params.get()).get_limit();
        if limit > MAX_ROWS_PER_BATCH {
            return Promise::err(capnp::Error::failed(format!(
                "row batch limit {} exceeds maximum {}",
                limit, MAX_ROWS_PER_BATCH
            )));
        }
        trace!("get_rows with limit: {}", limit);

        let batch = match self.read_rows(limit) {
            Ok(batch) => batch,
            Err(e) => return Promise::err(self.abort_after_read_error(e)),
        };

        trace!("get_rows count: {}", batch.len());
        let mut rows = results.get().init_rows(batch.len() as u32);
        for (row_index, row) in batch.iter().enumerate() {
            let mut values = rows
                .reborrow()
                .get(row_index as u32)
                .init_values(row.len() as u32);
            for (value_index, value) in row.iter().enumerate() {
                let mut output = values.reborrow().get(value_index as u32);
                match value {
                    Constant::I16(value) => output.set_int16(*value),
                    Constant::I32(value) => output.set_int32(*value),
                    Constant::String(value) => output.set_string(value),
                    Constant::Bool(value) => output.set_bool(*value),
                    Constant::Date(value) => {
                        let mut date = output.init_date();
                        date.set_year(value.year() as i16);
                        date.set_month(value.month() as u8);
                        date.set_day(value.day() as u8);
                    }
                }
            }
        }

        Promise::ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::Path,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    };

    use super::*;
    use crate::{
        materialize::sortscan::SortScan,
        query::updatescan::UpdateScan,
        record::{schema::Schema, tablescan::TableScan},
    };

    struct FailingScan {
        closed: Arc<AtomicBool>,
    }

    impl Scan for FailingScan {
        fn before_first(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
        fn next(&mut self) -> bool {
            true
        }
        fn get_i16(&mut self, _: &str) -> anyhow::Result<i16> {
            Err(anyhow::anyhow!("read failed"))
        }
        fn get_i32(&mut self, _: &str) -> anyhow::Result<i32> {
            Err(anyhow::anyhow!("read failed"))
        }
        fn get_string(&mut self, _: &str) -> anyhow::Result<String> {
            Err(anyhow::anyhow!("read failed"))
        }
        fn get_bool(&mut self, _: &str) -> anyhow::Result<bool> {
            Err(anyhow::anyhow!("read failed"))
        }
        fn get_date(&mut self, _: &str) -> anyhow::Result<chrono::NaiveDate> {
            Err(anyhow::anyhow!("read failed"))
        }
        fn get_val(&mut self, _: &str) -> anyhow::Result<Constant> {
            Err(anyhow::anyhow!("read failed"))
        }
        fn has_field(&self, _: &str) -> bool {
            true
        }
        fn close(&mut self) -> anyhow::Result<()> {
            self.closed.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn to_update_scan(&mut self) -> anyhow::Result<&mut dyn UpdateScan> {
            Err(anyhow::anyhow!("unsupported"))
        }
        fn as_table_scan(&mut self) -> anyhow::Result<&mut TableScan> {
            Err(anyhow::anyhow!("unsupported"))
        }
        fn as_sort_scan(&mut self) -> anyhow::Result<&mut SortScan> {
            Err(anyhow::anyhow!("unsupported"))
        }
    }

    fn result_set_for_test(
        db_path: &str,
    ) -> anyhow::Result<(RemoteResultSetImpl, Arc<AtomicBool>)> {
        if Path::new(db_path).exists() {
            fs::remove_dir_all(db_path)?;
        }
        let db = Arc::new(Mutex::new(SimpleDB::new_with(db_path, 400, 8)));
        let remote_conn = RemoteConnectionImpl::new(db)?;
        let tx_num = remote_conn.conn.borrow().current_tx_num()?;
        let closed = Arc::new(AtomicBool::new(false));
        let scan: Arc<Mutex<dyn Scan>> = Arc::new(Mutex::new(FailingScan {
            closed: Arc::clone(&closed),
        }));
        let mut schema = Schema::new();
        schema.add_i32_field("value");

        Ok((
            RemoteResultSetImpl {
                scan,
                sch: Arc::new(schema),
                conn: Rc::clone(&remote_conn.conn),
                tx_num,
                state: Cell::new(ResultSetState::Open),
            },
            closed,
        ))
    }

    #[test]
    fn read_error_aborts_result_set_and_renews_transaction() -> anyhow::Result<()> {
        let db_path = "_test/remote_result_set_read_error";
        let (result_set, scan_closed) = result_set_for_test(db_path)?;
        let original_tx_num = result_set.tx_num;

        let read_error = result_set.read_rows(1).unwrap_err();
        let rpc_error = result_set.abort_after_read_error(read_error);

        assert_eq!(result_set.state.get(), ResultSetState::Aborted);
        assert!(rpc_error.to_string().contains("read failed"));
        assert!(scan_closed.load(Ordering::SeqCst));
        assert_ne!(result_set.conn.borrow().current_tx_num()?, original_tx_num);
        result_set.conn.borrow_mut().close()?;
        fs::remove_dir_all(db_path)?;
        Ok(())
    }

    #[test]
    fn stale_result_set_does_not_finalize_current_transaction() -> anyhow::Result<()> {
        let db_path = "_test/remote_result_set_stale";
        let (result_set, scan_closed) = result_set_for_test(db_path)?;
        result_set.conn.borrow_mut().close()?;
        let current_tx_num = result_set.conn.borrow().current_tx_num()?;
        let stale_error = result_set
            .conn
            .borrow()
            .ensure_current_tx(result_set.tx_num)
            .unwrap_err();

        let rpc_error = result_set.reject_stale(stale_error);

        assert_eq!(result_set.state.get(), ResultSetState::Aborted);
        assert!(rpc_error
            .to_string()
            .contains("result set belongs to transaction"));
        assert!(scan_closed.load(Ordering::SeqCst));
        assert_eq!(result_set.conn.borrow().current_tx_num()?, current_tx_num);
        result_set.conn.borrow_mut().close()?;
        fs::remove_dir_all(db_path)?;
        Ok(())
    }
}
