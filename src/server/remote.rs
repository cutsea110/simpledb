use capnp::capability::Promise;
use capnp_rpc::pry;
use chrono::Datelike;
use log::{info, trace};
use std::{
    cell::RefCell,
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
        let dbname = pry!(pry!(params.get()).get_dbname()).to_str().unwrap();
        info!("connect db: {}", dbname);
        let db = self.server.lock().unwrap().get_database(dbname);
        let conn: remote_connection::Client = capnp_rpc::new_client(RemoteConnectionImpl::new(db));
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
    current_tx: Arc<Mutex<Transaction>>,
}
impl ConnectionInternal {
    pub fn close(&mut self) -> anyhow::Result<()> {
        self.dump_statistics();

        // Essential body
        let tx_num = self.current_tx.lock().unwrap().tx_num();
        trace!("close tx: {}", tx_num);
        self.current_tx.lock().unwrap().commit()?;
        self.renew_tx()
    }
    pub fn commit(&mut self) -> anyhow::Result<()> {
        self.dump_statistics();

        // Essential body
        let tx_num = self.current_tx.lock().unwrap().tx_num();
        trace!("commit tx: {}", tx_num);
        self.current_tx.lock().unwrap().commit()
    }
    pub fn rollback(&mut self) -> anyhow::Result<()> {
        self.dump_statistics();

        // Essential body
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
    // my own extends
    pub fn current_tx_num(&self) -> i32 {
        self.current_tx.lock().unwrap().tx_num()
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
        let sql = pry!(pry!(params.get()).get_sql()).to_str().unwrap();
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
        self: Rc<RemoteConnectionImpl>,
        _: remote_connection::CloseParams,
        mut results: remote_connection::CloseResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("close");
        let tx_num = self.conn.borrow().current_tx_num();
        self.conn.borrow_mut().close().expect("close");
        results.get().set_tx(tx_num);

        Promise::ok(())
    }
    fn commit(
        self: Rc<RemoteConnectionImpl>,
        _: remote_connection::CommitParams,
        mut results: remote_connection::CommitResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        let tx_num = self.conn.borrow_mut().current_tx.lock().unwrap().tx_num();
        trace!("commit tx: {}", tx_num);

        self.conn.borrow_mut().commit().expect("commit");
        self.conn.borrow_mut().renew_tx().expect("start new tx");

        results.get().set_tx(tx_num);

        Promise::ok(())
    }
    fn rollback(
        self: Rc<RemoteConnectionImpl>,
        _: remote_connection::RollbackParams,
        mut results: remote_connection::RollbackResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        let tx_num = self.conn.borrow_mut().current_tx.lock().unwrap().tx_num();
        trace!("rollback tx: {}", tx_num);
        self.conn.borrow_mut().rollback().expect("rollback");
        self.conn.borrow_mut().renew_tx().expect("start new tx");

        results.get().set_tx(tx_num);

        Promise::ok(())
    }
    fn get_table_schema(
        self: Rc<RemoteConnectionImpl>,
        params: remote_connection::GetTableSchemaParams,
        mut results: remote_connection::GetTableSchemaResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("get table schema");
        let tblname = pry!(pry!(params.get()).get_tblname()).to_str().unwrap();
        let schema = self
            .conn
            .borrow()
            .db
            .lock()
            .unwrap()
            .get_table_schema(tblname, Arc::clone(&self.conn.borrow().current_tx))
            .expect("table schema");
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
        let viewname = pry!(pry!(params.get()).get_viewname()).to_str().unwrap();
        let (_, def) = self
            .conn
            .borrow()
            .db
            .lock()
            .unwrap()
            .get_view_definitoin(viewname, Arc::clone(&self.conn.borrow().current_tx))
            .expect("get view definition");
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
        let tblname = pry!(pry!(params.get()).get_tblname()).to_str().unwrap();
        let indexinfo = self
            .conn
            .borrow()
            .db
            .lock()
            .unwrap()
            .get_index_info(tblname, Arc::clone(&self.conn.borrow().current_tx))
            .expect("get index info");
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
        match self
            .planner
            .borrow_mut()
            .create_query_plan(&self.sql, Arc::clone(&self.conn.borrow().current_tx))
        {
            Ok(plan) => {
                trace!("planned");
                let schema = plan.schema();
                let resultset = match RemoteResultSetImpl::new(plan, Rc::clone(&self.conn)) {
                    Ok(resultset) => resultset,
                    Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
                };
                let resultset: remote_result_set::Client = capnp_rpc::new_client(resultset);
                let mut result = results.get();
                result.set_result(resultset);
                set_schema(schema, &mut result.init_schema());

                return Promise::ok(());
            }
            Err(e) => {
                return Promise::err(capnp::Error::failed(format!(
                    "failed to create query plan: {}",
                    e
                )));
            }
        }
    }
    fn execute_update(
        self: Rc<RemoteStatementImpl>,
        _: remote_statement::ExecuteUpdateParams,
        mut results: remote_statement::ExecuteUpdateResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("execute update: {}", self.sql);
        let affected = match self
            .planner
            .borrow_mut()
            .execute_update(&self.sql, Arc::clone(&self.conn.borrow().current_tx))
        {
            Ok(affected) => affected,
            Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
        };
        let tx_num = self.conn.borrow().current_tx_num();
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
        let tx_num = self.conn.borrow().current_tx_num();
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
        let planrepr = self
            .planner
            .borrow_mut()
            .create_query_plan(&self.sql, Arc::clone(&self.conn.borrow().current_tx))
            .unwrap()
            .repr();

        let mut pr = results.get().init_planrepr();
        set_plan_repr(planrepr, &mut pr);

        Promise::ok(())
    }
}

pub struct RemoteResultSetImpl {
    scan: Arc<Mutex<dyn Scan>>,
    sch: Arc<Schema>,
    conn: Rc<RefCell<ConnectionInternal>>,
}
impl RemoteResultSetImpl {
    pub fn new(plan: Arc<dyn Plan>, conn: Rc<RefCell<ConnectionInternal>>) -> anyhow::Result<Self> {
        let scan = plan.open()?;
        let sch = plan.schema();
        Ok(Self { scan, sch, conn })
    }
}

impl remote_result_set::Server for RemoteResultSetImpl {
    fn close(
        self: Rc<RemoteResultSetImpl>,
        _: remote_result_set::CloseParams,
        mut results: remote_result_set::CloseResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        trace!("close");
        let tx_num = self.conn.borrow().current_tx_num();
        if let Err(e) = self.scan.lock().unwrap().close() {
            return Promise::err(capnp::Error::failed(e.to_string()));
        }
        if let Err(e) = self.conn.borrow_mut().close() {
            return Promise::err(capnp::Error::failed(e.to_string()));
        }
        results.get().set_tx(tx_num);

        Promise::ok(())
    }
    fn get_rows(
        self: Rc<RemoteResultSetImpl>,
        params: remote_result_set::GetRowsParams,
        mut results: remote_result_set::GetRowsResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        const MAX_ROWS_PER_BATCH: u16 = 1024;

        let limit = pry!(params.get()).get_limit();
        if limit > MAX_ROWS_PER_BATCH {
            return Promise::err(capnp::Error::failed(format!(
                "row batch limit {} exceeds maximum {}",
                limit, MAX_ROWS_PER_BATCH
            )));
        }
        trace!("get_rows with limit: {}", limit);

        let mut batch = Vec::with_capacity(limit as usize);
        {
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
                    };
                    match value {
                        Ok(value) => row.push(value),
                        Err(e) => return Promise::err(capnp::Error::failed(e.to_string())),
                    }
                }
                batch.push(row);
            }
        }

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
