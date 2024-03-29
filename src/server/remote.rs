use capnp::capability::Promise;
use capnp_rpc::pry;
use chrono::{Datelike, NaiveDate};
use log::{debug, info, trace};
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
};

use super::simpledb::SimpleDB;
use crate::{
    plan::{plan::Plan, planner::Planner},
    query::{constant::Constant, expression::Expression, scan::Scan},
    record::schema::{FieldType, Schema},
    remote_capnp::{
        self, affected, bool_box, date_box, int16_box, int32_box, remote_connection, remote_driver,
        remote_meta_data, remote_result_set, remote_statement, schema, string_box, tx_box,
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
        &mut self,
        params: remote_driver::ConnectParams,
        mut results: remote_driver::ConnectResults,
    ) -> Promise<(), capnp::Error> {
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
        &mut self,
        _: remote_driver::GetVersionParams,
        mut results: remote_driver::GetVersionResults,
    ) -> Promise<(), capnp::Error> {
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
    let mut fields = sch.reborrow().init_fields(schema.fields().len() as u32);
    for i in 0..schema.fields().len() {
        let fldname = schema.fields()[i].as_str();
        fields.set(i as u32, fldname);
    }
    let mut info = sch.reborrow().init_info();
    let mut entries = info
        .reborrow()
        .init_entries(schema.info().keys().len() as u32);
    for (i, (k, fi)) in schema.info().into_iter().enumerate() {
        let fldname = k.as_str();
        entries.reborrow().get(i as u32).set_key(fldname).unwrap();
        let mut val = entries.reborrow().get(i as u32).init_value();
        val.reborrow().set_length(fi.length as i32);
        let t = match fi.fld_type {
            FieldType::SMALLINT => remote_capnp::FieldType::SmallInt,
            FieldType::INTEGER => remote_capnp::FieldType::Integer,
            FieldType::VARCHAR => remote_capnp::FieldType::Varchar,
            FieldType::BOOL => remote_capnp::FieldType::Bool,
            FieldType::DATE => remote_capnp::FieldType::Date,
        };
        val.reborrow().set_type(t);
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
                let mut tpl = fns.reborrow().get(i as u32);
                tpl.set_fst(f.as_str()).unwrap();
                let mut v = tpl.init_snd();
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

pub struct TxImpl {
    tx: i32,
}
impl TxImpl {
    pub fn new(tx: i32) -> Self {
        Self { tx }
    }
}
impl tx_box::Server for TxImpl {
    fn read(
        &mut self,
        _: tx_box::ReadParams,
        mut results: tx_box::ReadResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_tx(self.tx);
        Promise::ok(())
    }
}

impl remote_connection::Server for RemoteConnectionImpl {
    fn create_statement(
        &mut self,
        params: remote_connection::CreateStatementParams,
        mut results: remote_connection::CreateStatementResults,
    ) -> Promise<(), capnp::Error> {
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
        &mut self,
        _: remote_connection::CloseParams,
        mut results: remote_connection::CloseResults,
    ) -> Promise<(), capnp::Error> {
        trace!("close");
        let tx_num = self.conn.borrow().current_tx_num();
        self.conn.borrow_mut().close().expect("close");
        let client: tx_box::Client = capnp_rpc::new_client(TxImpl::new(tx_num));
        results.get().set_res(client);

        Promise::ok(())
    }
    fn commit(
        &mut self,
        _: remote_connection::CommitParams,
        mut results: remote_connection::CommitResults,
    ) -> Promise<(), capnp::Error> {
        let tx_num = self.conn.borrow_mut().current_tx.lock().unwrap().tx_num();
        trace!("commit tx: {}", tx_num);

        self.conn.borrow_mut().commit().expect("commit");
        self.conn.borrow_mut().renew_tx().expect("start new tx");

        results.get().set_tx(tx_num);

        Promise::ok(())
    }
    fn rollback(
        &mut self,
        _: remote_connection::RollbackParams,
        mut results: remote_connection::RollbackResults,
    ) -> Promise<(), capnp::Error> {
        let tx_num = self.conn.borrow_mut().current_tx.lock().unwrap().tx_num();
        trace!("rollback tx: {}", tx_num);
        self.conn.borrow_mut().rollback().expect("rollback");
        self.conn.borrow_mut().renew_tx().expect("start new tx");

        results.get().set_tx(tx_num);

        Promise::ok(())
    }
    fn get_table_schema(
        &mut self,
        params: remote_connection::GetTableSchemaParams,
        mut results: remote_connection::GetTableSchemaResults,
    ) -> Promise<(), capnp::Error> {
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
        &mut self,
        params: remote_connection::GetViewDefinitionParams,
        mut results: remote_connection::GetViewDefinitionResults,
    ) -> Promise<(), capnp::Error> {
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
        &mut self,
        params: remote_connection::GetIndexInfoParams,
        mut results: remote_connection::GetIndexInfoResults,
    ) -> Promise<(), capnp::Error> {
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
        let mut ii = results.get().init_ii();
        let mut entries = ii.reborrow().init_entries(indexinfo.keys().len() as u32);
        for (i, (_, ii)) in indexinfo.into_iter().enumerate() {
            let idxname = ii.index_name();
            let fldname = ii.field_name();
            let mut val = entries.reborrow().get(i as u32).init_value();
            val.reborrow().set_idxname(idxname);
            val.reborrow().set_fldname(fldname);
        }

        Promise::ok(())
    }

    // extends for statistics by exercise 3.15
    fn nums_of_read_written_blocks(
        &mut self,
        _: remote_connection::NumsOfReadWrittenBlocksParams,
        mut results: remote_connection::NumsOfReadWrittenBlocksResults,
    ) -> Promise<(), capnp::Error> {
        trace!("nums of read/written blocks");
        let (r, w) = self.conn.borrow().numbers_of_read_written_blocks();
        results.get().set_r(r);
        results.get().set_w(w);

        Promise::ok(())
    }
    // extends for statistics by exercise 4.18
    fn nums_of_total_pinned_unpinned(
        &mut self,
        _: remote_connection::NumsOfTotalPinnedUnpinnedParams,
        mut results: remote_connection::NumsOfTotalPinnedUnpinnedResults,
    ) -> Promise<(), capnp::Error> {
        trace!("nums of total pinned/unpinned buffers");
        let (pinned, unpinned) = self.conn.borrow().numbers_of_total_pinned_unpinned();
        results.get().set_pinned(pinned);
        results.get().set_unpinned(unpinned);

        Promise::ok(())
    }
    // extends for statistics by exercise 4.18
    fn buffer_cache_hit_assigned(
        &mut self,
        _: remote_connection::BufferCacheHitAssignedParams,
        mut results: remote_connection::BufferCacheHitAssignedResults,
    ) -> Promise<(), capnp::Error> {
        trace!("buffer cache hit/assigned");
        let (hit, assigned) = self.conn.borrow().buffer_cache_hit_assigned();
        results.get().set_hit(hit);
        results.get().set_assigned(assigned);

        Promise::ok(())
    }
}

pub struct AffectedImpl {
    affected: i32,
    committed_tx: i32,
}
impl AffectedImpl {
    pub fn new(affected: i32, committed_tx: i32) -> Self {
        Self {
            affected,
            committed_tx,
        }
    }
}
impl affected::Server for AffectedImpl {
    fn read(
        &mut self,
        _: affected::ReadParams,
        mut results: affected::ReadResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_affected(self.affected);
        Promise::ok(())
    }
    fn committed_tx(
        &mut self,
        _: affected::CommittedTxParams,
        mut results: affected::CommittedTxResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_tx(self.committed_tx);
        Promise::ok(())
    }
}

pub struct Int16BoxImpl {
    val: i16,
}
impl Int16BoxImpl {
    pub fn new(val: i16) -> Self {
        Self { val }
    }
}
impl int16_box::Server for Int16BoxImpl {
    fn read(
        &mut self,
        _: int16_box::ReadParams,
        mut results: int16_box::ReadResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_val(self.val);
        Promise::ok(())
    }
}

pub struct Int32BoxImpl {
    val: i32,
}
impl Int32BoxImpl {
    pub fn new(val: i32) -> Self {
        Self { val }
    }
}
impl int32_box::Server for Int32BoxImpl {
    fn read(
        &mut self,
        _: int32_box::ReadParams,
        mut results: int32_box::ReadResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_val(self.val);
        Promise::ok(())
    }
}

pub struct StringBoxImpl {
    val: String,
}
impl StringBoxImpl {
    pub fn new(val: String) -> Self {
        Self {
            val: val.to_string(),
        }
    }
}
impl string_box::Server for StringBoxImpl {
    fn read(
        &mut self,
        _: string_box::ReadParams,
        mut results: string_box::ReadResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_val(self.val.as_str());
        Promise::ok(())
    }
}

pub struct BoolBoxImpl {
    exists: bool,
}
impl BoolBoxImpl {
    pub fn new(exists: bool) -> Self {
        Self { exists }
    }
}
impl bool_box::Server for BoolBoxImpl {
    fn read(
        &mut self,
        _: bool_box::ReadParams,
        mut results: bool_box::ReadResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_val(self.exists);
        Promise::ok(())
    }
}

pub struct DateBoxImpl {
    date: NaiveDate,
}
impl DateBoxImpl {
    pub fn new(date: NaiveDate) -> Self {
        Self { date }
    }
}
impl date_box::Server for DateBoxImpl {
    fn read(
        &mut self,
        _: date_box::ReadParams,
        mut results: date_box::ReadResults,
    ) -> Promise<(), capnp::Error> {
        let mut val = results.get().init_val();
        val.set_year(self.date.year() as i16);
        val.set_month(self.date.month() as u8);
        val.set_day(self.date.day() as u8);
        Promise::ok(())
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

impl remote_statement::Server for RemoteStatementImpl {
    fn execute_query(
        &mut self,
        _: remote_statement::ExecuteQueryParams,
        mut results: remote_statement::ExecuteQueryResults,
    ) -> Promise<(), capnp::Error> {
        trace!("execute query: {}", self.sql);
        match self
            .planner
            .create_query_plan(&self.sql, Arc::clone(&self.conn.borrow().current_tx))
        {
            Ok(plan) => {
                trace!("planned");
                let resultset: remote_result_set::Client =
                    capnp_rpc::new_client(RemoteResultSetImpl::new(plan, Rc::clone(&self.conn)));
                results.get().set_result(resultset);

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
        &mut self,
        _: remote_statement::ExecuteUpdateParams,
        mut results: remote_statement::ExecuteUpdateResults,
    ) -> Promise<(), capnp::Error> {
        trace!("execute update: {}", self.sql);
        let affected = self
            .planner
            .execute_update(&self.sql, Arc::clone(&self.conn.borrow().current_tx))
            .expect("execute update");
        let tx_num = self.conn.borrow().current_tx_num();
        self.conn.borrow_mut().close().expect("close");
        let affected: affected::Client = capnp_rpc::new_client(AffectedImpl::new(affected, tx_num));
        results.get().set_affected(affected);

        Promise::ok(())
    }
    fn close(
        &mut self,
        _: remote_statement::CloseParams,
        mut results: remote_statement::CloseResults,
    ) -> Promise<(), capnp::Error> {
        trace!("close");
        let tx_num = self.conn.borrow().current_tx_num();
        self.conn.borrow_mut().close().expect("close");
        let client: tx_box::Client = capnp_rpc::new_client(TxImpl::new(tx_num));
        results.get().set_res(client);

        Promise::ok(())
    }
    fn explain_plan(
        &mut self,
        _: remote_statement::ExplainPlanParams,
        mut results: remote_statement::ExplainPlanResults,
    ) -> Promise<(), capnp::Error> {
        trace!("explain plan");
        let planrepr = self
            .planner
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
    pub fn new(plan: Arc<dyn Plan>, conn: Rc<RefCell<ConnectionInternal>>) -> Self {
        let scan = plan.open().expect("open plan");
        let sch = plan.schema();
        Self { scan, sch, conn }
    }
}

impl remote_result_set::Server for RemoteResultSetImpl {
    fn next(
        &mut self,
        _: remote_result_set::NextParams,
        mut results: remote_result_set::NextResults,
    ) -> Promise<(), capnp::Error> {
        let has_next = self.scan.lock().unwrap().next();
        trace!("next: {}", has_next);
        let next: bool_box::Client = capnp_rpc::new_client(BoolBoxImpl::new(has_next));
        results.get().set_val(next);

        Promise::ok(())
    }
    fn close(
        &mut self,
        _: remote_result_set::CloseParams,
        mut results: remote_result_set::CloseResults,
    ) -> Promise<(), capnp::Error> {
        trace!("close");
        let tx_num = self.conn.borrow().current_tx_num();
        self.conn.borrow_mut().close().expect("close");
        let client: tx_box::Client = capnp_rpc::new_client(TxImpl::new(tx_num));
        results.get().set_res(client);

        Promise::ok(())
    }
    fn get_metadata(
        &mut self,
        _: remote_result_set::GetMetadataParams,
        mut results: remote_result_set::GetMetadataResults,
    ) -> Promise<(), capnp::Error> {
        trace!("get metadata");
        let client: remote_meta_data::Client =
            capnp_rpc::new_client(RemoteMetaDataImpl::new(Arc::clone(&self.sch)));
        results.get().set_metadata(client);

        Promise::ok(())
    }
    fn get_row(
        &mut self,
        _: remote_result_set::GetRowParams,
        mut results: remote_result_set::GetRowResults,
    ) -> Promise<(), capnp::Error> {
        trace!("get_row");
        let row = results.get().init_row();
        let mut map = row.init_map();
        let mut entries = map.reborrow().init_entries(self.sch.fields().len() as u32);
        for (i, (k, fi)) in self.sch.info().into_iter().enumerate() {
            entries
                .reborrow()
                .get(i as u32)
                .set_key(k.as_str())
                .unwrap();
            let mut val = entries.reborrow().get(i as u32).init_value();
            match fi.fld_type {
                FieldType::SMALLINT => {
                    if let Ok(v) = self.scan.lock().unwrap().get_i16(k) {
                        val.reborrow().set_int16(v);
                    }
                }
                FieldType::INTEGER => {
                    if let Ok(v) = self.scan.lock().unwrap().get_i32(k) {
                        val.reborrow().set_int32(v);
                    }
                }
                FieldType::VARCHAR => {
                    if let Ok(s) = self.scan.lock().unwrap().get_string(k) {
                        val.reborrow().set_string(s.as_str());
                    }
                }
                FieldType::BOOL => {
                    if let Ok(v) = self.scan.lock().unwrap().get_bool(k) {
                        val.reborrow().set_bool(v);
                    }
                }
                FieldType::DATE => {
                    if let Ok(v) = self.scan.lock().unwrap().get_date(k) {
                        let mut dt = val.reborrow().init_date();
                        dt.set_year(v.year() as i16);
                        dt.set_month(v.month() as u8);
                        dt.set_day(v.day() as u8);
                    }
                }
            }
        }

        Promise::ok(())
    }
    fn get_rows(
        &mut self,
        params: remote_result_set::GetRowsParams,
        mut results: remote_result_set::GetRowsResults,
    ) -> Promise<(), capnp::Error> {
        let limit = pry!(params.get()).get_limit();
        trace!("get_rows with limit: {}", limit);
        let mut rows = results.get().init_rows(limit);
        let mut c = 0;

        for i in 0..limit {
            let has_next = self.scan.lock().unwrap().next();
            if has_next {
                let mut map = rows.reborrow().get(i).init_map();
                let mut entries = map.reborrow().init_entries(self.sch.fields().len() as u32);
                for (i, (k, fi)) in self.sch.info().into_iter().enumerate() {
                    entries
                        .reborrow()
                        .get(i as u32)
                        .set_key(k.as_str())
                        .unwrap();
                    let mut val = entries.reborrow().get(i as u32).init_value();
                    match fi.fld_type {
                        FieldType::SMALLINT => {
                            if let Ok(v) = self.scan.lock().unwrap().get_i16(k) {
                                val.reborrow().set_int16(v);
                            }
                        }
                        FieldType::INTEGER => {
                            if let Ok(v) = self.scan.lock().unwrap().get_i32(k) {
                                val.reborrow().set_int32(v);
                            }
                        }
                        FieldType::VARCHAR => {
                            if let Ok(s) = self.scan.lock().unwrap().get_string(k) {
                                val.reborrow().set_string(s.as_str());
                            }
                        }
                        FieldType::BOOL => {
                            if let Ok(v) = self.scan.lock().unwrap().get_bool(k) {
                                val.reborrow().set_bool(v);
                            }
                        }
                        FieldType::DATE => {
                            if let Ok(v) = self.scan.lock().unwrap().get_date(k) {
                                let mut dt = val.reborrow().init_date();
                                dt.set_year(v.year() as i16);
                                dt.set_month(v.month() as u8);
                                dt.set_day(v.day() as u8);
                            }
                        }
                    }
                }
                c += 1;
            } else {
                break;
            }
        }
        // Set the true length.
        // Because it is not possible to know before the iteration.
        // Also, any blank space will cause an error in the next process.
        trace!("get_rows count: {}", c);
        results.get().set_count(c);

        Promise::ok(())
    }
    fn get_int16(
        &mut self,
        params: remote_result_set::GetInt16Params,
        mut results: remote_result_set::GetInt16Results,
    ) -> Promise<(), capnp::Error> {
        let fldname = pry!(pry!(params.get()).get_fldname()).to_str().unwrap();
        debug!("get int16 value: {}", fldname);
        let val = self
            .scan
            .lock()
            .unwrap()
            .get_i16(fldname)
            .expect("get int16");
        let val: int16_box::Client = capnp_rpc::new_client(Int16BoxImpl::new(val));
        results.get().set_val(val);

        Promise::ok(())
    }
    fn get_int32(
        &mut self,
        params: remote_result_set::GetInt32Params,
        mut results: remote_result_set::GetInt32Results,
    ) -> Promise<(), capnp::Error> {
        let fldname = pry!(pry!(params.get()).get_fldname()).to_str().unwrap();
        debug!("get int32 value: {}", fldname);
        let val = self
            .scan
            .lock()
            .unwrap()
            .get_i32(fldname)
            .expect("get int32");
        let val: int32_box::Client = capnp_rpc::new_client(Int32BoxImpl::new(val));
        results.get().set_val(val);

        Promise::ok(())
    }
    fn get_string(
        &mut self,
        params: remote_result_set::GetStringParams,
        mut results: remote_result_set::GetStringResults,
    ) -> Promise<(), capnp::Error> {
        let fldname = pry!(pry!(params.get()).get_fldname()).to_str().unwrap();
        debug!("get string value: {}", fldname);
        let val = self
            .scan
            .lock()
            .unwrap()
            .get_string(fldname)
            .expect("get string");
        let val: string_box::Client = capnp_rpc::new_client(StringBoxImpl::new(val));
        results.get().set_val(val);

        Promise::ok(())
    }
    fn get_bool(
        &mut self,
        params: remote_result_set::GetBoolParams,
        mut results: remote_result_set::GetBoolResults,
    ) -> Promise<(), capnp::Error> {
        let fldname = pry!(pry!(params.get()).get_fldname()).to_str().unwrap();
        debug!("get bool value: {}", fldname);
        let val = self
            .scan
            .lock()
            .unwrap()
            .get_bool(fldname)
            .expect("get bool");
        let val: bool_box::Client = capnp_rpc::new_client(BoolBoxImpl::new(val));
        results.get().set_val(val);

        Promise::ok(())
    }
    fn get_date(
        &mut self,
        params: remote_result_set::GetDateParams,
        mut results: remote_result_set::GetDateResults,
    ) -> Promise<(), capnp::Error> {
        let fldname = pry!(pry!(params.get()).get_fldname()).to_str().unwrap();
        debug!("get date value: {}", fldname);
        let val = self
            .scan
            .lock()
            .unwrap()
            .get_date(fldname)
            .expect("get date");
        let val: date_box::Client = capnp_rpc::new_client(DateBoxImpl::new(val));
        results.get().set_val(val);

        Promise::ok(())
    }
}

pub struct RemoteMetaDataImpl {
    sch: Arc<Schema>,
}
impl RemoteMetaDataImpl {
    pub fn new(sch: Arc<Schema>) -> Self {
        Self { sch }
    }
}
impl remote_meta_data::Server for RemoteMetaDataImpl {
    fn get_schema(
        &mut self,
        _: remote_meta_data::GetSchemaParams,
        mut results: remote_meta_data::GetSchemaResults,
    ) -> Promise<(), capnp::Error> {
        trace!("get schema");
        let mut schema = results.get().init_sch();
        set_schema(Arc::clone(&self.sch), &mut schema);

        Promise::ok(())
    }
}
