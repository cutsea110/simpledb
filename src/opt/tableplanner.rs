use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    index::planner::{indexjoinplan::IndexJoinPlan, indexselectplan::IndexSelectPlan},
    metadata::{indexmanager::IndexInfo, manager::MetadataMgr},
    multibuffer::multibufferproductplan::MultibufferProductPlan,
    plan::{plan::Plan, selectplan::SelectPlan, tableplan::TablePlan},
    query::predicate::Predicate,
    record::schema::Schema,
    tx::transaction::Transaction,
};

#[derive(Debug, Clone)]
pub struct TablePlanner {
    // static member (shared by all Materializeplan and Temptable)
    next_table_num: Arc<Mutex<i32>>,

    myplan: Arc<TablePlan>,
    mypred: Predicate,
    myschema: Arc<Schema>,
    indexes: HashMap<String, IndexInfo>,
    tx: Arc<Mutex<Transaction>>,
}

impl TablePlanner {
    pub fn new(
        next_table_num: Arc<Mutex<i32>>,
        tblname: &str,
        mypred: Predicate,
        tx: Arc<Mutex<Transaction>>,
        mdm: Arc<Mutex<MetadataMgr>>,
    ) -> Self {
        let myplan = Arc::new(TablePlan::new(tblname, Arc::clone(&tx), Arc::clone(&mdm)).unwrap());
        let myschema = myplan.schema();
        let mut mdm = mdm.lock().unwrap();
        let indexes = mdm.get_index_info(tblname, Arc::clone(&tx)).unwrap();

        Self {
            next_table_num,
            myplan,
            mypred,
            myschema,
            indexes,
            tx,
        }
    }
    pub fn make_select_plan(&self) -> Option<Arc<dyn Plan>> {
        let p = match self.make_index_select() {
            Some(p) => p,
            None => Arc::clone(&self.myplan),
        };

        self.add_select_pred(p)
    }
    pub fn make_join_plan(&self, current: Arc<dyn Plan>) -> Option<Arc<dyn Plan>> {
        let currsch = current.schema();
        let joinpred = self
            .mypred
            .join_sub_pred(Arc::clone(&self.myschema), Arc::clone(&currsch));
        if joinpred.is_none() {
            return None;
        }
        let mut p = self.make_index_join(Arc::clone(&current), Arc::clone(&currsch));
        if p.is_none() {
            p = self.make_product_join(Arc::clone(&current), Arc::clone(&currsch));
        }

        p
    }
    pub fn make_product_plan(&self, current: Arc<dyn Plan>) -> Option<Arc<dyn Plan>> {
        let myplan = Arc::clone(&self.myplan);
        if let Some(p) = self.add_select_pred(myplan) {
            return Some(Arc::new(MultibufferProductPlan::new(
                Arc::clone(&self.next_table_num),
                Arc::clone(&self.tx),
                current,
                p,
            )));
        }

        None
    }
    fn make_index_select(&self) -> Option<Arc<dyn Plan>> {
        for fldname in self.indexes.keys() {
            if let Some(val) = self.mypred.equates_with_constant(fldname) {
                let ii = self.indexes.get(fldname).unwrap();
                let myplan = Arc::clone(&self.myplan);
                let plan = IndexSelectPlan::new(myplan, ii.clone(), val.clone());
                return Some(Arc::new(plan));
            }
        }

        None
    }
    fn make_index_join(
        &self,
        current: Arc<dyn Plan>,
        currsch: Arc<Schema>,
    ) -> Option<Arc<dyn Plan>> {
        for fldname in self.indexes.keys() {
            if let Some(outerfield) = self.mypred.equates_with_field(fldname) {
                if currsch.has_field(outerfield) {
                    let ii = self.indexes.get(fldname).unwrap().clone();
                    let myplan = Arc::clone(&self.myplan);
                    let plan = IndexJoinPlan::new(current, myplan, ii, outerfield);
                    let mut p: Option<Arc<dyn Plan>> = Some(Arc::new(plan));
                    p = self.add_select_pred(p.unwrap());
                    return self.add_join_pred(p.unwrap(), currsch);
                }
            }
        }

        None
    }
    fn make_product_join(
        &self,
        current: Arc<dyn Plan>,
        currsch: Arc<Schema>,
    ) -> Option<Arc<dyn Plan>> {
        let p = self.make_product_plan(current).unwrap();
        self.add_join_pred(p, currsch)
    }
    fn add_select_pred(&self, p: Arc<dyn Plan>) -> Option<Arc<dyn Plan>> {
        if let Some(selectpred) = self.mypred.select_sub_pred(Arc::clone(&self.myschema)) {
            return Some(Arc::new(SelectPlan::new(p, selectpred)));
        }

        Some(p)
    }
    fn add_join_pred(&self, p: Arc<dyn Plan>, currsch: Arc<Schema>) -> Option<Arc<dyn Plan>> {
        if let Some(joinpred) = self
            .mypred
            .join_sub_pred(currsch, Arc::clone(&self.myschema))
        {
            return Some(Arc::new(SelectPlan::new(p, joinpred)));
        }

        Some(p)
    }
}
