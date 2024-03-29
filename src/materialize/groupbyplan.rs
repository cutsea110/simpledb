use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::{aggregationfn::AggregationFn, groupbyscan::GroupByScan, sortplan::SortPlan};
use crate::{
    plan::plan::Plan,
    query::{constant::Constant, scan::Scan},
    record::schema::Schema,
    repr::planrepr::{Operation, PlanRepr},
    tx::transaction::Transaction,
};

#[derive(Clone)]
pub struct GroupByPlan {
    p: Arc<dyn Plan>,
    groupfields: Vec<String>,
    aggfns: Vec<Arc<dyn AggregationFn>>,
    sch: Arc<Schema>,
}

impl GroupByPlan {
    pub fn new(
        next_table_num: Arc<Mutex<i32>>,
        tx: Arc<Mutex<Transaction>>,
        p: Arc<dyn Plan>,
        groupfields: Vec<String>,
        aggfns: Vec<Arc<dyn AggregationFn>>,
    ) -> Self {
        let plan = SortPlan::new(
            Arc::clone(&next_table_num),
            p,
            groupfields.clone(),
            Arc::clone(&tx),
        );
        let mut sch = Schema::new();

        for fldname in groupfields.iter() {
            sch.add(fldname, plan.schema());
        }
        for aggfn in aggfns.iter() {
            sch.add_i32_field(&aggfn.field_name());
        }

        Self {
            p: Arc::new(plan),
            groupfields,
            aggfns,
            sch: Arc::new(sch),
        }
    }
    // my own extends
    pub fn aggfns(&self) -> Vec<Arc<dyn AggregationFn>> {
        self.aggfns.clone()
    }
}

impl Plan for GroupByPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let s = self.p.open()?;
        let scan = GroupByScan::new(s, self.groupfields.clone(), self.aggfns.clone());

        Ok(Arc::new(Mutex::new(scan)))
    }
    fn blocks_accessed(&self) -> i32 {
        self.p.blocks_accessed()
    }
    fn records_output(&self) -> i32 {
        let mut numgroups = 1;
        for fldname in self.groupfields.iter() {
            numgroups *= self.p.distinct_values(fldname);
        }

        numgroups
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        if self.p.schema().has_field(fldname) {
            self.p.distinct_values(fldname)
        } else {
            self.records_output()
        }
    }
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.sch)
    }

    fn repr(&self) -> Arc<dyn PlanRepr> {
        Arc::new(GroupByPlanRepr {
            p: self.p.repr(),
            fields: self.groupfields.clone(),
            aggfns: self
                .aggfns()
                .iter()
                .map(|f| (f.field_name(), f.value()))
                .collect(),
            r: self.blocks_accessed(),
            w: self.records_output(),
        })
    }
}

#[derive(Clone)]
pub struct GroupByPlanRepr {
    p: Arc<dyn PlanRepr>,
    fields: Vec<String>,
    aggfns: Vec<(String, Constant)>,
    r: i32,
    w: i32,
}

impl PlanRepr for GroupByPlanRepr {
    fn operation(&self) -> Operation {
        Operation::GroupByScan {
            fields: self.fields.clone(),
            aggfns: self.aggfns.clone(),
        }
    }
    fn reads(&self) -> i32 {
        self.r
    }
    fn writes(&self) -> i32 {
        self.w
    }
    fn sub_plan_reprs(&self) -> Vec<Arc<dyn PlanRepr>> {
        vec![Arc::clone(&self.p)]
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::*;
    use crate::{
        materialize::aggregationfn::maxfn::MaxFn, metadata::manager::MetadataMgr,
        plan::tableplan::TablePlan, query::tests, server::simpledb::SimpleDB,
    };

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/groupbyplan").exists() {
            fs::remove_dir_all("_test/groupbyplan")?;
        }

        let simpledb = SimpleDB::new_with("_test/groupbyplan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let next_table_num = Arc::new(Mutex::new(0));
        let mut mdm = MetadataMgr::new(true, Arc::clone(&tx))?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        tests::init_sampledb(&mut mdm, Arc::clone(&tx))?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let mdm = Arc::new(Mutex::new(mdm));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let srcplan = Arc::new(TablePlan::new(
            "STUDENT",
            Arc::clone(&tx),
            Arc::clone(&mdm),
        )?);
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let plan = GroupByPlan::new(
            Arc::clone(&next_table_num),
            Arc::clone(&tx),
            srcplan,
            vec!["MajorId".to_string()],
            vec![Arc::new(MaxFn::new("GradYear"))],
        );

        let scan = plan.open()?;
        scan.lock().unwrap().before_first()?;
        let mut iter = scan.lock().unwrap();
        while iter.next() {
            let major_id = iter.get_i32("MajorId")?;
            let maxof_gradyear = iter.get_i32("maxofGradYear")?;
            println!("{:>8}{:>8}", major_id, maxof_gradyear);
        }
        tx.lock().unwrap().commit()?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        Ok(())
    }
}
