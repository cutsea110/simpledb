use anyhow::Result;
use combine::Parser;
use std::sync::{Arc, Mutex};

use super::{plan::Plan, queryplanner::QueryPlanner};
use crate::{
    metadata::manager::MetadataMgr,
    parser::{parser::query, querydata::QueryData},
    plan::{
        productplan::ProductPlan, projectplan::ProjectPlan, selectplan::SelectPlan,
        tableplan::TablePlan,
    },
    tx::transaction::Transaction,
};

#[derive(Debug, Clone)]
pub struct BasicQueryPlanner {
    mdm: Arc<Mutex<MetadataMgr>>,
}

impl QueryPlanner for BasicQueryPlanner {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<dyn Plan>> {
        // Step 1: Create a plan for each mentioned table or view
        let mut plans: Vec<Arc<dyn Plan>> = vec![];
        for tblname in data.tables() {
            let mut viewdef = self
                .mdm
                .lock()
                .unwrap()
                .get_view_def(tblname, Arc::clone(&tx))?;
            if !viewdef.is_empty() {
                // Recursively plan the view.
                let mut parser = query();
                // NOTE: query parser expect terminater.
                viewdef = format!("{};", viewdef);
                let (viewdata, _) = parser.parse(viewdef.as_str())?;
                plans.push(self.create_plan(viewdata, Arc::clone(&tx))?);
            } else {
                plans.push(Arc::new(TablePlan::new(
                    tblname,
                    Arc::clone(&tx),
                    self.mdm.clone(),
                )?))
            }
        }
        // Step 2: Create the product of all table plans
        let mut p = plans.remove(0);
        for nextplan in plans {
            p = Arc::new(ProductPlan::new(Arc::clone(&p), nextplan));
        }
        // Step 3: Add a selection plan for the predicate
        p = Arc::new(SelectPlan::new(Arc::clone(&p), data.pred().clone()));

        // Step 4: Project on the field names
        Ok(Arc::new(ProjectPlan::new(p, data.fields().clone())))
    }
}

impl BasicQueryPlanner {
    pub fn new(mdm: Arc<Mutex<MetadataMgr>>) -> Self {
        Self { mdm }
    }
}
