use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::tableplanner::TablePlanner;
use crate::{
    metadata::manager::MetadataMgr,
    parser::querydata::QueryData,
    plan::{plan::Plan, planner::Planner, projectplan::ProjectPlan, queryplanner::QueryPlanner},
    tx::transaction::Transaction,
};

#[derive(Debug)]
pub enum HeuristicQueryPlannerError {
    NoPlan,
}

impl std::error::Error for HeuristicQueryPlannerError {}
impl fmt::Display for HeuristicQueryPlannerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            HeuristicQueryPlannerError::NoPlan => {
                write!(f, "no plan")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct HeuristicQueryPlanner {
    // static member (shared by all Materializeplan and Temptable)
    next_table_num: Arc<Mutex<i32>>,

    tableplanners: Vec<TablePlanner>,
    mdm: Arc<Mutex<MetadataMgr>>,
}

impl HeuristicQueryPlanner {
    pub fn new(next_table_num: Arc<Mutex<i32>>, mdm: Arc<Mutex<MetadataMgr>>) -> Self {
        Self {
            next_table_num,
            tableplanners: vec![],
            mdm,
        }
    }
    fn get_lowest_select_plan(&mut self) -> Result<Arc<dyn Plan>> {
        // We must to keep this index in order to remove the TablePlanner after.
        let mut besttp = -1;
        let mut bestplan: Option<Arc<dyn Plan>> = None;
        for i in 0..self.tableplanners.len() {
            let tp = &self.tableplanners[i];
            let plan = tp.make_select_plan();
            if besttp == -1
                || plan.as_ref().unwrap().records_output()
                    < bestplan.as_ref().unwrap().records_output()
            {
                besttp = i as i32;
                bestplan = plan;
            }
        }

        self.tableplanners.remove(besttp as usize);
        bestplan.ok_or_else(|| From::from(HeuristicQueryPlannerError::NoPlan))
    }
    fn get_lowest_join_plan(&mut self, current: Arc<dyn Plan>) -> Result<Arc<dyn Plan>> {
        // We must to keep this index in order to remove the TablePlanner after.
        let mut besttp = -1;
        let mut bestplan: Option<Arc<dyn Plan>> = None;
        for i in 0..self.tableplanners.len() {
            let tp = &self.tableplanners[i];
            let plan = tp.make_join_plan(Arc::clone(&current));
            if plan.is_some()
                && (bestplan.is_none()
                    || plan.as_ref().unwrap().records_output()
                        < bestplan.as_ref().unwrap().records_output())
            {
                besttp = i as i32;
                bestplan = plan;
            }
        }

        if bestplan.is_some() {
            self.tableplanners.remove(besttp as usize);
        }
        bestplan.ok_or_else(|| From::from(HeuristicQueryPlannerError::NoPlan))
    }
    fn get_lowest_product_plan(&mut self, current: Arc<dyn Plan>) -> Result<Arc<dyn Plan>> {
        // We must to keep this index in order to remove the TablePlanner after.
        let mut besttp = -1;
        let mut bestplan: Option<Arc<dyn Plan>> = None;
        for i in 0..self.tableplanners.len() {
            let tp = &self.tableplanners[i];
            let plan = tp.make_product_plan(Arc::clone(&current));
            if bestplan.is_none()
                || plan.as_ref().unwrap().records_output()
                    < bestplan.as_ref().unwrap().records_output()
            {
                besttp = i as i32;
                bestplan = plan;
            }
        }

        self.tableplanners.remove(besttp as usize);
        bestplan.ok_or_else(|| From::from(HeuristicQueryPlannerError::NoPlan))
    }
    pub fn set_planner(&mut self, _p: Planner) {
        // for use in planning views, which
        // for simplicity this code doesn't do.
    }
}

impl QueryPlanner for HeuristicQueryPlanner {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<dyn Plan>> {
        // Step 1, Create a TablePlanner object for each mentioned table
        for tblname in data.tables().iter() {
            let tp = TablePlanner::new(
                Arc::clone(&self.next_table_num),
                tblname,
                data.pred().clone(),
                Arc::clone(&tx),
                Arc::clone(&self.mdm),
            );
            self.tableplanners.push(tp)
        }

        // Step 2, Choose the lowest-size plan to begin the join order
        let mut currentplan = self.get_lowest_select_plan()?;

        // Step 3, Repeatedly add a plan to the join order
        while !self.tableplanners.is_empty() {
            if let Ok(p) = self.get_lowest_join_plan(Arc::clone(&currentplan)) {
                currentplan = p;
            } else {
                // no applicable join
                currentplan = self.get_lowest_product_plan(currentplan)?;
            }
        }

        // Step 4, Project on the field names and return
        Ok(Arc::new(ProjectPlan::new(
            currentplan,
            data.fields().clone(),
        )))
    }
}
