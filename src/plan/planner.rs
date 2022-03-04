use anyhow::Result;
use combine::Parser;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{plan::Plan, queryplanner::QueryPlanner, updateplanner::UpdatePlanner};
use crate::{
    parser::parser::{query, update_cmd},
    parser::{ddl::DDL, dml::DML, sql::SQL},
    tx::transaction::Transaction,
};

#[derive(Debug)]
pub enum PlannerError {
    InvalidExecuteCommand,
}

impl std::error::Error for PlannerError {}
impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlannerError::InvalidExecuteCommand => {
                write!(f, "invalid execute command")
            }
        }
    }
}

#[derive(Clone)]
pub struct Planner {
    qplanner: Arc<Mutex<dyn QueryPlanner>>,
    uplanner: Arc<Mutex<dyn UpdatePlanner>>,
}

impl Planner {
    pub fn new(
        qplanner: Arc<Mutex<dyn QueryPlanner>>,
        uplanner: Arc<Mutex<dyn UpdatePlanner>>,
    ) -> Self {
        Self { qplanner, uplanner }
    }
    pub fn create_query_plan(
        &mut self,
        cmd: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<dyn Plan>> {
        let mut parser = query();
        let (data, _) = parser.parse(cmd)?;
        // TODO: code to verify the query should be here...
        self.qplanner.lock().unwrap().create_plan(data, tx)
    }
    pub fn execute_update(&mut self, cmd: &str, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        let mut parser = update_cmd();
        let (data, _) = parser.parse(cmd)?;
        match data {
            SQL::DML(dml) => match dml {
                DML::Insert(idata) => {
                    let planner = self.uplanner.lock().unwrap();
                    return planner.execute_insert(idata, tx);
                }
                DML::Delete(ddata) => {
                    let planner = self.uplanner.lock().unwrap();
                    return planner.execute_delete(ddata, tx);
                }
                DML::Modify(mdata) => {
                    let planner = self.uplanner.lock().unwrap();
                    return planner.execute_modify(mdata, tx);
                }
                _ => return Err(From::from(PlannerError::InvalidExecuteCommand)),
            },
            SQL::DDL(ddl) => match ddl {
                DDL::Table(ctdata) => {
                    let p = self.uplanner.lock().unwrap();
                    return p.execute_create_table(ctdata, tx);
                }
                DDL::View(cvdata) => {
                    let p = self.uplanner.lock().unwrap();
                    return p.execute_create_view(cvdata, tx);
                }
                DDL::Index(cidata) => {
                    let p = self.uplanner.lock().unwrap();
                    return p.execute_create_index(cidata, tx);
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::{fs, path::Path};

    use crate::{
        metadata::manager::MetadataMgr,
        plan::{basicqueryplanner::BasicQueryPlanner, basicupdateplanner::BasicUpdatePlanner},
        server::simpledb::SimpleDB,
    };

    use super::*;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/selectscan").exists() {
            fs::remove_dir_all("_test/selectscan")?;
        }

        let simpledb = SimpleDB::new_with("_test/selectscan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        let mdm = Arc::new(Mutex::new(MetadataMgr::new(true, Arc::clone(&tx))?));

        let qp = Arc::new(Mutex::new(BasicQueryPlanner::new(Arc::clone(&mdm))));
        let up = Arc::new(Mutex::new(BasicUpdatePlanner::new(Arc::clone(&mdm))));
        let _planner = Planner::new(qp, up);

        Ok(())
    }
}
