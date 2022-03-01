use itertools::Itertools;

use crate::query::constant::Constant;

pub struct InsertData {
    tblname: String,
    flds: Vec<String>,
    vals: Vec<Constant>,
}

impl InsertData {
    pub fn new(tblname: String, flds: Vec<String>, vals: Vec<Constant>) -> Self {
        Self {
            tblname,
            flds,
            vals,
        }
    }
    pub fn table_name(&self) -> &str {
        &self.tblname
    }
    pub fn fields(&self) -> Vec<&String> {
        self.flds.iter().collect()
    }
    pub fn vals(&self) -> Vec<&Constant> {
        self.vals.iter().collect()
    }
}
