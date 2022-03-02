use crate::query::predicate::Predicate;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct DeleteData {
    tblname: String,
    pred: Predicate,
}

impl DeleteData {
    pub fn new(tblname: String, pred: Predicate) -> Self {
        Self { tblname, pred }
    }
    pub fn table_name(&self) -> &str {
        &self.tblname
    }
    pub fn pred(&self) -> &Predicate {
        &self.pred
    }
}
