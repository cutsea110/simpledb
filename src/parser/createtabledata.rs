use crate::record::schema::Schema;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreateTableData {
    tblname: String,
    sch: Schema,
}

impl CreateTableData {
    pub fn new(tblname: String, sch: Schema) -> Self {
        Self { tblname, sch }
    }
    pub fn table_name(&self) -> &str {
        &self.tblname
    }
    pub fn new_schema(&self) -> &Schema {
        &self.sch
    }
}
