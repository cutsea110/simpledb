use crate::record::schema::Schema;

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
