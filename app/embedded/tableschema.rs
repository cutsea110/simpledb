use std::{collections::HashMap, sync::Arc};

use simpledb::{
    rdbc::model::IndexInfo,
    record::schema::{FieldType, Schema},
};

pub fn print_table_schema(
    tblname: &str,
    schema: Arc<Schema>,
    idx_info: HashMap<String, IndexInfo>,
) {
    println!(
        " * table: {} has {} fields.\n",
        tblname,
        schema.fields().len()
    );

    println!(" #   name             type");
    println!("--------------------------------------");
    for (i, fldname) in schema.fields().iter().enumerate() {
        let fldtyp = match schema.field_type(fldname) {
            FieldType::WORD => "int8".to_string(),
            FieldType::UWORD => "uint8".to_string(),
            FieldType::SHORT => "int16".to_string(),
            FieldType::USHORT => "uint16".to_string(),
            FieldType::INTEGER => "int32".to_string(),
            FieldType::UINTEGER => "uint32".to_string(),
            FieldType::VARCHAR => format!("varchar({})", schema.length(fldname)),
            FieldType::BOOL => "bool".to_string(),
            FieldType::DATE => "date".to_string(),
        };
        println!("{:>4} {:16} {:16}", i + 1, fldname, fldtyp);
    }
    println!();

    if !idx_info.is_empty() {
        println!(" * indexes on {}\n", tblname);

        println!(" #   name             field");
        println!("--------------------------------------");
        for (i, (_, ii)) in idx_info.iter().enumerate() {
            println!("{:>4} {:16} {:16}", i + 1, ii.index_name(), ii.field_name());
        }
        println!();
    }
}
