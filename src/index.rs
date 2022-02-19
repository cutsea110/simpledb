use anyhow::Result;

use crate::{query::constant::Constant, record::rid::RID};

pub mod hash;

pub trait Index {
    fn before_first(&self, searchkey: Constant) -> Result<()>;
    fn next(&self) -> bool;
    fn get_data_rid(&self) -> Result<RID>;
    fn insert(&mut self, dataval: Constant, datarid: RID) -> Result<()>;
    fn delete(&mut self, dataval: Constant, datarid: RID) -> Result<()>;
    fn close(&mut self) -> Result<()>;
}
