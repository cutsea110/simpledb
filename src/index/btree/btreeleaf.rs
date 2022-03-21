use std::sync::{Arc, Mutex};

use crate::{query::constant::Constant, record::layout::Layout, tx::transaction::Transaction};

use super::btpage::BTPage;

pub struct BTreeLeaf {
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    searchkey: Constant,
    constants: BTPage,
    currentslot: i32,
    filename: String,
}
