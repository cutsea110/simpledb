use std::sync::Arc;

use crate::query::{constant::Constant, predicate::Predicate};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum Operation {
    IndexJoinScan {
        idxname: String,
        idxfldname: String,
        joinfld: String,
    },
    IndexSelectScan {
        idxname: String,
        idxfldname: String,
        val: Constant,
    },
    GroupByScan {
        fields: Vec<String>,
        aggfns: Vec<(String, Constant)>,
    },
    Materialize,
    MergeJoinScan {
        fldname1: String,
        fldname2: String,
    },
    SortScan {
        compflds: Vec<String>,
    },
    MultibufferProductScan,
    ProductScan,
    ProjectScan,
    SelectScan {
        pred: Predicate,
    },
    TableScan {
        tblname: String,
    },
}

pub trait PlanRepr {
    fn operation(&self) -> Operation;
    fn reads(&self) -> i32;
    fn writes(&self) -> i32;
    fn sub_plan_reprs(&self) -> Vec<Arc<dyn PlanRepr>>;
}
