use std::sync::Arc;

use crate::{
    rdbc::planrepradapter::PlanReprAdapter,
    remote_capnp::remote_statement,
    repr::planrepr::{self, PlanRepr},
};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum Operation {
    IndexJoinScan,
    IndexSelectScan,
    GroupByScan,
    Materialize,
    MergeJoinScan,
    SortScan,
    MultibufferProductScan,
    ProductScan,
    ProjectScan,
    SelectScan,
    TableScan,
}
impl Operation {
    fn from_reader(op: remote_statement::Operation) -> Self {
        match op {
            remote_statement::Operation::IndexJoinScan => Self::IndexJoinScan,
            remote_statement::Operation::IndexSelectScan => Self::IndexSelectScan,
            remote_statement::Operation::GroupByScan => Self::GroupByScan,
            remote_statement::Operation::Materialize => Self::Materialize,
            remote_statement::Operation::MergeJoinScan => Self::MergeJoinScan,
            remote_statement::Operation::SortScan => Self::SortScan,
            remote_statement::Operation::MultibufferProductScan => Self::MultibufferProductScan,
            remote_statement::Operation::ProductScan => Self::ProductScan,
            remote_statement::Operation::ProjectScan => Self::ProjectScan,
            remote_statement::Operation::SelectScan => Self::SelectScan,
            remote_statement::Operation::TableScan => Self::TableScan,
        }
    }
}

impl From<Operation> for planrepr::Operation {
    fn from(op: Operation) -> Self {
        panic!("TODO")
    }
}

#[derive(Clone)]
pub struct NetworkPlanRepr {
    operation: Operation,
    reads: i32,
    writes: i32,
    sub_plan_reprs: Vec<Arc<dyn PlanRepr>>,
}

impl NetworkPlanRepr {
    pub fn from_reader(repr: remote_statement::plan_repr::Reader) -> Self {
        let mut subs = vec![];
        for v in repr.get_sub_plan_reprs().unwrap().iter() {
            let v = NetworkPlanRepr::from_reader(v).to_plan_repr();
            subs.push(v);
        }
        Self {
            operation: Operation::from_reader(repr.get_operation().unwrap()),
            reads: repr.get_reads(),
            writes: repr.get_writes(),
            sub_plan_reprs: subs,
        }
    }
    fn to_plan_repr(&self) -> Arc<dyn PlanRepr> {
        Arc::new(self.clone())
    }
}

impl PlanRepr for NetworkPlanRepr {
    fn operation(&self) -> planrepr::Operation {
        self.operation.clone().into()
    }
    fn reads(&self) -> i32 {
        self.reads
    }
    fn writes(&self) -> i32 {
        self.writes
    }
    fn sub_plan_reprs(&self) -> Vec<Arc<dyn PlanRepr>> {
        self.sub_plan_reprs.clone()
    }
}

impl PlanReprAdapter for NetworkPlanRepr {
    fn repr(&self) -> Arc<dyn PlanRepr> {
        self.to_plan_repr()
    }
}
