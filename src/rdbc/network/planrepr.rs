use std::sync::Arc;

use crate::{
    rdbc::planrepradapter::PlanReprAdapter, remote_capnp::remote_statement,
    repr::planrepr::PlanRepr,
};

pub struct NetworkPlanRepr {
    // TODO
}

impl NetworkPlanRepr {
    pub fn from_reader(repr: remote_statement::plan_repr::Reader) -> Self {
        panic!("TODO")
    }
}

impl PlanReprAdapter for NetworkPlanRepr {
    fn repr(&self) -> Arc<dyn PlanRepr> {
        panic!("TODO")
    }
}
