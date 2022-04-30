use std::sync::Arc;

use crate::{rdbc::planrepradapter::PlanReprAdapter, repr::planrepr::PlanRepr};

pub struct NetworkPlanRepr {
    // TODO
}

impl PlanReprAdapter for NetworkPlanRepr {
    fn repr(&self) -> Arc<dyn PlanRepr> {
        panic!("TODO")
    }
}
