use std::sync::Arc;

use crate::repr::planrepr::PlanRepr;

pub struct EmbeddedPlanRepr {
    plan_repr: Arc<dyn PlanRepr>,
}

impl EmbeddedPlanRepr {
    pub fn new(plan_repr: Arc<dyn PlanRepr>) -> Self {
        Self { plan_repr }
    }
    pub fn repr(&self) -> Arc<dyn PlanRepr> {
        Arc::clone(&self.plan_repr)
    }
}
