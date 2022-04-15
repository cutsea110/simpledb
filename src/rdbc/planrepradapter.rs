use std::sync::Arc;

use crate::repr::planrepr::PlanRepr;

pub trait PlanReprAdapter {
    fn repr(&self) -> Arc<dyn PlanRepr>;
}
