#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum Operation {
    // TODO
}

pub trait PlanRepr {
    fn operation(&self) -> Operation;
    fn reads(&self) -> Option<i32>;
    fn buffers(&self) -> Option<i32>;
}
