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

pub trait PlanRepr {
    fn operation(&self) -> Operation;
    fn reads(&self) -> i32;
    fn writes(&self) -> i32;
}
