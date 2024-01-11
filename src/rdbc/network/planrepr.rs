use chrono::NaiveDate;
use itertools::Itertools;
use std::sync::Arc;

use crate::{query, remote_capnp::remote_statement, repr, repr::planrepr::PlanRepr};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Constant {
    I16(i16),
    I32(i32),
    String(String),
    Bool(bool),
    Date(NaiveDate),
}
impl<'a> From<remote_statement::constant::Reader<'a>> for Constant {
    fn from(c: remote_statement::constant::Reader<'a>) -> Self {
        match c.which().unwrap() {
            remote_statement::constant::Int16(v) => Self::I16(v),
            remote_statement::constant::Int32(v) => Self::I32(v),
            remote_statement::constant::String(s) => Self::String(s.unwrap().to_string().unwrap()),
            remote_statement::constant::Bool(v) => Self::Bool(v),
            remote_statement::constant::Date(v) => {
                let v = v.unwrap();
                let year = v.get_year() as i32;
                let month = v.get_month() as u32;
                let day = v.get_day() as u32;
                Self::Date(NaiveDate::from_ymd_opt(year, month, day).unwrap())
            }
        }
    }
}
impl From<Constant> for query::constant::Constant {
    fn from(c: Constant) -> Self {
        match c {
            Constant::I16(v) => Self::I16(v),
            Constant::I32(v) => Self::I32(v),
            Constant::String(s) => Self::String(s),
            Constant::Bool(v) => Self::Bool(v),
            Constant::Date(v) => Self::Date(v),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Predicate {
    terms: Vec<Term>,
}
impl<'a> From<remote_statement::predicate::Reader<'a>> for Predicate {
    fn from(pred: remote_statement::predicate::Reader<'a>) -> Self {
        let terms = pred
            .get_terms()
            .unwrap()
            .into_iter()
            .map(|t| Term::from(t))
            .collect_vec();
        Self { terms }
    }
}
impl From<Predicate> for query::predicate::Predicate {
    fn from(pred: Predicate) -> Self {
        let terms = pred.terms.into_iter().map(|t| t.into()).collect_vec();
        let mut result = Self::new_empty();
        result.init_with_terms(terms);
        result
    }
}
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Term {
    lhs: Expression,
    rhs: Expression,
}
impl<'a> From<remote_statement::term::Reader<'a>> for Term {
    fn from(t: remote_statement::term::Reader<'a>) -> Self {
        let lhs = Expression::from(t.get_lhs().unwrap());
        let rhs = Expression::from(t.get_rhs().unwrap());
        Self { lhs, rhs }
    }
}
impl From<Term> for query::term::Term {
    fn from(t: Term) -> Self {
        Self::new(t.lhs.into(), t.rhs.into())
    }
}
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Expression {
    Val(Constant),
    Fldname(String),
}
impl<'a> From<remote_statement::expression::Reader<'a>> for Expression {
    fn from(expr: remote_statement::expression::Reader<'a>) -> Self {
        match expr.which().unwrap() {
            remote_statement::expression::Val(v) => {
                let c = Constant::from(v.unwrap());
                Self::Val(c)
            }
            remote_statement::expression::Fldname(s) => {
                let s = s.unwrap().to_string().unwrap();
                Self::Fldname(s)
            }
        }
    }
}
impl From<Expression> for query::expression::Expression {
    fn from(expr: Expression) -> Self {
        match expr {
            Expression::Val(v) => Self::Val(v.into()),
            Expression::Fldname(s) => Self::Fldname(s),
        }
    }
}

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
impl<'a> From<remote_statement::plan_repr::operation::Reader<'a>> for Operation {
    fn from(op: remote_statement::plan_repr::operation::Reader) -> Self {
        match op.which().unwrap() {
            remote_statement::plan_repr::operation::IndexJoinScan(v) => {
                let v = v.unwrap();
                let idxname = v.get_idxname().unwrap().to_string().unwrap();
                let idxfldname = v.get_idxfldname().unwrap().to_string().unwrap();
                let joinfld = v.get_joinfld().unwrap().to_string().unwrap();
                Self::IndexJoinScan {
                    idxname,
                    idxfldname,
                    joinfld,
                }
            }
            remote_statement::plan_repr::operation::IndexSelectScan(v) => {
                let v = v.unwrap();
                let idxname = v.get_idxname().unwrap().to_string().unwrap();
                let idxfldname = v.get_idxfldname().unwrap().to_string().unwrap();
                let val = Constant::from(v.get_val().unwrap());
                Self::IndexSelectScan {
                    idxname,
                    idxfldname,
                    val,
                }
            }
            remote_statement::plan_repr::operation::GroupByScan(v) => {
                let v = v.unwrap();
                let fields = v
                    .get_fields()
                    .unwrap()
                    .into_iter()
                    .map(|s| s.unwrap().to_string().unwrap())
                    .collect_vec();
                let aggfns = v
                    .get_aggfns()
                    .unwrap()
                    .into_iter()
                    .map(|f| {
                        let fst = f.get_fst().unwrap().to_string().unwrap();
                        let snd = Constant::from(f.get_snd().unwrap());
                        (fst, snd)
                    })
                    .collect_vec();
                Self::GroupByScan { fields, aggfns }
            }
            remote_statement::plan_repr::operation::Materialize(_) => Self::Materialize,
            remote_statement::plan_repr::operation::MergeJoinScan(v) => {
                let v = v.unwrap();
                let fldname1 = v.get_fldname1().unwrap().to_string().unwrap();
                let fldname2 = v.get_fldname2().unwrap().to_string().unwrap();
                Self::MergeJoinScan { fldname1, fldname2 }
            }
            remote_statement::plan_repr::operation::SortScan(v) => {
                let v = v.unwrap();
                let compflds = v
                    .get_compflds()
                    .unwrap()
                    .into_iter()
                    .map(|s| s.unwrap().to_string().unwrap())
                    .collect_vec();
                Self::SortScan { compflds }
            }
            remote_statement::plan_repr::operation::MultibufferProductScan(_) => {
                Self::MultibufferProductScan
            }
            remote_statement::plan_repr::operation::ProductScan(_) => Self::ProductScan,
            remote_statement::plan_repr::operation::ProjectScan(_) => Self::ProjectScan,
            remote_statement::plan_repr::operation::SelectScan(v) => {
                let v = v.unwrap();
                let pred = Predicate::from(v.get_pred().unwrap());
                Self::SelectScan { pred }
            }
            remote_statement::plan_repr::operation::TableScan(v) => {
                let tblname = v.unwrap().get_tblname().unwrap().to_string().unwrap();
                Self::TableScan { tblname }
            }
        }
    }
}

impl From<Operation> for repr::planrepr::Operation {
    fn from(op: Operation) -> Self {
        match op {
            Operation::IndexJoinScan {
                idxname,
                idxfldname,
                joinfld,
            } => Self::IndexJoinScan {
                idxname,
                idxfldname,
                joinfld,
            },
            Operation::IndexSelectScan {
                idxname,
                idxfldname,
                val,
            } => Self::IndexSelectScan {
                idxname,
                idxfldname,
                val: val.into(),
            },
            Operation::GroupByScan { fields, aggfns } => Self::GroupByScan {
                fields,
                aggfns: aggfns.into_iter().map(|(s, v)| (s, v.into())).collect_vec(),
            },
            Operation::Materialize => Self::Materialize,
            Operation::MergeJoinScan { fldname1, fldname2 } => {
                Self::MergeJoinScan { fldname1, fldname2 }
            }
            Operation::SortScan { compflds } => Self::SortScan { compflds },
            Operation::MultibufferProductScan => Self::MultibufferProductScan,
            Operation::ProductScan => Self::ProductScan,
            Operation::ProjectScan => Self::ProjectScan,
            Operation::SelectScan { pred } => Self::SelectScan { pred: pred.into() },
            Operation::TableScan { tblname } => Self::TableScan { tblname },
        }
    }
}

#[derive(Clone)]
pub struct NetworkPlanRepr {
    operation: repr::planrepr::Operation,
    reads: i32,
    writes: i32,
    sub_plan_reprs: Vec<Arc<dyn PlanRepr>>,
}

impl NetworkPlanRepr {
    pub fn repr(&self) -> Arc<dyn PlanRepr> {
        Arc::new(self.clone())
    }
}

impl<'a> From<remote_statement::plan_repr::Reader<'a>> for NetworkPlanRepr {
    fn from(repr: remote_statement::plan_repr::Reader<'a>) -> Self {
        let mut subs = vec![];
        for v in repr.get_sub_plan_reprs().unwrap().iter() {
            let v = NetworkPlanRepr::from(v).repr();
            subs.push(v);
        }
        Self {
            operation: Operation::from(repr.get_operation()).into(),
            reads: repr.get_reads(),
            writes: repr.get_writes(),
            sub_plan_reprs: subs,
        }
    }
}

impl PlanRepr for NetworkPlanRepr {
    fn operation(&self) -> repr::planrepr::Operation {
        self.operation.clone()
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
