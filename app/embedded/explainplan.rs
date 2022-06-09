use itertools::Itertools;
use std::{cell::RefCell, rc::Rc, sync::Arc};

use simpledb::repr::planrepr::{Operation, PlanRepr};

fn format_operation(op: Operation) -> String {
    match op {
        Operation::IndexJoinScan {
            idxname: _,
            idxfldname,
            joinfld,
        } => format!("INDEX JOIN SCAN BY {} = {}", idxfldname, joinfld),
        Operation::IndexSelectScan {
            idxname: _,
            idxfldname,
            val,
        } => format!("INDEX SELECT SCAN BY {} = {}", idxfldname, val),
        Operation::GroupByScan {
            fields: _,
            aggfns: _,
        } => format!("GROUP BY",),
        Operation::Materialize => format!("MATERIALIZE"),
        Operation::MergeJoinScan { fldname1, fldname2 } => {
            format!("MERGE JOIN SCAN BY {} = {}", fldname1, fldname2)
        }
        Operation::SortScan { compflds } => format!("SORT SCAN BY ({})", compflds.iter().join(",")),
        Operation::MultibufferProductScan => format!("MULTIBUFFER PRODUCT SCAN"),
        Operation::ProductScan => format!("PRODUCT SCAN"),
        Operation::ProjectScan => format!("PROJECT SCAN"),
        Operation::SelectScan { pred: _ } => format!("SELECT SCAN"),
        Operation::TableScan { tblname: _ } => format!("TABLE SCAN"),
    }
}

fn format_name(op: Operation) -> String {
    match op {
        Operation::IndexJoinScan {
            idxname,
            idxfldname: _,
            joinfld: _,
        } => format!("{}", idxname),
        Operation::IndexSelectScan {
            idxname,
            idxfldname: _,
            val: _,
        } => format!("{}", idxname),
        Operation::GroupByScan {
            fields: _,
            aggfns: _,
        } => format!(""),
        Operation::Materialize => format!(""),
        Operation::MergeJoinScan {
            fldname1: _,
            fldname2: _,
        } => format!(""),
        Operation::SortScan { compflds: _ } => format!(""),
        Operation::MultibufferProductScan => format!(""),
        Operation::ProductScan => format!(""),
        Operation::ProjectScan => format!(""),
        Operation::SelectScan { pred: _ } => format!(""),
        Operation::TableScan { tblname } => format!("{}", tblname),
    }
}

pub fn print_explain_plan(pr: Arc<dyn PlanRepr>) {
    const MAX_OP_WIDTH: usize = 60;

    fn print_pr(pr: Arc<dyn PlanRepr>, n: Rc<RefCell<i32>>, depth: usize) {
        let raw_op_str = format_operation(pr.operation());
        let mut indented_op_str = format!("{:width$}{}", "", raw_op_str, width = depth * 2);
        if indented_op_str.len() > MAX_OP_WIDTH {
            // 3 is length of "..."
            indented_op_str = format!("{}...", &indented_op_str[0..MAX_OP_WIDTH - 3]);
        }
        println!(
            "{:>2} {:<width$} {:<20} {:>8} {:>8}",
            n.borrow(),
            indented_op_str,
            format_name(pr.operation()),
            pr.reads(),
            pr.writes(),
            width = MAX_OP_WIDTH,
        );
        *n.borrow_mut() += 1;

        for sub_pr in pr.sub_plan_reprs() {
            print_pr(sub_pr, Rc::clone(&n), depth + 1);
        }
    }

    let row_num = Rc::new(RefCell::new(1));
    println!(
        "{:<2} {:<width$} {:<20} {:>8} {:>8}",
        "#",
        "Operation",
        "Name",
        "Reads",
        "Writes",
        width = MAX_OP_WIDTH
    );
    println!("{:-<width$}", "", width = 102);
    print_pr(pr, row_num, 0);
}
