use either::{Either, *};

use super::parsec::{Parser, *};
use crate::query::{constant::Constant, expression::Expression, predicate::Predicate, term::Term};

pub fn id_tok() -> impl Parser<String> {
    map(
        join(letter(), many(satisfy(|c| c.is_alphanumeric() || c == '_'))),
        |(c, mut cs)| {
            cs.insert(0, c);
            cs.into_iter().collect()
        },
    )
}

pub fn field() -> impl Parser<String> {
    lexeme(id_tok())
}

pub fn str_tok() -> impl Parser<Constant> {
    map(
        between(char('\''), char('\''), many(satisfy(|c| c != '\''))),
        |v: Vec<char>| Constant::new_string(v.into_iter().collect()),
    )
}

pub fn i32_tok() -> impl Parser<Constant> {
    // TODO: sign + digits
    map(natural(), |d| Constant::new_i32(d))
}

pub fn constant() -> impl Parser<Constant> {
    map(
        lexeme(meet(str_tok(), i32_tok())),
        |c: Either<Constant, Constant>| match c {
            Left(scon) => scon,
            Right(icon) => icon,
        },
    )
}

pub fn expression() -> impl Parser<Expression> {
    map(
        lexeme(meet(field(), constant())),
        |c: Either<String, Constant>| match c {
            Left(fldname) => Expression::new_fldname(fldname),
            Right(val) => Expression::new_val(val),
        },
    )
}

pub fn term() -> impl Parser<Term> {
    map(
        join(joinl(expression(), lexeme(char('='))), expression()),
        |(lhs, rhs): (Expression, Expression)| Term::new(lhs, rhs),
    )
}
/*
pub fn predicate() -> impl Parser<Predicate> {
    panic!("TODO")
}
*/
