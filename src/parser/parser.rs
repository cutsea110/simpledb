use combine::{
    any, attempt,
    error::ParseError,
    parser::char::{alpha_num, char, digit, letter, spaces, string, string_cmp},
    stream::Stream,
    {between, chainl1, many, many1, optional, satisfy, sep_by, sep_by1, Parser},
};
use std::usize;

use super::{
    createindexdata::CreateIndexData, createtabledata::CreateTableData,
    createviewdata::CreateViewData, ddl::DDL, deletedata::DeleteData, dml::DML,
    insertdata::InsertData, modifydata::ModifyData, querydata::QueryData, sql::SQL,
};
use crate::{
    query::{constant::Constant, expression::Expression, predicate::Predicate, term::Term},
    record::schema::{FieldInfo, FieldType, Schema},
};

/// primitive parser

fn keyword<Input>(s: &'static str) -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    string_cmp(s, |x, y| x.eq_ignore_ascii_case(&y))
        .map(|x| x.to_string())
        // lexeme
        .skip(spaces().silent())
}

fn kw_select<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("SELECT")
        // lexeme
        .skip(spaces().silent())
}

fn kw_from<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("FROM")
        // lexeme
        .skip(spaces().silent())
}

fn kw_where<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("WHERE")
        // lexeme
        .skip(spaces().silent())
}

fn kw_and<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("AND")
        // lexeme
        .skip(spaces().silent())
}

fn kw_insert<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("INSERT")
        // lexeme
        .skip(spaces().silent())
}

fn kw_into<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("INTO")
        // lexeme
        .skip(spaces().silent())
}

fn kw_values<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("VALUES")
        // lexeme
        .skip(spaces().silent())
}

fn kw_delete<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("DELETE")
        // lexeme
        .skip(spaces().silent())
}

fn kw_update<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("UPDATE")
        // lexeme
        .skip(spaces().silent())
}

fn kw_set<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("SET")
        // lexeme
        .skip(spaces().silent())
}

fn kw_create<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("CREATE")
        // lexeme
        .skip(spaces().silent())
}

fn kw_table<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("TABLE")
        // lexeme
        .skip(spaces().silent())
}

fn kw_int16<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("SMALLINT")
        // lexeme
        .skip(spaces().silent())
}

fn kw_int32<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("INTEGER")
        // lexeme
        .skip(spaces().silent())
}

fn kw_varchar<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("VARCHAR")
        // lexeme
        .skip(spaces().silent())
}

fn kw_bool<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("BOOL")
        // lexeme
        .skip(spaces().silent())
}

fn kw_date<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("DATE")
        // lexeme
        .skip(spaces().silent())
}

fn kw_view<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("VIEW")
        // lexeme
        .skip(spaces().silent())
}

fn kw_as<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("AS")
        // lexeme
        .skip(spaces().silent())
}

fn kw_index<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("INDEX")
        // lexeme
        .skip(spaces().silent())
}

fn kw_on<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("ON")
        // lexeme
        .skip(spaces().silent())
}

fn delim_parenl<Input>() -> impl Parser<Input, Output = char>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    char('(')
        // lexeme
        .skip(spaces().silent())
}

fn delim_parenr<Input>() -> impl Parser<Input, Output = char>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    char(')')
        // lexeme
        .skip(spaces().silent())
}

fn delim_comma<Input>() -> impl Parser<Input, Output = char>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    char(',')
        // lexeme
        .skip(spaces().silent())
}

fn binop_eq<Input>() -> impl Parser<Input, Output = char>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    char('=')
        // lexeme
        .skip(spaces().silent())
}

fn terminate<Input>() -> impl Parser<Input, Output = char>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    char(';')
        // lexeme
        .skip(spaces().silent())
}

/// token

fn id_tok<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    letter()
        .and(many(alpha_num().or(char('_'))))
        .map(|(x, mut xs): (char, Vec<char>)| {
            xs.insert(0, x);
            xs.into_iter().collect()
        })
        // lexeme
        .skip(spaces().silent())
}

fn i32_tok<Input>() -> impl Parser<Input, Output = i32>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    optional(char('-').or(char('+')))
        .and(many1(digit()).map(|s: String| s.parse::<i32>()))
        .map(|(s, v)| {
            if let Some(sign) = s {
                if sign == '-' {
                    return v.unwrap_or_default() * -1;
                }
            }
            v.unwrap_or_default()
        })
        // lexeme
        .skip(spaces().silent())
}

fn str_tok<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let almost = satisfy(|c| c != '\'' && c != '\\');
    let escaped = attempt(char('\\').with(any()));
    let quote_quote = attempt(char('\'').skip(char('\'')));
    let internal_string = almost.or(escaped).or(quote_quote);

    between(
        char('\''),
        char('\''),
        many(internal_string).map(|v: Vec<char>| v.into_iter().collect::<String>()),
    )
    // lexeme
    .skip(spaces().silent())
}

fn bool_tok<Input>() -> impl Parser<Input, Output = bool>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    string("false")
        .map(|_| false)
        .or(string("true").map(|_| true))
        // lexeme
        .skip(spaces().silent())
}

/// Methods for parsing predicates and their components

fn field<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    id_tok()
}

fn constant<Input>() -> impl Parser<Input, Output = Constant>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    str_tok()
        .map(|sval| Constant::new_string(sval))
        .or(i32_tok().map(|ival| Constant::new_i32(ival))) // pick it up as the largest signed integer
        .or(bool_tok().map(|bval| Constant::new_bool(bval)))
}

fn expression<Input>() -> impl Parser<Input, Output = Expression>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    // try constant first, because field can get bool value too.
    constant()
        .map(|c| Expression::Val(c))
        .or(field().map(|fldname| Expression::new_fldname(fldname)))
}

fn term<Input>() -> impl Parser<Input, Output = Term>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    expression()
        .skip(binop_eq())
        .and(expression())
        .map(|(lhs, rhs)| Term::new(lhs, rhs))
}

fn predicate<Input>() -> impl Parser<Input, Output = Predicate>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let pred1 = term().map(|t| Predicate::new(t));
    let conjoin = kw_and().map(|_| {
        |mut l: Predicate, mut r: Predicate| {
            l.conjoin_with(&mut r);
            l
        }
    });

    chainl1(pred1, conjoin)
}

/// Methods for parsing queries

pub fn query<Input>() -> impl Parser<Input, Output = QueryData>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let fields = kw_select().with(select_list());
    let tables = kw_from().with(table_list());
    let where_clause = kw_where().with(predicate());

    fields
        .and(tables)
        .and(optional(where_clause))
        .map(|((fs, ts), op)| {
            let pred = op.unwrap_or(Predicate::new_empty());
            QueryData::new(fs, ts, pred)
        })
}

fn select_list<Input>() -> impl Parser<Input, Output = Vec<String>>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let fld1 = field().map(|f| vec![f]);
    let sep = delim_comma().map(|_| {
        |mut x: Vec<String>, mut y: Vec<String>| {
            x.append(&mut y);
            x
        }
    });

    chainl1(fld1, sep)
}

fn table_list<Input>() -> impl Parser<Input, Output = Vec<String>>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let id_tok1 = id_tok().map(|f| vec![f]);
    let sep = delim_comma().map(|_| {
        |mut x: Vec<String>, mut y: Vec<String>| {
            x.append(&mut y);
            x
        }
    });

    chainl1(id_tok1, sep)
}

/// Methods for parsing the various update commands

pub fn update_cmd<Input>() -> impl Parser<Input, Output = SQL>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    attempt(insert().map(|i| SQL::DML(DML::Insert(i))))
        .or(attempt(delete().map(|d| SQL::DML(DML::Delete(d)))))
        .or(attempt(modify().map(|m| SQL::DML(DML::Modify(m)))))
        .or(ddl().map(|ddl| SQL::DDL(ddl)))
}

fn ddl<Input>() -> impl Parser<Input, Output = DDL>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    attempt(create_table().map(|t| DDL::Table(t)))
        .or(attempt(create_view().map(|v| DDL::View(v))))
        .or(attempt(create_index().map(|i| DDL::Index(i))))
}

/// Method for parsing delete commands

pub fn delete<Input>() -> impl Parser<Input, Output = DeleteData>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let prelude = kw_delete().and(kw_from());
    let where_clause = kw_where().with(predicate());

    prelude
        .with(id_tok())
        .and(optional(where_clause))
        .skip(terminate())
        .map(|(tblname, opred)| {
            let pred = opred.unwrap_or(Predicate::new_empty());
            DeleteData::new(tblname, pred)
        })
}

/// Methods for parsing insert commands

pub fn insert<Input>() -> impl Parser<Input, Output = InsertData>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let prelude = kw_insert().and(kw_into());
    let fields = between(delim_parenl(), delim_parenr(), field_list());
    let vals = kw_values().with(between(delim_parenl(), delim_parenr(), const_list()));

    prelude
        .with(id_tok())
        .and(fields)
        .and(vals)
        .skip(terminate())
        .map(|((t, fs), vs)| InsertData::new(t, fs, vs))
}

fn field_list<Input>() -> impl Parser<Input, Output = Vec<String>>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    sep_by1(field(), delim_comma())
}

fn const_list<Input>() -> impl Parser<Input, Output = Vec<Constant>>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    sep_by1(constant(), delim_comma())
}

/// Method for parsing modify commands

pub fn modify<Input>() -> impl Parser<Input, Output = ModifyData>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let sets = kw_set().with(field().skip(binop_eq()).and(expression()));
    let where_clause = kw_where().with(predicate());

    kw_update()
        .with(id_tok())
        .and(sets)
        .and(optional(where_clause))
        .skip(terminate())
        .map(|((t, (f, e)), op)| {
            let pred = op.unwrap_or(Predicate::new_empty());
            ModifyData::new(t, f, e, pred)
        })
}

/// Method for parsing create table commands

pub fn create_table<Input>() -> impl Parser<Input, Output = CreateTableData>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let prelude = kw_create().and(kw_table());
    let field_defs = between(delim_parenl(), delim_parenr(), field_defs());

    prelude
        .with(id_tok())
        .and(field_defs)
        .skip(terminate())
        .map(|(tblname, fdefs)| {
            let mut sch = Schema::new();
            for (fldname, fi) in fdefs.iter() {
                sch.add_field(fldname, fi.fld_type, fi.length)
            }
            CreateTableData::new(tblname, sch)
        })
}

fn field_defs<Input>() -> impl Parser<Input, Output = Vec<(String, FieldInfo)>>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    sep_by(field_def(), delim_comma())
}

fn field_def<Input>() -> impl Parser<Input, Output = (String, FieldInfo)>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    id_tok().and(type_def())
}

fn type_def<Input>() -> impl Parser<Input, Output = FieldInfo>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let int16_def = kw_int16().map(|_| FieldInfo::new(FieldType::SMALLINT, 0));
    let int32_def = kw_int32().map(|_| FieldInfo::new(FieldType::INTEGER, 0));
    let varchar_def = kw_varchar()
        .with(between(delim_parenl(), delim_parenr(), i32_tok()))
        .map(|n| FieldInfo::new(FieldType::VARCHAR, n as usize));
    let bool_def = kw_bool().map(|_| FieldInfo::new(FieldType::BOOL, 0));
    let date_def = kw_date().map(|_| FieldInfo::new(FieldType::DATE, 0));

    int32_def
        .or(int16_def)
        .or(varchar_def)
        .or(bool_def)
        .or(date_def)
}

/// Method for parsing create view commands

pub fn create_view<Input>() -> impl Parser<Input, Output = CreateViewData>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let prelude = kw_create().and(kw_view());

    prelude
        .with(id_tok())
        .and(kw_as().with(query()))
        .map(|(v, vq)| CreateViewData::new(v, vq))
}

/// Method for parsing create index commands

pub fn create_index<Input>() -> impl Parser<Input, Output = CreateIndexData>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let prelude = kw_create().and(kw_index());

    prelude
        .with(id_tok())
        .and(kw_on().with(id_tok()))
        .and(between(delim_parenl(), delim_parenr(), field()))
        .map(|((idxname, tblname), fldname)| CreateIndexData::new(idxname, tblname, fldname))
}

#[cfg(test)]
mod tests {
    use super::*;

    use combine::error::StringStreamError;
    use combine::Parser;

    #[test]
    fn id_tok_test() {
        let mut parser = id_tok();
        assert_eq!(parser.parse(""), Err(StringStreamError::UnexpectedParse));
        assert_eq!(parser.parse("a42"), Ok(("a42".to_string(), "")));
        assert_eq!(parser.parse("foo_id "), Ok(("foo_id".to_string(), "")));
        assert_eq!(
            parser.parse("'Hey, man!' I said."),
            Err(StringStreamError::UnexpectedParse)
        );
    }

    #[test]
    fn i32_tok_test() {
        let mut parser = i32_tok();
        assert_eq!(parser.parse(""), Err(StringStreamError::Eoi));
        assert_eq!(parser.parse("42"), Ok((42, "")));
        assert_eq!(parser.parse("42 "), Ok((42, "")));
        assert_eq!(parser.parse("-42 "), Ok((-42, "")));
    }

    #[test]
    fn str_tok_test() {
        let mut parser = str_tok();
        assert_eq!(parser.parse(""), Err(StringStreamError::Eoi));
        assert_eq!(parser.parse("a42"), Err(StringStreamError::UnexpectedParse));
        assert_eq!(
            parser.parse("'Hey, man!' He said."),
            Ok(("Hey, man!".to_string(), "He said."))
        );
        assert_eq!(
            parser.parse("'He is joe''s sun.'"),
            Ok(("He is joe's sun.".to_string(), ""))
        );
        assert_eq!(
            parser.parse("'What\\'s up?'"),
            Ok(("What's up?".to_string(), ""))
        );
    }

    #[test]
    fn bool_tok_test() {
        let mut parser = bool_tok();
        assert_eq!(parser.parse(""), Err(StringStreamError::UnexpectedParse));
        assert_eq!(parser.parse("42"), Err(StringStreamError::UnexpectedParse));
        assert_eq!(parser.parse("false"), Ok((false, "")));
        assert_eq!(parser.parse("false123"), Ok((false, "123")));
        assert_eq!(parser.parse("true"), Ok((true, "")));
        assert_eq!(parser.parse("trueabc"), Ok((true, "abc")));
    }

    #[test]
    fn constant_test() {
        let mut parser = constant();
        assert_eq!(parser.parse(""), Err(StringStreamError::Eoi));
        assert_eq!(parser.parse("42"), Ok((Constant::I32(42), "")));
        assert_eq!(
            parser.parse("'joe'"),
            Ok((Constant::String("joe".to_string()), ""))
        );
        assert_eq!(
            parser.parse("'2022-06-16'"),
            Ok((Constant::String("2022-06-16".to_string()), ""))
        );
        assert_eq!(parser.parse("true"), Ok((Constant::Bool(true), "")));
        assert_eq!(parser.parse("false"), Ok((Constant::Bool(false), "")));
    }

    #[test]
    fn expressin_test() {
        let mut parser = expression();
        assert_eq!(parser.parse(""), Err(StringStreamError::Eoi));
        assert_eq!(
            parser.parse("user_name"),
            Ok((Expression::Fldname("user_name".to_string()), ""))
        );
        assert_eq!(
            parser.parse("user_id   "),
            Ok((Expression::Fldname("user_id".to_string()), ""))
        );
        assert_eq!(
            parser.parse("42   "),
            Ok((Expression::Val(Constant::I32(42)), ""))
        );
        assert_eq!(
            parser.parse("'bob'   "),
            Ok((Expression::Val(Constant::String("bob".to_string())), ""))
        );
    }

    #[test]
    fn term_test() {
        let mut parser = term();
        assert_eq!(parser.parse(""), Err(StringStreamError::Eoi));
        assert_eq!(
            parser.parse("age=42"),
            Ok((
                Term::new(
                    Expression::Fldname("age".to_string()),
                    Expression::Val(Constant::I32(42))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("age =42"),
            Ok((
                Term::new(
                    Expression::Fldname("age".to_string()),
                    Expression::Val(Constant::I32(42))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("age= 42"),
            Ok((
                Term::new(
                    Expression::Fldname("age".to_string()),
                    Expression::Val(Constant::I32(42))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("age = 42"),
            Ok((
                Term::new(
                    Expression::Fldname("age".to_string()),
                    Expression::Val(Constant::I32(42))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("age   =    42"),
            Ok((
                Term::new(
                    Expression::Fldname("age".to_string()),
                    Expression::Val(Constant::I32(42))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("42   =    age"),
            Ok((
                Term::new(
                    Expression::Val(Constant::I32(42)),
                    Expression::Fldname("age".to_string())
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("name='joe'"),
            Ok((
                Term::new(
                    Expression::Fldname("name".to_string()),
                    Expression::Val(Constant::String("joe".to_string()))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("name ='joe'"),
            Ok((
                Term::new(
                    Expression::Fldname("name".to_string()),
                    Expression::Val(Constant::String("joe".to_string()))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("name= 'joe'"),
            Ok((
                Term::new(
                    Expression::Fldname("name".to_string()),
                    Expression::Val(Constant::String("joe".to_string()))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("name = 'joe'"),
            Ok((
                Term::new(
                    Expression::Fldname("name".to_string()),
                    Expression::Val(Constant::String("joe".to_string()))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("name   =    'joe'"),
            Ok((
                Term::new(
                    Expression::Fldname("name".to_string()),
                    Expression::Val(Constant::String("joe".to_string()))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("'joe' = name"),
            Ok((
                Term::new(
                    Expression::Val(Constant::String("joe".to_string())),
                    Expression::Fldname("name".to_string())
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("is_deleted=true"),
            Ok((
                Term::new(
                    Expression::Fldname("is_deleted".to_string()),
                    Expression::Val(Constant::Bool(true))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("is_deleted =true"),
            Ok((
                Term::new(
                    Expression::Fldname("is_deleted".to_string()),
                    Expression::Val(Constant::Bool(true))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("is_deleted= true"),
            Ok((
                Term::new(
                    Expression::Fldname("is_deleted".to_string()),
                    Expression::Val(Constant::Bool(true))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("is_deleted = true"),
            Ok((
                Term::new(
                    Expression::Fldname("is_deleted".to_string()),
                    Expression::Val(Constant::Bool(true))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("is_deleted    =    true"),
            Ok((
                Term::new(
                    Expression::Fldname("is_deleted".to_string()),
                    Expression::Val(Constant::Bool(true))
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("true = is_deleted"),
            Ok((
                Term::new(
                    Expression::Val(Constant::Bool(true)),
                    Expression::Fldname("is_deleted".to_string()),
                ),
                ""
            ))
        );
    }

    #[test]
    fn predicate_test() {
        let mut parser = predicate();
        assert_eq!(parser.parse(""), Err(StringStreamError::Eoi));
        assert_eq!(
            parser.parse("age = 18"),
            Ok((
                Predicate::new(Term::new(
                    Expression::Fldname("age".to_string()),
                    Expression::Val(Constant::I32(18))
                )),
                ""
            ))
        );
        let terms = vec![
            Term::new(
                Expression::Fldname("age".to_string()),
                Expression::Val(Constant::I32(18)),
            ),
            Term::new(
                Expression::Fldname("name".to_string()),
                Expression::Val(Constant::String("joe".to_string())),
            ),
        ];
        let expected = terms.iter().map(|t| Predicate::new(t.clone())).fold(
            Predicate::new_empty(),
            |mut p1, mut p2| {
                p1.conjoin_with(&mut p2);
                p1
            },
        );
        assert_eq!(
            parser.parse("age = 18 and name = 'joe'"),
            Ok((expected, ""))
        );
        let terms = vec![
            Term::new(
                Expression::Fldname("age".to_string()),
                Expression::Val(Constant::I32(18)),
            ),
            Term::new(
                Expression::Fldname("name".to_string()),
                Expression::Val(Constant::String("joe".to_string())),
            ),
            Term::new(
                Expression::Fldname("sex".to_string()),
                Expression::Val(Constant::String("male".to_string())),
            ),
            Term::new(
                Expression::Fldname("dev_id".to_string()),
                Expression::Fldname("major_id".to_string()),
            ),
        ];
        let expected = terms.iter().map(|t| Predicate::new(t.clone())).fold(
            Predicate::new_empty(),
            |mut p1, mut p2| {
                p1.conjoin_with(&mut p2);
                p1
            },
        );
        assert_eq!(
            parser.parse("age = 18 and name = 'joe' AND sex = 'male' And dev_id = major_id"),
            Ok((expected, ""))
        );

        assert_eq!(
            parser.parse("1 = 1"),
            Ok((
                Predicate::new(Term::new(
                    Expression::Val(Constant::I32(1)),
                    Expression::Val(Constant::I32(1))
                )),
                ""
            ))
        );
        assert_eq!(
            parser.parse("'julio' = 'willy'"),
            Ok((
                Predicate::new(Term::new(
                    Expression::Val(Constant::String("julio".to_string())),
                    Expression::Val(Constant::String("willy".to_string()))
                )),
                ""
            ))
        );
        assert_eq!(
            parser.parse("DId = MajorId"),
            Ok((
                Predicate::new(Term::new(
                    Expression::Fldname("DId".to_string()),
                    Expression::Fldname("MajorId".to_string())
                )),
                ""
            ))
        );
    }

    #[test]
    fn query_test() {
        let mut parser = query();
        assert_eq!(parser.parse(""), Err(StringStreamError::UnexpectedParse));
        assert_eq!(
            parser.parse("SELECT name, age FROM student"),
            Ok((
                QueryData::new(
                    vec!["name".to_string(), "age".to_string()],
                    vec!["student".to_string()],
                    Predicate::new_empty(),
                ),
                ""
            ))
        );
        let terms = vec![
            Term::new(
                Expression::Fldname("age".to_string()),
                Expression::Val(Constant::I32(18)),
            ),
            Term::new(
                Expression::Fldname("name".to_string()),
                Expression::Val(Constant::String("joe".to_string())),
            ),
            Term::new(
                Expression::Fldname("sex".to_string()),
                Expression::Val(Constant::String("male".to_string())),
            ),
            Term::new(
                Expression::Fldname("dev_id".to_string()),
                Expression::Fldname("major_id".to_string()),
            ),
        ];
        let expected = terms.iter().map(|t| Predicate::new(t.clone())).fold(
            Predicate::new_empty(),
            |mut p1, mut p2| {
                p1.conjoin_with(&mut p2);
                p1
            },
        );
        assert_eq!(
            parser.parse(
                "SELECT name, age \
                   FROM student, dept \
                  WHERE age = 18 AND name = 'joe' \
                    AND sex = 'male' AND dev_id = major_id"
            ),
            Ok((
                QueryData::new(
                    vec!["name".to_string(), "age".to_string()],
                    vec!["student".to_string(), "dept".to_string()],
                    expected.clone(),
                ),
                ""
            ))
        );
    }

    #[test]
    fn delete_test() {
        let mut parser = delete();
        assert_eq!(
            parser.parse("DELETE FROM STUDENT;"),
            Ok((
                DeleteData::new("STUDENT".to_string(), Predicate::new_empty()),
                ""
            ))
        );
        let mut parser = delete();
        assert_eq!(
            parser.parse("DELETE FROM STUDENT WHERE name = 'joe' ;"),
            Ok((
                DeleteData::new(
                    "STUDENT".to_string(),
                    Predicate::new(Term::new(
                        Expression::Fldname("name".to_string()),
                        Expression::Val(Constant::String("joe".to_string()))
                    ))
                ),
                ""
            ))
        );
    }

    #[test]
    fn insert_test() {
        let mut parser = insert();
        assert_eq!(
            parser.parse("INSERT INTO STUDENT (name, age, sex) VALUES ('Darci', 20, 'female');"),
            Ok((
                InsertData::new(
                    "STUDENT".to_string(),
                    vec!["name".to_string(), "age".to_string(), "sex".to_string()],
                    vec![
                        Constant::String("Darci".to_string()),
                        Constant::I32(20),
                        Constant::String("female".to_string())
                    ]
                ),
                ""
            ))
        );
    }

    #[test]
    fn modify_test() {
        let mut parser = modify();
        assert_eq!(
            parser.parse("UPDATE STUDENT SET age = 22;"),
            Ok((
                ModifyData::new(
                    "STUDENT".to_string(),
                    "age".to_string(),
                    Expression::Val(Constant::I32(22)),
                    Predicate::new_empty(),
                ),
                ""
            ))
        );
        assert_eq!(
            parser.parse("UPDATE STUDENT SET age = 22 WHERE age = 21;"),
            Ok((
                ModifyData::new(
                    "STUDENT".to_string(),
                    "age".to_string(),
                    Expression::Val(Constant::I32(22)),
                    Predicate::new(Term::new(
                        Expression::Fldname("age".to_string()),
                        Expression::Val(Constant::I32(21))
                    ))
                ),
                ""
            ))
        );
        let terms = vec![
            Term::new(
                Expression::Fldname("dep".to_string()),
                Expression::Val(Constant::String("math".to_string())),
            ),
            Term::new(
                Expression::Fldname("score".to_string()),
                Expression::Val(Constant::I32(100)),
            ),
        ];
        let expected = terms.iter().map(|t| Predicate::new(t.clone())).fold(
            Predicate::new_empty(),
            |mut p1, mut p2| {
                p1.conjoin_with(&mut p2);
                p1
            },
        );
        assert_eq!(
            parser.parse("UPDATE STUDENT SET grade = 'A+' WHERE dep = 'math' AND score = 100 ;"),
            Ok((
                ModifyData::new(
                    "STUDENT".to_string(),
                    "grade".to_string(),
                    Expression::Val(Constant::String("A+".to_string())),
                    expected,
                ),
                ""
            ))
        );
    }

    #[test]
    fn create_table_test() {
        let mut parser = create_table();
        let mut expected = Schema::new();
        expected.add_i32_field("SId");
        expected.add_string_field("SName", 10);
        expected.add_i32_field("GradYear");
        expected.add_i32_field("MajorId");

        assert_eq!(parser.parse(
	    "CREATE TABLE STUDENT (SId integer, SName varchar(10), GradYear integer, MajorId integer);"
	), Ok((CreateTableData::new("STUDENT".to_string(), expected), "")));
    }

    #[test]
    fn create_view_test() {
        let mut parser = create_view();
        assert_eq!(
            parser
                .parse("CREATE VIEW name_dep AS SELECT SName, DName FROM STUDENT, DEPT WHERE MajorId = DId"),
            Ok((
                CreateViewData::new(
                    "name_dep".to_string(),
                    QueryData::new(
                        vec!["SName".to_string(), "DName".to_string()],
                        vec!["STUDENT".to_string(), "DEPT".to_string()],
                        Predicate::new(Term::new(
                            Expression::Fldname("MajorId".to_string()),
                            Expression::Fldname("DId".to_string())
                        ))
                    )
                ),
                ""
            ))
        );
    }

    #[test]
    fn create_index_test() {
        let mut parser = create_index();
        assert_eq!(
            parser.parse("CREATE INDEX idx_grad_year ON STUDENT (GradYear)"),
            Ok((
                CreateIndexData::new(
                    "idx_grad_year".to_string(),
                    "STUDENT".to_string(),
                    "GradYear".to_string()
                ),
                ""
            ))
        );
    }
    #[test]
    fn update_cmd_test() {
        let mut parser = update_cmd();
        assert_eq!(
            parser.parse("select name, age from student where majorid = 20"),
            Err(StringStreamError::UnexpectedParse),
        );
        assert_eq!(
            parser.parse("insert into student (name, age) values ('Calvin', 9);"),
            Ok((
                SQL::DML(DML::Insert(InsertData::new(
                    "student".to_string(),
                    vec!["name".to_string(), "age".to_string()],
                    vec![Constant::String("Calvin".to_string()), Constant::I32(9)]
                ))),
                ""
            ))
        );
        assert_eq!(
            parser.parse("delete from student where name = 'joe';"),
            Ok((
                SQL::DML(DML::Delete(DeleteData::new(
                    "student".to_string(),
                    Predicate::new(Term::new(
                        Expression::Fldname("name".to_string()),
                        Expression::Val(Constant::String("joe".to_string()))
                    ))
                ))),
                ""
            ))
        );
        assert_eq!(
            parser.parse("update student set age = 10;"),
            Ok((
                SQL::DML(DML::Modify(ModifyData::new(
                    "student".to_string(),
                    "age".to_string(),
                    Expression::Val(Constant::I32(10)),
                    Predicate::new_empty(),
                ))),
                ""
            ))
        );
        let mut expected = Schema::new();
        expected.add_string_field("name", 10);
        expected.add_i32_field("age");
        assert_eq!(
            parser.parse("create table student (name varchar(10), age integer);"),
            Ok((
                SQL::DDL(DDL::Table(CreateTableData::new(
                    "student".to_string(),
                    expected
                ))),
                ""
            ))
        );
        assert_eq!(
            parser.parse(
                "create view name_dep AS select name, dep_name from student, dept where mid = did"
            ),
            Ok((
                SQL::DDL(DDL::View(CreateViewData::new(
                    "name_dep".to_string(),
                    QueryData::new(
                        vec!["name".to_string(), "dep_name".to_string()],
                        vec!["student".to_string(), "dept".to_string()],
                        Predicate::new(Term::new(
                            Expression::Fldname("mid".to_string()),
                            Expression::Fldname("did".to_string())
                        ))
                    )
                ))),
                ""
            ))
        );
        assert_eq!(
            parser.parse("create index idx_age on student (age)"),
            Ok((
                SQL::DDL(DDL::Index(CreateIndexData::new(
                    "idx_age".to_string(),
                    "student".to_string(),
                    "age".to_string()
                ))),
                ""
            ))
        );
    }
}
