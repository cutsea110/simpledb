use std::usize;

use combine::error::ParseError;
use combine::parser::char::{alpha_num, char, digit, letter, spaces, string_cmp};
use combine::stream::Stream;
use combine::{between, chainl1, many, many1, optional, satisfy, sep_by, sep_by1, Parser};

use super::createindexdata::CreateIndexData;
use super::createtabledata::CreateTableData;
use super::createviewdata::CreateViewData;
use super::deletedata::DeleteData;
use super::insertdata::InsertData;
use super::modifydata::ModifyData;
use super::querydata::QueryData;
use crate::query::constant::Constant;
use crate::query::expression::Expression;
use crate::query::predicate::Predicate;
use crate::query::term::Term;
use crate::record::schema::{FieldInfo, FieldType, Schema};

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

fn keyword_select<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("SELECT")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_from<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("FROM")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_where<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("WHERE")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_and<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("AND")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_insert<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("INSERT")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_into<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("INTO")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_values<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("VALUES")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_delete<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("DELETE")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_update<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("UPDATE")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_set<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("SET")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_create<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("CREATE")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_table<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("TABLE")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_varchar<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("VARCHAR")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_int32<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("INT32")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_view<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("VIEW")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_as<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("AS")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_index<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    keyword("INDEX")
        // lexeme
        .skip(spaces().silent())
}

fn keyword_on<Input>() -> impl Parser<Input, Output = String>
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
    between(
        char('\''),
        char('\''),
        // TODO: escape character
        many(satisfy(|c| c != '\'')).map(|v: Vec<char>| v.into_iter().collect::<String>()),
    )
    // lexeme
    .skip(spaces().silent())
}

/// Methods for parsing predicates and their components

pub fn field<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    id_tok()
}

pub fn constant<Input>() -> impl Parser<Input, Output = Constant>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    str_tok()
        .map(|sval| Constant::new_string(sval))
        .or(i32_tok().map(|ival| Constant::new_i32(ival)))
}

pub fn expression<Input>() -> impl Parser<Input, Output = Expression>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    field()
        .map(|fldname| Expression::new_fldname(fldname))
        .or(constant().map(|c| Expression::Val(c)))
}

pub fn term<Input>() -> impl Parser<Input, Output = Term>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    expression()
        .skip(binop_eq())
        .and(expression())
        .map(|(lhs, rhs)| Term::new(lhs, rhs))
}

pub fn predicate<Input>() -> impl Parser<Input, Output = Predicate>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let pred1 = term().map(|t| Predicate::new(t));
    let conjoin = keyword_and().map(|_| {
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
    let fields = keyword_select().with(select_list());
    let tables = keyword_from().with(table_list());
    let where_clause = keyword_where().with(predicate());

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

// TODO: updateCmd
// TODO: create

/// Method for parsing delete commands

pub fn delete<Input>() -> impl Parser<Input, Output = DeleteData>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let prelude = keyword_delete().and(keyword_from());
    let where_clause = keyword_where().with(predicate());

    prelude
        .with(id_tok())
        .and(optional(where_clause))
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
    let prelude = keyword_insert().and(keyword_into());
    let fields = between(delim_parenl(), delim_parenr(), field_list());
    let vals = keyword_values().with(between(delim_parenl(), delim_parenr(), const_list()));

    prelude
        .with(id_tok())
        .and(fields)
        .and(vals)
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
    let sets = keyword_set().with(field().skip(binop_eq()).and(expression()));
    let where_clause = keyword_where().with(predicate());

    keyword_update()
        .with(id_tok())
        .and(sets)
        .and(optional(where_clause))
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
    let prelude = keyword_create().and(keyword_table());
    let field_defs = between(delim_parenl(), delim_parenr(), field_defs());

    prelude
        .with(id_tok())
        .and(field_defs)
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
    let int32_def = keyword_int32().map(|_| FieldInfo::new(FieldType::INTEGER, 0));
    let varchar_def = keyword_varchar()
        .with(between(delim_parenl(), delim_parenr(), i32_tok()))
        .map(|n| FieldInfo::new(FieldType::VARCHAR, n as usize));

    int32_def.or(varchar_def)
}

/// Method for parsing create view commands

pub fn create_view<Input>() -> impl Parser<Input, Output = CreateViewData>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let prelude = keyword_create().and(keyword_view());

    prelude
        .with(id_tok())
        .and(keyword_as().with(query()))
        .map(|(v, vq)| CreateViewData::new(v, vq))
}

/// Method for parsing create index commands

pub fn create_index<Input>() -> impl Parser<Input, Output = CreateIndexData>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let prelude = keyword_create().and(keyword_index());

    prelude
        .with(id_tok())
        .and(keyword_on().with(id_tok()))
        .and(between(delim_parenl(), delim_parenr(), field()))
        .map(|((idxname, tblname), fldname)| CreateIndexData::new(idxname, tblname, fldname))
}

#[cfg(test)]
mod tests {
    use combine::error::StringStreamError;

    use super::*;

    #[test]
    fn unit_test() {
        let mut parser = id_tok();
        assert_eq!(parser.parse(""), Err(StringStreamError::UnexpectedParse));
        assert_eq!(parser.parse("a42"), Ok(("a42".to_string(), "")));
        assert_eq!(parser.parse("foo_id "), Ok(("foo_id".to_string(), "")));
        assert_eq!(
            parser.parse("'Hey, man!' I said."),
            Err(StringStreamError::UnexpectedParse)
        );

        let mut parser = i32_tok();
        assert_eq!(parser.parse(""), Err(StringStreamError::Eoi));
        assert_eq!(parser.parse("42"), Ok((42, "")));
        assert_eq!(parser.parse("42 "), Ok((42, "")));
        assert_eq!(parser.parse("-42 "), Ok((-42, "")));

        let mut parser = str_tok();
        assert_eq!(parser.parse(""), Err(StringStreamError::Eoi));
        assert_eq!(
            parser.parse("'Hey, man!' He said."),
            Ok(("Hey, man!".to_string(), "He said."))
        );
        assert_eq!(parser.parse("a42"), Err(StringStreamError::UnexpectedParse));

        let mut parser = constant();
        assert_eq!(parser.parse(""), Err(StringStreamError::Eoi));
        assert_eq!(parser.parse("42"), Ok((Constant::I32(42), "")));
        assert_eq!(
            parser.parse("'joje'"),
            Ok((Constant::String("joje".to_string()), ""))
        );

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

        let mut parser = delete();
        assert_eq!(
            parser.parse("DELETE FROM STUDENT"),
            Ok((
                DeleteData::new("STUDENT".to_string(), Predicate::new_empty()),
                ""
            ))
        );
        let mut parser = delete();
        assert_eq!(
            parser.parse("DELETE FROM STUDENT WHERE name = 'joe'"),
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

        let mut parser = insert();
        assert_eq!(
            parser.parse("INSERT INTO STUDENT (name, age, sex) VALUES ('Darci', 20, 'female')"),
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

        let mut parser = modify();
        assert_eq!(
            parser.parse("UPDATE STUDENT SET age = 22"),
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
            parser.parse("UPDATE STUDENT SET age = 22 WHERE age = 21"),
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
            parser.parse("UPDATE STUDENT SET grade = 'A+' WHERE dep = 'math' AND score = 100"),
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

        let mut parser = create_table();
        let mut expected = Schema::new();
        expected.add_i32_field("SId");
        expected.add_string_field("SName", 10);
        expected.add_i32_field("GradYear");
        expected.add_i32_field("MajorId");

        assert_eq!(parser.parse(
	    "CREATE TABLE STUDENT (SId int32, SName varchar(10), GradYear int32, MajorId int32 )"
	), Ok((CreateTableData::new("STUDENT".to_string(), expected), "")));

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
}
