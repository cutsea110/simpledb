use combine::error::ParseError;
use combine::parser::char::{alpha_num, char, digit, letter, spaces, string_cmp};
use combine::stream::Stream;
use combine::{between, chainl1, many, many1, optional, satisfy, Parser};

use super::querydata::QueryData;
use crate::query::constant::Constant;
use crate::query::expression::Expression;
use crate::query::predicate::Predicate;
use crate::query::term::Term;

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

fn into<Input>() -> impl Parser<Input, Output = String>
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
        .or(i32_tok().map(|ival| Constant::new_i32(ival)))
}

fn expression<Input>() -> impl Parser<Input, Output = Expression>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    field()
        .map(|fldname| Expression::new_fldname(fldname))
        .or(constant().map(|c| Expression::Val(c)))
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
    let conjoin = keyword_and().map(|_| {
        |mut l: Predicate, mut r: Predicate| {
            l.conjoin_with(&mut r);
            l
        }
    });

    chainl1(pred1, conjoin)
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

fn query<Input>() -> impl Parser<Input, Output = QueryData>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    let fields = keyword_select().with(select_list());
    let tables = keyword_from().with(table_list());
    let where_clause = keyword_where().with(predicate());
    let where_clauses = many(where_clause).map(|mut cs: Vec<Predicate>| {
        cs.iter_mut()
            .fold(Predicate::new_empty(), |mut p1, mut p2| {
                p1.conjoin_with(&mut p2);
                p1
            })
    });

    fields
        .and(tables)
        .and(where_clauses)
        .map(|((fs, ts), pred)| QueryData::new(fs, ts, pred))
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
                  WHERE age = 18 \
                    AND name = 'joe' \
                    AND sex = 'male' \
                    AND dev_id = major_id"
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
        assert_eq!(
            parser.parse(
                "SELECT name, age \
                   FROM student, dept \
                  WHERE age = 18 AND name = 'joe' \
                  WHERE sex = 'male' AND dev_id = major_id"
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
}
