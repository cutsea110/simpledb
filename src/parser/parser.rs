use combine::error::{ParseError, StdParseResult, StreamError};
use combine::parser::char::{alpha_num, char, digit, letter, spaces};
use combine::parser::combinator::AndThen;
use combine::stream::position;
use combine::stream::{Positioned, Stream};
use combine::{
    between, choice, many, many1, optional, parser, satisfy, sep_by, EasyParser, Parser,
};

use crate::query::constant::Constant;
use crate::query::expression::Expression;
use crate::query::term::Term;

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

fn field<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    id_tok()
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
        many(satisfy(|c| c != '\'')).map(|v: Vec<char>| v.into_iter().collect::<String>()),
    )
    // lexeme
    .skip(spaces().silent())
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

fn eq<Input>() -> impl Parser<Input, Output = char>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    char('=')
        // lexeme
        .skip(spaces().silent())
}

fn term<Input>() -> impl Parser<Input, Output = Term>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    expression()
        .skip(eq())
        .and(expression())
        .map(|(lhs, rhs)| Term::new(lhs, rhs))
}

#[cfg(test)]
mod tests {
    use combine::error::StringStreamError;

    use super::*;

    #[test]
    fn unit_test() {
        let mut parser = id_tok();
        assert_eq!(parser.parse("a42"), Ok(("a42".to_string(), "")));
        assert_eq!(parser.parse("foo_id "), Ok(("foo_id".to_string(), "")));
        assert_eq!(
            parser.parse("'Hey, man!' I said."),
            Err(StringStreamError::UnexpectedParse)
        );

        let mut parser = i32_tok();
        assert_eq!(parser.parse("42"), Ok((42, "")));
        assert_eq!(parser.parse("42 "), Ok((42, "")));
        assert_eq!(parser.parse("-42 "), Ok((-42, "")));

        let mut parser = str_tok();
        assert_eq!(
            parser.parse("'Hey, man!' He said."),
            Ok(("Hey, man!".to_string(), "He said."))
        );
        assert_eq!(parser.parse("a42"), Err(StringStreamError::UnexpectedParse));

        let mut parser = constant();
        assert_eq!(parser.parse("42"), Ok((Constant::I32(42), "")));
        assert_eq!(
            parser.parse("'joje'"),
            Ok((Constant::String("joje".to_string()), ""))
        );

        let mut parser = expression();
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
    }
}