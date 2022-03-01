use combine::error::{ParseError, StdParseResult, StreamError};
use combine::parser::char::{alpha_num, char, digit, letter, spaces};
use combine::parser::combinator::AndThen;
use combine::stream::position;
use combine::stream::{Positioned, Stream};
use combine::{
    between, choice, many, many1, optional, parser, satisfy, sep_by, EasyParser, Parser,
};

use crate::query::constant::Constant;

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
}

fn field<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    id_tok().skip(spaces().silent())
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

#[cfg(test)]
mod tests {
    use combine::error::StringStreamError;

    use super::*;

    #[test]
    fn unit_test() {
        let mut parser = id_tok();
        assert_eq!(parser.parse("a42"), Ok(("a42".to_string(), "")));
        assert_eq!(parser.parse("foo_id "), Ok(("foo_id".to_string(), " ")));
        assert_eq!(
            parser.parse("'Hey, man!' I said."),
            Err(StringStreamError::UnexpectedParse)
        );

        let mut parser = i32_tok();
        assert_eq!(parser.parse("42"), Ok((42, "")));
        assert_eq!(parser.parse("42 "), Ok((42, " ")));
        assert_eq!(parser.parse("-42 "), Ok((-42, " ")));

        let mut parser = str_tok();
        assert_eq!(
            parser.parse("'Hey, man!' He said."),
            Ok(("Hey, man!".to_string(), " He said."))
        );
        assert_eq!(parser.parse("a42"), Err(StringStreamError::UnexpectedParse));

        let mut parser = constant();
        assert_eq!(parser.parse("42"), Ok((Constant::I32(42), "")));
        assert_eq!(
            parser.parse("'joje'"),
            Ok((Constant::String("joje".to_string()), ""))
        );
    }
}

#[cfg(test)]
mod tests2 {
    use combine::parser::char::digit;
    use combine::parser::char::{alpha_num, char, letter, spaces, string, string_cmp};
    use combine::{between, many, many1, none_of, optional, satisfy, sep_by, Parser};

    #[test]
    fn unit_test() {
        let word = many1(letter());
        let mut parser = sep_by(word, spaces()).map(|words: Vec<String>| words);
        let result = parser.parse("Pick  up that  word!");
        assert_eq!(
            result,
            Ok((
                vec!["Pick", "up", "that", "word"]
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect(),
                "!"
            ))
        );

        let mut parser = string("hello").or(string("bye"));
        assert_eq!(parser.parse("hello"), Ok(("hello", "")));
        assert_eq!(parser.parse("bye"), Ok(("bye", "")));

        let select_ = string_cmp("select", |l, r| l.eq_ignore_ascii_case(&r)).skip(spaces());
        let from_ = string_cmp("from", |l, r| l.eq_ignore_ascii_case(&r)).skip(spaces());
        let where_ = string_cmp("where", |l, r| l.eq_ignore_ascii_case(&r)).skip(spaces());
        let mut parser = many(select_.or(from_).or(where_));
        assert_eq!(
            parser.parse("FROM SELECT WHERE INTO"),
            Ok((vec!["from", "select", "where"], "INTO"))
        );

        let mut parser = optional(char('-').or(char('+')))
            .and(many1(digit()).map(|s: String| s.parse::<i32>()))
            .map(|(s, v)| {
                if let Some(sign) = s {
                    if sign == '-' {
                        return v.unwrap_or_default() * -1;
                    }
                }
                v.unwrap_or_default()
            });
        assert_eq!(parser.parse("-123456"), Ok((-123456, "")));
        assert_eq!(parser.parse("-1234  "), Ok((-1234, "  ")));
        assert_eq!(parser.parse("123    "), Ok((123, "    ")));
        assert_eq!(parser.parse("+123   "), Ok((123, "   ")));

        let mut parser = letter()
            .and(many(alpha_num()))
            .map(|(x, mut xs): (char, Vec<char>)| {
                xs.insert(0, x);
                xs.into_iter().collect()
            });
        assert_eq!(parser.parse("a42"), Ok(("a42".to_string(), "")));

        let mut parser = between(
            char('\''),
            char('\''),
            many(satisfy(|c| c != '\'')).map(|v: Vec<char>| v.into_iter().collect::<String>()),
        );
        assert_eq!(
            parser.parse("'Hey man!' he said."),
            Ok(("Hey man!".to_string(), " he said."))
        );
    }
}
