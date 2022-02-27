use either::Either;
use Either::{Left, Right};

pub trait Parser<T>: Fn(&str) -> Option<(T, &str)> {}
impl<T, F> Parser<T> for F where F: Fn(&str) -> Option<(T, &str)> {}

//
fn generalize_lifetime<T, F>(f: F) -> F
where
    F: Fn(&str) -> Option<(T, &str)>,
{
    f
}

// char

pub fn one_of<'a>(cs: &'a str) -> impl Parser<char> + 'a {
    generalize_lifetime(move |s| {
        if let Some(c0) = s.chars().next() {
            let mut iter = cs.chars();
            while let Some(c) = iter.next() {
                if c == c0 {
                    return Some((c0, &s[1..]));
                }
            }
            return None;
        }

        None
    })
}

pub fn none_of<'a>(cs: &'a str) -> impl Parser<char> + 'a {
    generalize_lifetime(move |s| {
        if let Some(c0) = s.chars().next() {
            let mut iter = cs.chars();
            while let Some(c) = iter.next() {
                if c == c0 {
                    return None;
                }
            }
            return Some((c0, &s[1..]));
        }

        None
    })
}

pub fn spaces() -> impl Parser<()> {
    map(many1(satisfy(&|c: char| c.is_whitespace())), |_| ())
}

pub fn space() -> impl Parser<char> {
    satisfy(&|c: char| c.is_whitespace())
}

pub fn newline() -> impl Parser<char> {
    satisfy(&|c: char| c == '\n')
}

pub fn crlf() -> impl Parser<char> {
    joinr(satisfy(&|c: char| c == '\r'), newline())
}

pub fn end_of_line() -> impl Parser<char> {
    generalize_lifetime(move |s: &str| {
        if let Some((val, rest)) = meet(newline(), crlf())(s) {
            match val {
                Left(c) => return Some((c, rest)),
                Right(c) => return Some((c, rest)),
            }
        }

        None
    })
}

pub fn tab() -> impl Parser<char> {
    satisfy(&|c: char| c == '\t')
}

pub fn upper() -> impl Parser<char> {
    satisfy(&|c: char| c.is_uppercase())
}

pub fn lower() -> impl Parser<char> {
    satisfy(&|c: char| c.is_lowercase())
}

pub fn alpha_num() -> impl Parser<char> {
    satisfy(&|c: char| c.is_alphanumeric())
}

pub fn letter() -> impl Parser<char> {
    satisfy(&|c: char| c.is_alphabetic())
}

pub fn digit() -> impl Parser<char> {
    satisfy(&|c: char| c.is_ascii_digit())
}

pub fn hex_digit() -> impl Parser<char> {
    satisfy(&|c: char| c.is_ascii_hexdigit())
}

pub fn oct_digit() -> impl Parser<char> {
    satisfy(&|c: char| '0' <= c && c <= '7')
}

pub fn char(c: char) -> impl Parser<char> {
    generalize_lifetime(move |s| {
        let mut chars = s.chars();
        if chars.next() == Some(c) {
            Some((c, &s[1..]))
        } else {
            None
        }
    })
}

pub fn any_char() -> impl Parser<char> {
    satisfy(&|_| true)
}

pub fn satisfy(pred: &'static dyn Fn(char) -> bool) -> impl Parser<char> {
    generalize_lifetime(|s: &str| {
        let mut iter = s.chars();
        if let Some(c) = iter.next() {
            if pred(c) {
                return Some((c, iter.as_str()));
            }
        }
        None
    })
}

pub fn string<'a>(target: &'static str) -> impl Parser<&'a str> {
    generalize_lifetime(move |s| s.strip_prefix(target).map(|rest| (target, rest)))
}

// combinator

pub fn choice<T>(ps: Vec<impl Parser<T>>) -> impl Parser<T> {
    generalize_lifetime(move |s| {
        let mut iter = ps.iter();
        while let Some(parser) = iter.next() {
            if let Some(res) = parser(s) {
                return Some(res);
            }
        }
        None
    })
}

pub fn count<T>(n: usize, parser: impl Parser<T>) -> impl Parser<Vec<T>> {
    generalize_lifetime(move |mut s| {
        let mut result = vec![];

        for _ in 0..n {
            if let Some((val, rest)) = parser(s) {
                result.push(val);
                s = rest;
                continue;
            }
            return None;
        }

        Some((result, s))
    })
}

pub fn between<T, U, V>(
    open: impl Parser<T>,
    close: impl Parser<U>,
    parser: impl Parser<V>,
) -> impl Parser<V> {
    joinl(joinr(open, parser), close)
}

pub fn option<T: Clone>(x: T, parser: impl Parser<T>) -> impl Parser<T> {
    generalize_lifetime(move |s| {
        if let Some((val, rest)) = parser(s) {
            return Some((val, rest));
        }

        Some((x.clone(), s))
    })
}

pub fn option_maybe<T>(parser: impl Parser<T>) -> impl Parser<Option<T>> {
    generalize_lifetime(move |s| {
        if let Some((val, rest)) = parser(s) {
            return Some((Some(val), rest));
        }

        None
    })
}

pub fn optional<T>(parser: impl Parser<T>) -> impl Parser<()> {
    generalize_lifetime(move |s| {
        if let Some((_, rest)) = parser(s) {
            return Some(((), rest));
        }

        Some(((), s))
    })
}

pub fn skip_many1<T>(parser: impl Parser<T>) -> impl Parser<()> {
    generalize_lifetime(move |s| {
        if let Some((_, rest1)) = parser(s) {
            if let Some((_, rest2)) = skip_many(&parser)(rest1) {
                return Some(((), rest2));
            }
            return Some(((), rest1));
        }

        None
    })
}

pub fn many1<T>(parser: impl Parser<T>) -> impl Parser<Vec<T>> {
    generalize_lifetime(move |s| {
        if let Some((val1, rest1)) = parser(s) {
            if let Some((mut val2, rest2)) = many(&parser)(rest1) {
                val2.insert(0, val1);
                return Some((val2, rest2));
            }
            return Some((vec![val1], rest1));
        }

        None
    })
}

pub fn sep_by<T, U>(parser: impl Parser<T>, sep: impl Parser<U>) -> impl Parser<Vec<T>> {
    generalize_lifetime(move |s| {
        if let Some(res) = sep_by1(&parser, &sep)(s) {
            return Some(res);
        }
        return Some((vec![], s));
    })
}

pub fn sep_by1<T, U>(parser: impl Parser<T>, sep: impl Parser<U>) -> impl Parser<Vec<T>> {
    generalize_lifetime(move |s| {
        if let Some((val1, rest1)) = parser(s) {
            if let Some((mut val2, rest2)) = many(joinr(&sep, &parser))(rest1) {
                val2.insert(0, val1);
                return Some((val2, rest2));
            }
            return Some((vec![val1], rest1));
        }

        None
    })
}

pub fn end_by<T, U>(parser: impl Parser<T>, sep: impl Parser<U>) -> impl Parser<Vec<T>> {
    many(joinl(parser, sep))
}

pub fn end_by1<T, U>(parser: impl Parser<T>, sep: impl Parser<U>) -> impl Parser<Vec<T>> {
    many1(joinl(parser, sep))
}

pub fn sep_end_by<'a, T, U>(
    parser: &'a impl Parser<T>,
    sep: &'a impl Parser<U>,
) -> impl Parser<Vec<T>> + 'a {
    generalize_lifetime(move |s| {
        if let Some(res) = sep_end_by1(parser, sep)(s) {
            return Some(res);
        }
        return Some((vec![], s));
    })
}

pub fn sep_end_by1<'a, T, U>(
    parser: &'a impl Parser<T>,
    sep: &'a impl Parser<U>,
) -> impl Parser<Vec<T>> + 'a {
    generalize_lifetime(move |s| {
        if let Some((val1, rest1)) = parser(s) {
            if let Some((mut val2, rest2)) = joinr(sep, sep_end_by(parser, sep))(rest1) {
                val2.insert(0, val1);
                return Some((val2, rest2));
            }
            return Some((vec![val1], rest1));
        }

        None
    })
}

pub fn chainl<T, F>(parser: impl Parser<T>, op: impl Parser<F>, x: T) -> impl Parser<T>
where
    T: Clone,
    F: Fn(T, T) -> T,
{
    generalize_lifetime(move |s| {
        if let Some(res) = chainl1(&parser, &op)(s) {
            return Some(res);
        }

        Some((x.clone(), s))
    })
}

pub fn chainl1<T, F>(parser: impl Parser<T>, op: impl Parser<F>) -> impl Parser<T>
where
    F: Fn(T, T) -> T,
{
    generalize_lifetime(move |mut s| {
        if let Some((x, rest1)) = parser(s) {
            s = rest1;
            let mut result = x;
            while let Some(((f, y), rest2)) = join(&op, &parser)(s) {
                s = rest2;
                result = f(result, y);
            }
            return Some((result, s));
        }

        None
    })
}

pub fn chainr<'a, T, F>(
    parser: &'a impl Parser<T>,
    op: &'a impl Parser<F>,
    x: T,
) -> impl Parser<T> + 'a
where
    T: Clone + 'a,
    F: Fn(T, T) -> T,
{
    generalize_lifetime(move |s| {
        if let Some(res) = chainr1(parser, op)(s) {
            return Some(res);
        }

        Some((x.clone(), s))
    })
}

pub fn chainr1<'a, T, F>(parser: &'a impl Parser<T>, op: &'a impl Parser<F>) -> impl Parser<T> + 'a
where
    F: Fn(T, T) -> T,
{
    generalize_lifetime(move |s| {
        if let Some((x, rest1)) = parser(s) {
            if let Some(((f, y), rest2)) = join(op, chainr1(parser, op))(rest1) {
                return Some((f(x, y), rest2));
            }
            return Some((x, rest1));
        }

        None
    })
}

pub fn many_till<'a, T, U>(
    parser: &'a impl Parser<T>,
    end: &'a impl Parser<U>,
) -> impl Parser<Vec<T>> + 'a {
    generalize_lifetime(move |s| {
        if let Some((_, rest)) = end(s) {
            return Some((vec![], rest));
        } else if let Some((val1, rest1)) = parser(s) {
            if let Some((mut val2, rest2)) = many_till(parser, end)(rest1) {
                val2.insert(0, val1);
                return Some((val2, rest2));
            }
        }

        None
    })
}

// primitive

pub fn many<T>(parser: impl Parser<T>) -> impl Parser<Vec<T>> {
    many_accum(
        |x, xs| {
            xs.push(x);
            xs
        },
        parser,
    )
}

pub fn skip_many<T>(parser: impl Parser<T>) -> impl Parser<()> {
    map(many_accum(|_, xs| xs, parser), |_| ())
}

pub fn many_accum<T, F>(acc: F, parser: impl Parser<T>) -> impl Parser<Vec<T>>
where
    F: Fn(T, &mut Vec<T>) -> &mut Vec<T>, // no need return value
{
    generalize_lifetime(move |mut s| {
        let mut ret = vec![];
        while let Some((val, rest)) = parser(s) {
            acc(val, &mut ret);
            s = rest;
        }
        Some((ret, s))
    })
}

pub fn natural() -> impl Parser<i32> {
    map(many1(digit()), |ns: Vec<char>| {
        ns.iter().fold(0, |sum, &c| 10 * sum + ((c as i32) - 48))
    })
}

pub fn symbol<'a>(s: &'static str) -> impl Parser<&'a str> {
    lexeme(string(s))
}

pub fn lexeme<T>(parser: impl Parser<T>) -> impl Parser<T> {
    joinl(parser, many(space()))
}

pub fn map<A, B>(parser: impl Parser<A>, f: impl Fn(A) -> B) -> impl Parser<B> {
    generalize_lifetime(move |s| parser(s).map(|(val, rest)| (f(val), rest)))
}

// <*>
pub fn join<A, B>(parser1: impl Parser<A>, parser2: impl Parser<B>) -> impl Parser<(A, B)> {
    generalize_lifetime(move |s| {
        parser1(s)
            .and_then(|(val1, rest1)| parser2(rest1).map(|(val2, rest2)| ((val1, val2), rest2)))
    })
}

// <*
pub fn joinl<A, B>(parser1: impl Parser<A>, parser2: impl Parser<B>) -> impl Parser<A> {
    map(join(parser1, parser2), |(x, _)| x)
}

// *>
pub fn joinr<A, B>(parser1: impl Parser<A>, parser2: impl Parser<B>) -> impl Parser<B> {
    map(join(parser1, parser2), |(_, y)| y)
}

// <|>
pub fn meet<A, B>(parser1: impl Parser<A>, parser2: impl Parser<B>) -> impl Parser<Either<A, B>> {
    generalize_lifetime(move |s| {
        if let Some((val1, rest1)) = parser1(s) {
            return Some((Left(val1), rest1));
        } else if let Some((val2, rest2)) = parser2(s) {
            return Some((Right(val2), rest2));
        } else {
            return None;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_of_test() {
        assert_eq!(one_of("aeiou")("abc"), Some(('a', "bc")));
        assert_eq!(one_of("1234")("4567"), Some(('4', "567")));
        assert_eq!(one_of("aeiou")("bcd"), None);
        assert_eq!(one_of("aeiou")("ABC"), None);
        assert_eq!(one_of("aeiou")(""), None);
        assert_eq!(one_of("")("ABC"), None);
    }

    #[test]
    fn none_of_test() {
        assert_eq!(none_of("aeiou")("abc"), None);
        assert_eq!(none_of("1234")("4567"), None);
        assert_eq!(none_of("aeiou")("bcd"), Some(('b', "cd")));
        assert_eq!(none_of("aeiou")("ABC"), Some(('A', "BC")));
        assert_eq!(none_of("aeiou")(""), None);
        assert_eq!(none_of("")("ABC"), Some(('A', "BC")));
    }

    #[test]
    fn spaces_test() {
        assert_eq!(spaces()("   123"), Some(((), "123")));
        assert_eq!(spaces()("   hello"), Some(((), "hello")));
        assert_eq!(spaces()(""), None);
        assert_eq!(spaces()("   "), Some(((), "")));
    }

    #[test]
    fn space_test() {
        assert_eq!(space()("   123"), Some((' ', "  123")));
        assert_eq!(space()("\t  123"), Some(('\t', "  123")));
        assert_eq!(space()("\n  123"), Some(('\n', "  123")));
        assert_eq!(space()("\r  123"), Some(('\r', "  123")));
        assert_eq!(space()("123"), None);
        assert_eq!(space()(""), None);
        assert_eq!(space()("   "), Some((' ', "  ")));
    }

    #[test]
    fn newline_test() {
        assert_eq!(newline()("\n123"), Some(('\n', "123")));
        assert_eq!(newline()("\n\n123"), Some(('\n', "\n123")));
        assert_eq!(newline()("123"), None);
        assert_eq!(newline()("\r123"), None);
        assert_eq!(newline()("\t123"), None);
        assert_eq!(newline()(" 123"), None);
    }

    #[test]
    fn crlf_test() {
        assert_eq!(crlf()("\r\n123"), Some(('\n', "123")));
        assert_eq!(crlf()("\n\r123"), None);
        assert_eq!(crlf()("\r\r\n"), None);
        assert_eq!(crlf()("123"), None);
        assert_eq!(crlf()("null"), None);
        assert_eq!(crlf()(""), None);
    }

    #[test]
    fn end_of_line_test() {
        assert_eq!(end_of_line()("\r\n123"), Some(('\n', "123")));
        assert_eq!(end_of_line()("\n\r123"), Some(('\n', "\r123")));
        assert_eq!(end_of_line()("\r\r\n"), None);
        assert_eq!(end_of_line()("123"), None);
        assert_eq!(end_of_line()("null"), None);
        assert_eq!(end_of_line()(""), None);
    }

    #[test]
    fn tab_test() {
        assert_eq!(tab()("\t123"), Some(('\t', "123")));
        assert_eq!(tab()("\t\t123"), Some(('\t', "\t123")));
        assert_eq!(tab()("123"), None);
        assert_eq!(tab()(""), None);
    }

    #[test]
    fn upper_test() {
        assert_eq!(upper()("Hello"), Some(('H', "ello")));
        assert_eq!(upper()("hello"), None);
        assert_eq!(upper()("123"), None);
        assert_eq!(upper()("\n"), None);
        assert_eq!(upper()(""), None);
    }

    #[test]
    fn lower_test() {
        assert_eq!(lower()("Hello"), None);
        assert_eq!(lower()("hello"), Some(('h', "ello")));
        assert_eq!(lower()("123"), None);
        assert_eq!(lower()("\n"), None);
        assert_eq!(lower()(""), None);
    }

    #[test]
    fn alpha_num_test() {
        assert_eq!(alpha_num()("Hello"), Some(('H', "ello")));
        assert_eq!(alpha_num()("hello"), Some(('h', "ello")));
        assert_eq!(alpha_num()("123"), Some(('1', "23")));
        assert_eq!(alpha_num()("\n"), None);
        assert_eq!(alpha_num()(""), None);
    }

    #[test]
    fn letter_test() {
        assert_eq!(letter()("Hello"), Some(('H', "ello")));
        assert_eq!(letter()("hello"), Some(('h', "ello")));
        assert_eq!(letter()("123"), None);
        assert_eq!(letter()("\n"), None);
        assert_eq!(letter()(""), None);
    }

    #[test]
    fn digit_test() {
        assert_eq!(digit()("123"), Some(('1', "23")));
        assert_eq!(digit()("abc"), None);
        assert_eq!(digit()("ABC"), None);
        assert_eq!(digit()(""), None);
    }

    #[test]
    fn hex_digit_test() {
        assert_eq!(hex_digit()("123"), Some(('1', "23")));
        assert_eq!(hex_digit()("abc"), Some(('a', "bc")));
        assert_eq!(hex_digit()("ABC"), Some(('A', "BC")));
        assert_eq!(hex_digit()("HEX"), None);
        assert_eq!(hex_digit()(""), None);
    }

    #[test]
    fn oct_digit_test() {
        assert_eq!(oct_digit()("012"), Some(('0', "12")));
        assert_eq!(oct_digit()("789"), Some(('7', "89")));
        assert_eq!(oct_digit()("abc"), None);
        assert_eq!(oct_digit()("ABC"), None);
        assert_eq!(oct_digit()("HEX"), None);
        assert_eq!(oct_digit()(""), None);
    }

    #[test]
    fn char_test() {
        assert_eq!(char('a')("abc"), Some(('a', "bc")));
        assert_eq!(char('a')("ABC"), None);
        assert_eq!(char(';')(";;;"), Some((';', ";;")));
        assert_eq!(char('\n')("\n\r\t"), Some(('\n', "\r\t")));
    }

    #[test]
    fn any_char_test() {
        assert_eq!(any_char()("abc"), Some(('a', "bc")));
        assert_eq!(any_char()("ABC"), Some(('A', "BC")));
        assert_eq!(any_char()(" ABC"), Some((' ', "ABC")));
        assert_eq!(any_char()("\t ABC"), Some(('\t', " ABC")));
        assert_eq!(any_char()("# ABC"), Some(('#', " ABC")));
        assert_eq!(any_char()(", ABC"), Some((',', " ABC")));
        assert_eq!(any_char()("123"), Some(('1', "23")));
    }

    #[test]
    fn satisfy_test() {
        assert_eq!(satisfy(&|_| true)("123"), Some(('1', "23")));
        assert_eq!(
            satisfy(&|c: char| c.is_ascii_digit())("123"),
            Some(('1', "23"))
        );
        assert_eq!(satisfy(&|c: char| c.is_ascii_digit())("abc"), None);
        assert_eq!(
            satisfy(&|c: char| c.is_alphabetic())("abc"),
            Some(('a', "bc"))
        );
    }

    #[test]
    fn natural_test() {
        assert_eq!(natural()("123*456"), Some((123, "*456")));
        assert_eq!(natural()("ABCDEFG"), None);
    }

    #[test]
    fn symbol_test() {
        let parser = symbol("hello");
        assert_eq!(parser("hello world"), Some(("hello", "world")));
        assert_eq!(parser("hello    world"), Some(("hello", "world")));
        assert_eq!(parser("hello"), Some(("hello", "")));
        assert_eq!(parser("hello  "), Some(("hello", "")));
        assert_eq!(parser("  "), None);
        assert_eq!(parser("123"), None);
        assert_eq!(parser(""), None);
    }

    #[test]
    fn lexeme_test() {
        let parser = lexeme(natural());
        assert_eq!(parser("123   hello"), Some((123, "hello")));
        assert_eq!(parser("123\r\n\thello"), Some((123, "hello")));
    }

    #[test]
    fn string_test() {
        let parser = string("cut");
        assert_eq!(parser("cut fruits"), Some(("cut", " fruits")));
        assert_eq!(parser("cutty"), Some(("cut", "ty")));
        assert_eq!(parser("scutter"), None);
        assert_eq!(parser("cu oxygen"), None);
    }

    #[test]
    fn map_test() {
        let parser = map(natural(), |x| x + 1);
        assert_eq!(parser("1"), Some((2, "")));
        assert_eq!(parser("X"), None);
    }

    #[test]
    fn choice_test() {
        let hello = string("hello");
        let world = string("world");
        let parser = choice(vec![hello, world]);
        assert_eq!(parser("hello world"), Some(("hello", " world")));
        assert_eq!(parser("world hello"), Some(("world", " hello")));
        assert_eq!(parser("ABC"), None);
        assert_eq!(parser("\n\r"), None);
        assert_eq!(parser(""), None);
    }

    #[test]
    fn count_test() {
        let parser = count(3, digit());
        assert_eq!(parser("12345"), Some((vec!['1', '2', '3'], "45")));
        let hello = string("hello");
        let world = string("world");
        let parser = count(3, meet(hello, world));
        assert_eq!(
            parser("hellohelloworldhello"),
            Some((vec![Left("hello"), Left("hello"), Right("world")], "hello"))
        );
        let parser = count(8, hex_digit());
        assert_eq!(
            parser("deadbeef"),
            Some((vec!['d', 'e', 'a', 'd', 'b', 'e', 'e', 'f'], ""))
        );
        let parser = count(0, hex_digit());
        assert_eq!(parser("deadbeef"), Some((vec![], "deadbeef")));
        let parser = count(6, digit());
        assert_eq!(parser("12345"), None);
        let parser = count(0, digit());
        assert_eq!(parser("12345"), Some((vec![], "12345")));
        let parser = count(3, joinl(many(digit()), char(',')));
        assert_eq!(
            parser("123,45,6,789"),
            Some((vec![vec!['1', '2', '3'], vec!['4', '5'], vec!['6']], "789"))
        );
        assert_eq!(
            parser(",123,45,6,789"),
            Some((vec![vec![], vec!['1', '2', '3'], vec!['4', '5']], "6,789"))
        );
    }

    #[test]
    fn between_test() {
        let parser = between(char('"'), char('"'), many(none_of("\"")));
        assert_eq!(
            parser("\"Hello World\" I said."),
            Some((
                vec!['H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd'],
                " I said."
            ))
        );
        let d = many1(digit());
        let csv = join(&d, many(joinr(char(','), &d)));
        let parser = between(char('{'), char('}'), csv);
        assert_eq!(
            parser("{12,23,34}"),
            Some(((vec!['1', '2'], vec![vec!['2', '3'], vec!['3', '4']]), ""))
        );
        assert_eq!(parser("{42}"), Some(((vec!['4', '2'], vec![]), "")));
        assert_eq!(parser("{}"), None);
    }

    #[test]
    fn option_test() {
        let parser = option('0', digit());
        assert_eq!(parser("123"), Some(('1', "23")));
        assert_eq!(parser("abc"), Some(('0', "abc")));
        assert_eq!(parser(""), Some(('0', "")));
    }

    #[test]
    fn option_maybe_test() {
        let parser = option_maybe(digit());
        assert_eq!(parser("123"), Some((Some('1'), "23")));
        assert_eq!(parser("abc"), None);
        assert_eq!(parser(""), None);
    }

    #[test]
    fn optional_test() {
        let parser = optional(digit());
        assert_eq!(parser("123"), Some(((), "23")));
        assert_eq!(parser("abc"), Some(((), "abc")));
        assert_eq!(parser(""), Some(((), "")));
    }

    #[test]
    fn skip_many_test() {
        let parser = skip_many(space());
        assert_eq!(parser(" 123"), Some(((), "123")));
        assert_eq!(parser("    123"), Some(((), "123")));
        assert_eq!(parser("\t\nabc"), Some(((), "abc")));
        assert_eq!(parser("123"), Some(((), "123")));
        assert_eq!(parser(""), Some(((), "")));
    }

    #[test]
    fn skip_many1_test() {
        let parser = skip_many1(space());
        assert_eq!(parser(" 123"), Some(((), "123")));
        assert_eq!(parser("    123"), Some(((), "123")));
        assert_eq!(parser("\t\nabc"), Some(((), "abc")));
        assert_eq!(parser("123"), None);
        assert_eq!(parser(""), None);
    }

    #[test]
    fn join_test() {
        let plus_minus = choice(vec![char('+'), char('-')]);
        let parser = join(plus_minus, natural());
        assert_eq!(parser("+123"), Some((('+', 123), "")));
        assert_eq!(parser("-123"), Some((('-', 123), "")));
        assert_eq!(parser("+"), None);
        assert_eq!(parser("-"), None);
        assert_eq!(parser("123"), None);
        assert_eq!(parser("-abc"), None);
        assert_eq!(parser("*abc"), None);
    }

    #[test]
    fn joinl_test() {
        let plus_minus = choice(vec![char('+'), char('-')]);
        let parser = joinl(plus_minus, natural());
        assert_eq!(parser("+123"), Some(('+', "")));
        assert_eq!(parser("-123"), Some(('-', "")));
        assert_eq!(parser("+"), None);
        assert_eq!(parser("-"), None);
        assert_eq!(parser("123"), None);
        assert_eq!(parser("-abc"), None);
        assert_eq!(parser("*abc"), None);
    }

    #[test]
    fn joinr_test() {
        let plus_minus = choice(vec![char('+'), char('-')]);
        let parser = joinr(plus_minus, natural());
        assert_eq!(parser("+123"), Some((123, "")));
        assert_eq!(parser("-123"), Some((123, "")));
        assert_eq!(parser("+"), None);
        assert_eq!(parser("-"), None);
        assert_eq!(parser("123"), None);
        assert_eq!(parser("-abc"), None);
        assert_eq!(parser("*abc"), None);
    }

    #[test]
    fn meet_test() {
        let hello = string("hello");
        let parser = meet(natural(), hello);
        assert_eq!(parser("123hello"), Some((Left(123), "hello")));
        assert_eq!(parser("hello123"), Some((Right("hello"), "123")));
        assert_eq!(parser("bay123"), None);
        assert_eq!(parser(""), None);
    }

    #[test]
    fn many_test() {
        let parser = many(lexeme(natural()));
        assert_eq!(parser("10 20 30"), Some((vec![10, 20, 30], "")));
        assert_eq!(parser(""), Some((vec![], "")));
        assert_eq!(parser("10 hello"), Some((vec![10], "hello")));

        let parser = many(digit());
        assert_eq!(parser("123"), Some((vec!['1', '2', '3'], "")));
        assert_eq!(parser(""), Some((vec![], "")));
        assert_eq!(parser("abc"), Some((vec![], "abc")));
        assert_eq!(parser("10 20 30"), Some((vec!['1', '0'], " 20 30")));
        assert_eq!(parser("123abc"), Some((vec!['1', '2', '3'], "abc")));
    }

    #[test]
    fn many1_test() {
        let parser = many1(digit());
        assert_eq!(parser("123"), Some((vec!['1', '2', '3'], "")));
        assert_eq!(parser("1abc"), Some((vec!['1'], "abc")));
        assert_eq!(parser(""), None);
        assert_eq!(parser("abc"), None);
        assert_eq!(parser("10 20 30"), Some((vec!['1', '0'], " 20 30")));
        assert_eq!(parser("123abc"), Some((vec!['1', '2', '3'], "abc")));
    }

    #[test]
    fn sep_by_test() {
        let parser = sep_by(natural(), char(','));
        assert_eq!(parser("1,2,3"), Some((vec![1, 2, 3], "")));
        assert_eq!(parser("10,20,30"), Some((vec![10, 20, 30], "")));
        assert_eq!(parser("10,20,30,"), Some((vec![10, 20, 30], ",")));
        assert_eq!(parser("42"), Some((vec![42], "")));
        assert_eq!(parser("42,"), Some((vec![42], ",")));
        assert_eq!(parser("abc"), Some((vec![], "abc")));
        assert_eq!(parser(""), Some((vec![], "")));
    }

    #[test]
    fn sep_by1_test() {
        let parser = sep_by1(natural(), char(','));
        assert_eq!(parser("1,2,3"), Some((vec![1, 2, 3], "")));
        assert_eq!(parser("10,20,30"), Some((vec![10, 20, 30], "")));
        assert_eq!(parser("10,20,30,"), Some((vec![10, 20, 30], ",")));
        assert_eq!(parser("42"), Some((vec![42], "")));
        assert_eq!(parser("42,"), Some((vec![42], ",")));
        assert_eq!(parser("abc"), None);
        assert_eq!(parser(""), None);
    }

    #[test]
    fn end_by_test() {
        let parser = end_by(natural(), char(';'));
        assert_eq!(parser("1;2;3;"), Some((vec![1, 2, 3], "")));
        assert_eq!(parser("10;20;30;"), Some((vec![10, 20, 30], "")));
        assert_eq!(parser("10;20;30"), Some((vec![10, 20], "30")));
        assert_eq!(parser("42;"), Some((vec![42], "")));
        assert_eq!(parser("42"), Some((vec![], "42")));
        assert_eq!(parser("abc"), Some((vec![], "abc")));
        assert_eq!(parser(""), Some((vec![], "")));
    }

    #[test]
    fn end_by1_test() {
        let parser = end_by1(natural(), char(';'));
        assert_eq!(parser("1;2;3;"), Some((vec![1, 2, 3], "")));
        assert_eq!(parser("10;20;30;"), Some((vec![10, 20, 30], "")));
        assert_eq!(parser("10;20;30"), Some((vec![10, 20], "30")));
        assert_eq!(parser("42;"), Some((vec![42], "")));
        assert_eq!(parser("42"), None);
        assert_eq!(parser("abc"), None);
        assert_eq!(parser(""), None);
    }

    #[test]
    fn sep_end_by_test() {
        let sep = char(';');
        let nat = natural();
        let parser = sep_end_by(&nat, &sep);
        assert_eq!(parser("1;2;3;"), Some((vec![1, 2, 3], "")));
        assert_eq!(parser("10;20;30;"), Some((vec![10, 20, 30], "")));
        assert_eq!(parser("10;20;30"), Some((vec![10, 20, 30], "")));
        assert_eq!(parser("42;"), Some((vec![42], "")));
        assert_eq!(parser("42"), Some((vec![42], "")));
        assert_eq!(parser("abc"), Some((vec![], "abc")));
        assert_eq!(parser(""), Some((vec![], "")));
    }

    #[test]
    fn sep_end_by1_test() {
        let sep = char(';');
        let nat = natural();
        let parser = sep_end_by1(&nat, &sep);
        assert_eq!(parser("1;2;3;"), Some((vec![1, 2, 3], "")));
        assert_eq!(parser("10;20;30;"), Some((vec![10, 20, 30], "")));
        assert_eq!(parser("10;20;30"), Some((vec![10, 20, 30], "")));
        assert_eq!(parser("42;"), Some((vec![42], "")));
        assert_eq!(parser("42"), Some((vec![42], "")));
        assert_eq!(parser("abc"), None);
        assert_eq!(parser(""), None);
    }

    #[test]
    fn chainl_test() {
        let nat = natural();
        let plus = map(char('+'), |_| |x, y: i32| x + y);
        let parser = chainl(nat, plus, 0);
        assert_eq!(parser(""), Some((0, "")));
        assert_eq!(parser("1"), Some((1, "")));
        assert_eq!(parser("1+2"), Some((3, "")));
        assert_eq!(parser("1+2+3"), Some((6, "")));
        assert_eq!(parser("1+2+3+4+5+6+7+8+9+10"), Some((55, "")));
        assert_eq!(parser("1+"), Some((1, "+")));
        assert_eq!(parser("28+14abc"), Some((42, "abc")));
    }

    #[test]
    fn chainl1_test() {
        let nat = natural();
        let plus = map(char('+'), |_| |x, y: i32| x + y);
        let parser = chainl1(nat, plus);
        assert_eq!(parser(""), None);
        assert_eq!(parser("1"), Some((1, "")));
        assert_eq!(parser("1+2"), Some((3, "")));
        assert_eq!(parser("1+2+3"), Some((6, "")));
        assert_eq!(parser("1+2+3+4+5+6+7+8+9+10"), Some((55, "")));
        assert_eq!(parser("1+"), Some((1, "+")));
        assert_eq!(parser("28+14abc"), Some((42, "abc")));
    }

    #[test]
    fn chainr_test() {
        let nat = natural();
        let plus = map(char('+'), |_| |x, y: i32| x + y);
        let parser = chainr(&nat, &plus, 0);
        assert_eq!(parser(""), Some((0, "")));
        assert_eq!(parser("1"), Some((1, "")));
        assert_eq!(parser("1+2"), Some((3, "")));
        assert_eq!(parser("1+2+3"), Some((6, "")));
        assert_eq!(parser("1+2+3+4+5+6+7+8+9+10"), Some((55, "")));
        assert_eq!(parser("1+"), Some((1, "+")));
        assert_eq!(parser("28+14abc"), Some((42, "abc")));
    }

    #[test]
    fn chainr1_test() {
        let nat = natural();
        let plus = map(char('+'), |_| |x, y: i32| x + y);
        let parser = chainr1(&nat, &plus);
        assert_eq!(parser(""), None);
        assert_eq!(parser("1"), Some((1, "")));
        assert_eq!(parser("1+2"), Some((3, "")));
        assert_eq!(parser("1+2+3"), Some((6, "")));
        assert_eq!(parser("1+2+3+4+5+6+7+8+9+10"), Some((55, "")));
        assert_eq!(parser("1+"), Some((1, "+")));
        assert_eq!(parser("28+14abc"), Some((42, "abc")));
    }

    #[test]
    fn many_till_test() {
        let d = digit();
        let period = char('.');
        let parser = many_till(&d, &period);
        assert_eq!(parser("123.45"), Some((vec!['1', '2', '3'], "45")));
        assert_eq!(parser(".123.45"), Some((vec![], "123.45")));
        assert_eq!(parser("abc"), None);
        assert_eq!(parser(""), None);

        let begin = char('"');
        let s = none_of("\"");
        let end = char('"');
        let parser = joinr(begin, many_till(&s, &end));
        assert_eq!(
            parser("\"Hello World\""),
            Some((
                vec!['H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd'],
                ""
            ))
        );
    }
}
