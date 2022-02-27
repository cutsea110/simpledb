use itertools::Itertools;

pub trait Parser<T>: Fn(&str) -> Option<(T, &str)> {}
impl<T, F> Parser<T> for F where F: Fn(&str) -> Option<(T, &str)> {}

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

pub fn digit() -> impl Parser<i32> {
    map(satisfy(&|c: char| c.is_ascii_digit()), &|c: char| {
        c as i32 - 48
    })
}

pub fn char(c: char) -> impl Parser<()> {
    generalize_lifetime(move |s| {
        let mut chars = s.chars();
        if chars.next() == Some(c) {
            Some(((), chars.as_str()))
        } else {
            None
        }
    })
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

pub fn string(target: &'static str) -> impl Parser<()> {
    generalize_lifetime(move |s| s.strip_prefix(target).map(|rest| ((), rest)))
}

// combinator

pub fn choice<T>(parser1: impl Parser<T>, parser2: impl Parser<T>) -> impl Parser<T> {
    generalize_lifetime(move |s| parser1(s).or_else(|| parser2(s)))
}

pub fn many<T>(parser: impl Parser<T>) -> impl Parser<Vec<T>> {
    generalize_lifetime(move |mut s| {
        let mut ret = vec![];
        while let Some((val, rest)) = parser(s) {
            ret.push(val);
            s = rest;
        }
        Some((ret, s))
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

pub fn separated<T>(parser: impl Parser<T>, sep: impl Parser<()>) -> impl Parser<Vec<T>> {
    generalize_lifetime(move |mut s| {
        let mut ret = vec![];
        match parser(s) {
            Some((val, rest)) => {
                ret.push(val);
                s = rest;
            }
            None => return Some((ret, s)),
        }

        while let Some((_, rest)) = sep(s) {
            s = rest;
            match parser(s) {
                Some((val, rest)) => {
                    ret.push(val);
                    s = rest;
                }
                None => {
                    return None;
                }
            }
        }

        Some((ret, s))
    })
}

// uncategorized

pub fn digits() -> impl Parser<i32> {
    map(many1(digit()), |ns: Vec<i32>| {
        ns.iter().fold(0, |sum, x| 10 * sum + x)
    })
}

pub fn lexeme<T>(parser: impl Parser<T>) -> impl Parser<T> {
    generalize_lifetime(move |s| parser(s.trim_start()))
}

pub fn map<A, B>(parser: impl Parser<A>, f: impl Fn(A) -> B) -> impl Parser<B> {
    generalize_lifetime(move |s| parser(s).map(|(val, rest)| (f(val), rest)))
}

pub fn join<A, B>(parser1: impl Parser<A>, parser2: impl Parser<B>) -> impl Parser<(A, B)> {
    generalize_lifetime(move |s| {
        parser1(s)
            .and_then(|(val1, rest1)| parser2(rest1).map(|(val2, rest2)| ((val1, val2), rest2)))
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
    fn spaces_test() {
        assert_eq!(spaces()("   123"), Some(((), "123")));
        assert_eq!(spaces()("   hello"), Some(((), "hello")));
        assert_eq!(spaces()(""), None);
        assert_eq!(spaces()("   "), Some(((), "")));
    }

    #[test]
    fn digit_test() {
        assert_eq!(digit()("123"), Some((1, "23")));
        assert_eq!(digit()(""), None);
    }

    #[test]
    fn digits_test() {
        assert_eq!(digits()("123*456"), Some((123, "*456")));
        assert_eq!(digits()("ABCDEFG"), None);
    }

    #[test]
    fn lexeme_test() {
        let parser = lexeme(digits());
        assert_eq!(parser("   123   hello"), Some((123, "   hello")));
        assert_eq!(parser("\r\n\t123\n\t\rhello"), Some((123, "\n\t\rhello")));
    }

    #[test]
    fn string_test() {
        let parser = string("hello");
        assert_eq!(parser("hello world"), Some(((), " world")));
        assert_eq!(parser("hell world"), None);
    }

    #[test]
    fn map_test() {
        let parser = map(digits(), |x| x + 1);
        assert_eq!(parser("1"), Some((2, "")));
        assert_eq!(parser("X"), None);
    }

    #[test]
    fn choce_test() {
        let parser = choice(digits(), map(string("null"), |_| 0));
        assert_eq!(parser("1234"), Some((1234, "")));
        assert_eq!(parser("null"), Some((0, "")));
        assert_eq!(parser("hoge"), None);
    }

    #[test]
    fn join_test() {
        let plus_minus = choice(map(char('+'), |_| '+'), map(char('-'), |_| '-'));
        let parser = join(plus_minus, digits());
        assert_eq!(parser("+123"), Some((('+', 123), "")));
        assert_eq!(parser("-123"), Some((('-', 123), "")));
        assert_eq!(parser("+"), None);
        assert_eq!(parser("-"), None);
        assert_eq!(parser("123"), None);
        assert_eq!(parser("-abc"), None);
        assert_eq!(parser("*abc"), None);
    }

    #[test]
    fn many_test() {
        let parser = many(lexeme(digits()));
        assert_eq!(parser("10 20 30"), Some((vec![10, 20, 30], "")));
        assert_eq!(parser(""), Some((vec![], "")));
        assert_eq!(parser("10 hello"), Some((vec![10], " hello")));

        let parser = many(digit());
        assert_eq!(parser("123"), Some((vec![1, 2, 3], "")));
        assert_eq!(parser(""), Some((vec![], "")));
        assert_eq!(parser("abc"), Some((vec![], "abc")));
        assert_eq!(parser("10 20 30"), Some((vec![1, 0], " 20 30")));
        assert_eq!(parser("123abc"), Some((vec![1, 2, 3], "abc")));
    }

    #[test]
    fn many1_test() {
        let parser = many1(digit());
        assert_eq!(parser("123"), Some((vec![1, 2, 3], "")));
        assert_eq!(parser(""), None);
        assert_eq!(parser("abc"), None);
        assert_eq!(parser("10 20 30"), Some((vec![1, 0], " 20 30")));
        assert_eq!(parser("123abc"), Some((vec![1, 2, 3], "abc")));
    }

    #[test]
    fn separated_test() {
        let parser = separated(digits(), char(','));
        assert_eq!(parser("1,2,3"), Some((vec![1, 2, 3], "")));
        assert_eq!(parser(""), Some((vec![], "")));
    }
}
