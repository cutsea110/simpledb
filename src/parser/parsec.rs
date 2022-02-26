pub trait Parser<T>: Fn(&str) -> Option<(T, &str)> {}
impl<T, F> Parser<T> for F where F: Fn(&str) -> Option<(T, &str)> {}

fn generalize_lifetime<T, F>(f: F) -> F
where
    F: Fn(&str) -> Option<(T, &str)>,
{
    f
}

pub fn digit(s: &str) -> Option<(i32, &str)> {
    if let Some(ch) = s.chars().next() {
        if ch.is_ascii_digit() {
            return Some((ch as i32 - 48, &s[1..])); // 48 = '0'
        }
        return None;
    }

    None
}

pub fn digits(s: &str) -> Option<(i32, &str)> {
    let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    match s[..end].parse() {
        Ok(val) => Some((val, &s[end..])),
        Err(_) => None,
    }
}

pub fn space(s: &str) -> Option<((), &str)> {
    if !s.is_empty() {
        let mut pos = 0;
        while let Some(ch) = s.chars().nth(pos) {
            if !ch.is_whitespace() {
                break;
            }
            pos += 1;
            continue;
        }
        return Some(((), &s[pos..]));
    }

    None
}

pub fn lexeme<T>(parser: impl Parser<T>) -> impl Parser<T> {
    generalize_lifetime(move |s| parser(s.trim_start()))
}

pub fn chr(c: char) -> impl Parser<()> {
    generalize_lifetime(move |s| {
        let mut chars = s.chars();
        if chars.next() == Some(c) {
            Some(((), chars.as_str()))
        } else {
            None
        }
    })
}

pub fn string(target: &'static str) -> impl Parser<()> {
    generalize_lifetime(move |s| s.strip_prefix(target).map(|rest| ((), rest)))
}

pub fn map<A, B>(parser: impl Parser<A>, f: impl Fn(A) -> B) -> impl Parser<B> {
    generalize_lifetime(move |s| parser(s).map(|(val, rest)| (f(val), rest)))
}

pub fn choice<T>(parser1: impl Parser<T>, parser2: impl Parser<T>) -> impl Parser<T> {
    generalize_lifetime(move |s| parser1(s).or_else(|| parser2(s)))
}

pub fn join<A, B>(parser1: impl Parser<A>, parser2: impl Parser<B>) -> impl Parser<(A, B)> {
    generalize_lifetime(move |s| {
        parser1(s)
            .and_then(|(val1, rest1)| parser2(rest1).map(|(val2, rest2)| ((val1, val2), rest2)))
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn space_test() {
        assert_eq!(space("   123"), Some(((), "123")));
        assert_eq!(space("   hello"), Some(((), "hello")));
        assert_eq!(space(""), None);
        assert_eq!(space("   "), Some(((), "")));
    }

    #[test]
    fn digit_test() {
        assert_eq!(digit("123"), Some((1, "23")));
        assert_eq!(digit(""), None);
    }

    #[test]
    fn digits_test() {
        assert_eq!(digits("123*456"), Some((123, "*456")));
        assert_eq!(digits("ABCDEFG"), None);
    }

    #[test]
    fn lexeme_test() {
        let parser = lexeme(digits);
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
        let parser = map(digits, |x| x + 1);
        assert_eq!(parser("1"), Some((2, "")));
        assert_eq!(parser("X"), None);
    }

    #[test]
    fn choce_test() {
        let parser = choice(digits, map(string("null"), |_| 0));
        assert_eq!(parser("1234"), Some((1234, "")));
        assert_eq!(parser("null"), Some((0, "")));
        assert_eq!(parser("hoge"), None);
    }

    #[test]
    fn join_test() {
        let plus_minus = choice(map(chr('+'), |_| '+'), map(chr('-'), |_| '-'));
        let parser = join(plus_minus, digits);
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
        let parser = many(lexeme(digits));
        assert_eq!(parser("10 20 30"), Some((vec![10, 20, 30], "")));
        assert_eq!(parser(""), Some((vec![], "")));
        assert_eq!(parser("10 hello"), Some((vec![10], " hello")));
    }

    #[test]
    fn separated_test() {
        let parser = separated(digits, chr(','));
        assert_eq!(parser("1,2,3"), Some((vec![1, 2, 3], "")));
        assert_eq!(parser(""), Some((vec![], "")));
    }
}
