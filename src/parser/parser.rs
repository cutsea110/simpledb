#[cfg(test)]
mod tests {
    use combine::parser::char::digit;
    use combine::parser::char::{char, letter, spaces, string, string_cmp};
    use combine::{many, many1, optional, sep_by, Parser};

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
    }
}
