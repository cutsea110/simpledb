use anyhow::Result;
use core::fmt;

#[derive(Debug)]
pub enum TokenError {
    InvalidKeyword,
}

impl std::error::Error for TokenError {}
impl fmt::Display for TokenError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TokenError::InvalidKeyword => {
                write!(f, "invalid keyword")
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Token {
    DELIMITER(Delimiter),
    INT32(i32),
    STRING(String),
    KEYWORD(Keyword),
    IDENTIFIER(String),
    EOS,
    // illegal token
    ILLEGAL,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Delimiter {
    COMMA,
    EQ,
    SEMICOLON,
    LPAREN,
    RPAREN,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Keyword {
    SELECT,
    FROM,
    WHERE,
    AND,
    INSERT,
    INTO,
    VALUES,
    DELETE,
    UPDATE,
    SET,
    CREATE,
    TABLE,
    VARCHAR,
    INT,
    VIEW,
    AS,
    INDEX,
    ON,
}

type Location = i32;

pub struct Lexer {
    input: Vec<char>,         // source code
    pub position: usize,      // reading position
    pub read_position: usize, // current moving reading position
    pub ch: Option<char>,     // current read character
}

fn is_letter(ch: char) -> bool {
    'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

fn is_digit(ch: char) -> bool {
    '0' <= ch && ch <= '9'
}

fn is_whilespace(ch: char) -> bool {
    ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

fn is_string(ch: char) -> bool {
    ch == '\''
}

fn get_keyword_token(ident: &str) -> Result<Token> {
    match ident {
        "select" => Ok(Token::KEYWORD(Keyword::SELECT)),
        "from" => Ok(Token::KEYWORD(Keyword::FROM)),
        "where" => Ok(Token::KEYWORD(Keyword::WHERE)),
        "and" => Ok(Token::KEYWORD(Keyword::AND)),
        "insert" => Ok(Token::KEYWORD(Keyword::INSERT)),
        "into" => Ok(Token::KEYWORD(Keyword::INTO)),
        "values" => Ok(Token::KEYWORD(Keyword::VALUES)),
        "delete" => Ok(Token::KEYWORD(Keyword::DELETE)),
        "update" => Ok(Token::KEYWORD(Keyword::UPDATE)),
        "set" => Ok(Token::KEYWORD(Keyword::SET)),
        "create" => Ok(Token::KEYWORD(Keyword::CREATE)),
        "table" => Ok(Token::KEYWORD(Keyword::TABLE)),
        "varchar" => Ok(Token::KEYWORD(Keyword::VARCHAR)),
        "int" => Ok(Token::KEYWORD(Keyword::INT)),
        "view" => Ok(Token::KEYWORD(Keyword::VIEW)),
        "as" => Ok(Token::KEYWORD(Keyword::AS)),
        "index" => Ok(Token::KEYWORD(Keyword::INDEX)),
        "on" => Ok(Token::KEYWORD(Keyword::ON)),
        _ => Err(From::from(TokenError::InvalidKeyword)),
    }
}

impl Lexer {
    pub fn new(input: Vec<char>) -> Self {
        let mut lexer = Self {
            input,
            position: 0,
            read_position: 0,
            ch: None,
        };
        // init
        lexer.read_char();

        lexer
    }

    pub fn read_char(&mut self) {
        if self.read_position >= self.input.len() {
            self.ch = None;
        } else {
            self.ch = Some(self.input[self.read_position]);
        }
        self.position = self.read_position;
        self.read_position += 1;
    }

    pub fn skip_whitespace(&mut self) {
        while let Some(ch) = self.ch {
            if is_whilespace(ch) {
                self.read_char();
            } else {
                break;
            }
        }
    }

    pub fn next_token(&mut self) -> Token {
        let read_identifier = |l: &mut Lexer| -> Vec<char> {
            let position = l.position;
            while l.position < l.input.len() && is_letter(l.ch.unwrap()) {
                l.read_char();
            }
            l.input[position..l.position].to_vec()
        };

        let read_number = |l: &mut Lexer| -> Vec<char> {
            let position = l.position;
            while l.position < l.input.len() && is_digit(l.ch.unwrap()) {
                l.read_char();
            }
            l.input[position..l.position].to_vec()
        };

        let read_string = |l: &mut Lexer| -> Vec<char> {
            let position = l.position;
            while l.position < l.input.len() {
                l.read_char();
                if is_string(l.ch.unwrap()) {
                    l.read_char();
                    break;
                }
            }
            l.input[position..l.position].to_vec()
        };

        let tok: Token;
        self.skip_whitespace();
        if let Some(ch) = self.ch {
            match ch {
                ',' => tok = Token::DELIMITER(Delimiter::COMMA),
                '=' => tok = Token::DELIMITER(Delimiter::EQ),
                ';' => tok = Token::DELIMITER(Delimiter::SEMICOLON),
                '(' => tok = Token::DELIMITER(Delimiter::LPAREN),
                ')' => tok = Token::DELIMITER(Delimiter::RPAREN),
                _ => {
                    if is_letter(ch) {
                        let ident: String = read_identifier(self).into_iter().collect();
                        match get_keyword_token(&ident.to_lowercase()) {
                            Ok(keyword_token) => {
                                return keyword_token;
                            }
                            Err(_) => {
                                return Token::IDENTIFIER(ident);
                            }
                        }
                    } else if is_digit(ch) {
                        let digits: String = read_number(self).into_iter().collect();
                        let num = digits.parse().unwrap();
                        return Token::INT32(num);
                    } else if is_string(ch) {
                        let string: Vec<char> = read_string(self).into_iter().collect();
                        let s: String = string.into_iter().collect();
                        return Token::STRING(s);
                    } else {
                        return Token::ILLEGAL;
                    }
                }
            }
            self.read_char();
            return tok;
        }

        Token::EOS
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    fn lex(input: &str) {
        let mut l = Lexer::new(input.chars().collect());
        loop {
            match l.next_token() {
                Token::EOS => {
                    break;
                }
                token => {
                    println!("{:?}", token);
                }
            }
        }
        println!("{:?} {} {}", l.ch, l.position, l.read_position);
    }

    #[test]
    fn unit_test() {
        println!("\nlex empty sql");
        lex("");

        println!("\nlex simple sql");
        lex("SELECT SId, SName FROM STUDENT WHERE GradYear = 2020;");

        println!("\nlex sql with subquery");
        lex("SELECT SId, SName FROM (Select SId, SName, GradYear FROM STUDENT) WHERE GradYear = 2020;");

        println!("\nlex joined sql");
        lex("SELECT SName,GradYear,DName FROM STUDENT,DEPT WHERE GradYear=2020 AND MajorId=DId;");

        println!("\nlex sql with string");
        lex("SELECT SId, SName FROM STUDENT WHERE SName = 'joe';");

        println!("\nlex sql with redundant spaces");
        lex("SELECT  SId  , SName \
               FROM STUDENT \
              WHERE SName = 'joe';");
    }
}
