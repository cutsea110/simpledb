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
    pub ch: char,             // current read character
}

fn is_letetr(ch: char) -> bool {
    'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

fn is_digit(ch: char) -> bool {
    '0' <= ch && ch <= '9'
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
        Self {
            input,
            position: 0,
            read_position: 0,
            ch: '0',
        }
    }

    pub fn read_char(&mut self) {
        if self.read_position >= self.input.len() {
            self.ch = '0';
        } else {
            self.ch = self.input[self.read_position];
        }
        self.position = self.read_position;
        self.read_position += 1;
    }

    pub fn skip_whitespace(&mut self) {
        let ch = self.ch;
        if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
            self.read_char();
        }
    }

    pub fn next_token(&mut self) -> Token {
        let read_identifier = |l: &mut Lexer| -> Vec<char> {
            let position = l.position;
            while l.position < l.input.len() && is_letetr(l.ch) {
                l.read_char();
            }
            l.input[position..l.position].to_vec()
        };

        let read_number = |l: &mut Lexer| -> Vec<char> {
            let position = l.position;
            while l.position < l.input.len() && is_digit(l.ch) {
                l.read_char();
            }
            l.input[position..l.position].to_vec()
        };

        let tok: Token;
        self.skip_whitespace();
        match self.ch {
            ',' => tok = Token::DELIMITER(Delimiter::COMMA),
            '=' => tok = Token::DELIMITER(Delimiter::EQ),
            ';' => tok = Token::DELIMITER(Delimiter::SEMICOLON),
            '(' => tok = Token::DELIMITER(Delimiter::LPAREN),
            ')' => tok = Token::DELIMITER(Delimiter::RPAREN),
            '0' => tok = Token::EOS,
            _ => {
                if is_letetr(self.ch) {
                    let ident: String = read_identifier(self).into_iter().collect();
                    match get_keyword_token(&ident.to_lowercase()) {
                        Ok(keyword_token) => {
                            return keyword_token;
                        }
                        Err(_) => {
                            return Token::IDENTIFIER(ident);
                        }
                    }
                } else if is_digit(self.ch) {
                    let digits: String = read_number(self).into_iter().collect();
                    let num = digits.parse().unwrap();
                    return Token::INT32(num);
                } else {
                    return Token::ILLEGAL;
                }
            }
        }
        self.read_char();
        tok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_test() {
        let input = "SELECT SId, SName FROM Student WHERE GradYear = 2020;";
        let mut l = Lexer::new(input.chars().collect());
        l.read_char();
        loop {
            let token = l.next_token();
            if token == Token::EOS {
                break;
            } else {
                println!("{:?}", token);
            }
        }
        println!("{} {} {}", char::from(l.ch), l.position, l.read_position);
    }
}
