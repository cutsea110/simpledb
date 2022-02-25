pub enum Token {
    DELIMITER(Delimiter),
    INT32(i32),
    STRING(String),
    KEYWORD(Keyword),
    IDENTIFIER(String),
}

pub enum Delimiter {
    COMMA,
    SEMICOLON,
    LPAREN,
    RPAREN,
}

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
