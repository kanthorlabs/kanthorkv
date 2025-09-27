package parser

type TokenType string

type Token struct {
	Type    TokenType
	Literal string
}

func (t Token) String() string {
	if t.Type == EOF {
		return "EOF"
	} else if t.Type == LexerError {
		return "lexing error: " + t.Literal
	} else if t.Type == Int {
		return t.Literal
	} else if t.Type == String {
		return t.Literal
	} else if t.Type == Keyword {
		return t.Literal
	} else if t.Type == Identifier {
		return t.Literal
	} else if t.Type == Equal {
		return "="
	} else if t.Type == Comma {
		return ","
	} else if t.Type == OpenParen {
		return "("
	} else if t.Type == CloseParen {
		return ")"
	}
	return t.Literal
}

func NewToken(t TokenType, l string) Token {
	return Token{Type: t, Literal: l}
}
