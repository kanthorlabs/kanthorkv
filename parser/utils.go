package parser

import (
	"slices"
	"strings"
)

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isKeyword(s string) bool {
	return slices.Contains(keywords, strings.ToLower(s))
}
