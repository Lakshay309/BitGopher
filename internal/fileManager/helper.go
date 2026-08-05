package filemanager

import (
	"strings"
	"unicode"
)

func normalize(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	return strings.Join(strings.Fields(s), " ")
}

func tokenize(text string) []string {
	text = normalize(text)
	if text == "" {
		return nil
	}

	return strings.FieldsFunc(text, func(r rune) bool {
		switch r {
		case ' ', '\t', '\n', '\r',
			'_', '-', '.',
			'/', '\\',
			'(', ')',
			'[', ']',
			'{', '}',
			',', ';', ':':
			return true
		}

		return unicode.IsSpace(r)
	})
}
