package fileManager

import (
	"strings"
	"unicode"
)


func removeFileFromSlice(files []*FileInfo, target *FileInfo) []*FileInfo {
	for i, file := range files {
		if file == target {
			files[i] = files[len(files)-1]
			return files[:len(files)-1]
		}
	}
	return files
}


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
