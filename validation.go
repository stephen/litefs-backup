package lfsb

import "strings"

const (
	MaxDatabaseLen = 256
)

// ValidateDatabase returns nil if s is a valid database name.
func ValidateDatabase(s string) error {
	if s == "" {
		return ErrDatabaseRequired
	} else if len(s) > MaxDatabaseLen || !isWord(s) {
		return Errorf(ErrorTypeValidation, "EBADDB", "invalid database name: %q", s)
	}
	return nil
}

// isWord returns true if all runes in s are word characters. See isWordCh().
func isWord(s string) bool {
	if strings.HasPrefix(s, ".") {
		return false
	}

	for _, ch := range s {
		if !isWordCh(ch) {
			return false
		}
	}
	return true
}

// isWordCh returns true if ch is alphanumeric, a digit, an underscore, or hyphen.
func isWordCh(ch rune) bool {
	return (ch >= '0' && ch <= '9') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		ch == '_' || ch == '-' || ch == '.'
}
