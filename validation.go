package lfsb

import (
	"fmt"
	"strings"

	"github.com/superfly/ltx"
)

// Maximum identifier lengths.
const (
	MaxClusterLen  = 32
	MaxDatabaseLen = 256
)

// ValidateClusterName returns nil if s is a valid cluster.
func ValidateClusterName(s string) error {
	if s == "" {
		return ErrClusterRequired
	} else if len(s) > MaxClusterLen || !isWord(s) {
		return ErrClusterInvalid
	}
	return nil
}

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

// FormatLTXFilename returns a filename based on the min & max TXID.
func FormatLTXFilename(min, max ltx.TXID) string {
	return fmt.Sprintf("%s-%s.ltx", min.String(), max.String())
}
