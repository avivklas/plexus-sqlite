package pgwire

import (
	"strings"
	"unicode"
)

// QueryType identifies the class of SQL statement.
type QueryType int

const (
	QueryTypeUnknown QueryType = iota
	QueryTypeEmpty
	QueryTypeRead
	QueryTypeWrite
	QueryTypeSet
	QueryTypeTxBegin
	QueryTypeTxCommit
	QueryTypeTxRollback
)

// Classify returns the QueryType for a given SQL statement.
func Classify(query string) QueryType {
	cleaned := cleanQuery(query)
	if cleaned == "" {
		return QueryTypeEmpty
	}

	upper := strings.ToUpper(cleaned)

	// Transaction control
	if strings.HasPrefix(upper, "BEGIN") || strings.HasPrefix(upper, "START TRANSACTION") {
		return QueryTypeTxBegin
	}
	if strings.HasPrefix(upper, "COMMIT") || strings.HasPrefix(upper, "END") {
		return QueryTypeTxCommit
	}
	if strings.HasPrefix(upper, "ROLLBACK") || strings.HasPrefix(upper, "ABORT") {
		return QueryTypeTxRollback
	}

	// Client session settings (e.g. "SET client_encoding = 'UTF8'")
	if strings.HasPrefix(upper, "SET") || strings.HasPrefix(upper, "RESET") {
		return QueryTypeSet
	}

	// Read operations
	if strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		strings.HasPrefix(upper, "PRAGMA") ||
		strings.HasPrefix(upper, "SHOW") ||
		strings.HasPrefix(upper, "WITH") {
		return QueryTypeRead
	}

	// Mutating operations (DML & DDL)
	if strings.HasPrefix(upper, "INSERT") ||
		strings.HasPrefix(upper, "UPDATE") ||
		strings.HasPrefix(upper, "DELETE") ||
		strings.HasPrefix(upper, "CREATE") ||
		strings.HasPrefix(upper, "DROP") ||
		strings.HasPrefix(upper, "ALTER") ||
		strings.HasPrefix(upper, "REPLACE") ||
		strings.HasPrefix(upper, "TRUNCATE") ||
		strings.HasPrefix(upper, "VACUUM") {
		return QueryTypeWrite
	}

	return QueryTypeUnknown
}

// cleanQuery strips leading/trailing whitespace and comments.
func cleanQuery(sql string) string {
	s := strings.TrimSpace(sql)
	for {
		if strings.HasPrefix(s, "--") {
			idx := strings.IndexByte(s, '\n')
			if idx == -1 {
				return ""
			}
			s = strings.TrimSpace(s[idx+1:])
			continue
		}
		if strings.HasPrefix(s, "/*") {
			idx := strings.Index(s, "*/")
			if idx == -1 {
				return ""
			}
			s = strings.TrimSpace(s[idx+2:])
			continue
		}
		break
	}
	return strings.TrimRightFunc(s, func(r rune) bool {
		return unicode.IsSpace(r) || r == ';'
	})
}

// ConvertPlaceholders rewrites PostgreSQL parameter placeholders ($1, $2, ...)
// into SQLite positional placeholders (?) while preserving string literals.
func ConvertPlaceholders(query string) string {
	var out strings.Builder
	out.Grow(len(query))

	inSingleQuote := false
	inDoubleQuote := false
	runes := []rune(query)

	for i := 0; i < len(runes); i++ {
		r := runes[i]

		// Handle quotes
		if r == '\'' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
			out.WriteRune(r)
			continue
		}
		if r == '"' && !inSingleQuote {
			inDoubleQuote = !inDoubleQuote
			out.WriteRune(r)
			continue
		}

		// If outside quotes, convert $1, $2... to ?
		if !inSingleQuote && !inDoubleQuote && r == '$' && i+1 < len(runes) && unicode.IsDigit(runes[i+1]) {
			out.WriteByte('?')
			i++
			for i < len(runes) && unicode.IsDigit(runes[i]) {
				i++
			}
			i-- // compensate for loop increment
			continue
		}

		out.WriteRune(r)
	}

	return out.String()
}
