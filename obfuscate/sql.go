package obfuscate

import (
	"bytes"
	"regexp"
	"strings"
)

var (
	rePlaceholder = regexp.MustCompile(`\$\d+`)
	reNumber      = regexp.MustCompile(`[+-]?(?:\d+\.\d+|\d+\.|\.\d+|\d+)(?:e[+-]?\d+)?`)
	reWhitespace  = regexp.MustCompile(`\s+`)
	reTypecast    = regexp.MustCompile(`\s*::\s*"?\w+"?(?:\(\s*\d*\s*\))?(?:\[\s*\])?`)
	reOperator    = regexp.MustCompile(`([!#$%&*+\-/:<=>@^~|]+)`)
	rePunctuation = regexp.MustCompile(`([(),;[\]{}])`)
	reBoolean     = regexp.MustCompile(`(\W)(:?true|false|null)(\W|$)`)
	reValues      = regexp.MustCompile(`(values?)\s*(?:\(\s*\?\s*\)\s*,?\s*)+`)
)

func Sql(query string) string {
	query = strings.ToLower(query)
	query = removeCommentsAndStrings(query)
	query = reWhitespace.ReplaceAllString(query, " ")
	query = rePlaceholder.ReplaceAllString(query, "?")
	query = reTypecast.ReplaceAllString(query, "")
	query = reNumber.ReplaceAllString(query, "?")
	query = reBoolean.ReplaceAllString(query, "$1?$3")

	query = collapseLists(query)
	query = reValues.ReplaceAllString(query, "$1(?)")

	query = reOperator.ReplaceAllString(query, " $1 ")
	query = rePunctuation.ReplaceAllString(query, " $1 ")
	query = reWhitespace.ReplaceAllString(query, " ")
	query = strings.ReplaceAll(query, " ,", ",")
	query = strings.Trim(query, " ")
	return query
}

func removeCommentsAndStrings(query string) string {
	q := newRunes(query)
	var curr, next rune
	var res bytes.Buffer
	for i := 0; i < q.len; i++ {
		curr, next = q.get(i), q.get(i+1)
		switch {
		case curr == '\'': // string constant
			i++
			for ; i < q.len; i++ {
				if q.get(i) == '\'' {
					if q.get(i+1) == '\'' { // escaped quote
						i++
						continue
					}
					break
				}
			}
			res.WriteRune('?')
		case curr == 'e' && next == '\'': // postgres C-style escaped string
			i += 2
			for ; i < q.len; i++ {
				if q.get(i) == '\'' {
					if q.get(i+1) == '\'' || (q.get(i-1) == '\\' && q.get(i-2) != '\\') { // escaped quote
						i++
						continue
					}
					break
				}
			}
			res.WriteRune('?')
		case curr == '$' && next == '$': // postgres dollar-quoted string
			i += 2
			for ; i < q.len; i++ {
				if q.get(i-1) == '$' && q.get(i) == '$' {
					break
				}
			}
			res.WriteRune('?')
		case (curr == 'b' || curr == 'x') && next == '\'': // postgres bit string
			i += 2
			for ; i < q.len; i++ {
				if q.get(i) == '\'' {
					break
				}
			}
			res.WriteRune('?')
		case curr == '-' && next == '-': // single-line comment
			i += 2
			for ; i < q.len; i++ {
				if q.get(i) == '\n' {
					res.WriteRune('\n')
					break
				}
			}
		case curr == '/' && next == '*': // multi-line comment
			i += 2
			for ; i < q.len; i++ {
				if q.get(i-1) == '*' && q.get(i) == '/' {
					break
				}
			}
		default:
			res.WriteRune(curr)
		}
	}
	return res.String()
}

func collapseLists(query string) string {
	q := newRunes(query)
	var res bytes.Buffer
	for i := 0; i < q.len; i++ {
		curr := q.get(i)
		switch curr {
		case '(':
			j := i + 1
			for level := 1; j < q.len && level > 0; j++ {
				switch q.get(j) {
				case '(':
					level++
					continue
				case ')':
					level--
					continue
				case '?', ' ', ',':
					continue
				default:
					goto OUT
				}
			}
			res.WriteString("(?)")
			i = j - 1
			continue
		case '[':
			j := i + 1
			for level := 1; j < q.len && level > 0; j++ {
				switch q.get(j) {
				case '[':
					level++
					continue
				case ']':
					level--
					continue
				case '?', ' ', ',':
					continue
				default:
					goto OUT
				}
			}
			res.WriteString("[?]")
			i = j - 1
			continue
		}
	OUT:
		res.WriteRune(curr)
	}
	return res.String()
}
