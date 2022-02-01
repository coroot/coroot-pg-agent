package obfuscate

type runes struct {
	data []rune
	len  int
}

func newRunes(s string) *runes {
	d := []rune(s)
	return &runes{data: d, len: len(d)}
}

func (r *runes) get(i int) rune {
	if i >= r.len {
		return 0
	}
	return r.data[i]
}
