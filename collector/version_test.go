package collector

import (
	"github.com/blang/semver"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_parsePgVersion(t *testing.T) {
	check := func(src, orig string, ver semver.Version) {
		o, v, err := parsePgVersion(src)
		assert.Nil(t, err)
		assert.Equal(t, orig, o)
		assert.True(t, ver.Equals(v))
	}
	check("14.4 (Ubuntu 14.4-1.pgdg18.04+1)", "14.4", semver.Version{Major: 14, Minor: 4})
	check("9.4.1", "9.4.1", semver.Version{Major: 9, Minor: 4, Patch: 1})
	check("11.2-YB-2.15.0.1-b0", "11.2-YB-2.15.0.1-b0", semver.Version{Major: 11, Minor: 2})
}
