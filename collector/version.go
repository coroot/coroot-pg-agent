package collector

import (
	"github.com/blang/semver"
	"regexp"
	"strings"
)

var trimVersionRe = regexp.MustCompile("[^0-9.].*")

func parsePgVersion(v string) (string, semver.Version, error) {
	original := strings.Fields(v)[0]
	version, err := semver.ParseTolerant(trimVersionRe.ReplaceAllString(v, ""))
	if err != nil {
		original = ""
	}
	return original, version, err
}
