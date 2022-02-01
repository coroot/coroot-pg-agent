package collector

import (
	"github.com/blang/semver"
	"strings"
)

func (c *Collector) refreshVersion() error {
	var version string
	err := c.db.QueryRow(`SELECT setting FROM pg_settings WHERE name='server_version'`).Scan(&version)
	if err != nil {
		return err
	}
	ver, err := semver.ParseTolerant(strings.Fields(version)[0])
	if err != nil {
		return err
	}
	c.version = ver
	return nil
}
