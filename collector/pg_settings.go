package collector

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
)

func (c *Collector) collectPgSettingsMetrics(ch chan<- prometheus.Metric) {
	rows, err := c.db.Query(`SELECT name, setting, unit, vartype FROM pg_settings WHERE vartype in ('integer','real', 'bool')`)
	if err != nil {
		c.logger.Warning("failed to query pg_settings:", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name, value, unit, vartype sql.NullString
		err := rows.Scan(&name, &value, &unit, &vartype)
		if err != nil {
			c.logger.Warning("failed to scan pg_settings row:", err)
			continue
		}
		var v float64
		switch vartype.String {
		case "integer", "real":
			v, err = strconv.ParseFloat(value.String, 64)
			if err != nil {
				c.logger.Warningf("failed to parse value for %s=%s setting: %s", name.String, value.String, err)
				continue
			}
		case "bool":
			if value.String == "on" {
				v = 1.
			}
		default:
			continue
		}
		ch <- gauge(dSettings, v, name.String, unit.String)
	}
}
