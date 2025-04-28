package collector

import (
	"context"
	"database/sql"
	"strconv"
)

type Setting struct {
	Name  string
	Unit  string
	Value float64
}

func (c *Collector) getSettings(ctx context.Context) ([]Setting, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT name, setting, unit, vartype FROM pg_settings WHERE vartype in ('integer','real', 'bool')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []Setting
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
		res = append(res, Setting{Name: name.String, Unit: unit.String, Value: v})
	}
	return res, nil
}
