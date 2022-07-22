package collector

import (
	"database/sql"
	"fmt"
	"github.com/blang/semver"
	"github.com/coroot/coroot-pg-agent/obfuscate"
	"time"
)

type ssRow struct {
	queryText sql.NullString
	calls     sql.NullInt64
	rows      sql.NullInt64
	totalTime sql.NullFloat64
	ioTime    sql.NullFloat64
}

func (r ssRow) QueryKey(id statementId) QueryKey {
	return QueryKey{Query: obfuscate.Sql(r.queryText.String), User: id.user.String, DB: id.db.String}
}

type statementId struct {
	id   sql.NullInt64
	user sql.NullString
	db   sql.NullString
}

type ssSnapshot struct {
	ts   time.Time
	rows map[statementId]ssRow
}

func (c *Collector) getStatStatements(version semver.Version) (*ssSnapshot, error) {
	snapshot := &ssSnapshot{ts: time.Now(), rows: map[statementId]ssRow{}}
	var query string
	switch {
	case semver.MustParseRange(">=9.4.0 <13.0.0")(version):
		query = `SELECT d.datname, r.rolname, s.query, s.queryid, s.calls, s.total_time, s.blk_read_time + s.blk_write_time`
	case semver.MustParseRange(">=13.0.0")(version):
		query = `SELECT d.datname, r.rolname, s.query, s.queryid, s.calls, s.total_plan_time + s.total_exec_time, s.blk_read_time + s.blk_write_time`
	default:
		return nil, fmt.Errorf("postgres version %s is not supported", version)
	}
	query += ` FROM pg_stat_statements s JOIN pg_roles r ON r.oid=s.userid JOIN pg_database d ON d.oid=s.dbid AND NOT d.datistemplate`
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id statementId
		r := ssRow{}
		err := rows.Scan(&id.db, &id.user, &r.queryText, &id.id, &r.calls, &r.totalTime, &r.ioTime)
		if err != nil {
			c.logger.Warning("failed to scan pg_stat_statements row:", err)
			continue
		}
		if id.user.String == "" || id.db.String == "" || !id.id.Valid {
			continue
		}
		snapshot.rows[id] = r
	}
	return snapshot, nil
}
