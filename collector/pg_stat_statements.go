package collector

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/blang/semver"
	"github.com/coroot/coroot-pg-agent/obfuscate"
)

type ssRow struct {
	obfuscatedQueryText string
	calls               sql.NullInt64
	rows                sql.NullInt64
	totalTime           sql.NullFloat64
	ioTime              sql.NullFloat64
}

func (r ssRow) QueryKey(id statementId) QueryKey {
	return QueryKey{Query: r.obfuscatedQueryText, User: id.user.String, DB: id.db.String}
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

func (c *Collector) getStatStatements(ctx context.Context, version semver.Version, querySizeLimit int, prev map[statementId]ssRow) (*ssSnapshot, error) {
	snapshot := &ssSnapshot{ts: time.Now(), rows: map[statementId]ssRow{}}
	var query string
	switch {
	case semver.MustParseRange(">=9.4.0 <13.0.0")(version):
		query = `SELECT d.datname, r.rolname, LEFT(s.query, %d), s.queryid, s.calls, s.total_time, s.blk_read_time + s.blk_write_time`
	case semver.MustParseRange(">=13.0.0 <17.0.0")(version):
		query = `SELECT d.datname, r.rolname, LEFT(s.query, %d), s.queryid, s.calls, s.total_plan_time + s.total_exec_time, s.blk_read_time + s.blk_write_time`
	case semver.MustParseRange(">=17.0.0")(version):
		query = `SELECT d.datname, r.rolname, LEFT(s.query, %d), s.queryid, s.calls, s.total_plan_time + s.total_exec_time, s.shared_blk_read_time + s.shared_blk_write_time + s.local_blk_read_time + s.local_blk_write_time + s.temp_blk_read_time + s.temp_blk_write_time`
	default:
		return nil, fmt.Errorf("postgres version %s is not supported", version)
	}
	query += ` FROM pg_stat_statements s JOIN pg_roles r ON r.oid=s.userid JOIN pg_database d ON d.oid=s.dbid AND NOT d.datistemplate`
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf(query, querySizeLimit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var queryText sql.NullString
	for rows.Next() {
		var id statementId
		r := ssRow{}
		err := rows.Scan(&id.db, &id.user, &queryText, &id.id, &r.calls, &r.totalTime, &r.ioTime)
		if err != nil {
			c.logger.Warning("failed to scan pg_stat_statements row:", err)
			continue
		}
		if id.user.String == "" || id.db.String == "" || !id.id.Valid {
			continue
		}
		if p, ok := prev[id]; ok {
			r.obfuscatedQueryText = p.obfuscatedQueryText
		} else {
			r.obfuscatedQueryText = obfuscate.Sql(queryText.String)
		}
		snapshot.rows[id] = r
	}
	return snapshot, nil
}
