package collector

import (
	"database/sql"
	"fmt"
	"github.com/blang/semver"
	"github.com/coroot/coroot-pg-agent/obfuscate"
	"strings"
	"time"
)

type ssRow struct {
	calls     sql.NullInt64
	rows      sql.NullInt64
	totalTime sql.NullFloat64
	ioTime    sql.NullFloat64
	summary   *QuerySummary
}

type statementId struct {
	id   sql.NullInt64
	user sql.NullString
	db   sql.NullString
}

type queryKey struct {
	query string
	db    string
	user  string
}

func (k queryKey) equalByQueryPrefix(other queryKey) bool {
	if k.user != other.user || k.db != other.db {
		return false
	}
	if strings.HasPrefix(k.query, other.query) {
		return true
	}
	return false
}

func (c *Collector) callPgStatStatements(now time.Time) error {
	var query string
	switch {
	case semver.MustParseRange(">=9.4.0 <13.0.0")(c.version):
		query = `SELECT d.datname, r.rolname, s.query, s.queryid, s.calls, s.total_time, s.blk_read_time + s.blk_write_time`
	case semver.MustParseRange(">=13.0.0")(c.version):
		query = `SELECT d.datname, r.rolname, s.query, s.queryid, s.calls, s.total_plan_time + s.total_exec_time, s.blk_read_time + s.blk_write_time`
	default:
		return fmt.Errorf("postgres version %s is not supported", c.version)
	}
	query += ` FROM pg_stat_statements s JOIN pg_roles r ON r.oid=s.userid JOIN pg_database d ON d.oid=s.dbid AND NOT d.datistemplate`
	rows, err := c.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	currentStatements := map[statementId]ssRow{}
	for rows.Next() {
		var query sql.NullString
		var id statementId
		curr := ssRow{}
		err := rows.Scan(&id.db, &id.user, &query, &id.id, &curr.calls, &curr.totalTime, &curr.ioTime)
		if err != nil {
			c.logger.Warning("failed to scan pg_stat_statements row:", err)
			continue
		}
		if id.user.String == "" || id.db.String == "" || !id.id.Valid {
			continue
		}
		prev, ok := c.prevStatements[id]
		if ok {
			curr.summary = prev.summary
			if curr.summary == nil {
				key := queryKey{query: obfuscate.Sql(query.String), user: id.user.String, db: id.db.String}
				if curr.summary, ok = c.summaries[key]; !ok {
					curr.summary = newQuerySummary()
					c.summaries[key] = curr.summary
				}
			}
			curr.summary.updateFromStatStatements(now, curr, prev)
		}

		dbSummary, ok := c.perDbSummaries[id.db.String]
		if !ok {
			dbSummary = newQuerySummary()
			c.perDbSummaries[id.db.String] = dbSummary
		}
		dbSummary.updateFromStatStatements(now, curr, prev)

		currentStatements[id] = curr
	}
	c.prevStatements = currentStatements
	return nil
}
