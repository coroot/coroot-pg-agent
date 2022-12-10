package collector

import (
	"database/sql"
	"fmt"
	"github.com/blang/semver"
	"github.com/coroot/coroot-pg-agent/obfuscate"
	"strings"
	"time"
)

type Connection struct {
	DB            sql.NullString
	User          sql.NullString
	Query         sql.NullString
	State         sql.NullString
	QueryStart    sql.NullTime
	BackendType   sql.NullString
	WaitEventType sql.NullString
	BlockingPid   sql.NullInt32
}

func (c Connection) IsClientBackend() bool {
	return c.BackendType.String == "" || c.BackendType.String == "client backend"
}

func (c Connection) QueryKey() QueryKey {
	return QueryKey{Query: obfuscate.Sql(c.Query.String), User: c.User.String, DB: c.DB.String}
}

type saSnapshot struct {
	ts          time.Time
	connections map[int]Connection
}

func (c *Collector) getPgStatActivity(version semver.Version, querySizeLimit int) (*saSnapshot, error) {
	snapshot := &saSnapshot{connections: map[int]Connection{}}
	var query string
	switch {
	case semver.MustParseRange(">=9.3.0 <9.6.0")(version):
		query = "SELECT s.pid, s.datname, s.usename, LEFT(s.query, %d), s.state, now(), s.query_start, s.waiting, null, null, null"
	case semver.MustParseRange(">=9.6.0 <10.0.0")(version):
		query = "SELECT s.pid, s.datname, s.usename, LEFT(s.query, %d), s.state, now(), s.query_start, null, s.wait_event_type, null, (pg_blocking_pids(s.pid))[1]"
	case semver.MustParseRange(">=10.0.0")(version):
		query = "SELECT s.pid, s.datname, s.usename, LEFT(s.query, %d), s.state, now(), s.query_start, null, s.wait_event_type, s.backend_type, (pg_blocking_pids(s.pid))[1]"
	default:
		return nil, fmt.Errorf("postgres version %s is not supported", version)
	}
	query += " FROM pg_stat_activity s JOIN pg_database d ON s.datid = d.oid AND NOT d.datistemplate"
	rows, err := c.db.Query(fmt.Sprintf(query, querySizeLimit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			conn            Connection
			pid             int
			oldStyleWaiting sql.NullBool
		)
		err := rows.Scan(
			&pid, &conn.DB, &conn.User, &conn.Query, &conn.State, &snapshot.ts, &conn.QueryStart,
			&oldStyleWaiting, &conn.WaitEventType, &conn.BackendType, &conn.BlockingPid,
		)
		if err != nil {
			c.logger.Warning("failed to scan pg_stat_activity row:", err)
			continue
		}
		if conn.DB.String == "" || conn.User.String == "" || conn.State.String == "" {
			continue
		}
		if oldStyleWaiting.Bool {
			conn.WaitEventType.String = "Lock"
		}
		if conn.State.String != "active" && !strings.HasPrefix(conn.State.String, "idle in transaction") {
			conn.Query.String = ""
		}
		snapshot.connections[pid] = conn
	}
	return snapshot, nil
}
