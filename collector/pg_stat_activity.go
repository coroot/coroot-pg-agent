package collector

import (
	"database/sql"
	"fmt"
	"github.com/blang/semver"
	"github.com/coroot/coroot-pg-agent/obfuscate"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
	"time"
)

type sessionId struct {
	queryKey
	state         string
	waitEventType string
}

type session struct {
	sessionId
	queryDuration float64
	backendType   string
	blockingPid   int
}

func (c *Collector) callPgStatActivity(now time.Time, ch chan<- prometheus.Metric) error {
	var query string
	switch {
	case semver.MustParseRange(">=9.3.0 <9.6.0")(c.version):
		query = `SELECT s.pid, s.datname, s.usename, s.query, s.state, extract(epoch from (clock_timestamp()-s.query_start)), s.waiting, null, null, null FROM pg_stat_activity s JOIN pg_database d ON s.datid = d.oid AND NOT d.datistemplate`
	case semver.MustParseRange(">=9.6.0 <10.0.0")(c.version):
		query = "SELECT s.pid, s.datname, s.usename, s.query, s.state, extract(epoch from (clock_timestamp()-s.query_start)), null, s.wait_event_type, null, (pg_blocking_pids(s.pid))[1] FROM pg_stat_activity s JOIN pg_database d ON s.datid = d.oid AND NOT d.datistemplate"
	case semver.MustParseRange(">=10.0.0")(c.version):
		query = "SELECT s.pid, s.datname, s.usename, s.query, s.state, extract(epoch from (clock_timestamp()-s.query_start)), null, s.wait_event_type, s.backend_type, (pg_blocking_pids(s.pid))[1] FROM pg_stat_activity s JOIN pg_database d ON s.datid = d.oid AND NOT d.datistemplate"
	default:
		return fmt.Errorf("postgres version %s is not supported", c.version)
	}
	rows, err := c.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	sessionsCount := map[sessionId]float64{}
	sessionsByPid := map[int]session{}
	awaitingQueriesByBlockingPid := map[int]float64{}

	for rows.Next() {
		var (
			pid                                                int
			db, user, query, state, backendType, waitEventType sql.NullString
			queryDuration                                      sql.NullFloat64
			oldStyleWaiting                                    sql.NullBool
			blockingPid                                        sql.NullInt32
		)
		err := rows.Scan(&pid, &db, &user, &query, &state, &queryDuration, &oldStyleWaiting, &waitEventType, &backendType, &blockingPid)
		if err != nil {
			c.logger.Warning("failed to scan pg_stat_activity row", err)
			continue
		}
		if db.String == "" || user.String == "" || state.String == "" {
			continue
		}
		s := session{
			sessionId: sessionId{
				queryKey:      queryKey{db: db.String, user: user.String},
				waitEventType: waitEventType.String, state: state.String,
			},
			queryDuration: queryDuration.Float64,
			blockingPid:   int(blockingPid.Int32),
			backendType:   backendType.String,
		}
		if oldStyleWaiting.Bool {
			s.waitEventType = "Lock"
		}
		if s.state == "active" || strings.HasPrefix(s.state, "idle in transaction") {
			s.query = obfuscate.Sql(query.String)
		}
		sessionsByPid[pid] = s
		if s.blockingPid > 0 {
			awaitingQueriesByBlockingPid[s.blockingPid]++
		}
		sessionsCount[s.sessionId]++
		if s.state == "active" && len(c.summaries) > 0 {
			summary, ok := c.summaries[s.queryKey]
			if !ok {
				for qk, summ := range c.summaries {
					if qk.equalByQueryPrefix(s.queryKey) {
						summary = summ
						break
					}
				}
				if summary == nil {
					summary = newQuerySummary()
					c.summaries[s.queryKey] = summary
				}
			}
			summary.updateFromStatActivity(now, s)

			dbSummary, ok := c.perDbSummaries[s.db]
			if !ok {
				dbSummary = newQuerySummary()
				c.perDbSummaries[s.db] = dbSummary
			}
			dbSummary.updateFromStatActivity(now, s)
		}
	}

	for k, count := range sessionsCount {
		ch <- gauge(dConnections, count, k.db, k.user, k.state, k.waitEventType, k.query)
	}

	for blockingPid, awaitingQueries := range awaitingQueriesByBlockingPid {
		blocking, ok := sessionsByPid[blockingPid]
		if !ok {
			continue
		}
		ch <- gauge(dLockAwaitingQueries, awaitingQueries, blocking.db, blocking.user, blocking.query)
	}
	return nil
}
