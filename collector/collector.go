package collector

import (
	"context"
	"database/sql"
	"github.com/blang/semver"
	"github.com/coroot/logger"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
	"sync"
	"time"
)

const (
	topQueriesN        = 20
	hardQuerySizeLimit = 4096
)

var (
	dUp          = desc("pg_up", "Is the server reachable")
	dProbe       = desc("pg_probe_seconds", "Empty query execution time")
	dScrapeError = desc("pg_scrape_error", "Scrape error", "error", "warning")

	dInfo     = desc("pg_info", "Server info", "server_version")
	dSettings = desc("pg_setting", "Value of the pg_setting variable", "name", "unit")

	dConnections = desc("pg_connections", "Number of database connections", "db", "user", "state", "wait_event_type", "query")

	dLatency = desc("pg_latency_seconds", "Query execution time", "summary")

	dDbQueries = desc("pg_db_queries_per_second", "Number of queries executed in the database per second", "db")

	dTopQueryCalls  = desc("pg_top_query_calls_per_second", "Number of times the query was executed", "db", "user", "query")
	dTopQueryTime   = desc("pg_top_query_time_per_second", "Time spent executing the query", "db", "user", "query")
	dTopQueryIOTime = desc("pg_top_query_io_time_per_second", "Time the query spent awaiting IO", "db", "user", "query")

	dLockAwaitingQueries = desc("pg_lock_awaiting_queries", "Number of queries awaiting a lock", "db", "user", "blocking_query")

	dWalReceiverStatus = desc("pg_wal_receiver_status", "WAL receiver status: 1 if the receiver is connected, otherwise 0", "sender_host", "sender_port")
	dWalReplayPaused   = desc("pg_wal_replay_paused", "Whether WAL replay paused or not")
	dWalCurrentLsn     = desc("pg_wal_current_lsn", "Current WAL sequence number")
	dWalReceiveLsn     = desc("pg_wal_receive_lsn", "WAL sequence number that has been received and synced to disk by streaming replication")
	dWalReplyLsn       = desc("pg_wal_reply_lsn", "WAL sequence number that has been replayed during recovery")
)

type QueryKey struct {
	Query string
	DB    string
	User  string
}

func (k QueryKey) EqualByQueryPrefix(other QueryKey) bool {
	if k.User != other.User || k.DB != other.DB {
		return false
	}
	if strings.HasPrefix(k.Query, other.Query) {
		return true
	}
	return false
}

type ConnectionKey struct {
	QueryKey
	State         string
	WaitEventType string
}

type Collector struct {
	ctxCancelFunc context.CancelFunc

	db          *sql.DB
	origVersion string

	statsDumpInterval time.Duration
	ssCurr            *ssSnapshot
	ssPrev            *ssSnapshot
	saCurr            *saSnapshot
	saPrev            *saSnapshot
	settings          []Setting
	replicationStatus *replicationStatus
	scrapeErrors      map[string]bool

	lock   sync.RWMutex
	logger logger.Logger
}

func New(dsn string, scrapeInterval time.Duration, logger logger.Logger) (*Collector, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	c := &Collector{logger: logger, ctxCancelFunc: cancelFunc, scrapeErrors: map[string]bool{}}
	var err error
	c.db, err = sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	c.db.SetMaxOpenConns(1)
	if err := c.db.Ping(); err != nil {
		c.logger.Warning("probe failed:", err)
	}
	go func() {
		ticker := time.NewTicker(scrapeInterval)
		c.snapshot()
		for {
			select {
			case <-ticker.C:
				c.snapshot()
			case <-ctx.Done():
				c.logger.Info("stopping pg collector")
				return
			}
		}
	}()
	return c, nil
}

func (c *Collector) snapshot() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.scrapeErrors = map[string]bool{}

	c.origVersion = ""
	var version semver.Version
	var rawVersion string
	err := c.db.QueryRow(`SELECT setting FROM pg_settings WHERE name='server_version'`).Scan(&rawVersion)
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
	c.origVersion, version, err = parsePgVersion(rawVersion)
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}

	if c.settings, err = c.getSettings(); err != nil {
		c.scrapeErrors[err.Error()] = true
		c.logger.Warning(err)
	}

	if c.replicationStatus, err = c.getReplicationStatus(version); err != nil {
		c.scrapeErrors[err.Error()] = true
		c.logger.Warning(err)
	}

	querySizeLimit := 0
	for _, s := range c.settings {
		if s.Name == "track_activity_query_size" {
			switch s.Unit {
			case "B":
				querySizeLimit = int(s.Value)
			case "kB":
				querySizeLimit = int(s.Value) * 1024
			default:
				querySizeLimit = int(s.Value)
			}
			break
		}
	}
	if querySizeLimit == 0 || querySizeLimit > hardQuerySizeLimit {
		querySizeLimit = hardQuerySizeLimit
	}

	c.ssPrev = c.ssCurr
	c.saPrev = c.saCurr
	prevStatements := map[statementId]ssRow{}
	if c.ssPrev != nil {
		prevStatements = c.ssPrev.rows
	}
	c.ssCurr, err = c.getStatStatements(version, querySizeLimit, prevStatements)
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
	c.saCurr, err = c.getPgStatActivity(version, querySizeLimit)
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
}

func (c *Collector) summaries() (map[QueryKey]*QuerySummary, time.Duration) {
	if c.saCurr == nil || c.saPrev == nil || c.ssCurr == nil || c.ssPrev == nil {
		return nil, 0
	}
	res := map[QueryKey]*QuerySummary{}
	getOrCreateSummary := func(k QueryKey, searchByPrefix bool) *QuerySummary {
		s := res[k]
		if s == nil && searchByPrefix {
			for qk, ss := range res {
				if qk.EqualByQueryPrefix(k) {
					s = ss
					break
				}
			}
		}
		if s == nil {
			s = &QuerySummary{}
			res[k] = s
		}
		return s
	}

	for id, r := range c.ssCurr.rows {
		getOrCreateSummary(r.QueryKey(id), false).updateFromStatStatements(r, c.ssPrev.rows[id])
	}
	for _, conn := range c.saCurr.connections {
		getOrCreateSummary(conn.QueryKey(), true).updateFromStatActivity(c.saPrev.ts, c.saCurr.ts, conn)
	}
	for pid, prev := range c.saPrev.connections {
		if !prev.IsClientBackend() || prev.State.String != "active" {
			continue
		}
		curr, ok := c.saCurr.connections[pid]
		if ok && curr.State.String == "active" && curr.QueryStart.Time.Equal(prev.QueryStart.Time) { // still executing
			continue
		}
		// prev query finished
		getOrCreateSummary(prev.QueryKey(), true).correctFromPrevStatActivity(c.saPrev.ts, prev)
	}
	return res, c.ssCurr.ts.Sub(c.ssPrev.ts)
}

func (c *Collector) connectionMetrics(ch chan<- prometheus.Metric) {
	if c.saCurr == nil {
		return
	}
	byPid := map[int]QueryKey{}
	awaitingQueriesByBlockingPid := map[int]float64{}
	connectionsByKey := map[ConnectionKey]float64{}

	for pid, conn := range c.saCurr.connections {
		queryKey := conn.QueryKey()
		byPid[pid] = queryKey
		if conn.BlockingPid.Int32 > 0 {
			awaitingQueriesByBlockingPid[int(conn.BlockingPid.Int32)]++
		}
		key := ConnectionKey{
			QueryKey:      queryKey,
			State:         conn.State.String,
			WaitEventType: conn.WaitEventType.String,
		}
		connectionsByKey[key]++
	}

	for k, count := range connectionsByKey {
		ch <- gauge(dConnections, count, k.DB, k.User, k.State, k.WaitEventType, k.Query)
	}

	awaitingQueriesByBlockingQuery := map[QueryKey]float64{}
	for blockingPid, awaitingQueries := range awaitingQueriesByBlockingPid {
		blockingQuery, ok := byPid[blockingPid]
		if !ok {
			continue
		}
		awaitingQueriesByBlockingQuery[blockingQuery] += awaitingQueries
	}
	for blockingQuery, awaitingQueries := range awaitingQueriesByBlockingQuery {
		ch <- gauge(dLockAwaitingQueries, awaitingQueries, blockingQuery.DB, blockingQuery.User, blockingQuery.Query)
	}
}

func (c *Collector) queryMetrics(ch chan<- prometheus.Metric) {
	summaries, interval := c.summaries()
	if summaries == nil {
		c.logger.Warning("no summaries")
		return
	}

	latency := NewLatencySummary()
	queriesByDB := map[string]float64{}
	for k, summary := range summaries {
		latency.Add(summary.TotalTime, uint64(summary.Queries))
		queriesByDB[k.DB] += summary.Queries
	}
	for s, v := range latency.GetSummaries(50, 75, 95, 99) {
		ch <- gauge(dLatency, v, s)
	}

	for db, queries := range queriesByDB {
		ch <- gauge(dDbQueries, queries/interval.Seconds(), db)
	}

	for k, summary := range top(summaries, topQueriesN) {
		ch <- gauge(dTopQueryCalls, summary.Queries/interval.Seconds(), k.DB, k.User, k.Query)
		ch <- gauge(dTopQueryTime, summary.TotalTime/interval.Seconds(), k.DB, k.User, k.Query)
		ch <- gauge(dTopQueryIOTime, summary.IOTime/interval.Seconds(), k.DB, k.User, k.Query)
	}
}

func (c *Collector) Close() error {
	c.ctxCancelFunc()
	return c.db.Close()
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	now := time.Now()
	if err := c.db.Ping(); err != nil {
		c.logger.Warning("probe failed:", err)
		ch <- gauge(dUp, 0)
		ch <- gauge(dScrapeError, 1, err.Error(), "")
		return
	}
	ch <- gauge(dUp, 1)
	ch <- gauge(dProbe, time.Since(now).Seconds())
	if c.origVersion != "" {
		ch <- gauge(dInfo, 1, c.origVersion)
	}

	c.lock.RLock()
	defer c.lock.RUnlock()

	if len(c.scrapeErrors) > 0 {
		for e := range c.scrapeErrors {
			ch <- gauge(dScrapeError, 1, "", e)
		}
	} else {
		ch <- gauge(dScrapeError, 0, "", "")
	}

	c.connectionMetrics(ch)
	c.queryMetrics(ch)
	for _, s := range c.settings {
		ch <- gauge(dSettings, s.Value, s.Name, s.Unit)
	}

	if c.replicationStatus != nil {
		rs := c.replicationStatus
		if rs.isInRecovery {
			if rs.receiveLsn.Valid {
				ch <- counter(dWalReceiveLsn, float64(rs.receiveLsn.Int64))
			}
			if rs.replyLsn.Valid {
				ch <- counter(dWalReplyLsn, float64(rs.replyLsn.Int64))
			}
			isReplayPaused := 0.0
			if rs.isReplayPaused {
				isReplayPaused = 1.0
			}
			ch <- gauge(dWalReplayPaused, isReplayPaused)
			host, port, err := rs.primaryHostPort()
			if err != nil {
				c.logger.Warning(err)
			}
			ch <- gauge(dWalReceiverStatus, float64(rs.walReceiverStatus), host, port)
		} else {
			if rs.currentLsn.Valid {
				ch <- counter(dWalCurrentLsn, float64(rs.currentLsn.Int64))
			}
		}
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- dUp
	ch <- dProbe
	ch <- dScrapeError
	ch <- dInfo
	ch <- dConnections
	ch <- dLatency
	ch <- dLockAwaitingQueries
	ch <- dSettings
	ch <- dTopQueryCalls
	ch <- dTopQueryTime
	ch <- dTopQueryIOTime
	ch <- dDbQueries
	ch <- dWalReceiverStatus
	ch <- dWalReplayPaused
	ch <- dWalCurrentLsn
	ch <- dWalReceiveLsn
	ch <- dWalReplyLsn
}

func desc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(name, help, labels, nil)
}

func gauge(desc *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
}

func counter(desc *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.CounterValue, value, labels...)
}
