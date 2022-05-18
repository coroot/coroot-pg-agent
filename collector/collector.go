package collector

import (
	"database/sql"
	"fmt"
	"github.com/blang/semver"
	"github.com/coroot/logger"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

const topQueriesN = 20

var (
	dUp    = desc("pg_up", "Is the server reachable")
	dProbe = desc("pg_probe_seconds", "Empty query execution time")

	dInfo     = desc("pg_info", "Server info", "server_version")
	dSettings = desc("pg_setting", "Value of the pg_setting variable", "name", "unit")

	dConnections = desc("pg_connections", "Number of database connections", "db", "user", "state", "wait_event_type", "query")

	dLatency = desc("pg_latency_seconds", "Query execution time", "quantile")

	dDbQueries   = desc("pg_db_queries_total", "Total number of queries executed in the database", "db")
	dDbTotalTime = desc("pg_db_query_time_seconds_total", "Total time spent executing queries in the database", "db")

	dTopQueryCalls  = desc("pg_top_query_calls_total", "Total number of times the query was executed", "db", "user", "query")
	dTopQueryTime   = desc("pg_top_query_time_seconds_total", "Total time spent executing the query", "db", "user", "query")
	dTopQueryIOTime = desc("pg_top_query_io_time_seconds_total", "Total time the query spent awaiting IO", "db", "user", "query")

	dLockAwaitingQueries = desc("pg_lock_awaiting_queries", "Number of queries awaiting a lock", "db", "user", "blocking_query")
)

type Collector struct {
	db      *sql.DB
	version semver.Version

	prevStatements map[statementId]ssRow
	summaries      map[queryKey]*QuerySummary
	perDbSummaries map[string]*QuerySummary
	lock           sync.RWMutex

	prevStatActivityCallTs time.Time

	logger logger.Logger
}

func New(dsn string, logger logger.Logger) (*Collector, error) {
	c := &Collector{
		summaries:      map[queryKey]*QuerySummary{},
		perDbSummaries: map[string]*QuerySummary{},
		prevStatements: map[statementId]ssRow{},

		logger: logger,
	}
	var err error
	c.db, err = sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	c.db.SetMaxOpenConns(1)
	if err := c.db.Ping(); err != nil {
		c.logger.Warning("probe failed:", err)
	}
	return c, nil
}

func (c *Collector) Close() error {
	return c.db.Close()
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	now := time.Now()
	if err := c.db.Ping(); err != nil {
		c.logger.Warning("probe failed:", err)
		ch <- gauge(dUp, 0)
		return
	}
	ch <- gauge(dUp, 1)
	ch <- gauge(dProbe, time.Since(now).Seconds())
	if err := c.refreshVersion(); err != nil {
		c.logger.Error("failed to get postgres version:", err)
		return
	}
	ch <- gauge(dInfo, 1, c.version.String())

	c.collectPgSettingsMetrics(ch)

	c.lock.Lock()
	defer c.lock.Unlock()
	now = time.Now()
	for _, s := range c.summaries {
		s.resetTemporaryValues()
	}
	for _, s := range c.perDbSummaries {
		s.resetTemporaryValues()
	}

	if err := c.callPgStatStatements(now); err != nil {
		c.logger.Error("failed to query pg_stat_statements:", err)
		return
	}
	if err := c.callPgStatActivity(now, ch); err != nil {
		c.logger.Error("failed to query pg_stat_activity:", err)
		return
	}

	latencySummary := NewLatencySummary()
	for _, summary := range c.summaries {
		latencySummary.Add(summary.lastTotalTimeDelta, uint64(summary.lastCallsDelta))
	}
	for q, v := range latencySummary.GetQuantiles(0.5, 0.95, 0.99) {
		ch <- gauge(dLatency, v, fmt.Sprintf("%.2f", q))
	}

	for db, summary := range c.perDbSummaries {
		ch <- counter(dDbQueries, summary.calls.value(), db)
		ch <- counter(dDbTotalTime, summary.totalTime.value(), db)
	}

	for k, summary := range top(c.summaries, topQueriesN) {
		ch <- counter(dTopQueryCalls, summary.calls.value(), k.db, k.user, k.query)
		ch <- counter(dTopQueryTime, summary.totalTime.value(), k.db, k.user, k.query)
		ch <- counter(dTopQueryIOTime, summary.ioTime.value(), k.db, k.user, k.query)
	}

	for db, s := range c.perDbSummaries {
		if s.lastSeen.Before(now) {
			delete(c.perDbSummaries, db)
		}
	}
	for key, s := range c.summaries {
		if s.lastSeen.Before(now) {
			delete(c.summaries, key)
		}
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- dUp
	ch <- dProbe
	ch <- dInfo
	ch <- dConnections
	ch <- dLatency
	ch <- dLockAwaitingQueries
	ch <- dSettings
	ch <- dTopQueryCalls
	ch <- dTopQueryTime
	ch <- dTopQueryIOTime
	ch <- dDbQueries
	ch <- dDbTotalTime
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
