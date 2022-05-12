package collector

import (
	"sort"
	"time"
)

type multiplexedCounter struct {
	ssValue float64
	saValue float64
	max     float64
}

func (c *multiplexedCounter) value() float64 {
	if sum := c.ssValue + c.saValue; sum > c.max {
		c.max = sum
	}
	return c.max
}

type QuerySummary struct {
	calls     *multiplexedCounter
	totalTime *multiplexedCounter
	ioTime    *multiplexedCounter

	lastSeen           time.Time
	lastTotalTimeDelta float64
	lastCallsDelta     float64
}

func newQuerySummary() *QuerySummary {
	return &QuerySummary{
		calls:     &multiplexedCounter{},
		totalTime: &multiplexedCounter{},
		ioTime:    &multiplexedCounter{},
	}
}

func (s *QuerySummary) resetTemporaryValues() {
	s.lastTotalTimeDelta = 0
	s.lastCallsDelta = 0
	s.calls.saValue = 0
	s.totalTime.saValue = 0
	s.ioTime.saValue = 0
}

func (s *QuerySummary) updateFromStatActivity(timestamp time.Time, sess session) {
	s.lastSeen = timestamp
	if sess.backendType == "" || sess.backendType == "client backend" {
		s.calls.saValue++
		s.totalTime.saValue += sess.queryDuration
		s.lastTotalTimeDelta += sess.queryDuration
		s.lastCallsDelta += 1
	}
	if sess.waitEventType == "IO" {
		s.ioTime.saValue += sess.queryDuration
	}
}

func (s *QuerySummary) updateFromStatStatements(timestamp time.Time, cur, prev ssRow) {
	s.lastSeen = timestamp
	totalTimeDelta := (cur.totalTime.Float64 - prev.totalTime.Float64) / 1000
	callsDelta := float64(cur.calls.Int64 - prev.calls.Int64)
	if totalTimeDelta < 0 || callsDelta < 0 {
		return
	}
	s.lastTotalTimeDelta += totalTimeDelta
	s.lastCallsDelta += callsDelta
	s.totalTime.ssValue += totalTimeDelta
	s.calls.ssValue += callsDelta
	s.ioTime.ssValue += (cur.ioTime.Float64 - prev.ioTime.Float64) / 1000
}

type summaryWithKey struct {
	key     queryKey
	last    float64
	summary *QuerySummary
}

func top(all map[queryKey]*QuerySummary, n int) map[queryKey]*QuerySummary {
	withKeys := make([]summaryWithKey, 0, len(all))
	for k, s := range all {
		withKeys = append(withKeys, summaryWithKey{key: k, summary: s, last: s.lastTotalTimeDelta})
	}
	sort.Slice(withKeys, func(i, j int) bool {
		return withKeys[i].last > withKeys[j].last
	})
	if n > len(withKeys) {
		n = len(withKeys)
	}
	res := make(map[queryKey]*QuerySummary, n)
	for _, i := range withKeys[:n] {
		res[i.key] = i.summary
	}
	return res
}
