package collector

import (
	"sort"
	"time"
)

type QuerySummary struct {
	Queries   float64
	TotalTime float64
	IOTime    float64
}

func (s *QuerySummary) updateFromStatActivity(prevTs, ts time.Time, conn Connection) {
	if conn.State.String != "active" {
		return
	}
	if !conn.QueryStart.Valid {
		return
	}
	duration := ts.Sub(conn.QueryStart.Time)
	if duration < 0 {
		return
	}
	interval := ts.Sub(prevTs)
	if duration > interval {
		duration = interval
	}
	if conn.IsClientBackend() {
		s.Queries += 1
		s.TotalTime += duration.Seconds()
	}
	if conn.WaitEventType.String == "IO" {
		s.IOTime += duration.Seconds()
	}
}

func (s *QuerySummary) correctFromPrevStatActivity(ts time.Time, conn Connection) {
	if !conn.QueryStart.Valid {
		return
	}
	duration := ts.Sub(conn.QueryStart.Time).Seconds()
	if duration < 0 {
		return
	}
	if conn.IsClientBackend() && s.Queries > 0 && s.TotalTime > duration {
		s.Queries -= 1
		s.TotalTime -= duration
	}
	if conn.WaitEventType.String == "IO" && s.IOTime > duration {
		s.IOTime -= duration
	}
}

func (s *QuerySummary) updateFromStatStatements(cur, prev ssRow) {
	callsDelta := float64(cur.calls.Int64 - prev.calls.Int64)
	totalTimeDelta := (cur.totalTime.Float64 - prev.totalTime.Float64) / 1000
	ioTimeDelta := (cur.ioTime.Float64 - prev.ioTime.Float64) / 1000
	if totalTimeDelta < 0 || callsDelta < 0 || ioTimeDelta < 0 {
		return
	}
	s.Queries += callsDelta
	s.TotalTime += totalTimeDelta
	s.IOTime += ioTimeDelta
}

type summaryWithKey struct {
	key     QueryKey
	last    float64
	summary *QuerySummary
}

func top(all map[QueryKey]*QuerySummary, n int) map[QueryKey]*QuerySummary {
	withKeys := make([]summaryWithKey, 0, len(all))
	for k, s := range all {
		withKeys = append(withKeys, summaryWithKey{key: k, summary: s, last: s.TotalTime})
	}
	sort.Slice(withKeys, func(i, j int) bool {
		return withKeys[i].last > withKeys[j].last
	})
	if n > len(withKeys) {
		n = len(withKeys)
	}
	res := make(map[QueryKey]*QuerySummary, n)
	for _, i := range withKeys[:n] {
		res[i.key] = i.summary
	}
	return res
}
