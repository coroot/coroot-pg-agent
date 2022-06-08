package collector

import (
	"github.com/dustin/go-humanize"
	"sort"
)

type Batch struct {
	avg    float64
	events uint64
}

type LatencySummary struct {
	totalEvents uint64
	totalTime   float64
	batches     []Batch
}

func NewLatencySummary() *LatencySummary {
	return &LatencySummary{}
}

func (s *LatencySummary) Add(totalTime float64, numberOfEvents uint64) {
	if numberOfEvents == 0 {
		return
	}
	s.totalEvents += numberOfEvents
	s.totalTime += totalTime
	s.batches = append(s.batches, Batch{avg: totalTime / float64(numberOfEvents), events: numberOfEvents})
}

func (s *LatencySummary) GetCalls() float64 {
	return float64(s.totalEvents)
}

func (s *LatencySummary) GetTotalTime() float64 {
	return s.totalTime
}

func (s *LatencySummary) GetSummaries(percentiles ...float64) map[string]float64 {
	if len(s.batches) == 0 || len(percentiles) == 0 {
		return nil
	}
	sort.Slice(s.batches, func(i, j int) bool {
		return s.batches[i].avg < s.batches[j].avg
	})
	res := map[string]float64{
		"avg": s.totalTime / float64(s.totalEvents),
		"max": s.batches[len(s.batches)-1].avg,
	}

	for _, p := range percentiles {
		q := p / 100
		if q <= 0 || q > 1 {
			return nil
		}
		idx := uint64(float64(s.totalEvents) * q)
		var counter uint64
		for _, b := range s.batches {
			counter += b.events
			if counter >= idx {
				res["p"+humanize.Ftoa(p)] = b.avg
				break
			}
		}
	}
	return res
}
