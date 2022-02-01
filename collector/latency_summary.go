package collector

import (
	"sort"
)

type Batch struct {
	avg    float64
	events uint64
}

type LatencySummary struct {
	totalEvents uint64
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
	s.batches = append(s.batches, Batch{avg: totalTime / float64(numberOfEvents), events: numberOfEvents})
}

func (s *LatencySummary) GetQuantiles(quantiles ...float64) map[float64]float64 {
	if len(s.batches) == 0 || len(quantiles) == 0 {
		return nil
	}
	sort.Slice(s.batches, func(i, j int) bool {
		return s.batches[i].avg < s.batches[j].avg
	})
	res := map[float64]float64{}

	for _, q := range quantiles {
		if q <= 0 || q > 1 {
			return nil
		}
		idx := uint64(float64(s.totalEvents) * q)
		var counter uint64
		for _, b := range s.batches {
			counter += b.events
			if counter >= idx {
				res[q] = b.avg
				break
			}
		}
	}
	return res
}
