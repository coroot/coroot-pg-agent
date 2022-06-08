package collector

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLatencySummary(t *testing.T) {
	s := NewLatencySummary()
	s.Add(0, 0)
	assert.Nil(t, s.GetSummaries(0.5))

	s.Add(0.1*50, 50)
	s.Add(0.2*40, 40)
	s.Add(0.3*10, 10)

	assert.Nil(t, s.GetSummaries())
	assert.Nil(t, s.GetSummaries(-1))
	assert.Nil(t, s.GetSummaries(200))

	assert.Equal(t,
		map[string]float64{
			"avg": 0.16,
			"max": 0.3,
			"p50": 0.1,
			"p90": 0.2,
			"p95": 0.3,
		},
		s.GetSummaries(50, 90, 95),
	)
}
