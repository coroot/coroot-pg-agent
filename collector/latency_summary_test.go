package collector

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLatencySummary(t *testing.T) {
	s := NewLatencySummary()
	s.Add(0, 0)
	assert.Nil(t, s.GetQuantiles(0.5))

	s.Add(0.1*50, 50)
	s.Add(0.2*40, 40)
	s.Add(0.3*10, 10)

	assert.Nil(t, s.GetQuantiles())
	assert.Nil(t, s.GetQuantiles(-1))
	assert.Nil(t, s.GetQuantiles(2))

	assert.Equal(t,
		map[float64]float64{
			0.5:  0.1,
			0.9:  0.2,
			0.95: 0.3,
		},
		s.GetQuantiles(0.5, 0.90, 0.95),
	)
}
