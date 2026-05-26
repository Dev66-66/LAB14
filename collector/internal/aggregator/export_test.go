package aggregator

import (
	"time"

	"github.com/Dev66-66/LAB14/collector/internal/domain"
)

// NewTumblingAggregatorWithSize is a test-only constructor that accepts
// windowSize directly, bypassing the env-based NewTumblingAggregator.
// Used by the external aggregator_test package via the standard Go
// export_test.go pattern.
func NewTumblingAggregatorWithSize(size time.Duration) *TumblingAggregator {
	return &TumblingAggregator{
		windowSize: size,
		current:    NewWindow(time.Now().UTC(), size),
		output:     make(chan domain.AggregatedWindow, 16),
	}
}
