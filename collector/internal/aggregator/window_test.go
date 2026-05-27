package aggregator_test

import (
	"math"
	"testing"
	"time"

	"github.com/Dev66-66/LAB14/collector/internal/aggregator"
	"github.com/Dev66-66/LAB14/collector/internal/domain"
)

// event creates a minimal UserEvent for use in aggregator tests.
func event(userID, page string, et domain.EventType, duration float64) domain.UserEvent {
	return domain.NewUserEvent(userID, "session-1", et, page, duration, nil)
}

func TestWindow_Add_IncreasesCount(t *testing.T) {
	w := aggregator.NewWindow(time.Now(), 10*time.Second)
	for range 3 {
		w.Add(event("user-1", "/", domain.EventTypeClick, 100))
	}
	if got := w.Aggregate().TotalEvents; got != 3 {
		t.Errorf("TotalEvents: want 3, got %d", got)
	}
}

func TestWindow_Aggregate_UniqueUsers(t *testing.T) {
	w := aggregator.NewWindow(time.Now(), 10*time.Second)
	// user-1 ×2, user-2 ×2, user-3 ×1 → 3 unique users.
	for range 2 {
		w.Add(event("user-1", "/", domain.EventTypeClick, 100))
	}
	for range 2 {
		w.Add(event("user-2", "/", domain.EventTypeClick, 100))
	}
	w.Add(event("user-3", "/", domain.EventTypeClick, 100))

	if got := w.Aggregate().UniqueUsers; got != 3 {
		t.Errorf("UniqueUsers: want 3, got %d", got)
	}
}

func TestWindow_Aggregate_EventCounts(t *testing.T) {
	w := aggregator.NewWindow(time.Now(), 10*time.Second)
	for range 2 {
		w.Add(event("user-1", "/", domain.EventTypeClick, 100))
	}
	for range 3 {
		w.Add(event("user-1", "/", domain.EventTypePageView, 100))
	}
	w.Add(event("user-1", "/", domain.EventTypePurchase, 100))

	agg := w.Aggregate()
	if got := agg.EventCounts[domain.EventTypeClick]; got != 2 {
		t.Errorf("EventCounts[click]: want 2, got %d", got)
	}
	if got := agg.EventCounts[domain.EventTypePageView]; got != 3 {
		t.Errorf("EventCounts[page_view]: want 3, got %d", got)
	}
}

func TestWindow_Aggregate_TopPages(t *testing.T) {
	w := aggregator.NewWindow(time.Now(), 10*time.Second)
	for range 5 {
		w.Add(event("user-1", "/", domain.EventTypePageView, 100))
	}
	for range 3 {
		w.Add(event("user-1", "/catalog", domain.EventTypePageView, 100))
	}
	w.Add(event("user-1", "/cart", domain.EventTypePageView, 100))

	agg := w.Aggregate()
	if len(agg.TopPages) == 0 {
		t.Fatal("TopPages is empty")
	}
	if agg.TopPages[0].Page != "/" {
		t.Errorf("TopPages[0].Page: want %q, got %q", "/", agg.TopPages[0].Page)
	}
	if agg.TopPages[0].Count != 5 {
		t.Errorf("TopPages[0].Count: want 5, got %d", agg.TopPages[0].Count)
	}
}

func TestWindow_Aggregate_AvgDuration(t *testing.T) {
	w := aggregator.NewWindow(time.Now(), 10*time.Second)
	for _, d := range []float64{100, 200, 300} {
		w.Add(event("user-1", "/", domain.EventTypeClick, d))
	}
	if got := w.Aggregate().AvgDuration; math.Abs(got-200.0) > 0.001 {
		t.Errorf("AvgDuration: want 200.0, got %.4f", got)
	}
}

func TestTumblingAggregator_OutputsWindow(t *testing.T) {
	ta := aggregator.NewTumblingAggregatorWithSize(100 * time.Millisecond)
	ta.Start(t.Context())

	for range 10 {
		ta.Add(event("user-1", "/", domain.EventTypeClick, 100))
	}

	time.Sleep(150 * time.Millisecond)

	select {
	case result, ok := <-ta.Output():
		if !ok {
			t.Fatal("output channel closed unexpectedly")
		}
		if result.TotalEvents < 10 {
			t.Errorf("TotalEvents: want >=10, got %d", result.TotalEvents)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("no result in output channel after window rotation")
	}
}
