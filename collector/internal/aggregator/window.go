package aggregator

import (
	"sort"
	"sync"
	"time"

	"github.com/Dev66-66/LAB14/collector/internal/domain"
)

// Window представляет одно временное окно для агрегации событий.
type Window struct {
	start  time.Time
	end    time.Time
	events []domain.UserEvent
	mu     sync.Mutex
}

// NewWindow создаёт новое окно с заданным началом и длительностью.
func NewWindow(start time.Time, duration time.Duration) *Window {
	return &Window{
		start:  start,
		end:    start.Add(duration),
		events: make([]domain.UserEvent, 0, 256),
	}
}

// Add добавляет событие в окно (thread-safe).
func (w *Window) Add(event domain.UserEvent) {
	w.mu.Lock()
	w.events = append(w.events, event)
	w.mu.Unlock()
}

// Aggregate вычисляет AggregatedWindow из накопленных событий.
func (w *Window) Aggregate() domain.AggregatedWindow {
	w.mu.Lock()
	events := make([]domain.UserEvent, len(w.events))
	copy(events, w.events)
	w.mu.Unlock()

	result := domain.AggregatedWindow{
		WindowStart: w.start,
		WindowEnd:   w.end,
		TotalEvents: int64(len(events)),
		EventCounts: make(map[domain.EventType]int64),
	}

	if len(events) == 0 {
		return result
	}

	uniqueUsers := make(map[string]struct{})
	pageCounts := make(map[string]int64)
	var totalDuration float64

	for _, e := range events {
		result.EventCounts[e.EventType]++
		uniqueUsers[e.UserID] = struct{}{}
		pageCounts[e.Page]++
		totalDuration += e.Duration
	}

	result.UniqueUsers = int64(len(uniqueUsers))
	result.AvgDuration = totalDuration / float64(len(events))

	// Топ-5 страниц по убыванию количества событий.
	pages := make([]domain.PageStat, 0, len(pageCounts))
	for page, count := range pageCounts {
		pages = append(pages, domain.PageStat{Page: page, Count: count})
	}
	sort.Slice(pages, func(i, j int) bool {
		if pages[i].Count != pages[j].Count {
			return pages[i].Count > pages[j].Count
		}
		return pages[i].Page < pages[j].Page // стабильный порядок при равных счётчиках
	})
	if len(pages) > 5 {
		pages = pages[:5]
	}
	result.TopPages = pages

	return result
}
