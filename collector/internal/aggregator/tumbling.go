package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Dev66-66/LAB14/collector/internal/domain"
)

// TumblingAggregator управляет серией tumbling windows.
type TumblingAggregator struct {
	windowSize time.Duration
	current    *Window
	output     chan domain.AggregatedWindow
	mu         sync.Mutex
}

// NewTumblingAggregator создаёт агрегатор.
// windowSize берётся из env WINDOW_SIZE_SECONDS (дефолт 10).
func NewTumblingAggregator() *TumblingAggregator {
	secs := getEnvInt("WINDOW_SIZE_SECONDS", 10)
	return &TumblingAggregator{
		windowSize: time.Duration(secs) * time.Second,
		current:    NewWindow(time.Now().UTC(), time.Duration(secs)*time.Second),
		output:     make(chan domain.AggregatedWindow, 16),
	}
}

// Start запускает фоновую горутину смены окон.
// Каждые windowSize секунд: заменяет текущее окно, агрегирует старое,
// отправляет результат в output и логирует в JSON.
func (a *TumblingAggregator) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(a.windowSize)
		defer ticker.Stop()
		defer close(a.output)

		for {
			select {
			case <-ctx.Done():
				// Финальная агрегация остатка.
				a.flush()
				return
			case t := <-ticker.C:
				a.rotate(t)
			}
		}
	}()
}

// Add добавляет событие в текущее окно (thread-safe).
func (a *TumblingAggregator) Add(event domain.UserEvent) {
	a.mu.Lock()
	w := a.current
	a.mu.Unlock()
	w.Add(event)
}

// Output возвращает канал с готовыми агрегатами.
func (a *TumblingAggregator) Output() <-chan domain.AggregatedWindow {
	return a.output
}

// rotate атомарно меняет текущее окно и публикует агрегат старого.
func (a *TumblingAggregator) rotate(now time.Time) {
	newWindow := NewWindow(now.UTC(), a.windowSize)

	a.mu.Lock()
	old := a.current
	a.current = newWindow
	a.mu.Unlock()

	agg := old.Aggregate()
	a.publish(agg)
}

// flush агрегирует текущее окно без замены (используется при остановке).
func (a *TumblingAggregator) flush() {
	a.mu.Lock()
	w := a.current
	a.mu.Unlock()

	agg := w.Aggregate()
	if agg.TotalEvents > 0 {
		a.publish(agg)
	}
}

// publish отправляет агрегат в канал и логирует его.
func (a *TumblingAggregator) publish(agg domain.AggregatedWindow) {
	logAggregate(agg)
	select {
	case a.output <- agg:
	default:
		// Канал переполнен — логируем потерю, чтобы она не была невидимой.
		rec := map[string]any{
			"level":        "WARN",
			"ts":           time.Now().UTC().Format(time.RFC3339),
			"msg":          "window dropped: output buffer full",
			"total_events": agg.TotalEvents,
			"window_start": agg.WindowStart.Format(time.RFC3339),
		}
		b, _ := json.Marshal(rec)
		fmt.Println(string(b))
	}
}

// logAggregate выводит агрегат в JSON-формат в stdout.
func logAggregate(agg domain.AggregatedWindow) {
	rec := map[string]any{
		"level":        "INFO",
		"ts":           time.Now().UTC().Format(time.RFC3339),
		"msg":          "window aggregated",
		"window_start": agg.WindowStart.Format(time.RFC3339),
		"window_end":   agg.WindowEnd.Format(time.RFC3339),
		"total_events": agg.TotalEvents,
		"unique_users": agg.UniqueUsers,
		"avg_duration": fmt.Sprintf("%.2f", agg.AvgDuration),
	}
	b, _ := json.Marshal(rec)
	fmt.Println(string(b))
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
