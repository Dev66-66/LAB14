package domain

import (
	"time"

	"github.com/google/uuid"
)

// EventType представляет тип пользовательского события.
type EventType string

const (
	EventTypeClick    EventType = "click"
	EventTypePageView EventType = "page_view"
	EventTypePurchase EventType = "purchase"
	EventTypeLogin    EventType = "login"
	EventTypeLogout   EventType = "logout"
	EventTypeSearch   EventType = "search"
)

// UserEvent представляет одно пользовательское событие из веб-приложения.
type UserEvent struct {
	ID        string            `json:"id"`
	UserID    string            `json:"user_id"`
	SessionID string            `json:"session_id"`
	EventType EventType         `json:"event_type"`
	Page      string            `json:"page"`
	Duration  float64           `json:"duration_ms"`
	Metadata  map[string]string `json:"metadata"`
	Timestamp time.Time         `json:"timestamp"`
}

// AggregatedWindow представляет агрегированное окно событий (tumbling window).
type AggregatedWindow struct {
	WindowStart time.Time           `json:"window_start"`
	WindowEnd   time.Time           `json:"window_end"`
	TotalEvents int64               `json:"total_events"`
	EventCounts map[EventType]int64 `json:"event_counts"`
	UniqueUsers int64               `json:"unique_users"`
	AvgDuration float64             `json:"avg_duration_ms"`
	TopPages    []PageStat          `json:"top_pages"`
}

// PageStat представляет статистику по одной странице.
type PageStat struct {
	Page  string `json:"page"`
	Count int64  `json:"count"`
}

// NewUserEvent создаёт новое пользовательское событие с заполненным ID и Timestamp.
func NewUserEvent(
	userID, sessionID string,
	eventType EventType,
	page string,
	duration float64,
	metadata map[string]string,
) UserEvent {
	return UserEvent{
		ID:        uuid.NewString(),
		UserID:    userID,
		SessionID: sessionID,
		EventType: eventType,
		Page:      page,
		Duration:  duration,
		Metadata:  metadata,
		Timestamp: time.Now().UTC(),
	}
}
