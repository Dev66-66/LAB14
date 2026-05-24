package domain

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

// Emitter генерирует реалистичные пользовательские события.
type Emitter struct {
	users    []string // пул из 50 пользователей
	sessions []string // пул из 20 активных сессий
	pages    []string // список страниц приложения
	rng      *rand.Rand
}

var (
	devices   = []string{"desktop", "mobile", "tablet"}
	browsers  = []string{"chrome", "firefox", "safari", "edge"}
	countries = []string{"RU", "US", "DE", "FR", "CN", "BR", "IN", "GB"}
	eventTypes = []EventType{
		EventTypeClick,
		EventTypePageView,
		EventTypePurchase,
		EventTypeLogin,
		EventTypeLogout,
		EventTypeSearch,
	}
)

// NewEmitter создаёт эмулятор с предзаполненными пулами пользователей и сессий.
func NewEmitter() *Emitter {
	users := make([]string, 50)
	for i := range users {
		users[i] = fmt.Sprintf("user-%03d", i+1)
	}

	sessions := make([]string, 20)
	for i := range sessions {
		sessions[i] = uuid.NewString()
	}

	pages := []string{
		"/",
		"/catalog",
		"/product/123",
		"/product/456",
		"/product/789",
		"/cart",
		"/checkout",
		"/profile",
	}

	return &Emitter{
		users:    users,
		sessions: sessions,
		pages:    pages,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Generate генерирует случайное UserEvent со случайным типом, страницей,
// длительностью (10-5000ms), метаданными (device, browser, country).
func (e *Emitter) Generate() UserEvent {
	metadata := map[string]string{
		"device":  devices[e.rng.Intn(len(devices))],
		"browser": browsers[e.rng.Intn(len(browsers))],
		"country": countries[e.rng.Intn(len(countries))],
	}

	duration := 10 + e.rng.Float64()*4990 // [10, 5000) ms

	return NewUserEvent(
		e.users[e.rng.Intn(len(e.users))],
		e.sessions[e.rng.Intn(len(e.sessions))],
		eventTypes[e.rng.Intn(len(eventTypes))],
		e.pages[e.rng.Intn(len(e.pages))],
		duration,
		metadata,
	)
}

// GenerateBatch генерирует n событий.
func (e *Emitter) GenerateBatch(n int) []UserEvent {
	batch := make([]UserEvent, n)
	for i := range batch {
		batch[i] = e.Generate()
	}
	return batch
}
