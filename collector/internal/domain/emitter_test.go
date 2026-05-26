package domain_test

import (
	"strings"
	"testing"

	"github.com/Dev66-66/LAB14/collector/internal/domain"
)

var validEventTypes = map[domain.EventType]bool{
	domain.EventTypeClick:    true,
	domain.EventTypePageView: true,
	domain.EventTypePurchase: true,
	domain.EventTypeLogin:    true,
	domain.EventTypeLogout:   true,
	domain.EventTypeSearch:   true,
}

func TestEmitter_Generate_ValidEvent(t *testing.T) {
	e := domain.NewEmitter()
	ev := e.Generate()

	if ev.ID == "" {
		t.Error("ID must not be empty")
	}
	if ev.UserID == "" {
		t.Error("UserID must not be empty")
	}
	if ev.SessionID == "" {
		t.Error("SessionID must not be empty")
	}
	if !validEventTypes[ev.EventType] {
		t.Errorf("unexpected EventType: %q", ev.EventType)
	}
	if ev.Duration < 0 {
		t.Errorf("Duration must be >= 0, got %f", ev.Duration)
	}
}

func TestEmitter_GenerateBatch_CorrectCount(t *testing.T) {
	e := domain.NewEmitter()
	batch := e.GenerateBatch(50)
	if len(batch) != 50 {
		t.Errorf("GenerateBatch(50): want 50 events, got %d", len(batch))
	}
}

func TestEmitter_Generate_PageStartsWithSlash(t *testing.T) {
	e := domain.NewEmitter()
	for i := range 100 {
		ev := e.Generate()
		if !strings.HasPrefix(ev.Page, "/") {
			t.Errorf("Page must start with '/', got %q (iteration %d)", ev.Page, i)
		}
	}
}
