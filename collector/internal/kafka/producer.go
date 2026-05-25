package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/Dev66-66/LAB14/collector/internal/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer отвечает за отправку UserEvent в Kafka-топик.
type Producer struct {
	client *kgo.Client
	topic  string
}

// NewProducer создаёт нового Kafka-продюсера.
// Адрес брокера берётся из env KAFKA_BROKERS (дефолт "localhost:9092").
// Топик берётся из env KAFKA_TOPIC (дефолт "user-events").
func NewProducer() (*Producer, error) {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "user-events")

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.MaxBufferedRecords(1000),
		kgo.RecordRetries(3),
		kgo.RetryBackoffFn(func(attempt int) time.Duration {
			// Экспоненциальный backoff: 100ms * 2^attempt, максимум 5s.
			d := time.Duration(float64(100*time.Millisecond) * math.Pow(2, float64(attempt)))
			if d > 5*time.Second {
				return 5 * time.Second
			}
			return d
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: NewProducer: %w", err)
	}

	return &Producer{client: client, topic: topic}, nil
}

// SendEvent сериализует событие в JSON и отправляет в Kafka.
// Ключ сообщения = event.UserID (для партиционирования по пользователю).
func (p *Producer) SendEvent(ctx context.Context, event domain.UserEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("kafka: SendEvent: marshal: %w", err)
	}

	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(event.UserID),
		Value: payload,
	}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("kafka: SendEvent: produce: %w", err)
	}
	return nil
}

// SendBatch отправляет пакет событий, используя ProduceSync для атомарности.
func (p *Producer) SendBatch(ctx context.Context, events []domain.UserEvent) error {
	records := make([]*kgo.Record, 0, len(events))
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("kafka: SendBatch: marshal event %s: %w", e.ID, err)
		}
		records = append(records, &kgo.Record{
			Topic: p.topic,
			Key:   []byte(e.UserID),
			Value: payload,
		})
	}

	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("kafka: SendBatch: produce: %w", err)
	}
	return nil
}

// Flush принудительно сбрасывает все буферизованные записи брокеру.
// Вызывать перед Close() при graceful shutdown.
func (p *Producer) Flush(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("kafka: Flush: %w", err)
	}
	return nil
}

// Close корректно закрывает соединение с брокером.
func (p *Producer) Close() {
	p.client.Close()
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
