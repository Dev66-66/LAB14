package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Dev66-66/LAB14/collector/internal/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer читает AggregatedWindow из Kafka для мониторинга (опционально).
type Consumer struct {
	client  *kgo.Client
	topic   string
	groupID string
}

// NewConsumer создаёт Kafka-консьюмера.
// GroupID берётся из env KAFKA_GROUP_ID.
func NewConsumer() (*Consumer, error) {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "user-events")
	groupID := getEnv("KAFKA_GROUP_ID", "analyzer-group")

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: NewConsumer: %w", err)
	}

	return &Consumer{client: client, topic: topic, groupID: groupID}, nil
}

// ReadEvents читает батч событий из топика с таймаутом ctx.
// Возвращает slice UserEvent.
func (c *Consumer) ReadEvents(ctx context.Context) ([]domain.UserEvent, error) {
	fetches := c.client.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		return nil, fmt.Errorf("kafka: ReadEvents: %v", errs[0].Err)
	}

	var events []domain.UserEvent
	fetches.EachRecord(func(rec *kgo.Record) {
		var e domain.UserEvent
		if err := json.Unmarshal(rec.Value, &e); err == nil {
			events = append(events, e)
		}
	})

	return events, nil
}

// Close закрывает консьюмера.
func (c *Consumer) Close() {
	c.client.Close()
}
