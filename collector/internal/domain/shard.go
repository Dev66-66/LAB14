package domain

import "time"

// ShardInfo описывает шард (партицию Kafka), назначенный конкретному экземпляру сборщика.
type ShardInfo struct {
	CollectorID string    `json:"collector_id"`
	Partitions  []int32   `json:"partitions"`
	AssignedAt  time.Time `json:"assigned_at"`
}
