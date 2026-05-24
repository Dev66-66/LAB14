package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Dev66-66/LAB14/collector/internal/domain"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	collectorPrefix = "/collectors/"
	leaseTTL        = 30 // секунды
)

// Coordinator управляет распределением партиций Kafka между
// несколькими экземплярами сборщика через etcd.
type Coordinator struct {
	client      *clientv3.Client
	collectorID string
	leaseID     clientv3.LeaseID
}

// NewCoordinator создаёт координатора и подключается к etcd.
// ETCD_ENDPOINTS из env (дефолт "localhost:2379").
// collectorID генерируется как первые 8 символов uuid.
func NewCoordinator() (*Coordinator, error) {
	endpoints := strings.Split(getEnv("ETCD_ENDPOINTS", "localhost:2379"), ",")

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd: NewCoordinator: dial: %w", err)
	}

	return &Coordinator{
		client:      client,
		collectorID: uuid.New().String()[:8],
	}, nil
}

// Register регистрирует этот экземпляр сборщика в etcd с TTL 30s.
// Ключ: /collectors/{collectorID}
// Значение: JSON {id, registered_at, status}.
// Запускает горутину keepalive для обновления lease.
func (c *Coordinator) Register(ctx context.Context) error {
	lease, err := c.client.Grant(ctx, leaseTTL)
	if err != nil {
		return fmt.Errorf("etcd: Register: grant lease: %w", err)
	}
	c.leaseID = lease.ID

	value, err := json.Marshal(map[string]string{
		"id":            c.collectorID,
		"registered_at": time.Now().UTC().Format(time.RFC3339),
		"status":        "active",
	})
	if err != nil {
		return fmt.Errorf("etcd: Register: marshal: %w", err)
	}

	key := collectorPrefix + c.collectorID
	_, err = c.client.Put(ctx, key, string(value), clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("etcd: Register: put key %s: %w", key, err)
	}

	// Keepalive горутина обновляет lease до отмены ctx.
	keepAlive, err := c.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return fmt.Errorf("etcd: Register: keepalive: %w", err)
	}
	go func() {
		for range keepAlive {
			// Дренируем канал; etcd сам обновляет TTL.
		}
	}()

	return nil
}

// GetShards возвращает назначенные этому экземпляру партиции.
// Распределение: детерминированный round-robin по отсортированному списку коллекторов.
func (c *Coordinator) GetShards(ctx context.Context, totalPartitions int) (domain.ShardInfo, error) {
	resp, err := c.client.Get(ctx, collectorPrefix, clientv3.WithPrefix())
	if err != nil {
		return domain.ShardInfo{}, fmt.Errorf("etcd: GetShards: get: %w", err)
	}

	// 1. Собираем ID всех активных коллекторов.
	ids := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		id := strings.TrimPrefix(string(kv.Key), collectorPrefix)
		ids = append(ids, id)
	}

	// 2. Детерминированная сортировка.
	sort.Strings(ids)

	// 3. Индекс текущего коллектора.
	myIndex := -1
	for i, id := range ids {
		if id == c.collectorID {
			myIndex = i
			break
		}
	}
	if myIndex == -1 {
		return domain.ShardInfo{}, fmt.Errorf("etcd: GetShards: collector %s not found in registry", c.collectorID)
	}

	// 4. Round-robin распределение партиций [0..totalPartitions-1].
	total := len(ids)
	partitions := make([]int32, 0, totalPartitions/total+1)
	for p := range totalPartitions {
		if p%total == myIndex {
			partitions = append(partitions, int32(p))
		}
	}

	// 5. Возвращаем ShardInfo.
	return domain.ShardInfo{
		CollectorID: c.collectorID,
		Partitions:  partitions,
		AssignedAt:  time.Now().UTC(),
	}, nil
}

// WatchShards подписывается на изменения в /collectors/ и отправляет
// новый ShardInfo в канал при каждом изменении состава коллекторов.
func (c *Coordinator) WatchShards(ctx context.Context, totalPartitions int) <-chan domain.ShardInfo {
	ch := make(chan domain.ShardInfo, 1)

	go func() {
		defer close(ch)

		watchCh := c.client.Watch(ctx, collectorPrefix, clientv3.WithPrefix())
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-watchCh:
				if !ok {
					return
				}
				shard, err := c.GetShards(ctx, totalPartitions)
				if err != nil {
					continue
				}
				// Неблокирующая отправка: если получатель не читает,
				// старое значение вытесняется новым.
				select {
				case ch <- shard:
				case <-ch:
					ch <- shard
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return ch
}

// Deregister удаляет регистрацию из etcd при завершении работы.
func (c *Coordinator) Deregister(ctx context.Context) error {
	key := collectorPrefix + c.collectorID
	_, err := c.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("etcd: Deregister: delete key %s: %w", key, err)
	}
	// Отзываем lease — etcd автоматически удалит все связанные ключи.
	if c.leaseID != 0 {
		_, _ = c.client.Revoke(ctx, c.leaseID)
	}
	return nil
}

// Close закрывает соединение с etcd.
func (c *Coordinator) Close() error {
	return c.client.Close()
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
