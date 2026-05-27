package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/Dev66-66/LAB14/collector/internal/aggregator"
	arrow_flight "github.com/Dev66-66/LAB14/collector/internal/arrow"
	"github.com/Dev66-66/LAB14/collector/internal/domain"
	"github.com/Dev66-66/LAB14/collector/internal/kafka"
	"github.com/joho/godotenv"
)

func main() {
	// 1. Загрузка .env; ошибка игнорируется (файл может отсутствовать в k8s).
	_ = godotenv.Load()

	// 2. Создание компонентов конвейера.
	producer, err := kafka.NewProducer()
	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}

	consumer, err := kafka.NewConsumer()
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}

	emitter := domain.NewEmitter()
	agg := aggregator.NewTumblingAggregator()
	flightSrv := arrow_flight.NewFlightServer()

	// 3. Контекст, отменяемый по SIGINT / SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 4. HTTP-сервер для Kubernetes liveness / readiness probe.
	httpSrv := startHTTPServer(":8080")

	var wg sync.WaitGroup

	// 5. Arrow Flight RPC-сервер (Python-клиент читает агрегаты отсюда).
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := flightSrv.Serve(ctx); err != nil && ctx.Err() == nil {
			logJSONf("ERROR", "arrow flight server: "+err.Error())
		}
	}()

	// 6. Tumbling window агрегатор (ротирует окна каждые WINDOW_SIZE_SECONDS).
	agg.Start(ctx)

	// 7. Перекладывает готовые окна из агрегатора в буфер Arrow Flight сервера.
	go func() {
		for w := range agg.Output() {
			flightSrv.AddWindow(w)
		}
	}()

	// 8. Kafka Consumer → Aggregator: читает события из Redpanda и агрегирует.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			events, err := consumer.ReadEvents(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logJSONf("WARN", "consumer read: "+err.Error())
				continue
			}
			for _, e := range events {
				agg.Add(e)
			}
		}
	}()

	// 9. Emitter воркеры → Kafka Producer.
	numWorkers := getEnvInt("KAFKA_PARTITIONS", 3)
	batchSize := getEnvInt("BATCH_SIZE", 100)

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, workerID, batchSize, producer, emitter)
		}(i)
	}

	// Ждём сигнала завершения.
	<-ctx.Done()

	// 10. Graceful shutdown.
	logJSON("INFO", "shutdown signal received, waiting for workers")
	wg.Wait()

	consumer.Close()

	flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer flushCancel()
	if err := producer.Flush(flushCtx); err != nil {
		logJSON("WARN", "producer flush: "+err.Error())
	}
	producer.Close()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)

	logJSON("INFO", "graceful shutdown complete")
}

func runWorker(ctx context.Context, workerID, batchSize int, producer *kafka.Producer, emitter *domain.Emitter) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batch := emitter.GenerateBatch(batchSize)
		if err := producer.SendBatch(ctx, batch); err != nil {
			if ctx.Err() != nil {
				return
			}
			logJSONf("ERROR", fmt.Sprintf("worker %d: send batch failed: %v", workerID, err))
			continue
		}

		logBatchSent(workerID, batchSize)

		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// startHTTPServer запускает HTTP-сервер с /health и /ready эндпоинтами.
func startHTTPServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http server error: %v", err)
		}
	}()
	return srv
}

// logBatchSent выводит JSON-лог об отправленном батче.
func logBatchSent(workerID, count int) {
	rec := map[string]any{
		"level":  "INFO",
		"ts":     time.Now().UTC().Format(time.RFC3339),
		"msg":    "batch sent",
		"count":  count,
		"worker": workerID,
	}
	b, _ := json.Marshal(rec)
	fmt.Println(string(b))
}

func logJSON(level, msg string) {
	rec := map[string]any{
		"level": level,
		"ts":    time.Now().UTC().Format(time.RFC3339),
		"msg":   msg,
	}
	b, _ := json.Marshal(rec)
	fmt.Println(string(b))
}

func logJSONf(level, msg string) { logJSON(level, msg) }

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
