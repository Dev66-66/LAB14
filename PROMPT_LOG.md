# Prompt Log — Лабораторная работа №14

## Журнал выполненных промптов

## Промпт 0.1 — Инициализация репозитория

**Дата:** 2026-05-24

**Промпт:** Инициализация Git-репозитория. Создание файловой структуры
LAB14/, .gitignore, .env.example, README.md, PROMPT_LOG.md.

**Результат:**
- Создана файловая структура проекта с директориями:
  collector/cmd/, collector/internal/{domain,kafka,aggregator,arrow,etcd}/,
  validator/, analyzer/{domain,adapters,usecases,presentation}/,
  dashboard/, k8s/, docker/, data/, docs/
- .gitignore покрывает Go, Python, Rust, IDE, секреты, данные
- .env.example содержит 16 переменных окружения
- README.md содержит заголовок и данные студента
- PROMPT_LOG.md инициализирован
- Репозиторий подключён к https://github.com/Dev66-66/LAB14

**Git:** `ccb54a4` — `chore: init repo structure, .gitignore, .env.example`

---

## Промпт 1.1 — Docker Compose: Redpanda и etcd

**Дата:** 2026-05-24

**Промпт:** Создание инфраструктурного слоя: Redpanda (Kafka-совместимый
брокер) с инициализацией топика user-events (3 партиции, retention 1h)
и etcd для координации сборщиков.

**Результат:**
- docker-compose.yml: сервисы redpanda, redpanda-init, etcd
- redpanda: dev-container режим, 1GB RAM, healthcheck через rpk
- redpanda-init: создаёт топик user-events автоматически при старте
- etcd: bitnami/etcd:3.5, healthcheck через etcdctl
- Все сервисы в сети pipeline-network
- Named volumes: redpanda-data, etcd-data, parquet-data

**Git:** `chore: add Docker Compose with Redpanda and etcd`

---

## Промпт 1.2 — Kubernetes манифесты

**Дата:** 2026-05-24

**Промпт:** Создание Kubernetes манифестов для развёртывания конвейера.

**Результат:**
- k8s/namespace.yaml: Namespace lab14
- k8s/configmap.yaml: ConfigMap с переменными окружения
- k8s/collector-deployment.yaml: Deployment коллектора (2 реплики)
- k8s/collector-service.yaml: ClusterIP сервис коллектора
- k8s/hpa.yaml: HPA (1-5 реплик, CPU 70%, Kafka lag 100)
- k8s/analyzer-deployment.yaml: Deployment анализатора
- k8s/dashboard-deployment.yaml: Deployment дашборда
- k8s/dashboard-service.yaml: NodePort сервис дашборда (30501)

**Git:** `chore: add Kubernetes manifests (Deployment, Service, HPA, ConfigMap)`

---

## Промпт 2.1 — Go: domain модели

**Дата:** 2026-05-24

**Промпт:** Инициализация Go-модуля и создание domain-моделей:
UserEvent, AggregatedWindow, PageStat, ShardInfo.

**Результат:**
- go.mod инициализирован (github.com/Dev66-66/LAB14/collector)
- Зависимости: franz-go, etcd/client/v3, arrow/go/v15, uuid, godotenv, grpc
- domain/event.go: EventType (6 констант), UserEvent, AggregatedWindow,
  PageStat с JSON-тегами, конструктор NewUserEvent
- domain/shard.go: ShardInfo

**Git:** `feat: add UserEvent and AggregatedWindow domain models`

---

## Промпт 2.2 — Go: Kafka адаптер

**Дата:** 2026-05-24

**Промпт:** Реализация Kafka-адаптера: Producer с пакетной отправкой
и экспоненциальным backoff, Consumer для чтения событий.

**Результат:**
- kafka/producer.go: Producer, NewProducer, SendEvent, SendBatch, Close
  Ключ = UserID для партиционирования, MaxBufferedRecords(1000),
  RetryBackoffFn с экспоненциальным backoff
- kafka/consumer.go: Consumer, NewConsumer, ReadEvents, Close
  GroupID из env KAFKA_GROUP_ID

**Git:** `feat: add Kafka producer adapter (Redpanda)`

---

## Промпт 2.3 — Go: эмулятор событий

**Дата:** 2026-05-24

**Промпт:** Реализация эмулятора пользовательских событий с горутинами
и graceful shutdown.

**Результат:**
- domain/emitter.go: Emitter с пулом 50 пользователей, 20 сессий,
  Generate/GenerateBatch, случайные метаданные (device/browser/country)
- cmd/main.go: N горутин-воркеров (N=KAFKA_PARTITIONS), пакетная
  отправка (BATCH_SIZE=100), JSON-логирование, graceful shutdown
  через WaitGroup, HTTP /health + /ready на :8080

**Git:** `feat: add event emitter with goroutines and graceful shutdown`

---

## Промпт 2.4 — Go: etcd координатор шардов

**Дата:** 2026-05-24

**Промпт:** Реализация координатора шардов через etcd для распределения
партиций Kafka между несколькими экземплярами сборщика.

**Результат:**
- etcd/coordinator.go: Coordinator с lease TTL 30s, keepalive горутиной
- Register: ключ /collectors/{id}, JSON-значение, автообновление lease
- GetShards: детерминированное round-robin распределение партиций
- WatchShards: реактивный канал при изменении состава коллекторов
- Deregister: очистка при graceful shutdown

**Git:** `feat: add etcd shard coordinator`

---

## Промпт 2.5 — Go: tumbling window агрегатор

**Дата:** 2026-05-25

**Промпт:** Реализация агрегатора с tumbling window для предагрегации
событий на стороне Go перед отправкой в Python.

**Результат:**
- aggregator/window.go: Window с thread-safe Add, Aggregate (топ-5 страниц,
  уникальные пользователи, EventCounts, AvgDuration)
- aggregator/tumbling.go: TumblingAggregator, фоновая горутина смены окон,
  windowSize из env WINDOW_SIZE_SECONDS (дефолт 10s), JSON-логирование

**Git:** `feat: add tumbling window aggregator (10s)`

---

## Промпт 2.6 — Go: Apache Arrow Flight RPC сервер

**Дата:** 2026-05-25

**Промпт:** Реализация Arrow Flight RPC сервера для передачи
агрегированных окон Python-клиенту без сериализации через JSON.

**Результат:**
- arrow/schema.go: EventSchema (7 колонок), WindowSchema (5 колонок)
- arrow/server.go: FlightServer с буфером окон (RWMutex),
  GetFlightInfo (дескриптор "aggregated_windows"),
  DoGet (сериализация в RecordBatch + стриминг + очистка буфера),
  Serve (gRPC на ARROW_FLIGHT_PORT)

**Git:** `feat: add Apache Arrow Flight RPC server`

---

## Промпт 2.7 — Go: буферизация и Dockerfile

**Дата:** 2026-05-25

**Промпт:** Оптимизация буферизации Kafka-продюсера и контейнеризация
Go-сборщика.

**Результат:**
- kafka/producer.go: добавлен метод Flush для принудительного сброса
  буфера перед graceful shutdown
- docker/collector.Dockerfile: двухэтапная сборка (golang:1.22-alpine
  → alpine:3.19), CGO_ENABLED=0, -ldflags="-w -s", непривилегированный
  пользователь, HEALTHCHECK через wget
- docker-compose.yml: сервис collector добавлен

**Git:** `fix: add buffer tuning and batch write optimization`
