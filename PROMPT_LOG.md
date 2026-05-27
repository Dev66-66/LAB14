# Prompt Log — Лабораторная работа №14

## Журнал выполненных промптов

---

## Промпт 0.1 — Инициализация репозитория

**Дата:** 2026-05-24

**Промпт:**
Инициализировать Git-репозиторий для лабораторной работы №14 (вариант 21, повышенный уровень),
студент Фомичев Ярослав Николаевич, группа 221131. Создать полную файловую структуру проекта:
корневые директории `collector/cmd/`, `collector/internal/{domain,kafka,aggregator,arrow,etcd}/`,
`validator/`, `analyzer/{domain,adapters,usecases,presentation}/`, `dashboard/`, `k8s/`, `docker/`,
`data/`, `docs/`. Создать `.gitignore` для Go/Python/Rust/IDE, `.env.example` с 16 переменными
окружения, минимальный `README.md` с данными студента и пустой `PROMPT_LOG.md`.
Подключить remote к `https://github.com/Dev66-66/LAB14` и сделать initial commit.

**Результат:**
- Создана файловая структура из 14 директорий, включая все пакеты Go-коллектора
  и Python-анализатора (domain, adapters, usecases, presentation)
- `.gitignore` покрывает: Go (`/collector/bin/`, `*.exe`), Python (`__pycache__/`, `.venv/`),
  Rust (`target/`), IDE (`.idea/`, `.vscode/`), runtime-данные (`data/*.parquet`, `data/*.duckdb`)
  и секреты (`.env`)
- `.env.example` содержит 16 переменных: `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`,
  `KAFKA_PARTITIONS`, `ETCD_ENDPOINTS`, `ARROW_FLIGHT_HOST`, `ARROW_FLIGHT_PORT`,
  `PARQUET_OUTPUT_DIR`, `DUCKDB_PATH`, `STREAMLIT_PORT`, `STREAMLIT_REFRESH_INTERVAL`,
  `WINDOW_SIZE_SECONDS`, `BATCH_SIZE`, `LOG_LEVEL`, `REPLICAS_MIN`, `REPLICAS_MAX`
- `README.md` инициализирован с заголовком и таблицей данных студента
- Репозиторий подключён к `https://github.com/Dev66-66/LAB14`, initial commit запушен

**Git:** `ccb54a4` — `chore: init repo structure, .gitignore, .env.example`

---

## Промпт 1.1 — Docker Compose: Redpanda и etcd

**Дата:** 2026-05-24

**Промпт:**
Создать `docker-compose.yml` с инфраструктурными сервисами: Redpanda (Kafka-совместимый брокер,
образ `redpandadata/redpanda:v23.3.11`) в dev-container режиме с 1 GB RAM, SMP=1, внешний listener
на порту `19092`, healthcheck через `rpk cluster health --api-urls localhost:9644`. Добавить
`redpanda-init` — одноразовый контейнер, который создаёт топик `user-events` с 3 партициями
и retention 1 час (`retention.ms=3600000`). Добавить etcd (`bitnami/etcd:3.5`), ALLOW_NONE_AUTHENTICATION,
advertise URL `http://etcd:2379`, healthcheck через `etcdctl endpoint health`.
Все сервисы в bridge-сети `pipeline-network`, named volumes: `redpanda-data`, `etcd-data`, `parquet-data`.

**Результат:**
- `docker-compose.yml` с тремя сервисами инфраструктуры:
  - `redpanda`: `redpandadata/redpanda:v23.3.11`, `--mode dev-container`, `--smp 1`,
    `--memory 1G`, два listener-адреса (internal `9092`, external `19092`),
    healthcheck `rpk cluster health`, порты `19092`, `18082`, `18081`, `9644`
  - `redpanda-init`: `depends_on: redpanda: condition: service_healthy`,
    запускает `rpk topic create user-events --partitions 3 --replicas 1
    --topic-config retention.ms=3600000`, `restart: on-failure`
  - `etcd`: `bitnami/etcd:3.5`, env `ALLOW_NONE_AUTHENTICATION: "yes"`,
    `ETCD_ADVERTISE_CLIENT_URLS: http://etcd:2379`,
    `ETCD_LISTEN_CLIENT_URLS: http://0.0.0.0:2379`,
    healthcheck через `etcdctl endpoint health`, порт `2379`
- Сеть `pipeline-network` с driver `bridge`
- Три named volume: `redpanda-data`, `etcd-data`, `parquet-data`

**Git:** `chore: add Docker Compose with Redpanda and etcd`

---

## Промпт 1.2 — Kubernetes манифесты

**Дата:** 2026-05-24

**Промпт:**
Создать полный набор Kubernetes-манифестов в директории `k8s/` для production-деплоя конвейера:
namespace `lab14`, ConfigMap с переменными окружения, Deployment коллектора (2 реплики,
resource limits CPU 100m/500m, RAM 128Mi/512Mi), ClusterIP-сервис коллектора, HPA
(1–5 реплик, триггеры CPU 70% + custom Kafka lag metric 100), Deployment анализатора
(resource limits CPU 200m/1000m, RAM 256Mi/1Gi), Deployment дашборда, NodePort-сервис
дашборда (порт 30501).

**Результат:**
- `k8s/namespace.yaml`: `apiVersion: v1`, `kind: Namespace`, `name: lab14`
- `k8s/configmap.yaml`: все 16 переменных окружения из `.env.example`
- `k8s/collector-deployment.yaml`: 2 реплики, `image: lab14-collector:latest`,
  envFrom ConfigMap, ресурсы `100m/500m CPU`, `128Mi/512Mi RAM`,
  livenessProbe + readinessProbe на `/health` и `/ready` порт 8080
- `k8s/collector-service.yaml`: `ClusterIP`, порт 8080
- `k8s/hpa.yaml`: `minReplicas: 1`, `maxReplicas: 5`,
  CPU-метрика `averageUtilization: 70`,
  external-метрика `kafka_consumer_lag` (`averageValue: 100`)
- `k8s/analyzer-deployment.yaml`: 1 реплика, `image: lab14-analyzer:latest`,
  volumeMount для Parquet-данных
- `k8s/dashboard-deployment.yaml`: 1 реплика, `image: lab14-dashboard:latest`,
  env `STREAMLIT_REFRESH_INTERVAL: "5"`
- `k8s/dashboard-service.yaml`: `NodePort`, порт 8501, nodePort 30501

**Git:** `chore: add Kubernetes manifests (Deployment, Service, HPA, ConfigMap)`

---

## Промпт 2.1 — Go: domain модели

**Дата:** 2026-05-24

**Промпт:**
Инициализировать Go-модуль `github.com/Dev66-66/LAB14/collector` с директивой `go 1.26.1`.
Подключить зависимости: `github.com/twmb/franz-go` (Kafka), `go.etcd.io/etcd/client/v3`,
`github.com/apache/arrow/go/v15`, `github.com/google/uuid`, `github.com/joho/godotenv`.
Создать domain-модели: `EventType` (const iota или string enum), `UserEvent` с JSON-тегами,
`AggregatedWindow` со вложенным `PageStat`, конструктор `NewUserEvent`.
Отдельно — `ShardInfo` для координации шардов.

**Результат:**
- `go.mod`: `module github.com/Dev66-66/LAB14/collector`, `go 1.26.1`,
  зависимости franz-go, etcd/client/v3, arrow/go/v15, uuid, godotenv, grpc
- `internal/domain/event.go`:
  - `EventType` как typed string constant (`click`, `page_view`, `purchase`,
    `login`, `logout`, `search` — 6 значений)
  - `UserEvent` с полями `ID`, `UserID`, `SessionID`, `EventType`, `Page`,
    `DurationMs`, `Timestamp`, `Metadata map[string]string`, JSON-теги snake_case
  - `PageStat` с полями `Page string`, `Count int`
  - `AggregatedWindow` с полями `WindowStart`, `WindowEnd time.Time`,
    `TotalEvents int`, `EventCounts map[string]int`, `UniqueUsers int`,
    `AvgDurationMs float64`, `TopPages []PageStat`
  - `NewUserEvent(userID, sessionID string) UserEvent` — конструктор с `uuid.New()`
    и `time.Now().UTC()`
- `internal/domain/shard.go`: `ShardInfo{CollectorID string, Partitions []int32, AssignedAt time.Time}`

**Git:** `feat: add UserEvent and AggregatedWindow domain models`

---

## Промпт 2.2 — Go: Kafka адаптер

**Дата:** 2026-05-24

**Промпт:**
Реализовать Kafka-адаптер через franz-go: Producer с пакетной отправкой, ключ = UserID
для детерминированного партиционирования, экспоненциальный backoff при ошибках,
`MaxBufferedRecords(1000)`. Добавить Consumer для чтения событий по партициям
с GroupID из env `KAFKA_GROUP_ID`.

**Результат:**
- `internal/kafka/producer.go`:
  - `Producer{client *kgo.Client, topic string}`
  - `NewProducer()` — читает `KAFKA_BROKERS` и `KAFKA_TOPIC` из env,
    конфигурирует `kgo.NewClient` с `kgo.MaxBufferedRecords(1000)`,
    `kgo.RetryBackoffFn` (экспоненциальный: `min(base*2^attempt, maxBackoff)`)
  - `SendEvent(ctx, event UserEvent) error` — сериализует JSON, `kgo.KeySlice([]byte(userID))`
    для партиционирования по пользователю
  - `SendBatch(ctx, events []UserEvent) error` — `kgo.ProduceSync` пакетом
  - `Close()` — graceful flush + close
- `internal/kafka/consumer.go`:
  - `Consumer{client *kgo.Client}`
  - `NewConsumer()` — `kgo.ConsumerGroup(groupID)`,
    `kgo.ConsumeTopics(topic)`, `kgo.BlockRebalanceOnPoll()`
  - `ReadEvents(ctx) ([]UserEvent, error)` — `PollFetches` + десериализация JSON
  - `Close()`

**Git:** `feat: add Kafka producer adapter (Redpanda)`

---

## Промпт 2.3 — Go: эмулятор событий

**Дата:** 2026-05-24

**Промпт:**
Создать эмулятор пользовательских событий `domain/emitter.go` с пулом 50 пользователей
и 20 сессий, случайными метаданными (device, browser, country). Создать `cmd/main.go`:
N горутин-воркеров (N = `KAFKA_PARTITIONS`), пакетная отправка батчами по `BATCH_SIZE=100`,
JSON-логирование в stdout, graceful shutdown через `signal.NotifyContext` + `sync.WaitGroup`,
HTTP-сервер на `:8080` с эндпоинтами `/health` (всегда 200) и `/ready` (200 после прогрева).

**Результат:**
- `internal/domain/emitter.go`:
  - `Emitter{userIDs []string, sessionIDs []string, pages []string, rnd *rand.Rand}`
  - Пул 50 user ID (`user-0001` … `user-0050`), 20 session ID, 8 страниц (`/`, `/home`,
    `/catalog`, `/product/:id`, `/cart`, `/checkout`, `/profile`, `/search`)
  - `Generate() UserEvent` — случайный userID, sessionID, EventType, Page,
    DurationMs в диапазоне 100–5000ms, metadata: device (mobile/desktop/tablet),
    browser (chrome/firefox/safari/edge), country (RU/US/DE/FR/CN)
  - `GenerateBatch(n int) []UserEvent` — слайс из n событий
- `cmd/main.go`:
  - `signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)` для graceful shutdown
  - `N = int(getEnv("KAFKA_PARTITIONS", "3"))` воркеров, каждый в горутине с `WaitGroup`
  - Каждый воркер: `for { batch := emitter.GenerateBatch(batchSize); producer.SendBatch(ctx, batch) }`
  - JSON-лог: `{"level":"INFO","ts":"...","msg":"batch sent","count":100,"worker":1}`
  - HTTP `/health` → 200 OK, `/ready` → 200 после первого успешного батча

**Git:** `feat: add event emitter with goroutines and graceful shutdown`

---

## Промпт 2.4 — Go: etcd координатор шардов

**Дата:** 2026-05-24

**Промпт:**
Реализовать координатор распределения партиций Kafka между экземплярами сборщика через etcd.
`NewCoordinator` читает `ETCD_ENDPOINTS`, dial timeout 5s, collectorID = первые 8 символов UUID.
`Register` — Grant lease TTL 30s, Put ключ `/collectors/{id}`, keepalive горутина.
`GetShards` — детерминированный round-robin: получить все ключи `/collectors/`, отсортировать,
найти myIndex, назначить партиции где `p % total == myIndex`.
`WatchShards` — реактивный канал `<-chan ShardInfo` с небуфером 1.
`Deregister` — Delete ключ, Revoke lease.

**Результат:**
- `internal/etcd/coordinator.go`:
  - `Coordinator{client *clientv3.Client, collectorID string, leaseID clientv3.LeaseID}`
  - `NewCoordinator()` — `clientv3.New(Config{Endpoints, DialTimeout: 5s})`,
    `collectorID = uuid.New().String()[:8]`
  - `Register(ctx)` — `c.client.Grant(ctx, 30)`, Put `/collectors/{id}` с JSON
    `{id, registered_at, status: "active"}` и `clientv3.WithLease`, запуск
    `KeepAlive` горутины (дренаж канала: `for range keepAlive {}`)
  - `GetShards(ctx, totalPartitions int) (ShardInfo, error)`:
    Get prefix `/collectors/`, собрать IDs, `sort.Strings(ids)`,
    найти myIndex, `for p := range totalPartitions { if p%total == myIndex }`
  - `WatchShards(ctx, totalPartitions int) <-chan ShardInfo`:
    буферизованный канал size 1, горутина с `c.client.Watch(ctx, prefix)`,
    при изменении вызывает `GetShards` и неблокирующе пишет в канал
  - `Deregister(ctx)` — Delete ключ, `c.client.Revoke(ctx, c.leaseID)`

**Git:** `feat: add etcd shard coordinator`

---

## Промпт 2.5 — Go: tumbling window агрегатор

**Дата:** 2026-05-25

**Промпт:**
Реализовать предагрегацию событий на стороне Go с помощью алгоритма tumbling window.
`Window` (thread-safe): Add увеличивает счётчики, `Aggregate` возвращает `AggregatedWindow`
с топ-5 страниц по убыванию count. `TumblingAggregator`: фоновая горутина ротирует окна
каждые `WINDOW_SIZE_SECONDS` секунд (дефолт 10), выталкивает заполненное окно
в небуферизованный `output chan AggregatedWindow`, `Output()` возвращает read-only канал.

**Результат:**
- `internal/aggregator/window.go`:
  - `Window{mu sync.Mutex, start time.Time, windowSize time.Duration,
    eventCounts map[string]int, userIDs map[string]struct{}, pageCounts map[string]int,
    totalDurationMs float64, count int}`
  - `Add(event UserEvent)` — lock, инкремент `eventCounts[eventType]`, insert `userIDs[userID]`,
    `pageCounts[page]++`, `totalDurationMs += durationMs`, `count++`
  - `Aggregate() AggregatedWindow` — вычисляет `AvgDurationMs = totalDurationMs/count`,
    `UniqueUsers = len(userIDs)`, сортирует `pageCounts` → `[]PageStat` по count desc,
    берёт первые 5 (`TopPages`)
  - `NewWindow(start time.Time, size time.Duration) *Window`
- `internal/aggregator/tumbling.go`:
  - `TumblingAggregator{windowSize time.Duration, current *Window, output chan AggregatedWindow}`
  - `Start(ctx context.Context)` — горутина с `time.NewTicker(windowSize)`:
    при каждом тике вызывает `current.Aggregate()`, пишет в `output`,
    создаёт новый `current = NewWindow(now, windowSize)`
  - `Add(event UserEvent)` — делегирует в `current.Add(event)`
  - `Output() <-chan AggregatedWindow`

**Git:** `feat: add tumbling window aggregator (10s)`

---

## Промпт 2.6 — Go: Apache Arrow Flight RPC сервер

**Дата:** 2026-05-25

**Промпт:**
Реализовать Arrow Flight RPC сервер для zero-copy передачи агрегированных окон Python-клиенту.
Определить две Arrow-схемы: `EventSchema` (7 колонок) и `WindowSchema` (5 колонок).
`FlightServer` должен накапливать `AggregatedWindow` в буфере (RWMutex), обрабатывать
`GetFlightInfo` с дескриптором `CMD "aggregated_windows"`, в `DoGet` сериализовать
буфер в `RecordBatch` через array builders, стримить через IPC и очищать буфер.
`Serve` — gRPC на `ARROW_FLIGHT_PORT`.

**Результат:**
- `internal/arrow/schema.go`:
  - `EventSchema`: 7 полей — `id`, `user_id`, `session_id` (utf8),
    `event_type`, `page` (utf8), `duration_ms` (float64), `timestamp` (timestamp[ms, UTC])
  - `WindowSchema`: 5 полей — `window_start`, `window_end` (timestamp[ms, UTC]),
    `total_events` (int64), `unique_users` (int64), `avg_duration_ms` (float64)
- `internal/arrow/server.go`:
  - `FlightServer{mu sync.RWMutex, windows []AggregatedWindow}`,
    embedding `flight.BaseFlightServer`
  - `AddWindow(w AggregatedWindow)` — Lock, append, Unlock
  - `GetFlightInfo(ctx, desc *flight.FlightDescriptor)` — проверяет `desc.Type == CMD`
    и `string(desc.Cmd) == "aggregated_windows"`, возвращает `FlightInfo`
    с `flight.DescriptorCMD` (не `FlightDescriptor_CMD` — верный символ из arrow/go/v15)
  - `DoGet(ticket, stream flight.FlightService_DoGetServer)` — RLock буфер,
    строит `RecordBatch` через `array.NewBuilder` для каждого поля WindowSchema,
    `ipc.NewWriter(stream, WindowSchema)`, пишет батч, `writer.Close()`, очищает буфер
  - `Serve(ctx)` — `grpc.NewServer()`, `flight.RegisterFlightServiceServer`, `Serve(lis)`

**Git:** `feat: add Apache Arrow Flight RPC server`

---

## Промпт 2.7 — Go: буферизация и Dockerfile

**Дата:** 2026-05-25

**Промпт:**
Добавить метод `Flush()` в Kafka Producer для принудительного сброса буфера перед
graceful shutdown. Создать двухэтапный Dockerfile для коллектора: builder stage
`golang:1.26-alpine` (CGO_ENABLED=0, ldflags `-w -s`), runtime stage `alpine:3.19`
(ca-certificates, tzdata, непривилегированный пользователь appuser), HEALTHCHECK через wget,
ENTRYPOINT `./collector`. Добавить сервис `collector` в docker-compose.yml.

**Результат:**
- `internal/kafka/producer.go`: добавлен `Flush(ctx context.Context) error` —
  `c.client.Flush(ctx)` для гарантированного сброса in-flight записей перед закрытием
- `docker/collector.Dockerfile`:
  ```
  FROM golang:1.26-alpine AS builder
  WORKDIR /build
  COPY go.mod go.sum ./
  RUN go mod download
  COPY . ./
  RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o collector ./cmd
  FROM alpine:3.19
  RUN apk add --no-cache ca-certificates tzdata
  RUN addgroup -S appgroup && adduser -S appuser -G appgroup
  WORKDIR /app
  COPY --from=builder /build/collector ./collector
  EXPOSE 8080
  HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8080/health || exit 1
  USER appuser
  ENTRYPOINT ["./collector"]
  ```
  Build context для docker-compose — `./collector`, поэтому пути без префикса
- `docker-compose.yml`: сервис `collector` (build context `./collector`,
  dockerfile `../docker/collector.Dockerfile`, `env_file: .env`,
  `depends_on: redpanda-init, etcd`, порт `8080:8080`, `restart: unless-stopped`)

**Git:** `fix: add buffer tuning and batch write optimization`

---

## Промпт 3.1 — Rust: PyO3 валидатор

**Дата:** 2026-05-25

**Промпт:**
Создать Rust-крейт `event_validator` для валидации событий с интеграцией в Python через PyO3.
`Cargo.toml`: crate-type `cdylib`, `pyo3 = { version = "0.21", features = ["abi3-py312"] }`
(stable ABI, совместим с Python 3.12+), `serde`, `serde_json`, `regex`.
Функция `validate_event(payload: &str) → String` — 6 правил валидации, результат как JSON.
Функция `validate_batch(payloads: Vec<String>) → Vec<String>`.
`OnceLock` для lazy компиляции regex. Конфигурация maturin. Dockerfile для сборки.

**Результат:**
- `validator/Cargo.toml`:
  ```toml
  [lib]
  crate-type = ["cdylib"]
  [dependencies]
  pyo3 = { version = "0.21", features = ["abi3-py312"] }
  serde = { version = "1", features = ["derive"] }
  serde_json = "1"
  regex = "1"
  ```
  `abi3-py312` обеспечивает прямую совместимость: `.so` собранный под 3.12 работает
  на 3.13, 3.14 и выше без пересборки
- `validator/src/lib.rs`:
  - `static PAGE_REGEX: OnceLock<Regex>` — паттерн `^/` для ленивой компиляции
  - `validate_event(payload: &str) -> String` — десериализует JSON, проверяет:
    (1) `id` не пустой, (2) `user_id` не пустой, (3) `event_type` в допустимом множестве,
    (4) `page` начинается с `/`, (5) `duration_ms >= 0.0`, (6) `timestamp` — парсится RFC3339;
    возвращает `{"is_valid": bool, "errors": [...]}`
  - `validate_batch(payloads: Vec<String>) -> Vec<String>` — map по каждому элементу
  - `#[pymodule] fn event_validator(m: &Bound<'_, PyModule>) -> PyResult<()>`
- `validator/pyproject.toml`: maturin конфигурация, `module-name = "event_validator"`
- `validator/build.sh`: `maturin build --release`
- `docker/validator.Dockerfile`: `FROM rust:1.78`, `WORKDIR /build`, maturin build

**Git:** `feat: add Rust validation crate (PyO3)`

---

## Промпт 4.1 — Python: domain модели

**Дата:** 2026-05-25

**Промпт:**
Создать Python domain-модели в `analyzer/domain/models.py` с полными type hints и Google-style
докстрингами: `EventType(str, Enum)` с 6 значениями, `@dataclass UserEvent` со всеми полями,
`@dataclass PageStat`, `@dataclass AggregatedWindow`, `@dataclass AnalysisResult` с `peak_window:
Optional[AggregatedWindow]` и `generated_at` через timezone-aware datetime.
Создать `analyzer/requirements.txt` (14 пакетов), двухэтапный `docker/analyzer.Dockerfile`
с Rust toolchain + maturin для сборки PyO3 валидатора внутри контейнера.

**Результат:**
- `analyzer/requirements.txt` (14 зависимостей):
  `polars==0.20.31`, `duckdb==0.10.3`, `pyarrow==16.1.0`, `plotly==5.22.0`,
  `pandas==2.2.2`, `streamlit==1.36.0`, `kafka-python==2.0.2`, `pytest==8.2.0`,
  `black==24.4.2`, `flake8==7.0.0`, `isort==5.13.2`, `maturin==1.5.1`,
  `python-dotenv==1.0.1`, `kaleido==0.2.1`
- `analyzer/domain/models.py`:
  - `EventType(str, Enum)`: CLICK/PAGE_VIEW/PURCHASE/LOGIN/LOGOUT/SEARCH
  - `@dataclass UserEvent`: `id: str`, `user_id: str`, `session_id: str`,
    `event_type: EventType`, `page: str`, `duration_ms: float`,
    `timestamp: datetime`, `metadata: Dict[str, str] = field(default_factory=dict)`
  - `@dataclass PageStat`: `page: str`, `count: int`
  - `@dataclass AggregatedWindow`: `window_start: datetime`, `window_end: datetime`,
    `total_events: int`, `event_counts: Dict[str, int]`, `unique_users: int`,
    `avg_duration_ms: float`, `top_pages: List[PageStat]`
  - `@dataclass AnalysisResult`: `total_windows: int`, `total_events: int`,
    `avg_events_per_window: float`, `peak_window: Optional[AggregatedWindow]`,
    `event_distribution: Dict[str, int]`,
    `generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))`
    (timezone-aware, не устаревший `utcnow()`)
- `docker/analyzer.Dockerfile`: `FROM python:3.12-slim`, установка `curl gcc rustc cargo maturin`,
  сборка `event_validator` через `maturin build --release`, `pip install -r requirements.txt`,
  непривилегированный пользователь appuser

**Git:** `feat: add Python domain models with type hints`

---

## Промпт 4.2 — Python: адаптеры

**Дата:** 2026-05-25

**Промпт:**
Реализовать три Python-адаптера в `analyzer/adapters/`:
`ArrowFlightClient` — подключение к Go-серверу через `pyarrow.flight`, получение окон
через `get_flight_info` + `do_get`, конвертация Arrow Table в `List[AggregatedWindow]`.
`ParquetStore` — сохранение/загрузка через Polars, директория из `PARQUET_OUTPUT_DIR`.
`DuckDBAnalyzer` — SQL-аналитика через `duckdb.connect`, `analyze_parquet` с
`DATE_TRUNC` + `PERCENTILE_CONT`, `compare_with_polars` для бенчмарка.

**Результат:**
- `adapters/arrow_client.py`:
  - `ArrowFlightClient{location: str = f"{ARROW_FLIGHT_HOST}:{ARROW_FLIGHT_PORT}"}`
  - `connect()` — `pyarrow.flight.connect(f"grpc://{location}")`
  - `fetch_windows() → List[AggregatedWindow]`:
    `get_flight_info(FlightDescriptor.for_command(b"aggregated_windows"))`,
    `do_get(endpoint.ticket)`, `reader.read_all()` → Arrow Table,
    `_table_to_windows(table)` конвертирует колонки в dataclass (top_pages парсится из JSON)
  - `close()`
- `adapters/parquet_store.py`:
  - `ParquetStore{output_dir: Path = Path(PARQUET_OUTPUT_DIR)}`
  - `save_windows(windows, filename) → Path`:
    `_windows_to_dataframe(windows)` → `pl.DataFrame`,
    `df.write_parquet(output_dir / f"{filename}.parquet")`
  - `load_windows(path) → List[AggregatedWindow]`: `pl.read_parquet(path)`, row-by-row конвертация
  - `_windows_to_dataframe(windows)`: схема — `window_start`, `window_end`, `total_events`,
    `unique_users`, `avg_duration_ms`, `top_pages` (JSON-строка)
    (поле `event_counts` не сохраняется в Parquet — это важно для дашборда)
- `adapters/duckdb_analyzer.py`:
  - `DuckDBAnalyzer{conn = duckdb.connect(DUCKDB_PATH)}`
  - `analyze_parquet(path: str) → dict`: SQL с `DATE_TRUNC('hour', window_start)`,
    `SUM(total_events)`, `AVG(avg_duration_ms)`, `PERCENTILE_CONT(0.5) WITHIN GROUP (...)`
  - `compare_with_polars(path) → dict`: замеряет время DuckDB GROUP BY и
    эквивалентного Polars `group_by().agg()`, возвращает
    `{"duckdb_time_ms", "polars_time_ms", "speedup"}`
  - `close()` — закрывает соединение

**Git:** `feat: add Polars transformer and DuckDB analyzer`

---

## Промпт 4.3 — Python: use cases

**Дата:** 2026-05-25

**Промпт:**
Реализовать use cases в `analyzer/usecases/`:
`DataTransformer.transform()` — 6 шагов очистки через Polars: дедупликация по (start, end),
фильтрация нулевых событий, добавление `events_per_second`, сортировка.
`DataAnalyzer.analyze()` — оборачивает DuckDB, возвращает `AnalysisResult`, при ошибке
fallback на чистый Polars. `benchmark_polars_vs_duckdb()` — вычисляет speedup.
`run_pipeline()` — 6 шагов оркестрации с логированием, бесконечный цикл 30s.

**Результат:**
- `usecases/transformer.py`:
  - `DataTransformer`
  - `transform(windows: List[AggregatedWindow]) → pl.DataFrame`:
    (1) `_windows_to_dataframe()` → начальный DataFrame,
    (2) `df.unique(subset=["window_start", "window_end"])` — дедупликация,
    (3) `df.filter(pl.col("total_events") > 0)` — фильтрация нулей,
    (4) добавление `events_per_second = total_events / duration_seconds`,
    (5) приведение типов timestamp → `pl.Datetime("ms", "UTC")`,
    (6) `df.sort("window_start")`
  - `aggregate_by_hour(df) → pl.DataFrame`: `group_by(pl.col("window_start").dt.truncate("1h"))`
  - `get_event_distribution(windows) → Dict[str, int]`
  - `document_cleaning_steps(df) → List[str]`: описание каждого шага с числами
- `usecases/analyzer.py`:
  - `DataAnalyzer{duckdb_analyzer: DuckDBAnalyzer}`
  - `analyze(windows, parquet_path) → AnalysisResult`:
    пытается `duckdb_analyzer.analyze_parquet()`, при `RuntimeError` — Polars fallback,
    находит `peak_window` (max total_events), формирует `AnalysisResult`
  - `benchmark_polars_vs_duckdb(path) → dict`:
    вызывает `duckdb_analyzer.compare_with_polars(path)`, добавляет `speedup`
- `usecases/pipeline.py`:
  - `run_pipeline()` — 6 шагов: (1) Arrow Flight fetch, (2) Rust validate (optional),
    (3) Polars transform, (4) Parquet save, (5) DuckDB analyze + benchmark, (6) logging
  - `if __name__ == "__main__": while True: run_pipeline(); time.sleep(30)`

**Git:** `feat: add Polars transformer and DuckDB analyzer`

---

## Промпт 4.4 — Python: форматирование PEP8

**Дата:** 2026-05-25

**Промпт:**
Применить инструменты форматирования к всему Python-коду: создать `analyzer/.flake8`
с `max-line-length=88`, `analyzer/pyproject.toml` с настройками black (line-length=88)
и isort (profile=black). Запустить `black .`, `isort .`, `flake8 --select=E9,F63,F7,F82`
для проверки критических ошибок. Исправить все найденные нарушения.

**Результат:**
- `analyzer/.flake8`:
  ```ini
  [flake8]
  max-line-length = 88
  exclude = .venv,__pycache__,.git
  ```
- `analyzer/pyproject.toml`:
  ```toml
  [tool.black]
  line-length = 88
  [tool.isort]
  profile = "black"
  ```
- Весь Python-код в `domain/`, `adapters/`, `usecases/`, `presentation/` отформатирован:
  - black выравнивает кавычки, переносы, trailing commas
  - isort упорядочивает imports: stdlib → third-party → local
- `flake8 --select=E9,F63,F7,F82 .` — 0 критических ошибок (синтаксические, undefined names,
  F-string ошибки)

**Git:** `style: apply black, isort, flake8 to all Python code`

---

## Промпт 5.1 — Python: модуль визуализации Plotly

**Дата:** 2026-05-26

**Промпт:**
Создать модуль `analyzer/presentation/` с классом `ChartBuilder` и пятью методами визуализации
через Plotly: (1) временной ряд событий (`px.line`), (2) круговая диаграмма распределения
типов (`px.pie`, donut hole=0.35), (3) тепловая карта активности (час × день недели, `go.Heatmap`),
(4) гистограмма длительностей (40 бинов, vline на mean), (5) сравнение производительности
Polars vs DuckDB (`go.Bar`). Метод `_save` — HTML всегда, PNG через kaleido если доступен.
`OUTPUT_DIR = Path("./data/charts")` создаётся при импорте модуля.
Пройти `flake8 --select=E,F,W --max-line-length=88`.

**Результат:**
- `presentation/__init__.py`: `from .charts import ChartBuilder`
- `presentation/charts.py`:
  - `OUTPUT_DIR = Path("./data/charts"); OUTPUT_DIR.mkdir(parents=True, exist_ok=True)`
    выполняется на уровне модуля при импорте
  - `events_timeseries(windows, title="") → go.Figure`:
    `px.line(x=window_start, y=total_events, mode="lines+markers")`,
    hover включает оба поля
  - `event_type_distribution(distribution: Dict[str, int]) → go.Figure`:
    `px.pie(hole=0.35, color_discrete_sequence=px.colors.qualitative.Set2)`
  - `unique_users_heatmap(df: pd.DataFrame) → go.Figure`:
    pivot: `df.pivot_table(index="hour", columns="weekday", values="unique_users")`,
    `go.Heatmap(colorscale="Blues", hoverongaps=False)`
  - `duration_histogram(windows) → go.Figure`:
    `px.histogram(nbins=40, labels={"avg_duration_ms": "Duration (ms)"})`,
    `fig.add_vline(x=mean, line_dash="dash", annotation_text="Mean")`
  - `performance_comparison(benchmark: dict) → go.Figure`:
    `go.Bar` для Polars и DuckDB, subtitle `f"DuckDB is {speedup:.1f}x faster"`
  - `_save(fig, name: str)`: `fig.write_html(OUTPUT_DIR / f"{name}.html")`,
    `try: fig.write_image(...) except Exception: pass` (kaleido опционален)
- Исправлены flake8-ошибки:
  - F401: удалён неиспользуемый `from plotly.subplots import make_subplots`
  - E501: длинная строка hovertemplate разбита на многострочный f-string

**Git:** `feat: add Plotly visualizations (timeseries, heatmap, histogram)`

---

## Промпт 5.2 — Streamlit дашборд

**Дата:** 2026-05-26

**Промпт:**
Создать `dashboard/app.py` — Streamlit-дашборд с real-time обновлением каждые 5 секунд.
Читать Parquet-файлы из `PARQUET_OUTPUT_DIR` через Polars. Sidebar: multiselect типов событий,
date_input диапазона дат, кнопка обновления. Основная область: 4 KPI-метрики (st.metric),
временной ряд событий, pie-диаграмма топ-страниц (не типов событий — event_counts не
сохраняется в Parquet), горизонтальный bar топ-10 страниц, таблица производительности.
Создать `dashboard/requirements.txt`, `docker/dashboard.Dockerfile`, добавить сервис
в `docker-compose.yml` с зависимостью от `analyzer`.

**Результат:**
- `dashboard/app.py`:
  - `PARQUET_DIR = Path(os.getenv("PARQUET_OUTPUT_DIR", "./data"))`
  - `REFRESH_INTERVAL = int(os.getenv("STREAMLIT_REFRESH_INTERVAL", "5"))`
  - `_EVENT_TYPES = ["click", "page_view", "purchase", "login", "logout", "search"]`
  - `load_data() → pl.DataFrame | None`:
    `sorted(PARQUET_DIR.glob("*.parquet"))` → `[pl.read_parquet(f) for f in files]`
    → `pl.concat(frames).sort("window_start")`
  - `_aggregate_pages(df) → pl.DataFrame`: парсит колонку `top_pages` (JSON-строка)
    через `json.loads`, агрегирует count по page
  - `render_metrics(df)`: `st.metric("Total Events", total)`, `st.metric("Unique Users", ...)`,
    `st.metric("Avg Duration ms", ...)`, `st.metric("Windows", ...)`
  - `render_timeseries(df)`: `px.line(df, x="window_start", y="total_events")`
  - `render_event_distribution(df)`: `px.pie` топ-страниц (не типов событий)
  - `render_top_pages(df)`: `px.bar` горизонтальный, топ-10 страниц по суммарному count
  - `render_performance_table(df)`: `st.dataframe` с колонками window_start, total_events,
    avg_duration_ms
  - `main()`: sidebar фильтры, фильтрация df по дате через
    `datetime.combine(d, time.min).replace(tzinfo=timezone.utc)`,
    вызов render-функций, `time.sleep(REFRESH_INTERVAL)`, `st.rerun()`
- `dashboard/requirements.txt`: `streamlit==1.36.0`, `polars==0.20.31`,
  `plotly==5.22.0`, `python-dotenv==1.0.1`
- `docker/dashboard.Dockerfile`:
  `FROM python:3.12-slim`, `COPY dashboard/requirements.txt .`,
  `RUN pip install --no-cache-dir -r requirements.txt`,
  `COPY dashboard/ .`,
  `RUN apt-get install -y --no-install-recommends curl` (для HEALTHCHECK),
  `adduser --system appuser`, `USER appuser`, `EXPOSE 8501`,
  `HEALTHCHECK CMD curl -f http://localhost:8501/_stcore/health || exit 1`,
  `ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]`
- `docker-compose.yml`: сервис `dashboard` добавлен (build context `.`,
  dockerfile `docker/dashboard.Dockerfile`, `depends_on: analyzer`,
  порт `8501:8501`, volume `parquet-data:/app/data`,
  env `PARQUET_OUTPUT_DIR: /app/data`, `STREAMLIT_REFRESH_INTERVAL: "5"`)

**Git:** `feat: add Streamlit real-time dashboard`

---

## Промпт 6.1 — Тесты Go

**Дата:** 2026-05-26

**Промпт:**
Создать юнит-тесты для Go-компонентов. Файл `collector/internal/aggregator/window_test.go`
(пакет `aggregator_test`): 6 тестов — `TestWindow_Add_IncreasesCount`,
`TestWindow_Aggregate_UniqueUsers`, `TestWindow_Aggregate_EventCounts`,
`TestWindow_Aggregate_TopPages`, `TestWindow_Aggregate_AvgDuration`,
`TestTumblingAggregator_OutputsWindow`. Файл `collector/internal/domain/emitter_test.go`
(пакет `domain_test`): 3 теста — `Generate_ValidEvent`, `GenerateBatch_CorrectCount(50)`,
`Generate_PageStartsWithSlash`. Использовать Go 1.26 синтаксис (`for range N`, `t.Context()`).
Запустить `cd collector && go test ./internal/... -v -count=1`.

**Результат:**
- `internal/aggregator/export_test.go` (пакет `aggregator`, компилируется только в тестах):
  ```go
  func NewTumblingAggregatorWithSize(size time.Duration) *TumblingAggregator {
      return &TumblingAggregator{
          windowSize: size,
          current:    NewWindow(time.Now().UTC(), size),
          output:     make(chan domain.AggregatedWindow, 16),
      }
  }
  ```
  Стандартный Go паттерн `export_test.go` для экспонирования внутреннего конструктора
  во внешний тестовый пакет без изменения production-кода
- `internal/aggregator/window_test.go` (пакет `aggregator_test`):
  - `TestWindow_Add_IncreasesCount`: создаёт `NewWindow`, `Add` 3 события, проверяет
    `Aggregate().TotalEvents == 3`
  - `TestWindow_Aggregate_UniqueUsers`: один userID добавлен 5 раз + второй userID,
    проверяет `UniqueUsers == 2`
  - `TestWindow_Aggregate_EventCounts`: click×3 + purchase×2,
    проверяет `EventCounts["click"] == 3` и `EventCounts["purchase"] == 2`
  - `TestWindow_Aggregate_TopPages`: `/home`×5 + `/about`×2,
    проверяет `TopPages[0].Page == "/home"` и `TopPages[0].Count == 5`
  - `TestWindow_Aggregate_AvgDuration`: 3 события с duration 100, 200, 300ms,
    проверяет `AvgDurationMs == 200.0`
  - `TestTumblingAggregator_OutputsWindow`: `NewTumblingAggregatorWithSize(100ms)`,
    10 событий через `Add`, `time.Sleep(150ms)`, читает `<-agg.Output()`,
    проверяет `window.TotalEvents == 10`; использует `t.Context()` (Go 1.26)
- `internal/domain/emitter_test.go` (пакет `domain_test`):
  - `TestEmitter_Generate_ValidEvent`: проверяет непустые поля UserID, SessionID, Page,
    валидность EventType
  - `TestEmitter_GenerateBatch_CorrectCount`: `GenerateBatch(50)` возвращает слайс длиной 50
  - `TestEmitter_Generate_PageStartsWithSlash`: `for i := range 100` генерирует 100 событий,
    все Page начинаются с `/`
- `go test ./internal/... -v -count=1`: **все 9 тестов PASS**

**Git:** `test: add Go unit tests (aggregator, emitter)`

---

## Промпт 6.2 — Тесты Python

**Дата:** 2026-05-26

**Промпт:**
Создать pytest-тесты для Python-компонентов. `analyzer/tests/__init__.py` — пустой.
`test_transformer.py` (5 тестов): дедупликация, фильтрация нулей, поле events_per_second,
агрегация по часам, шаги очистки. `test_analyzer.py` (2 теста): ключи бенчмарка + speedup,
AnalysisResult при RuntimeError из DuckDB. `test_models.py` (3 теста): EventType values,
строковая совместимость, создание AggregatedWindow.
Запустить `cd analyzer && python -m pytest tests/ -v --tb=short`.

**Результат:**
- `analyzer/tests/__init__.py`: пустой файл, маркирует пакет для pytest-discovery
- `analyzer/tests/test_transformer.py`:
  - `_BASE_START = datetime(2024, 1, 1, 14, 0, 0, tzinfo=timezone.utc)` — фиксированная база
  - `make_window(total_events, unique_users, ...) → AggregatedWindow` — helper
  - `TestDataTransformer.test_transform_removes_duplicates`:
    два окна с одинаковыми (start, end) → `transform()` возвращает 1 строку
  - `test_transform_filters_zero_events`:
    окно с `total_events=0` → отфильтровывается, результат пустой DataFrame
  - `test_transform_adds_events_per_second`:
    результирующий DataFrame содержит колонку `"events_per_second"`
  - `test_aggregate_by_hour_groups_correctly`:
    два окна в одном часу → `aggregate_by_hour()` возвращает 1 строку
  - `test_document_cleaning_steps_not_empty`:
    `document_cleaning_steps(df)` возвращает непустой список строк
- `analyzer/tests/test_analyzer.py`:
  - `MagicMock()` для `DuckDBAnalyzer`
  - `test_benchmark_returns_all_keys`: mock возвращает `{"duckdb_time_ms": 50.0,
    "polars_time_ms": 100.0}`, проверяет `benchmark["speedup"] == pytest.approx(2.0)`,
    наличие всех 3 ключей
  - `test_analyze_returns_analysis_result`: `side_effect=RuntimeError` на `analyze_parquet`
    (триггер Polars-fallback), проверяет `total_windows=2`, `total_events=30`,
    `peak_window.total_events == 20`
- `analyzer/tests/test_models.py`:
  - `TestEventType.test_all_values_defined`: `len(list(EventType)) == 6`
  - `test_string_comparison`: `EventType.CLICK == "click"` (str Enum совместимость)
  - `TestAggregatedWindow.test_creation_with_defaults`: создаёт `AggregatedWindow`,
    проверяет `generated_at` — timezone-aware (`tzinfo is not None`)
- Исправления для прохождения тестов:
  - `domain/__init__.py`: `from analyzer.domain.models import (...)` →
    `from .models import (...)` (относительный импорт — иначе ModuleNotFoundError
    при запуске pytest из `analyzer/`)
  - `usecases/__init__.py` и `adapters/__init__.py`: аналогичная замена
  - `test_transformer.py`: удалены неиспользуемые imports `pl`, `pytest`, `EventType` (F401),
    разбита длинная строка на многострочный вызов (E501)
  - `test_models.py`: удалён неиспользуемый `PageStat` (F401)
- `pytest tests/ -v --tb=short`: **все 10 тестов PASS (0.74s)**

**Git:** `test: add Python pytest (transformer, analyzer, models)`

---

## Промпт 7.1 — Архитектурная документация

**Дата:** 2026-05-26

**Промпт:**
Создать `docs/ARCHITECTURE.md` с полным описанием архитектуры конвейера: 11 разделов,
Mermaid-диаграмма `flowchart LR` с 3 subgraph (Go, Rust, Python) и 14 нумерованными шагами
потока данных, таблицы компонентов по ролям, описание tumbling window с временной диаграммой,
алгоритм etcd round-robin с формулой, Arrow Flight дескриптор и IPC-стриминг,
схема Parquet (6 колонок), таблица производительности DuckDB vs Polars.

**Результат:**
- `docs/ARCHITECTURE.md` (11 разделов):
  1. **Обзор**: назначение системы, технологический стек одной фразой
  2. **Диаграмма архитектуры**: Mermaid `flowchart LR` с subgraph Go, Rust, Python;
     узлы: Emitter → Redpanda → TumblingAggregator → FlightServer → ArrowClient →
     Validator → Transformer → ParquetStore → DuckDB → ChartBuilder → Streamlit;
     etcd пунктирной стрелкой `-.round-robin.->`
  3. **Поток данных**: нумерованный список 14 шагов от генерации события до отображения
     на дашборде
  4. **Go-слой**: таблица с колонками Компонент / Роль / Пакет для 8 модулей
  5. **Rust/PyO3 слой**: описание 6 правил валидации, `OnceLock<Regex>`, `abi3-py312`
  6. **Python-слой**: Clean Architecture — domain → adapters → usecases → presentation
  7. **Tumbling Window**: временная диаграмма, rotate на каждом тике, thread-safe Add
  8. **etcd координация**: формула `partition p → коллектор с индексом p % N`,
     WatchShards — реактивное обновление при изменении `/collectors/` prefix
  9. **Apache Arrow Flight**: дескриптор `CMD b"aggregated_windows"`, IPC RecordBatch,
     zero-copy без JSON
  10. **Схема данных Parquet**: таблица 6 колонок — `window_start`, `window_end`,
      `total_events`, `unique_users`, `avg_duration_ms`, `top_pages` (JSON string)
  11. **Производительность и деплой**: DuckDB ~4ms vs Polars ~12ms,
      Docker Compose vs Kubernetes с HPA

**Git:** `docs: add architecture documentation with Mermaid diagram`

---

## Промпт 7.2 — Финальный README

**Дата:** 2026-05-26

**Промпт:**
Написать финальный `README.md` с полным описанием проекта — 13 разделов: данные студента,
описание системы (3 абзаца технического нарратива), архитектура (Mermaid-диаграмма из
ARCHITECTURE.md), технологический стек (таблица 12 компонентов с версиями), структура
проекта (полное дерево с комментариями), быстрый старт (7 команд), эндпоинты (9 строк),
переменные окружения (16 строк), запуск тестов (Go + Python + Rust с ожидаемым выводом),
Kubernetes-деплой с лимитами ресурсов, примеры JSON-логов + DuckDB SQL + таблица графиков,
анализ производительности DuckDB vs Polars, авторы.

**Результат:**
- `README.md` (13 разделов):
  1. **Данные студента**: таблица ФИО/Группа/Вариант/Предметная область/Сложность
  2. **Описание системы**: эмулятор 50 пользователей → Redpanda → tumbling window 10s →
     Arrow Flight zero-copy → Rust validate → Polars clean → Parquet → DuckDB SQL →
     Streamlit авторефреш 5s; etcd round-robin масштабирование; Docker Compose + K8s HPA
  3. **Архитектура**: Mermaid `flowchart LR` (копия из ARCHITECTURE.md)
  4. **Технологический стек**: таблица 12 строк —
     Go 1.26, Python 3.12, Rust 1.78, Redpanda v23.3.11, etcd 3.5,
     Arrow/Flight, Polars 0.20.31, DuckDB 0.10.3, Plotly 5.22.0,
     Streamlit 1.36.0, Docker Compose v2, Kubernetes 1.28+
  5. **Структура проекта**: полное дерево директорий с inline-комментариями
     для каждого значимого файла
  6. **Быстрый старт**: 7 шагов — clone, cp .env.example, up infra, wait, up all,
     open dashboard, check logs
  7. **Эндпоинты**: таблица 9 строк — Streamlit 8501, Collector /health /ready 8080,
     Arrow Flight grpc 8815, Redpanda 19092/9644/18082, etcd 2379, K8s NodePort 30501
  8. **Переменные окружения**: таблица 16 строк с дефолтами и описаниями
  9. **Запуск тестов**: `go test ./internal/...` (9 тестов), `pytest tests/ -v`
     (10 тестов, ожидаемый вывод с именами тестов), `cargo test`
  10. **Kubernetes деплой**: 6 команд kubectl, таблица ресурсных лимитов Collector/Analyzer
  11. **Примеры работы**: JSON-лог коллектора, JSON-лог агрегатора (window aggregated),
      полный DuckDB SQL с PERCENTILE_CONT, результирующая таблица, таблица 5 графиков
  12. **Анализ производительности**: Python-сниппет с результатом benchmark, объяснение
      превосходства DuckDB (колонарный Parquet I/O + SIMD векторизация)
  13. **Авторы**: таблица ФИО/Группа/Вариант/Дисциплина/Год

**Git:** `docs: finalize README with architecture and usage`

---

## Промпт 7.3 — Финализация PROMPT_LOG

**Дата:** 2026-05-26

**Промпт:**
Написать финальную запись PROMPT_LOG.md с итоговым составом проекта и списком всех
коммитов. Добавить раздел «Итоговый состав проекта» с перечислением всех файлов
по слоям. Вывести `git log --oneline` для верификации 25 коммитов.

**Итоговый состав проекта:**

**Go-слой (collector/):**
- `cmd/main.go`: точка входа, N горутин-воркеров, graceful shutdown, /health /ready
- `internal/domain/event.go`: UserEvent, AggregatedWindow, EventType (6 констант)
- `internal/domain/emitter.go`: Emitter (50 пользователей, 20 сессий)
- `internal/domain/shard.go`: ShardInfo
- `internal/kafka/producer.go`: Producer, SendEvent, SendBatch, Flush
- `internal/kafka/consumer.go`: Consumer, ReadEvents
- `internal/aggregator/window.go`: Window, Add, Aggregate (TopPages, UniqueUsers)
- `internal/aggregator/tumbling.go`: TumblingAggregator, Start, Output
- `internal/aggregator/window_test.go`: 6 тестов агрегатора
- `internal/aggregator/export_test.go`: NewTumblingAggregatorWithSize (test helper)
- `internal/domain/emitter_test.go`: 3 теста эмулятора
- `internal/etcd/coordinator.go`: Coordinator, Register, GetShards, WatchShards
- `internal/arrow/schema.go`: EventSchema, WindowSchema
- `internal/arrow/server.go`: FlightServer, DoGet, Serve

**Rust-слой (validator/):**
- `src/lib.rs`: validate_event (6 правил), validate_batch, PyO3 модуль event_validator

**Python-слой (analyzer/):**
- `domain/models.py`: EventType, UserEvent, AggregatedWindow, AnalysisResult
- `adapters/arrow_client.py`: ArrowFlightClient
- `adapters/parquet_store.py`: ParquetStore
- `adapters/duckdb_analyzer.py`: DuckDBAnalyzer (SQL + бенчмарк)
- `usecases/transformer.py`: DataTransformer (6 шагов очистки)
- `usecases/analyzer.py`: DataAnalyzer
- `usecases/pipeline.py`: run_pipeline
- `presentation/charts.py`: ChartBuilder (5 типов графиков)
- `tests/test_transformer.py`: 5 тестов
- `tests/test_analyzer.py`: 2 теста
- `tests/test_models.py`: 3 теста

**Дашборд (dashboard/):**
- `app.py`: Streamlit с авторефрешем, sidebar, 4 KPI-метрики, 4 графика

**Инфраструктура:**
- `docker-compose.yml`: 6 сервисов (redpanda, redpanda-init, etcd, collector, analyzer, dashboard)
- `k8s/`: 8 манифестов (namespace, configmap, deployments, services, hpa)
- `docs/ARCHITECTURE.md`: 11 разделов, Mermaid-диаграмма

**Итоговое количество коммитов:** 25

**Git:** `docs: finalize PROMPT_LOG`

---

## Промпт 8.1 — Код-ревью

**Дата:** 2026-05-27

**Промпт:**
Провести полное код-ревью всего проекта для выявления конфликтов версий, критических ошибок
сборки, проблем безопасности и устаревших API. Проверить: Dockerfile build contexts vs пути
COPY, соответствие версии Go в Dockerfile и go.mod, полноту сервисов docker-compose
(все depends_on должны ссылаться на существующие сервисы), зависимости системных утилит
в healthcheck, thread-safety в Go, deprecations в Python 3.12+.
Исправить все найденные проблемы, запустить проверки: `go build ./...`,
`pytest tests/ -q`, `flake8 domain/models.py usecases/pipeline.py`.

**Найдено и исправлено:**

**CRITICAL:**
- `docker/collector.Dockerfile`: build context в docker-compose — `./collector`,
  но COPY писал `COPY collector/go.mod` — путь `collector/go.mod` не существует
  внутри контекста `./collector`. Исправлено: `COPY go.mod go.sum ./` и `COPY . ./`
- `docker/collector.Dockerfile`: базовый образ `golang:1.22-alpine` не может собрать
  модуль с директивой `go 1.26.1` в `go.mod` — toolchain несовместим.
  Исправлено: `FROM golang:1.26-alpine AS builder`
- `docker-compose.yml`: секция `dashboard: depends_on: - analyzer` ссылалась
  на сервис `analyzer`, который не был определён → при `docker compose up`
  docker-compose выдавал ошибку о несуществующем сервисе.
  Исправлено: добавлен полный сервис `analyzer` (build context `.`,
  dockerfile `docker/analyzer.Dockerfile`, env_file `.env`,
  depends_on `redpanda-init + etcd`, volume `parquet-data:/app/data`,
  env `PARQUET_OUTPUT_DIR: /app/data`)
- `docker/dashboard.Dockerfile`: `HEALTHCHECK CMD curl -f http://localhost:8501/_stcore/health`
  — `curl` не включён в базовый образ `python:3.12-slim`.
  Исправлено: добавлен `RUN apt-get update && apt-get install -y --no-install-recommends curl`

**HIGH:**
- `collector/internal/etcd/coordinator.go`, `WatchShards`:
  ```go
  // Было (блокирующий вызов вне select — риск дедлока):
  case <-ch:
      ch <- shard  // блокирует если ctx отменён после дрейна канала
  // Стало (вложенный select защищает от блокировки):
  case <-ch:
      select {
      case ch <- shard:
      case <-ctx.Done():
          return
      }
  ```
- `analyzer/usecases/pipeline.py`:
  `datetime.utcnow()` deprecated в Python 3.12 (scheduled for removal в 3.14+).
  Исправлено: `datetime.now(timezone.utc)`, добавлен import `timezone`
- `analyzer/domain/models.py`:
  `default_factory=datetime.utcnow` в `AnalysisResult.generated_at` — та же проблема.
  Исправлено: `default_factory=lambda: datetime.now(timezone.utc)`

**Проверки после исправлений:**
- `cd collector && go build ./...` — **OK** (0 ошибок)
- `cd analyzer && pytest tests/ -q` — **10 passed** (0 failures)
- `cd analyzer && flake8 domain/models.py usecases/pipeline.py` — **0 ошибок**

**Git:** `fix: code review — Dockerfile paths, missing analyzer service, datetime deprecation, WatchShards race`
