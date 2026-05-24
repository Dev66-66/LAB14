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
