# ── Этап 1: сборка бинарника ──────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /build

# Кешируем зависимости отдельным слоем — пересборка только при изменении go.mod.
COPY collector/go.mod collector/go.sum ./
RUN go mod download

COPY collector/ ./

# CGO_ENABLED=0 для статического бинарника; -w -s убирают отладочные символы.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o collector ./cmd

# ── Этап 2: минимальный runtime-образ ─────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

# Непривилегированный пользователь для запуска процесса.
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app
COPY --from=builder /build/collector ./collector

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:8080/health || exit 1

USER appuser
ENTRYPOINT ["./collector"]
