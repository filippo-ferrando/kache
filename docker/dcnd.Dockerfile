# ==========================================
# Stage 1: Build daemon
# ==========================================
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o dcdnd ./cmd/dcdnd/main.go

# ==========================================
# Stage 2: Final container
# ==========================================
FROM alpine:latest

WORKDIR /app

RUN apk add --no-cache darkhttpd

COPY --from=builder /app/dcdnd /app/dcdnd

COPY ui/server /app/ui/server

EXPOSE 80 8080 9000

RUN echo '#!/bin/sh' > /app/start.sh && \
  echo 'darkhttpd /app/ui/server --port 80 --daemon' >> /app/start.sh && \
  echo 'exec /app/dcdnd "$@"' >> /app/start.sh && \
  chmod +x /app/start.sh

ENTRYPOINT ["/app/start.sh"]
