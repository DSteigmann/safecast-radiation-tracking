FROM golang:1.26.2-bookworm AS builder

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        gcc \
        libc6-dev \
        librdkafka-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN GOOS=linux go build -tags dynamic -o /middleware ./cmd/api

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /middleware /middleware
EXPOSE 8080
ENTRYPOINT ["/middleware"]
