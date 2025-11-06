# Redis Event-Driven Architecture with Go

This project demonstrates an event-driven architecture implementation using Redis Streams and Go, featuring separate producer and consumer applications with support for multiple streams, consumer groups, health checks, and OpenTelemetry tracing.

## Project Structure

```
redis-pub-sub/
├── consumer/
│   ├── cmd/
│   │   └── consumer/
│   │       └── main.go
│   └── internal/
│       └── redisconsumer/
│           └── redisconsumer.go
├── producer/
│   └── cmd/
│       └── producer/
│           └── main.go
└── docker-compose.yaml
```

## Prerequisites

- Go 1.21 or later
- Docker and Docker Compose
- Redis 6.x or later

## Quick Start

1. Start Redis and Jaeger using Docker Compose:
   ```bash
   docker compose up -d
   ```

2. Start the consumer (in one terminal):
   ```bash
   cd consumer
   go run cmd/consumer/main.go
   ```

3. Start the producer (in another terminal):
   ```bash
   cd producer
   go run cmd/producer/main.go
   ```

## Producer Usage

The producer supports various command-line flags for configuration:

```bash
go run cmd/producer/main.go [flags]

Flags:
  --redis-addr string     Redis server address (default "localhost:6380")
  --redis-pass string     Redis password (default "redis123")
  --stream string         Stream to publish to (default "events")
  --type string          Message type (default "test")
  --interval duration    Interval between messages (default 1s)
```

Examples:

```bash
# Publish to events stream every second
go run cmd/producer/main.go

# Publish to logs stream every 2 seconds
go run cmd/producer/main.go --stream logs --interval 2s

# Publish alerts to notifications stream
go run cmd/producer/main.go --stream notifications --type alert
```

## Consumer Configuration

The consumer application connects to Redis and processes messages from multiple streams using consumer groups. Features include:

- Multiple stream support (events, logs, notifications)
- Consumer group configuration
- Concurrent message processing
- Automatic message acknowledgment
- Health check endpoint
- OpenTelemetry tracing

## Features

### Health Check

The consumer provides a health check endpoint at `/healthz`. Default port is 8083.

```bash
curl http://localhost:8083/healthz
```

### Tracing

The application uses OpenTelemetry with Jaeger for distributed tracing. Access the Jaeger UI at:

```
http://localhost:16686
```

### Message Format

Messages are published with the following structure:

```json
{
  "message": "Message content",
  "type": "message type",
  "timestamp": "ISO 8601 timestamp"
}
```

## Redis Configuration

Redis is configured with password authentication and runs on port 6380. Configuration can be found in `docker-compose.yaml`:

```yaml
redis:
  image: redis:latest
  command: 'redis-server --requirepass "redis123"'
  ports:
    - "6380:6379"
```

## Development

### Adding New Streams

1. Add new stream names to the consumer configuration:
   ```go
   Streams: []string{"events", "logs", "notifications", "new-stream"}
   ```

2. Use the producer with the new stream:
   ```bash
   go run cmd/producer/main.go --stream new-stream
   ```

### Error Handling

The applications include comprehensive error handling for:
- Redis connection issues
- Stream creation errors
- Message processing failures
- Authentication problems

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

MIT License
