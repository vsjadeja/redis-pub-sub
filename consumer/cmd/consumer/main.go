package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"consumer/internal/redisconsumer"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

func initTracer() (*sdktrace.TracerProvider, error) {
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint("http://localhost:14268/api/traces")))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("redis-consumer"),
		)),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}

func main() {
	tp, err := initTracer()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

	tracer := tp.Tracer("redis-consumer")

	cfg := redisconsumer.ConsumerConfig{
		Addr:         "localhost:6380",
		Password:     "redis123",
		DB:           0,
		Streams:      []string{"events", "logs", "notifications"},
		Group:        "workers",
		ConsumerName: "c1",
		Concurrency:  10, // 10 goroutines per stream
		AckOnSuccess: true,
		HealthAddr:   ":8083",
		Tracer:       tracer,
	}

	handler := func(ctx context.Context, stream string, msg redis.XMessage) error {
		fmt.Printf("Stream=%s Message ID=%s Data=%v\n", stream, msg.ID, msg.Values)
		return nil
	}

	consumer, err := redisconsumer.NewConsumer(cfg, handler)
	if err != nil {
		log.Fatalf("new consumer: %v", err)
	}

	ctx := context.Background()
	if err := consumer.Start(ctx); err != nil {
		log.Fatalf("start: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consumer.Stop(shutdownCtx); err != nil {
		log.Printf("stop error: %v", err)
	}
}
