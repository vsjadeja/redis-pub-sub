package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
	jaegerHost := os.Getenv("JAEGER_HOST")
	if jaegerHost == "" {
		return nil, errors.New(`JAEGER_HOST is not set`)
	}

	jaegerPort := os.Getenv("JAEGER_PORT")
	if jaegerPort == "" {
		jaegerPort = "14268"
	}
	jaegerApiEndpoint := fmt.Sprintf("http://%s:%s/api/traces", jaegerHost, jaegerPort)
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(jaegerApiEndpoint)))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("redis-consumers"),
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

	tracer := tp.Tracer("redis-consumers")

	rHost := os.Getenv("REDIS_HOST")
	if rHost == "" {
		log.Fatal("REDIS_HOST is not set")
	}

	rPort := os.Getenv("REDIS_PORT")
	if rPort == "" {
		log.Fatal("REDIS_PORT is not set")
	}

	concurency := os.Getenv("CONCURRENCY")
	if concurency == "" {
		concurency = "1"
	}
	concurrency, _ := strconv.Atoi(concurency)

	// Read streams names from environment variable
	strStreams := os.Getenv("REDIS_STREAMS")
	if strStreams == "" {
		strStreams = "logs"
	}
	streams := []string{}
	for _, s := range strings.Split(strStreams, ",") {
		streams = append(streams, s)
	}

	cfg := redisconsumer.ConsumerConfig{
		Addr:         fmt.Sprintf(`%s:%s`, rHost, rPort),
		Password:     os.Getenv(`REDIS_PASSWORD`),
		DB:           0,
		Streams:      streams,
		Group:        "workers",
		ConsumerName: "c1",
		Concurrency:  concurrency, // N goroutines per stream
		AckOnSuccess: true,
		HealthAddr:   fmt.Sprintf(`:%s`, os.Getenv(`HEALTH_PORT`)),
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
