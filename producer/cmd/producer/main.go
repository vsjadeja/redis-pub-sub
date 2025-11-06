package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	var (
		redisAddr   = flag.String("redis-addr", "localhost:6380", "Redis server address")
		redisPass   = flag.String("redis-pass", "redis123", "Redis password")
		stream      = flag.String("stream", "events", "Stream to publish to (events, logs, notifications)")
		messageType = flag.String("type", "test", "Message type")
		interval    = flag.Duration("interval", 1*time.Second, "Interval between messages")
	)
	flag.Parse()

	// Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: *redisPass,
		DB:       0,
	})
	defer rdb.Close()

	// Test Redis connection
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Printf("Connected to Redis at %s", *redisAddr)

	// Create a message counter
	counter := 1

	// Setup signal handling for graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	// Create a ticker for periodic message publishing
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	log.Printf("Publishing messages to stream '%s' every %v", *stream, *interval)
	log.Println("Press Ctrl+C to stop")

	// Main publishing loop
	for {
		select {
		case <-sig:
			log.Println("\nShutting down...")
			return
		case <-ticker.C:
			// Create message values
			message := fmt.Sprintf("Message #%d", counter)
			values := map[string]interface{}{
				"message":   message,
				"type":      *messageType,
				"timestamp": time.Now().Format(time.RFC3339),
			}

			// Publish message to stream
			msgID, err := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: *stream,
				Values: values,
			}).Result()

			if err != nil {
				log.Printf("Error publishing message: %v", err)
				continue
			}

			log.Printf("Published message ID=%s: %v", msgID, values)
			counter++
		}
	}
}
