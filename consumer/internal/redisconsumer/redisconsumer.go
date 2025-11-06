package redisconsumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Handler func(ctx context.Context, stream string, msg redis.XMessage) error

type ConsumerConfig struct {
	Addr         string        // Redis address (e.g., "localhost:6379")
	Password     string        // optional
	User         string        // optional
	DB           int           // Redis DB number
	Streams      []string      // List of Redis stream keys
	Group        string        // Consumer group name
	ConsumerName string        // Consumer instance name
	ReadCount    int64         // Number of messages to read at once
	BlockTime    time.Duration // How long to block while reading
	Concurrency  int           // Number of goroutines for processing each stream
	HealthAddr   string        // HTTP health check (e.g., ":8082")
	AckOnSuccess bool          // Automatically acknowledge on success
	Tracer       trace.Tracer
}

type Consumer struct {
	cfg     ConsumerConfig
	client  *redis.Client
	handler Handler
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	started bool
	startMu sync.Mutex
	mu      sync.Mutex
	// Tracking last poll time per stream
	lastPollMu sync.Mutex
	lastPoll   time.Time
}

func NewConsumer(cfg ConsumerConfig, handler Handler) (*Consumer, error) {
	if handler == nil {
		return nil, errors.New("handler required")
	}
	if len(cfg.Streams) == 0 || cfg.Group == "" {
		return nil, errors.New("streams and group are required")
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.ReadCount == 0 {
		cfg.ReadCount = 10
	}
	if cfg.BlockTime == 0 {
		cfg.BlockTime = 5 * time.Second
	}
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("redisconsumer")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	c := &Consumer{
		cfg:      cfg,
		client:   rdb,
		handler:  handler,
		lastPoll: time.Now(),
	}
	return c, nil
}

func (c *Consumer) Start(ctx context.Context) error {
	c.startMu.Lock()
	defer c.startMu.Unlock()
	if c.started {
		return errors.New("consumer already started")
	}
	c.started = true

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	// Ensure stream group exists
	for _, stream := range c.cfg.Streams {
		if err := c.client.XGroupCreateMkStream(ctx, stream, c.cfg.Group, "$").Err(); err != nil {
			if err.Error() != "BUSYGROUP Consumer Group name already exists" {
				log.Printf("[redisconsumer] group create error stream=%s: %v", stream, err)
			}
		}
	}

	// Start health check server if requested
	if c.cfg.HealthAddr != "" {
		go c.serveHealth(ctx, c.cfg.HealthAddr)
	}

	// Start consumers for each stream
	for _, stream := range c.cfg.Streams {
		c.wg.Add(1)
		go c.consumeStream(ctx, stream)
	}

	return nil
}

func (c *Consumer) consumeStream(ctx context.Context, stream string) {
	defer c.wg.Done()

	// Create a separate goroutine pool for this stream
	for i := 0; i < c.cfg.Concurrency; i++ {
		c.wg.Add(1)
		go c.consumeLoop(ctx, stream, i)
	}
}

func (c *Consumer) consumeLoop(ctx context.Context, stream string, id int) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    c.cfg.Group,
				Consumer: fmt.Sprintf("%s-%d", c.cfg.ConsumerName, id),
				Streams:  []string{stream, ">"},
				Count:    c.cfg.ReadCount,
				Block:    c.cfg.BlockTime,
			}).Result()

			if err == redis.Nil {
				continue // Timeout case, try again
			}
			if err != nil {
				log.Printf("[redisconsumer] read error stream=%s: %v", stream, err)
				time.Sleep(time.Second)
				continue
			}

			// Update the last poll time (health tracking)
			c.lastPollMu.Lock()
			c.lastPoll = time.Now()
			c.lastPollMu.Unlock()

			for _, s := range streams {
				for _, msg := range s.Messages {
					c.processMessage(ctx, stream, msg)
				}
			}
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, stream string, msg redis.XMessage) {
	tracer := c.cfg.Tracer
	hctx, span := tracer.Start(ctx, "redis.process_message",
		trace.WithAttributes(
			attribute.String("redis.stream", stream),
			attribute.String("redis.msg_id", msg.ID),
		))
	defer span.End()

	// Call the user-defined handler
	if err := c.handler(hctx, stream, msg); err != nil {
		log.Printf("[redisconsumer] handler error stream=%s id=%s err=%v", stream, msg.ID, err)
		return
	}

	if c.cfg.AckOnSuccess {
		if err := c.client.XAck(ctx, stream, c.cfg.Group, msg.ID).Err(); err != nil {
			log.Printf("[redisconsumer] ack error stream=%s id=%s err=%v", stream, msg.ID, err)
		}
	}
}

func (c *Consumer) Stop(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (c *Consumer) Health() error {
	c.lastPollMu.Lock()
	defer c.lastPollMu.Unlock()
	if time.Since(c.lastPoll) > 30*time.Second {
		return errors.New("no recent poll")
	}
	return nil
}

func (c *Consumer) serveHealth(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := c.Health(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("[redisconsumer] health server error: %v", err)
	} else {
		log.Printf("[redisconsumer] health server running at %s", addr)
	}
}
