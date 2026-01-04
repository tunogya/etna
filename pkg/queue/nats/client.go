package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Config holds NATS client configuration
type Config struct {
	URL           string
	StreamName    string
	RetryAttempts int
	RetryDelay    time.Duration
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	return Config{
		URL:           "nats://localhost:4222",
		StreamName:    "etna",
		RetryAttempts: 3,
		RetryDelay:    time.Second,
	}
}

// Client wraps NATS JetStream functionality
type Client struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	config Config
}

// NewClient creates a new NATS client with JetStream support
func NewClient(cfg Config) (*Client, error) {
	nc, err := nats.Connect(cfg.URL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(cfg.RetryAttempts),
		nats.ReconnectWait(cfg.RetryDelay),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &Client{
		nc:     nc,
		js:     js,
		config: cfg,
	}, nil
}

// CreateStream creates a JetStream stream for message persistence
func (c *Client) CreateStream(ctx context.Context, subjects []string) error {
	_, err := c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      c.config.StreamName,
		Subjects:  subjects,
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
		MaxAge:    24 * time.Hour, // Retain messages for 24 hours
	})
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	return nil
}

// Publish publishes a message to a subject
func (c *Client) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := c.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

// MessageHandler is called when a message is received
type MessageHandler func(msg jetstream.Msg) error

// Subscribe creates a durable consumer and subscribes to messages
func (c *Client) Subscribe(ctx context.Context, subject string, consumerName string, handler MessageHandler) (jetstream.ConsumeContext, error) {
	consumer, err := c.js.CreateOrUpdateConsumer(ctx, c.config.StreamName, jetstream.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    3,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		if err := handler(msg); err != nil {
			msg.Nak()
			return
		}
		msg.Ack()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start consuming: %w", err)
	}

	return consumeCtx, nil
}

// Close closes the NATS connection
func (c *Client) Close() {
	if c.nc != nil {
		c.nc.Close()
	}
}

// IsConnected returns true if connected to NATS
func (c *Client) IsConnected() bool {
	return c.nc != nil && c.nc.IsConnected()
}
