package milvus

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

// Client manages Milvus connections
type Client struct {
	conn client.Client
	addr string
}

// Config holds Milvus connection configuration
type Config struct {
	Address  string // Milvus server address (e.g., "localhost:19530")
	Username string // Optional username for authentication
	Password string // Optional password for authentication
}

// DefaultConfig returns a Config with default values
func DefaultConfig() Config {
	return Config{
		Address: "localhost:19530",
	}
}

// NewClient creates a new Milvus client
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	var conn client.Client
	var err error

	if cfg.Username != "" && cfg.Password != "" {
		conn, err = client.NewClient(ctx, client.Config{
			Address:  cfg.Address,
			Username: cfg.Username,
			Password: cfg.Password,
		})
	} else {
		conn, err = client.NewClient(ctx, client.Config{
			Address: cfg.Address,
		})
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to milvus: %w", err)
	}

	return &Client{
		conn: conn,
		addr: cfg.Address,
	}, nil
}

// Close closes the Milvus connection
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Connection returns the underlying Milvus client connection
func (c *Client) Connection() client.Client {
	return c.conn
}

// HasCollection checks if a collection exists
func (c *Client) HasCollection(ctx context.Context, name string) (bool, error) {
	return c.conn.HasCollection(ctx, name)
}

// CreateIndex creates an IVF_FLAT index on the embedding field
func (c *Client) CreateIndex(ctx context.Context, collectionName, fieldName string) error {
	idx, err := entity.NewIndexIvfFlat(entity.COSINE, 128)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	return c.conn.CreateIndex(ctx, collectionName, fieldName, idx, false)
}

// LoadCollection loads a collection into memory
func (c *Client) LoadCollection(ctx context.Context, collectionName string) error {
	return c.conn.LoadCollection(ctx, collectionName, false)
}

// ReleaseCollection releases a collection from memory
func (c *Client) ReleaseCollection(ctx context.Context, collectionName string) error {
	return c.conn.ReleaseCollection(ctx, collectionName)
}

// DropCollection drops a collection
func (c *Client) DropCollection(ctx context.Context, collectionName string) error {
	return c.conn.DropCollection(ctx, collectionName)
}
