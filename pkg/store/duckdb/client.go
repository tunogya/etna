package duckdb

import (
	"database/sql"
	"fmt"

	_ "github.com/marcboeker/go-duckdb"
)

// Client manages DuckDB connections
type Client struct {
	db   *sql.DB
	path string
}

// NewClient creates a new DuckDB client
// path can be a file path for persistent storage or ":memory:" for in-memory
func NewClient(path string) (*Client, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping duckdb: %w", err)
	}

	client := &Client{
		db:   db,
		path: path,
	}

	return client, nil
}

// DB returns the underlying sql.DB connection
func (c *Client) DB() *sql.DB {
	return c.db
}

// Close closes the database connection
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Exec executes a query without returning results
func (c *Client) Exec(query string, args ...interface{}) error {
	_, err := c.db.Exec(query, args...)
	return err
}

// Query executes a query and returns rows
func (c *Client) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return c.db.Query(query, args...)
}

// QueryRow executes a query that returns at most one row
func (c *Client) QueryRow(query string, args ...interface{}) *sql.Row {
	return c.db.QueryRow(query, args...)
}

// Begin starts a new transaction
func (c *Client) Begin() (*sql.Tx, error) {
	return c.db.Begin()
}
