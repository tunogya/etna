package duckdb

import (
	"context"
	"fmt"

	"github.com/tunogya/etna/pkg/model"
)

// WindowRepo handles window data persistence
type WindowRepo struct {
	client *Client
}

// NewWindowRepo creates a new window repository
func NewWindowRepo(client *Client) *WindowRepo {
	return &WindowRepo{client: client}
}

// Insert inserts a single window
func (r *WindowRepo) Insert(ctx context.Context, w *model.Window) error {
	query := `
		INSERT INTO windows (window_id, symbol, timeframe, t_end, w, feature_version, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (window_id) DO NOTHING
	`
	return r.client.Exec(query,
		w.WindowID, w.Symbol, w.Timeframe, w.TEnd, w.W, w.FeatureVersion, w.CreatedAt,
	)
}

// InsertBatch inserts multiple windows in a transaction
func (r *WindowRepo) InsertBatch(ctx context.Context, windows []*model.Window) error {
	tx, err := r.client.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO windows (window_id, symbol, timeframe, t_end, w, feature_version, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (window_id) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, w := range windows {
		_, err := stmt.Exec(
			w.WindowID, w.Symbol, w.Timeframe, w.TEnd, w.W, w.FeatureVersion, w.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert window: %w", err)
		}
	}

	return tx.Commit()
}

// Exists checks if a window exists by ID
func (r *WindowRepo) Exists(ctx context.Context, windowID string) (bool, error) {
	var count int
	row := r.client.QueryRow("SELECT COUNT(*) FROM windows WHERE window_id = ?", windowID)
	err := row.Scan(&count)
	return count > 0, err
}

// GetByID retrieves a window by ID
func (r *WindowRepo) GetByID(ctx context.Context, windowID string) (*model.Window, error) {
	query := `
		SELECT window_id, symbol, timeframe, t_end, w, feature_version, created_at
		FROM windows
		WHERE window_id = ?
	`

	row := r.client.QueryRow(query, windowID)
	var w model.Window
	err := row.Scan(&w.WindowID, &w.Symbol, &w.Timeframe, &w.TEnd, &w.W, &w.FeatureVersion, &w.CreatedAt)
	if err != nil {
		return nil, err
	}

	return &w, nil
}

// Count returns the total number of windows
func (r *WindowRepo) Count(ctx context.Context, symbol, timeframe string) (int64, error) {
	var count int64
	row := r.client.QueryRow(
		"SELECT COUNT(*) FROM windows WHERE symbol = ? AND timeframe = ?",
		symbol, timeframe,
	)
	err := row.Scan(&count)
	return count, err
}
