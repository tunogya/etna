package duckdb

import (
	"context"
	"fmt"
	"time"

	"github.com/tunogya/etna/pkg/model"
)

// CandleRepo handles candle data persistence
type CandleRepo struct {
	client *Client
}

// NewCandleRepo creates a new candle repository
func NewCandleRepo(client *Client) *CandleRepo {
	return &CandleRepo{client: client}
}

// Insert inserts a single candle
func (r *CandleRepo) Insert(ctx context.Context, c *model.Candle) error {
	query := `
		INSERT INTO candles (symbol, timeframe, open_time, close_time, open, high, low, close, volume, trades, vwap)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (symbol, timeframe, open_time) DO UPDATE SET
			close_time = EXCLUDED.close_time,
			open = EXCLUDED.open,
			high = EXCLUDED.high,
			low = EXCLUDED.low,
			close = EXCLUDED.close,
			volume = EXCLUDED.volume,
			trades = EXCLUDED.trades,
			vwap = EXCLUDED.vwap
	`
	return r.client.Exec(query,
		c.Symbol, c.Timeframe, c.OpenTime, c.CloseTime,
		c.Open, c.High, c.Low, c.Close, c.Volume, c.Trades, c.VWAP,
	)
}

// InsertBatch inserts multiple candles in a transaction
func (r *CandleRepo) InsertBatch(ctx context.Context, candles []model.Candle) error {
	tx, err := r.client.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO candles (symbol, timeframe, open_time, close_time, open, high, low, close, volume, trades, vwap)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (symbol, timeframe, open_time) DO UPDATE SET
			close_time = EXCLUDED.close_time,
			open = EXCLUDED.open,
			high = EXCLUDED.high,
			low = EXCLUDED.low,
			close = EXCLUDED.close,
			volume = EXCLUDED.volume,
			trades = EXCLUDED.trades,
			vwap = EXCLUDED.vwap
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, c := range candles {
		_, err := stmt.Exec(
			c.Symbol, c.Timeframe, c.OpenTime, c.CloseTime,
			c.Open, c.High, c.Low, c.Close, c.Volume, c.Trades, c.VWAP,
		)
		if err != nil {
			return fmt.Errorf("failed to insert candle: %w", err)
		}
	}

	return tx.Commit()
}

// GetByTimeRange retrieves candles within a time range
func (r *CandleRepo) GetByTimeRange(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]model.Candle, error) {
	query := `
		SELECT symbol, timeframe, open_time, close_time, open, high, low, close, volume, trades, vwap
		FROM candles
		WHERE symbol = ? AND timeframe = ? AND open_time >= ? AND open_time <= ?
		ORDER BY open_time ASC
	`

	rows, err := r.client.Query(query, symbol, timeframe, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query candles: %w", err)
	}
	defer rows.Close()

	var candles []model.Candle
	for rows.Next() {
		var c model.Candle
		var closeTime, vwap interface{}
		var trades interface{}

		err := rows.Scan(
			&c.Symbol, &c.Timeframe, &c.OpenTime, &closeTime,
			&c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &trades, &vwap,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan candle: %w", err)
		}

		if ct, ok := closeTime.(time.Time); ok {
			c.CloseTime = ct
		}
		if t, ok := trades.(int64); ok {
			c.Trades = t
		}
		if v, ok := vwap.(float64); ok {
			c.VWAP = v
		}

		candles = append(candles, c)
	}

	return candles, nil
}

// GetLatest retrieves the most recent N candles
func (r *CandleRepo) GetLatest(ctx context.Context, symbol, timeframe string, limit int) ([]model.Candle, error) {
	query := `
		SELECT symbol, timeframe, open_time, close_time, open, high, low, close, volume, trades, vwap
		FROM candles
		WHERE symbol = ? AND timeframe = ?
		ORDER BY open_time DESC
		LIMIT ?
	`

	rows, err := r.client.Query(query, symbol, timeframe, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query candles: %w", err)
	}
	defer rows.Close()

	var candles []model.Candle
	for rows.Next() {
		var c model.Candle
		var closeTime, vwap interface{}
		var trades interface{}

		err := rows.Scan(
			&c.Symbol, &c.Timeframe, &c.OpenTime, &closeTime,
			&c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &trades, &vwap,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan candle: %w", err)
		}

		if ct, ok := closeTime.(time.Time); ok {
			c.CloseTime = ct
		}
		if t, ok := trades.(int64); ok {
			c.Trades = t
		}
		if v, ok := vwap.(float64); ok {
			c.VWAP = v
		}

		candles = append(candles, c)
	}

	// Reverse to get chronological order
	for i, j := 0, len(candles)-1; i < j; i, j = i+1, j-1 {
		candles[i], candles[j] = candles[j], candles[i]
	}

	return candles, nil
}

// Count returns the total number of candles for a symbol/timeframe
func (r *CandleRepo) Count(ctx context.Context, symbol, timeframe string) (int64, error) {
	var count int64
	row := r.client.QueryRow(
		"SELECT COUNT(*) FROM candles WHERE symbol = ? AND timeframe = ?",
		symbol, timeframe,
	)
	err := row.Scan(&count)
	return count, err
}
