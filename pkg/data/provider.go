package data

import (
	"context"
	"time"

	"github.com/tunogya/etna/pkg/model"
)

// CandleProvider defines the interface for fetching historical candle data
type CandleProvider interface {
	// FetchCandles retrieves historical K-line data for a given symbol and timeframe
	// Returns candles ordered by time (oldest first)
	FetchCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]model.Candle, error)

	// FetchLatestCandles retrieves the most recent N candles
	FetchLatestCandles(ctx context.Context, symbol, timeframe string, limit int) ([]model.Candle, error)
}

// StreamProvider defines the interface for real-time candle subscription
type StreamProvider interface {
	// Subscribe starts a real-time subscription to K-line data
	// Returns a channel that receives new candles as they are completed
	Subscribe(ctx context.Context, symbol, timeframe string) (<-chan model.Candle, error)

	// Unsubscribe stops the real-time subscription
	Unsubscribe(symbol, timeframe string) error

	// Close closes all subscriptions and cleans up resources
	Close() error
}

// BackfillConfig holds configuration for backfill operations
type BackfillConfig struct {
	Symbol        string        // Trading pair (e.g., "BTCUSDT")
	Timeframe     string        // Timeframe (e.g., "1m", "5m")
	StartTime     time.Time     // Start of backfill range
	EndTime       time.Time     // End of backfill range
	BatchSize     int           // Number of candles per batch
	ReverseOrder  bool          // If true, fetch newest data first (recommended)
	RetryAttempts int           // Number of retry attempts for failed requests
	RetryDelay    time.Duration // Delay between retry attempts
}

// DefaultBackfillConfig returns a BackfillConfig with sensible defaults
func DefaultBackfillConfig(symbol, timeframe string) BackfillConfig {
	return BackfillConfig{
		Symbol:        symbol,
		Timeframe:     timeframe,
		StartTime:     time.Now().AddDate(0, -1, 0), // 1 month ago
		EndTime:       time.Now(),
		BatchSize:     1000,
		ReverseOrder:  true, // Prioritize recent data
		RetryAttempts: 3,
		RetryDelay:    time.Second * 2,
	}
}

// BackfillProgress tracks the progress of a backfill operation
type BackfillProgress struct {
	TotalCandles     int64
	ProcessedCandles int64
	CurrentTime      time.Time
	StartTime        time.Time
	EndTime          time.Time
	Errors           []error
}

// ProgressCallback is called during backfill to report progress
type ProgressCallback func(progress BackfillProgress)
