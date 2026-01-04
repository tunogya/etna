package data

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/tunogya/etna/pkg/model"
)

// CSVProvider implements CandleProvider for CSV files
type CSVProvider struct {
	filePath string
	candles  []model.Candle
	loaded   bool
}

// NewCSVProvider creates a new CSV-based candle provider
func NewCSVProvider(filePath string) *CSVProvider {
	return &CSVProvider{
		filePath: filePath,
		candles:  make([]model.Candle, 0),
		loaded:   false,
	}
}

// loadIfNeeded loads the CSV file if not already loaded
func (p *CSVProvider) loadIfNeeded() error {
	if p.loaded {
		return nil
	}

	file, err := os.Open(p.filePath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Parse column indices
	colMap := make(map[string]int)
	for i, col := range header {
		colMap[col] = i
	}

	// Read all records
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV record: %w", err)
		}

		candle, err := p.parseRecord(record, colMap)
		if err != nil {
			continue // Skip invalid records
		}
		p.candles = append(p.candles, candle)
	}

	p.loaded = true
	return nil
}

// parseRecord parses a CSV record into a Candle
func (p *CSVProvider) parseRecord(record []string, colMap map[string]int) (model.Candle, error) {
	getValue := func(name string) string {
		if idx, ok := colMap[name]; ok && idx < len(record) {
			return record[idx]
		}
		return ""
	}

	openTimeMs, err := strconv.ParseInt(getValue("open_time"), 10, 64)
	if err != nil {
		return model.Candle{}, fmt.Errorf("invalid open_time: %w", err)
	}

	closeTimeMs, err := strconv.ParseInt(getValue("close_time"), 10, 64)
	if err != nil {
		closeTimeMs = openTimeMs + 60000 // Default to 1 minute
	}

	open, _ := strconv.ParseFloat(getValue("open"), 64)
	high, _ := strconv.ParseFloat(getValue("high"), 64)
	low, _ := strconv.ParseFloat(getValue("low"), 64)
	close, _ := strconv.ParseFloat(getValue("close"), 64)
	volume, _ := strconv.ParseFloat(getValue("volume"), 64)
	trades, _ := strconv.ParseInt(getValue("trades"), 10, 64)

	return model.Candle{
		Symbol:    getValue("symbol"),
		Timeframe: getValue("timeframe"),
		OpenTime:  time.UnixMilli(openTimeMs),
		CloseTime: time.UnixMilli(closeTimeMs),
		Open:      open,
		High:      high,
		Low:       low,
		Close:     close,
		Volume:    volume,
		Trades:    trades,
	}, nil
}

// FetchCandles retrieves candles within the specified time range
func (p *CSVProvider) FetchCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]model.Candle, error) {
	if err := p.loadIfNeeded(); err != nil {
		return nil, err
	}

	var result []model.Candle
	for _, c := range p.candles {
		if c.OpenTime.Before(start) || c.OpenTime.After(end) {
			continue
		}
		if symbol != "" && c.Symbol != symbol {
			continue
		}
		if timeframe != "" && c.Timeframe != timeframe {
			continue
		}
		result = append(result, c)
	}

	return result, nil
}

// FetchLatestCandles retrieves the most recent N candles
func (p *CSVProvider) FetchLatestCandles(ctx context.Context, symbol, timeframe string, limit int) ([]model.Candle, error) {
	if err := p.loadIfNeeded(); err != nil {
		return nil, err
	}

	var filtered []model.Candle
	for _, c := range p.candles {
		if symbol != "" && c.Symbol != symbol {
			continue
		}
		if timeframe != "" && c.Timeframe != timeframe {
			continue
		}
		filtered = append(filtered, c)
	}

	if len(filtered) <= limit {
		return filtered, nil
	}

	return filtered[len(filtered)-limit:], nil
}

// MemoryProvider implements CandleProvider with in-memory storage
type MemoryProvider struct {
	candles []model.Candle
}

// NewMemoryProvider creates a new in-memory candle provider
func NewMemoryProvider(candles []model.Candle) *MemoryProvider {
	return &MemoryProvider{
		candles: candles,
	}
}

// AddCandles adds candles to the provider
func (p *MemoryProvider) AddCandles(candles []model.Candle) {
	p.candles = append(p.candles, candles...)
}

// FetchCandles retrieves candles within the specified time range
func (p *MemoryProvider) FetchCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]model.Candle, error) {
	var result []model.Candle
	for _, c := range p.candles {
		if c.OpenTime.Before(start) || c.OpenTime.After(end) {
			continue
		}
		if symbol != "" && c.Symbol != symbol {
			continue
		}
		if timeframe != "" && c.Timeframe != timeframe {
			continue
		}
		result = append(result, c)
	}
	return result, nil
}

// FetchLatestCandles retrieves the most recent N candles
func (p *MemoryProvider) FetchLatestCandles(ctx context.Context, symbol, timeframe string, limit int) ([]model.Candle, error) {
	var filtered []model.Candle
	for _, c := range p.candles {
		if symbol != "" && c.Symbol != symbol {
			continue
		}
		if timeframe != "" && c.Timeframe != timeframe {
			continue
		}
		filtered = append(filtered, c)
	}

	if len(filtered) <= limit {
		return filtered, nil
	}

	return filtered[len(filtered)-limit:], nil
}
