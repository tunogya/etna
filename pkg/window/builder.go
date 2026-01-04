package window

import (
	"github.com/tunogya/etna/pkg/model"
)

// Builder manages sliding window construction from a stream of candles
type Builder struct {
	W              int // Window length (number of candles)
	S              int // Step size (candles between window outputs)
	Warmup         int // Minimum candles before first window output
	FeatureVersion int // Version for window ID generation
	Symbol         string
	Timeframe      string

	buffer    *RingBuffer
	stepCount int  // Counter for step-based output
	warmedUp  bool // Whether warmup period is complete
}

// Config holds configuration for window builder
type Config struct {
	W              int    // Window length
	S              int    // Step size
	Warmup         int    // Warmup period (defaults to W if 0)
	FeatureVersion int    // Feature version (defaults to 1)
	Symbol         string // Trading pair
	Timeframe      string // Timeframe
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig(symbol, timeframe string) Config {
	return Config{
		W:              60, // 60 candles per window
		S:              1,  // Output every candle
		Warmup:         0,  // Will default to W
		FeatureVersion: 1,
		Symbol:         symbol,
		Timeframe:      timeframe,
	}
}

// NewBuilder creates a new window builder with the given configuration
func NewBuilder(cfg Config) *Builder {
	warmup := cfg.Warmup
	if warmup <= 0 {
		warmup = cfg.W
	}

	return &Builder{
		W:              cfg.W,
		S:              cfg.S,
		Warmup:         warmup,
		FeatureVersion: cfg.FeatureVersion,
		Symbol:         cfg.Symbol,
		Timeframe:      cfg.Timeframe,
		buffer:         NewRingBuffer(cfg.W),
		stepCount:      0,
		warmedUp:       false,
	}
}

// Push adds a new candle and potentially produces a window
// Returns a window if one should be emitted, and a bool indicating if a window was produced
func (b *Builder) Push(c model.Candle) (*model.Window, bool) {
	b.buffer.Push(c)
	b.stepCount++

	// Check if warmup is complete
	if !b.warmedUp && b.buffer.Size() >= b.Warmup {
		b.warmedUp = true
		b.stepCount = b.S // Force first output after warmup
	}

	// Check if we should emit a window
	if !b.warmedUp || !b.buffer.IsFull() {
		return nil, false
	}

	// Check step condition
	if b.stepCount < b.S {
		return nil, false
	}

	// Reset step counter and emit window
	b.stepCount = 0

	candles := b.buffer.ToSlice()
	last := b.buffer.Last()
	if last == nil {
		return nil, false
	}

	window := model.NewWindow(
		b.Symbol,
		b.Timeframe,
		last.CloseTime,
		b.W,
		b.FeatureVersion,
		candles,
	)

	return window, true
}

// Reset clears the builder state
func (b *Builder) Reset() {
	b.buffer.Clear()
	b.stepCount = 0
	b.warmedUp = false
}

// IsWarmedUp returns true if the warmup period is complete
func (b *Builder) IsWarmedUp() bool {
	return b.warmedUp
}

// CurrentSize returns the current number of candles in the buffer
func (b *Builder) CurrentSize() int {
	return b.buffer.Size()
}

// ProcessCandles processes a batch of candles and returns all produced windows
func (b *Builder) ProcessCandles(candles []model.Candle) []*model.Window {
	var windows []*model.Window

	for _, c := range candles {
		if w, ok := b.Push(c); ok {
			windows = append(windows, w)
		}
	}

	return windows
}
