package model

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// Window represents a sliding window of candles
type Window struct {
	WindowID       string    `json:"window_id"`
	Symbol         string    `json:"symbol"`
	Timeframe      string    `json:"timeframe"`
	TEnd           time.Time `json:"t_end"`           // end timestamp of the window
	W              int       `json:"w"`               // window length
	FeatureVersion int       `json:"feature_version"` // version for idempotency
	Candles        []Candle  `json:"candles"`
	CreatedAt      time.Time `json:"created_at"`
}

// GenerateWindowID creates a deterministic window ID based on key parameters
// Format: hash(symbol|tf|t_end|W|feature_version)
// This ensures idempotent writes - same parameters always produce same ID
func GenerateWindowID(symbol, timeframe string, tEnd time.Time, w, featureVersion int) string {
	data := fmt.Sprintf("%s|%s|%d|%d|%d",
		symbol,
		timeframe,
		tEnd.Unix(),
		w,
		featureVersion,
	)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:16]) // use first 16 bytes (32 hex chars)
}

// NewWindow creates a new Window with generated ID
func NewWindow(symbol, timeframe string, tEnd time.Time, w, featureVersion int, candles []Candle) *Window {
	return &Window{
		WindowID:       GenerateWindowID(symbol, timeframe, tEnd, w, featureVersion),
		Symbol:         symbol,
		Timeframe:      timeframe,
		TEnd:           tEnd,
		W:              w,
		FeatureVersion: featureVersion,
		Candles:        candles,
		CreatedAt:      time.Now(),
	}
}

// IsComplete returns true if the window has the expected number of candles
func (w *Window) IsComplete() bool {
	return len(w.Candles) == w.W
}

// FirstCandle returns the first candle in the window
func (w *Window) FirstCandle() *Candle {
	if len(w.Candles) == 0 {
		return nil
	}
	return &w.Candles[0]
}

// LastCandle returns the last candle in the window
func (w *Window) LastCandle() *Candle {
	if len(w.Candles) == 0 {
		return nil
	}
	return &w.Candles[len(w.Candles)-1]
}

// TStart returns the start timestamp of the window (first candle's open time)
func (w *Window) TStart() time.Time {
	if first := w.FirstCandle(); first != nil {
		return first.OpenTime
	}
	return time.Time{}
}
