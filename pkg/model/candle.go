package model

import "time"

// Candle represents a single K-line (candlestick) data point
type Candle struct {
	Symbol    string    `json:"symbol"`
	Timeframe string    `json:"timeframe"`
	OpenTime  time.Time `json:"open_time"`
	CloseTime time.Time `json:"close_time"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    float64   `json:"volume"`
	Trades    int64     `json:"trades,omitempty"` // optional: number of trades
	VWAP      float64   `json:"vwap,omitempty"`   // optional: volume weighted average price
}

// Returns calculates the percentage return of this candle
func (c *Candle) Returns() float64 {
	if c.Open == 0 {
		return 0
	}
	return (c.Close - c.Open) / c.Open
}

// Range calculates the high-low range as a percentage of open
func (c *Candle) Range() float64 {
	if c.Open == 0 {
		return 0
	}
	return (c.High - c.Low) / c.Open
}

// UpperWick calculates the upper wick as a percentage of the range
func (c *Candle) UpperWick() float64 {
	rangeVal := c.High - c.Low
	if rangeVal == 0 {
		return 0
	}
	body := c.Close
	if c.Open > c.Close {
		body = c.Open
	}
	return (c.High - body) / rangeVal
}

// LowerWick calculates the lower wick as a percentage of the range
func (c *Candle) LowerWick() float64 {
	rangeVal := c.High - c.Low
	if rangeVal == 0 {
		return 0
	}
	body := c.Open
	if c.Open > c.Close {
		body = c.Close
	}
	return (body - c.Low) / rangeVal
}

// IsBullish returns true if close > open
func (c *Candle) IsBullish() bool {
	return c.Close > c.Open
}

// IsBearish returns true if close < open
func (c *Candle) IsBearish() bool {
	return c.Close < c.Open
}
