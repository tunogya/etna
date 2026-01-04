package outcome

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/tunogya/etna/pkg/model"
	"github.com/tunogya/etna/pkg/store/duckdb"
)

// Engine calculates forward-looking statistics for windows
type Engine struct {
	candleRepo *duckdb.CandleRepo
}

// NewEngine creates a new outcome engine
func NewEngine(candleRepo *duckdb.CandleRepo) *Engine {
	return &Engine{candleRepo: candleRepo}
}

// Config holds configuration for outcome calculation
type Config struct {
	Horizons []int // Forward horizons (number of bars)
}

// DefaultConfig returns default configuration
func DefaultConfig() Config {
	return Config{
		Horizons: []int{5, 20, 60},
	}
}

// Result holds outcome statistics for a single window-horizon pair
type Result struct {
	WindowID   string
	Horizon    int
	FwdRetMean float64
	FwdRetP10  float64
	FwdRetP50  float64
	FwdRetP90  float64
	MDDP95     float64
	FwdCandles int // Number of forward candles actually found
}

// Calculate computes outcome statistics for the given windows
func (e *Engine) Calculate(ctx context.Context, windows []*model.Window, horizons []int) ([]Result, error) {
	var results []Result

	for _, w := range windows {
		last := w.LastCandle()
		if last == nil {
			continue
		}

		// Fetch forward candles from the window's end time
		endTime := last.CloseTime.Add(time.Hour * 24 * 30) // Look up to 30 days ahead
		candles, err := e.candleRepo.GetByTimeRange(ctx, w.Symbol, w.Timeframe, last.CloseTime, endTime)
		if err != nil {
			continue
		}

		basePrice := last.Close
		if basePrice == 0 {
			continue
		}

		for _, horizon := range horizons {
			if len(candles) < horizon {
				// Not enough forward data
				results = append(results, Result{
					WindowID:   w.WindowID,
					Horizon:    horizon,
					FwdCandles: len(candles),
				})
				continue
			}

			forwardCandles := candles[:horizon]
			result := calculateStats(w.WindowID, horizon, basePrice, forwardCandles)
			results = append(results, result)
		}
	}

	return results, nil
}

// CalculateForWindowIDs computes outcomes for window IDs (requires fetching windows first)
func (e *Engine) CalculateForWindowIDs(ctx context.Context, windowIDs []string, symbol, timeframe string, horizons []int, windowRepo *duckdb.WindowRepo) ([]Result, error) {
	var windows []*model.Window

	for _, id := range windowIDs {
		w, err := windowRepo.GetByID(ctx, id)
		if err != nil {
			continue
		}
		windows = append(windows, w)
	}

	return e.Calculate(ctx, windows, horizons)
}

// calculateStats computes statistics for a set of forward candles
func calculateStats(windowID string, horizon int, basePrice float64, candles []model.Candle) Result {
	if len(candles) == 0 {
		return Result{
			WindowID:   windowID,
			Horizon:    horizon,
			FwdCandles: 0,
		}
	}

	// Calculate returns for each candle
	returns := make([]float64, len(candles))
	for i, c := range candles {
		returns[i] = (c.Close - basePrice) / basePrice
	}

	// Calculate MDD (maximum drawdown from base price)
	mdd := calculateMDD(basePrice, candles)

	// Sort returns for percentile calculation
	sortedReturns := make([]float64, len(returns))
	copy(sortedReturns, returns)
	sort.Float64s(sortedReturns)

	return Result{
		WindowID:   windowID,
		Horizon:    horizon,
		FwdRetMean: mean(returns),
		FwdRetP10:  percentile(sortedReturns, 10),
		FwdRetP50:  percentile(sortedReturns, 50),
		FwdRetP90:  percentile(sortedReturns, 90),
		MDDP95:     mdd, // For single window, just use the actual MDD
		FwdCandles: len(candles),
	}
}

// calculateMDD computes maximum drawdown from a base price
func calculateMDD(basePrice float64, candles []model.Candle) float64 {
	if len(candles) == 0 || basePrice == 0 {
		return 0
	}

	peak := basePrice
	maxDD := 0.0

	for _, c := range candles {
		if c.High > peak {
			peak = c.High
		}
		dd := (peak - c.Low) / peak
		if dd > maxDD {
			maxDD = dd
		}
	}

	return maxDD
}

// mean calculates the arithmetic mean
func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// percentile calculates the p-th percentile (p in 0-100)
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}

	// Linear interpolation method
	rank := (p / 100) * float64(len(sorted)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))

	if lower == upper {
		return sorted[lower]
	}

	fraction := rank - float64(lower)
	return sorted[lower] + fraction*(sorted[upper]-sorted[lower])
}

// AggregateResults aggregates outcomes from multiple windows into summary statistics
func AggregateResults(results []Result) map[int]AggregatedOutcome {
	// Group by horizon
	byHorizon := make(map[int][]Result)
	for _, r := range results {
		byHorizon[r.Horizon] = append(byHorizon[r.Horizon], r)
	}

	aggregated := make(map[int]AggregatedOutcome)
	for horizon, horizonResults := range byHorizon {
		if len(horizonResults) == 0 {
			continue
		}

		means := make([]float64, len(horizonResults))
		p10s := make([]float64, len(horizonResults))
		p50s := make([]float64, len(horizonResults))
		p90s := make([]float64, len(horizonResults))
		mdds := make([]float64, len(horizonResults))

		for i, r := range horizonResults {
			means[i] = r.FwdRetMean
			p10s[i] = r.FwdRetP10
			p50s[i] = r.FwdRetP50
			p90s[i] = r.FwdRetP90
			mdds[i] = r.MDDP95
		}

		sort.Float64s(mdds)

		aggregated[horizon] = AggregatedOutcome{
			Horizon:     horizon,
			SampleCount: len(horizonResults),
			MeanReturn:  mean(means),
			MedianP10:   mean(p10s),
			MedianP50:   mean(p50s),
			MedianP90:   mean(p90s),
			MDDP95:      percentile(mdds, 95),
		}
	}

	return aggregated
}

// AggregatedOutcome represents aggregated statistics across multiple windows
type AggregatedOutcome struct {
	Horizon     int
	SampleCount int
	MeanReturn  float64
	MedianP10   float64
	MedianP50   float64
	MedianP90   float64
	MDDP95      float64
}

// String returns a formatted string representation
func (a AggregatedOutcome) String() string {
	return fmt.Sprintf(
		"Horizon: %d bars | Samples: %d | Mean: %.4f | P10: %.4f | P50: %.4f | P90: %.4f | MDD95: %.4f",
		a.Horizon, a.SampleCount, a.MeanReturn, a.MedianP10, a.MedianP50, a.MedianP90, a.MDDP95,
	)
}
