package feature

import (
	"math"

	"github.com/tunogya/etna/pkg/model"
)

// Extractor extracts features from windows
type Extractor struct {
	DataVersion int
	VectorDim   int     // Target dimension for ShapeVector (96 or 128)
	ClipStd     float64 // Standard deviations for clipping (default 3.0)
}

// NewExtractor creates a new feature extractor
func NewExtractor(dataVersion, vectorDim int) *Extractor {
	return &Extractor{
		DataVersion: dataVersion,
		VectorDim:   vectorDim,
		ClipStd:     3.0,
	}
}

// Extract extracts features from a window and returns FeatureRow and ShapeVector
func (e *Extractor) Extract(w *model.Window) (*model.FeatureRow, model.ShapeVector, error) {
	if !w.IsComplete() {
		return nil, nil, nil
	}

	candles := w.Candles

	// Calculate structured features
	trendSlope := calculateTrendSlope(candles)
	rv := calculateRealizedVolatility(candles)
	mdd := calculateMaxDrawdown(candles)
	atr := calculateATR(candles)
	volZScore := calculateVolumeZScore(candles)

	featureRow := &model.FeatureRow{
		WindowID:           w.WindowID,
		TrendSlope:         trendSlope,
		RealizedVolatility: rv,
		MaxDrawdown:        mdd,
		ATR:                atr,
		VolZScore:          volZScore,
		VolBucket:          model.ClassifyVolBucket(volZScore),
		TrendBucket:        model.ClassifyTrendBucket(trendSlope),
		DataVersion:        e.DataVersion,
	}

	// Build shape vector
	shapeVector := e.buildShapeVector(candles)

	return featureRow, shapeVector, nil
}

// buildShapeVector creates a fixed-length vector from candle data
func (e *Extractor) buildShapeVector(candles []model.Candle) model.ShapeVector {
	// Normalize different aspects
	returns := NormalizeReturns(candles, e.ClipStd)
	ranges := NormalizeRanges(candles, e.ClipStd)
	upperWicks, lowerWicks := NormalizeWicks(candles)
	volumes := NormalizeVolumes(candles, e.ClipStd)

	// Calculate how many candles to use based on target dimension
	// For dim=96: use 24 candles × 4 features (returns, range, upperWick, lowerWick)
	// For dim=128: use 32 candles × 4 features
	samplesPerFeature := e.VectorDim / 4
	if samplesPerFeature > len(candles) {
		samplesPerFeature = len(candles)
	}

	// Downsample if needed
	returns = downsample(returns, samplesPerFeature)
	ranges = downsample(ranges, samplesPerFeature)
	upperWicks = downsample(upperWicks, samplesPerFeature)
	lowerWicks = downsample(lowerWicks, samplesPerFeature)
	volumes = downsample(volumes, samplesPerFeature)

	// Concatenate into shape vector
	vector := model.NewShapeVector(e.VectorDim)
	idx := 0

	// Fill with returns
	for i := 0; i < samplesPerFeature && idx < e.VectorDim; i++ {
		if i < len(returns) {
			vector[idx] = float32(returns[i])
		}
		idx++
	}

	// Fill with ranges
	for i := 0; i < samplesPerFeature && idx < e.VectorDim; i++ {
		if i < len(ranges) {
			vector[idx] = float32(ranges[i])
		}
		idx++
	}

	// Fill with upper wicks
	for i := 0; i < samplesPerFeature && idx < e.VectorDim; i++ {
		if i < len(upperWicks) {
			vector[idx] = float32(upperWicks[i])
		}
		idx++
	}

	// Fill with lower wicks (or volumes if space allows)
	for i := 0; i < samplesPerFeature && idx < e.VectorDim; i++ {
		if i < len(lowerWicks) {
			vector[idx] = float32(lowerWicks[i])
		}
		idx++
	}

	return vector
}

// downsample reduces the number of samples using simple averaging
func downsample(values []float64, targetLen int) []float64 {
	if len(values) <= targetLen {
		return values
	}

	result := make([]float64, targetLen)
	ratio := float64(len(values)) / float64(targetLen)

	for i := 0; i < targetLen; i++ {
		start := int(float64(i) * ratio)
		end := int(float64(i+1) * ratio)
		if end > len(values) {
			end = len(values)
		}

		sum := 0.0
		count := 0
		for j := start; j < end; j++ {
			sum += values[j]
			count++
		}
		if count > 0 {
			result[i] = sum / float64(count)
		}
	}

	return result
}

// calculateTrendSlope calculates linear regression slope of close prices
func calculateTrendSlope(candles []model.Candle) float64 {
	if len(candles) < 2 {
		return 0
	}

	n := float64(len(candles))
	var sumX, sumY, sumXY, sumX2 float64

	// Normalize prices to percentage change from first close
	basePrice := candles[0].Close
	if basePrice == 0 {
		return 0
	}

	for i, c := range candles {
		x := float64(i)
		y := (c.Close - basePrice) / basePrice // Percentage change
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	denominator := n*sumX2 - sumX*sumX
	if denominator == 0 {
		return 0
	}

	slope := (n*sumXY - sumX*sumY) / denominator
	return slope
}

// calculateRealizedVolatility calculates standard deviation of returns
func calculateRealizedVolatility(candles []model.Candle) float64 {
	if len(candles) < 2 {
		return 0
	}

	returns := make([]float64, len(candles)-1)
	for i := 1; i < len(candles); i++ {
		if candles[i-1].Close != 0 {
			returns[i-1] = (candles[i].Close - candles[i-1].Close) / candles[i-1].Close
		}
	}

	_, std := meanStd(returns)
	return std
}

// calculateMaxDrawdown calculates the maximum peak-to-trough decline
func calculateMaxDrawdown(candles []model.Candle) float64 {
	if len(candles) < 2 {
		return 0
	}

	peak := candles[0].Close
	maxDD := 0.0

	for _, c := range candles {
		if c.Close > peak {
			peak = c.Close
		}
		if peak > 0 {
			dd := (peak - c.Close) / peak
			if dd > maxDD {
				maxDD = dd
			}
		}
	}

	return maxDD
}

// calculateATR calculates Average True Range
func calculateATR(candles []model.Candle) float64 {
	if len(candles) < 2 {
		return 0
	}

	var sumTR float64
	for i := 1; i < len(candles); i++ {
		curr := candles[i]
		prev := candles[i-1]

		tr := math.Max(
			curr.High-curr.Low,
			math.Max(
				math.Abs(curr.High-prev.Close),
				math.Abs(curr.Low-prev.Close),
			),
		)
		sumTR += tr
	}

	// Normalize by first candle's close price
	basePrice := candles[0].Close
	if basePrice == 0 {
		return 0
	}

	atr := sumTR / float64(len(candles)-1)
	return atr / basePrice
}

// calculateVolumeZScore calculates the z-score of the last candle's volume
func calculateVolumeZScore(candles []model.Candle) float64 {
	if len(candles) < 2 {
		return 0
	}

	volumes := make([]float64, len(candles))
	for i, c := range candles {
		volumes[i] = c.Volume
	}

	mean, std := meanStd(volumes)
	if std == 0 {
		return 0
	}

	lastVolume := candles[len(candles)-1].Volume
	return (lastVolume - mean) / std
}
