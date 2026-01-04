package feature

import (
	"math"

	"github.com/tunogya/etna/pkg/model"
)

// Normalize contains functions for normalizing candle data

// NormalizeReturns calculates normalized returns for a slice of candles
// Returns a slice of return values, each in range [-1, 1] after clipping
func NormalizeReturns(candles []model.Candle, clipStd float64) []float64 {
	if len(candles) == 0 {
		return nil
	}

	returns := make([]float64, len(candles))
	for i, c := range candles {
		returns[i] = c.Returns()
	}

	mean, std := meanStd(returns)
	if std == 0 {
		std = 1
	}

	// Z-score normalization with clipping
	for i := range returns {
		z := (returns[i] - mean) / std
		if z > clipStd {
			z = clipStd
		}
		if z < -clipStd {
			z = -clipStd
		}
		// Scale to [-1, 1]
		returns[i] = z / clipStd
	}

	return returns
}

// NormalizeRanges calculates normalized high-low ranges
func NormalizeRanges(candles []model.Candle, clipStd float64) []float64 {
	if len(candles) == 0 {
		return nil
	}

	ranges := make([]float64, len(candles))
	for i, c := range candles {
		ranges[i] = c.Range()
	}

	mean, std := meanStd(ranges)
	if std == 0 {
		std = 1
	}

	for i := range ranges {
		z := (ranges[i] - mean) / std
		if z > clipStd {
			z = clipStd
		}
		if z < -clipStd {
			z = -clipStd
		}
		ranges[i] = z / clipStd
	}

	return ranges
}

// NormalizeWicks calculates normalized upper and lower wick ratios
func NormalizeWicks(candles []model.Candle) (upper, lower []float64) {
	if len(candles) == 0 {
		return nil, nil
	}

	upper = make([]float64, len(candles))
	lower = make([]float64, len(candles))

	for i, c := range candles {
		// Wick ratios are already in [0, 1] range
		upper[i] = c.UpperWick()
		lower[i] = c.LowerWick()
	}

	return upper, lower
}

// NormalizeVolumes calculates volume z-scores
func NormalizeVolumes(candles []model.Candle, clipStd float64) []float64 {
	if len(candles) == 0 {
		return nil
	}

	volumes := make([]float64, len(candles))
	for i, c := range candles {
		volumes[i] = c.Volume
	}

	mean, std := meanStd(volumes)
	if std == 0 {
		std = 1
	}

	for i := range volumes {
		z := (volumes[i] - mean) / std
		if z > clipStd {
			z = clipStd
		}
		if z < -clipStd {
			z = -clipStd
		}
		volumes[i] = z / clipStd
	}

	return volumes
}

// MinMaxNormalize scales values to [0, 1] range
func MinMaxNormalize(values []float64) []float64 {
	if len(values) == 0 {
		return nil
	}

	min, max := values[0], values[0]
	for _, v := range values {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	rangeVal := max - min
	if rangeVal == 0 {
		rangeVal = 1
	}

	result := make([]float64, len(values))
	for i, v := range values {
		result[i] = (v - min) / rangeVal
	}

	return result
}

// meanStd calculates mean and standard deviation
func meanStd(values []float64) (mean, std float64) {
	if len(values) == 0 {
		return 0, 0
	}

	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean = sum / float64(len(values))

	sumSquares := 0.0
	for _, v := range values {
		diff := v - mean
		sumSquares += diff * diff
	}
	variance := sumSquares / float64(len(values))
	std = math.Sqrt(variance)

	return mean, std
}
