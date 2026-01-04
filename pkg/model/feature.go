package model

// FeatureRow contains structured features extracted from a window
// These features are used for filtering and statistical analysis
type FeatureRow struct {
	WindowID           string  `json:"window_id"`
	TrendSlope         float64 `json:"trend_slope"`         // linear regression slope of close prices
	RealizedVolatility float64 `json:"realized_volatility"` // standard deviation of returns
	MaxDrawdown        float64 `json:"max_drawdown"`        // maximum peak-to-trough decline
	ATR                float64 `json:"atr"`                 // average true range
	VolZScore          float64 `json:"vol_z_score"`         // volume z-score
	VolBucket          int     `json:"vol_bucket"`          // volume bucket (0-9)
	TrendBucket        int     `json:"trend_bucket"`        // trend bucket (-2 to +2)
	DataVersion        int     `json:"data_version"`        // schema version for compatibility
}

// ShapeVector is a fixed-length float32 vector for similarity search
// Typically 96 or 128 dimensions, combining normalized returns, wicks, and ranges
type ShapeVector []float32

// VectorDim constants for common embedding dimensions
const (
	VectorDim96  = 96
	VectorDim128 = 128
)

// NewShapeVector creates a new ShapeVector with the specified dimension
func NewShapeVector(dim int) ShapeVector {
	return make(ShapeVector, dim)
}

// Dim returns the dimension of the shape vector
func (sv ShapeVector) Dim() int {
	return len(sv)
}

// Copy creates a deep copy of the shape vector
func (sv ShapeVector) Copy() ShapeVector {
	result := make(ShapeVector, len(sv))
	copy(result, sv)
	return result
}

// ToFloat64 converts the shape vector to float64 slice
func (sv ShapeVector) ToFloat64() []float64 {
	result := make([]float64, len(sv))
	for i, v := range sv {
		result[i] = float64(v)
	}
	return result
}

// FromFloat64 creates a ShapeVector from float64 slice
func FromFloat64(data []float64) ShapeVector {
	result := make(ShapeVector, len(data))
	for i, v := range data {
		result[i] = float32(v)
	}
	return result
}

// Outcome represents the forward-looking statistics for a window
type Outcome struct {
	WindowID   string  `json:"window_id"`
	Horizon    int     `json:"horizon"`      // number of bars to look forward
	FwdRetMean float64 `json:"fwd_ret_mean"` // mean forward return
	FwdRetP10  float64 `json:"fwd_ret_p10"`  // 10th percentile
	FwdRetP50  float64 `json:"fwd_ret_p50"`  // median
	FwdRetP90  float64 `json:"fwd_ret_p90"`  // 90th percentile
	MDDP95     float64 `json:"mdd_p95"`      // 95th percentile max drawdown
}

// TrendBucket constants
const (
	TrendStrongDown = -2
	TrendDown       = -1
	TrendNeutral    = 0
	TrendUp         = 1
	TrendStrongUp   = 2
)

// ClassifyTrendBucket classifies a trend slope into a bucket
func ClassifyTrendBucket(slope float64) int {
	switch {
	case slope < -0.02:
		return TrendStrongDown
	case slope < -0.005:
		return TrendDown
	case slope < 0.005:
		return TrendNeutral
	case slope < 0.02:
		return TrendUp
	default:
		return TrendStrongUp
	}
}

// ClassifyVolBucket classifies a volume z-score into a bucket (0-9)
func ClassifyVolBucket(zScore float64) int {
	// Map z-score to bucket 0-9
	// z < -2: 0, z > 2: 9, linear interpolation in between
	bucket := int((zScore + 2) * 2.25)
	if bucket < 0 {
		return 0
	}
	if bucket > 9 {
		return 9
	}
	return bucket
}
