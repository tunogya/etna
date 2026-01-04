package rerank

import (
	"math"
	"sort"
	"time"

	"github.com/tunogya/etna/pkg/store/milvus"
)

// TimeDecayConfig holds configuration for time decay reranking
type TimeDecayConfig struct {
	Lambda float64 // Exponential decay rate (higher = faster decay)
	// Segment weights for different time ranges (optional, used if UseSegments is true)
	UseSegments  bool
	RecentDays   float64 // Days considered "recent" (e.g., 3)
	MediumDays   float64 // Days considered "medium" (e.g., 30)
	RecentWeight float64 // Weight for recent (<= RecentDays)
	MediumWeight float64 // Weight for medium (RecentDays < x <= MediumDays)
	OldWeight    float64 // Weight for old (> MediumDays)
}

// DefaultTimeDecayConfig returns a default configuration
func DefaultTimeDecayConfig() TimeDecayConfig {
	return TimeDecayConfig{
		Lambda:       0.1, // Moderate decay
		UseSegments:  false,
		RecentDays:   3,
		MediumDays:   30,
		RecentWeight: 1.0,
		MediumWeight: 0.7,
		OldWeight:    0.4,
	}
}

// SegmentConfig returns a configuration using segment-based weights
func SegmentConfig() TimeDecayConfig {
	return TimeDecayConfig{
		UseSegments:  true,
		RecentDays:   3,
		MediumDays:   30,
		RecentWeight: 1.0,
		MediumWeight: 0.7,
		OldWeight:    0.4,
	}
}

// RankedResult extends SearchResult with reranked score
type RankedResult struct {
	milvus.SearchResult
	OriginalScore float32
	TimeWeight    float64
	FinalScore    float64
}

// Reranker performs time-based reranking of search results
type Reranker struct {
	config TimeDecayConfig
}

// NewReranker creates a new reranker with the given configuration
func NewReranker(config TimeDecayConfig) *Reranker {
	return &Reranker{config: config}
}

// Rerank reranks search results based on time decay
func (r *Reranker) Rerank(results []milvus.SearchResult, now time.Time) []RankedResult {
	ranked := make([]RankedResult, len(results))

	for i, result := range results {
		ageDays := now.Sub(result.TEnd).Hours() / 24
		if ageDays < 0 {
			ageDays = 0
		}

		var weight float64
		if r.config.UseSegments {
			weight = r.segmentWeight(ageDays)
		} else {
			weight = r.exponentialDecay(ageDays)
		}

		ranked[i] = RankedResult{
			SearchResult:  result,
			OriginalScore: result.Score,
			TimeWeight:    weight,
			FinalScore:    float64(result.Score) * weight,
		}
	}

	// Sort by final score (descending)
	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].FinalScore > ranked[j].FinalScore
	})

	return ranked
}

// exponentialDecay calculates decay using exponential function
func (r *Reranker) exponentialDecay(ageDays float64) float64 {
	return math.Exp(-r.config.Lambda * ageDays)
}

// segmentWeight returns weight based on time segments
func (r *Reranker) segmentWeight(ageDays float64) float64 {
	switch {
	case ageDays <= r.config.RecentDays:
		return r.config.RecentWeight
	case ageDays <= r.config.MediumDays:
		return r.config.MediumWeight
	default:
		return r.config.OldWeight
	}
}

// TopN returns the top N results after reranking
func (r *Reranker) TopN(results []milvus.SearchResult, now time.Time, n int) []RankedResult {
	ranked := r.Rerank(results, now)
	if len(ranked) <= n {
		return ranked
	}
	return ranked[:n]
}

// FilterByMinScore filters results by minimum final score
func FilterByMinScore(results []RankedResult, minScore float64) []RankedResult {
	var filtered []RankedResult
	for _, r := range results {
		if r.FinalScore >= minScore {
			filtered = append(filtered, r)
		}
	}
	return filtered
}
