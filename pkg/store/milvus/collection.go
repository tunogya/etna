package milvus

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

const (
	// DefaultCollectionName is the default collection name for kline windows
	DefaultCollectionName = "kline_windows"
)

// CollectionConfig holds configuration for creating a collection
type CollectionConfig struct {
	Name      string
	Dimension int // Vector dimension (96 or 128)
	Shards    int // Number of shards
}

// DefaultCollectionConfig returns default collection configuration
func DefaultCollectionConfig() CollectionConfig {
	return CollectionConfig{
		Name:      DefaultCollectionName,
		Dimension: 96,
		Shards:    2,
	}
}

// CreateCollection creates the kline_windows collection
func (c *Client) CreateCollection(ctx context.Context, cfg CollectionConfig) error {
	// Check if collection already exists
	exists, err := c.HasCollection(ctx, cfg.Name)
	if err != nil {
		return fmt.Errorf("failed to check collection: %w", err)
	}
	if exists {
		return nil // Collection already exists
	}

	// Define schema
	schema := &entity.Schema{
		CollectionName: cfg.Name,
		Description:    "K-line window embeddings for similarity search",
		Fields: []*entity.Field{
			{
				Name:       "window_id",
				DataType:   entity.FieldTypeVarChar,
				PrimaryKey: true,
				AutoID:     false,
				TypeParams: map[string]string{
					"max_length": "64",
				},
			},
			{
				Name:     "embedding",
				DataType: entity.FieldTypeFloatVector,
				TypeParams: map[string]string{
					"dim": fmt.Sprintf("%d", cfg.Dimension),
				},
			},
			{
				Name:     "symbol",
				DataType: entity.FieldTypeVarChar,
				TypeParams: map[string]string{
					"max_length": "32",
				},
			},
			{
				Name:     "timeframe",
				DataType: entity.FieldTypeVarChar,
				TypeParams: map[string]string{
					"max_length": "8",
				},
			},
			{
				Name:     "t_end",
				DataType: entity.FieldTypeInt64,
			},
			{
				Name:     "vol_bucket",
				DataType: entity.FieldTypeInt32,
			},
			{
				Name:     "trend_bucket",
				DataType: entity.FieldTypeInt32,
			},
			{
				Name:     "data_version",
				DataType: entity.FieldTypeInt32,
			},
		},
	}

	err = c.conn.CreateCollection(ctx, schema, int32(cfg.Shards))
	if err != nil {
		return fmt.Errorf("failed to create collection: %w", err)
	}

	return nil
}

// WindowData holds data for inserting a window into Milvus
type WindowData struct {
	WindowID    string
	Embedding   []float32
	Symbol      string
	Timeframe   string
	TEnd        time.Time
	VolBucket   int32
	TrendBucket int32
	DataVersion int32
}

// Insert inserts a single window embedding
func (c *Client) Insert(ctx context.Context, collectionName string, data *WindowData) error {
	return c.InsertBatch(ctx, collectionName, []*WindowData{data})
}

// InsertBatch inserts multiple window embeddings
func (c *Client) InsertBatch(ctx context.Context, collectionName string, dataList []*WindowData) error {
	if len(dataList) == 0 {
		return nil
	}

	// Prepare column data
	windowIDs := make([]string, len(dataList))
	embeddings := make([][]float32, len(dataList))
	symbols := make([]string, len(dataList))
	timeframes := make([]string, len(dataList))
	tEnds := make([]int64, len(dataList))
	volBuckets := make([]int32, len(dataList))
	trendBuckets := make([]int32, len(dataList))
	dataVersions := make([]int32, len(dataList))

	for i, d := range dataList {
		windowIDs[i] = d.WindowID
		embeddings[i] = d.Embedding
		symbols[i] = d.Symbol
		timeframes[i] = d.Timeframe
		tEnds[i] = d.TEnd.Unix()
		volBuckets[i] = d.VolBucket
		trendBuckets[i] = d.TrendBucket
		dataVersions[i] = d.DataVersion
	}

	// Create column entities
	columns := []entity.Column{
		entity.NewColumnVarChar("window_id", windowIDs),
		entity.NewColumnFloatVector("embedding", len(embeddings[0]), embeddings),
		entity.NewColumnVarChar("symbol", symbols),
		entity.NewColumnVarChar("timeframe", timeframes),
		entity.NewColumnInt64("t_end", tEnds),
		entity.NewColumnInt32("vol_bucket", volBuckets),
		entity.NewColumnInt32("trend_bucket", trendBuckets),
		entity.NewColumnInt32("data_version", dataVersions),
	}

	_, err := c.conn.Insert(ctx, collectionName, "", columns...)
	if err != nil {
		return fmt.Errorf("failed to insert: %w", err)
	}

	return nil
}

// SearchResult represents a single search result
type SearchResult struct {
	WindowID    string
	Score       float32
	Symbol      string
	Timeframe   string
	TEnd        time.Time
	VolBucket   int32
	TrendBucket int32
	DataVersion int32
}

// Search performs a TopK similarity search
func (c *Client) Search(ctx context.Context, collectionName string, embedding []float32, filter string, topK int) ([]SearchResult, error) {
	// Create search vectors
	vectors := []entity.Vector{entity.FloatVector(embedding)}

	// Search parameters
	sp, err := entity.NewIndexIvfFlatSearchParam(16) // nprobe
	if err != nil {
		return nil, fmt.Errorf("failed to create search param: %w", err)
	}

	// Output fields
	outputFields := []string{"window_id", "symbol", "timeframe", "t_end", "vol_bucket", "trend_bucket", "data_version"}

	// Execute search
	results, err := c.conn.Search(
		ctx,
		collectionName,
		nil,          // partitions
		filter,       // expression filter
		outputFields, // output fields
		vectors,
		"embedding",
		entity.COSINE,
		topK,
		sp,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to search: %w", err)
	}

	if len(results) == 0 {
		return nil, nil
	}

	// Parse results
	searchResults := make([]SearchResult, 0, results[0].ResultCount)
	for i := 0; i < results[0].ResultCount; i++ {
		result := SearchResult{
			Score: results[0].Scores[i],
		}

		// Extract fields from columns
		for _, field := range results[0].Fields {
			switch field.Name() {
			case "window_id":
				if col, ok := field.(*entity.ColumnVarChar); ok {
					val, _ := col.ValueByIdx(i)
					result.WindowID = val
				}
			case "symbol":
				if col, ok := field.(*entity.ColumnVarChar); ok {
					val, _ := col.ValueByIdx(i)
					result.Symbol = val
				}
			case "timeframe":
				if col, ok := field.(*entity.ColumnVarChar); ok {
					val, _ := col.ValueByIdx(i)
					result.Timeframe = val
				}
			case "t_end":
				if col, ok := field.(*entity.ColumnInt64); ok {
					val, _ := col.ValueByIdx(i)
					result.TEnd = time.Unix(val, 0)
				}
			case "vol_bucket":
				if col, ok := field.(*entity.ColumnInt32); ok {
					val, _ := col.ValueByIdx(i)
					result.VolBucket = val
				}
			case "trend_bucket":
				if col, ok := field.(*entity.ColumnInt32); ok {
					val, _ := col.ValueByIdx(i)
					result.TrendBucket = val
				}
			case "data_version":
				if col, ok := field.(*entity.ColumnInt32); ok {
					val, _ := col.ValueByIdx(i)
					result.DataVersion = val
				}
			}
		}

		searchResults = append(searchResults, result)
	}

	return searchResults, nil
}

// Flush flushes the collection to ensure data persistence
func (c *Client) Flush(ctx context.Context, collectionName string) error {
	return c.conn.Flush(ctx, collectionName, false)
}
