package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/tunogya/etna/pkg/data"
	"github.com/tunogya/etna/pkg/feature"
	"github.com/tunogya/etna/pkg/model"
	"github.com/tunogya/etna/pkg/outcome"
	"github.com/tunogya/etna/pkg/rerank"
	"github.com/tunogya/etna/pkg/store/duckdb"
	"github.com/tunogya/etna/pkg/store/milvus"
	"github.com/tunogya/etna/pkg/window"
)

// Config holds backfill configuration
type Config struct {
	// Data source
	CSVPath   string
	Symbol    string
	Timeframe string

	// Window configuration
	WindowLength   int
	StepSize       int
	FeatureVersion int

	// Storage
	DuckDBPath string
	MilvusAddr string
	VectorDim  int

	// Processing
	BatchSize int
}

func main() {
	// Parse flags
	cfg := parseFlags()

	log.Printf("Starting backfill for %s %s", cfg.Symbol, cfg.Timeframe)
	log.Printf("Window: W=%d, S=%d, Dim=%d", cfg.WindowLength, cfg.StepSize, cfg.VectorDim)

	ctx := context.Background()

	// Initialize DuckDB
	log.Println("Connecting to DuckDB...")
	duckClient, err := duckdb.NewClient(cfg.DuckDBPath)
	if err != nil {
		log.Fatalf("Failed to connect to DuckDB: %v", err)
	}
	defer duckClient.Close()

	// Initialize schema
	if err := duckdb.InitializeSchema(duckClient); err != nil {
		log.Fatalf("Failed to initialize schema: %v", err)
	}
	log.Println("DuckDB schema initialized")

	// Initialize repos
	candleRepo := duckdb.NewCandleRepo(duckClient)
	windowRepo := duckdb.NewWindowRepo(duckClient)
	featureRepo := duckdb.NewFeatureRepo(duckClient)

	// Initialize Milvus
	log.Println("Connecting to Milvus...")
	milvusClient, err := milvus.NewClient(ctx, milvus.Config{Address: cfg.MilvusAddr})
	if err != nil {
		log.Fatalf("Failed to connect to Milvus: %v", err)
	}
	defer milvusClient.Close()

	// Create collection
	collectionCfg := milvus.CollectionConfig{
		Name:      milvus.DefaultCollectionName,
		Dimension: cfg.VectorDim,
		Shards:    2,
	}
	if err := milvusClient.CreateCollection(ctx, collectionCfg); err != nil {
		log.Fatalf("Failed to create Milvus collection: %v", err)
	}
	log.Println("Milvus collection ready")

	// Load data
	log.Printf("Loading data from %s...", cfg.CSVPath)
	provider := data.NewCSVProvider(cfg.CSVPath)
	candles, err := provider.FetchCandles(ctx, cfg.Symbol, cfg.Timeframe, time.Time{}, time.Now())
	if err != nil {
		log.Fatalf("Failed to load candles: %v", err)
	}
	log.Printf("Loaded %d candles", len(candles))

	// Store candles in DuckDB
	log.Println("Storing candles in DuckDB...")
	if err := candleRepo.InsertBatch(ctx, candles); err != nil {
		log.Fatalf("Failed to insert candles: %v", err)
	}

	// Build windows
	log.Println("Building windows...")
	builder := window.NewBuilder(window.Config{
		W:              cfg.WindowLength,
		S:              cfg.StepSize,
		FeatureVersion: cfg.FeatureVersion,
		Symbol:         cfg.Symbol,
		Timeframe:      cfg.Timeframe,
	})

	windows := builder.ProcessCandles(candles)
	log.Printf("Built %d windows", len(windows))

	// Extract features and store
	log.Println("Extracting features...")
	extractor := feature.NewExtractor(cfg.FeatureVersion, cfg.VectorDim)

	var milvusData []*milvus.WindowData
	var features []*model.FeatureRow

	for i, w := range windows {
		featureRow, shapeVector, err := extractor.Extract(w)
		if err != nil {
			log.Printf("Warning: failed to extract features for window %s: %v", w.WindowID, err)
			continue
		}

		features = append(features, featureRow)
		milvusData = append(milvusData, &milvus.WindowData{
			WindowID:    w.WindowID,
			Embedding:   shapeVector,
			Symbol:      w.Symbol,
			Timeframe:   w.Timeframe,
			TEnd:        w.TEnd,
			VolBucket:   int32(featureRow.VolBucket),
			TrendBucket: int32(featureRow.TrendBucket),
			DataVersion: int32(featureRow.DataVersion),
		})

		if (i+1)%1000 == 0 {
			log.Printf("Processed %d/%d windows", i+1, len(windows))
		}
	}

	// Store windows in DuckDB
	log.Println("Storing windows in DuckDB...")
	if err := windowRepo.InsertBatch(ctx, windows); err != nil {
		log.Fatalf("Failed to insert windows: %v", err)
	}

	// Store features in DuckDB
	log.Println("Storing features in DuckDB...")
	if err := featureRepo.InsertBatch(ctx, features); err != nil {
		log.Fatalf("Failed to insert features: %v", err)
	}

	// Store vectors in Milvus
	log.Println("Storing vectors in Milvus...")
	batchSize := cfg.BatchSize
	for i := 0; i < len(milvusData); i += batchSize {
		end := i + batchSize
		if end > len(milvusData) {
			end = len(milvusData)
		}
		if err := milvusClient.InsertBatch(ctx, milvus.DefaultCollectionName, milvusData[i:end]); err != nil {
			log.Fatalf("Failed to insert vectors: %v", err)
		}
	}

	// Flush Milvus
	if err := milvusClient.Flush(ctx, milvus.DefaultCollectionName); err != nil {
		log.Printf("Warning: failed to flush Milvus: %v", err)
	}

	// Create index
	log.Println("Creating Milvus index...")
	if err := milvusClient.CreateIndex(ctx, milvus.DefaultCollectionName, "embedding"); err != nil {
		log.Printf("Warning: failed to create index: %v", err)
	}

	// Load collection
	if err := milvusClient.LoadCollection(ctx, milvus.DefaultCollectionName); err != nil {
		log.Printf("Warning: failed to load collection: %v", err)
	}

	log.Println("Backfill completed successfully!")
	log.Printf("Summary: %d candles → %d windows → %d vectors", len(candles), len(windows), len(milvusData))

	// Demo: query with the last window
	if len(windows) > 0 {
		demoQuery(ctx, windows[len(windows)-1], extractor, milvusClient, candleRepo)
	}
}

func parseFlags() Config {
	cfg := Config{}

	flag.StringVar(&cfg.CSVPath, "csv", "", "Path to CSV file with candle data")
	flag.StringVar(&cfg.Symbol, "symbol", "BTCUSDT", "Trading symbol")
	flag.StringVar(&cfg.Timeframe, "timeframe", "1m", "Timeframe")
	flag.IntVar(&cfg.WindowLength, "window", 60, "Window length (number of candles)")
	flag.IntVar(&cfg.StepSize, "step", 5, "Step size between windows")
	flag.IntVar(&cfg.FeatureVersion, "version", 1, "Feature version")
	flag.StringVar(&cfg.DuckDBPath, "duckdb", "etna.duckdb", "DuckDB file path")
	flag.StringVar(&cfg.MilvusAddr, "milvus", "localhost:19530", "Milvus server address")
	flag.IntVar(&cfg.VectorDim, "dim", 96, "Vector dimension")
	flag.IntVar(&cfg.BatchSize, "batch", 1000, "Batch size for inserts")

	flag.Parse()

	if cfg.CSVPath == "" {
		fmt.Println("Usage: backfill -csv <path> [options]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	return cfg
}

func demoQuery(ctx context.Context, w *model.Window, extractor *feature.Extractor, milvusClient *milvus.Client, candleRepo *duckdb.CandleRepo) {
	log.Println("\n=== Demo Query ===")
	log.Printf("Query window: %s (TEnd: %s)", w.WindowID, w.TEnd.Format(time.RFC3339))

	// Extract embedding
	_, embedding, _ := extractor.Extract(w)

	// Search
	filter := fmt.Sprintf("symbol == \"%s\" && timeframe == \"%s\"", w.Symbol, w.Timeframe)
	results, err := milvusClient.Search(ctx, milvus.DefaultCollectionName, embedding, filter, 10)
	if err != nil {
		log.Printf("Search failed: %v", err)
		return
	}

	log.Printf("Found %d similar windows:", len(results))

	// Rerank by time
	reranker := rerank.NewReranker(rerank.DefaultTimeDecayConfig())
	ranked := reranker.Rerank(results, time.Now())

	for i, r := range ranked[:min(5, len(ranked))] {
		log.Printf("  %d. %s (Score: %.4f, TimeWeight: %.4f, Final: %.4f, TEnd: %s)",
			i+1, r.WindowID, r.OriginalScore, r.TimeWeight, r.FinalScore, r.TEnd.Format("2006-01-02 15:04"))
	}

	// Calculate outcomes
	log.Println("\nOutcome statistics (placeholder - requires forward candle data):")
	engine := outcome.NewEngine(candleRepo)
	outcomes, err := engine.Calculate(ctx, []*model.Window{w}, []int{5, 20, 60})
	if err != nil {
		log.Printf("Outcome calculation failed: %v", err)
		return
	}

	for _, o := range outcomes {
		log.Printf("  Horizon %d: Mean=%.4f%%, P50=%.4f%%, MDD=%.4f%%",
			o.Horizon, o.FwdRetMean*100, o.FwdRetP50*100, o.MDDP95*100)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
