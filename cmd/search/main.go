package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/tunogya/etna/pkg/feature"
	"github.com/tunogya/etna/pkg/rerank"
	"github.com/tunogya/etna/pkg/store/duckdb"
	"github.com/tunogya/etna/pkg/store/milvus"
	"github.com/tunogya/etna/pkg/window"
)

type Config struct {
	Symbol    string
	Timeframe string

	WindowLength   int
	StepSize       int
	FeatureVersion int

	DuckDBPath string
	MilvusAddr string
	TopK       int
}

func main() {
	cfg := parseFlags()

	ctx := context.Background()

	// Initialize DuckDB
	log.Println("Connecting to DuckDB...")
	duckClient, err := duckdb.NewClient(cfg.DuckDBPath)
	if err != nil {
		log.Fatalf("Failed to connect to DuckDB: %v", err)
	}
	defer duckClient.Close()

	candleRepo := duckdb.NewCandleRepo(duckClient)

	// Fetch latest candles for the window
	// Need enough candles for one window
	log.Printf("Fetching latest %d candles for %s %s...", cfg.WindowLength, cfg.Symbol, cfg.Timeframe)

	// We fetch plenty more just in case, but really we just need the latest N
	// Since we don't have a GetLatest API on repo easily without scanning, let's just fetch a recent range
	// Or better, fetch from Time.Now() backwards if supported, or just fetch all and take last (inefficient but works for now)
	// A better approach: DuckDB query "SELECT * FROM candles WHERE symbol=? AND timeframe=? ORDER BY close_time DESC LIMIT ?"

	candles, err := candleRepo.GetLatest(ctx, cfg.Symbol, cfg.Timeframe, cfg.WindowLength)
	if err != nil {
		log.Fatalf("Failed to fetch latest candles: %v", err)
	}

	if len(candles) < cfg.WindowLength {
		log.Fatalf("Not enough candles found. Need %d, got %d", cfg.WindowLength, len(candles))
	}

	// Ensure they are sorted by time (GetLatest usually returns DESC, we need ASC)
	sort.Slice(candles, func(i, j int) bool {
		return candles[i].OpenTime.Before(candles[j].OpenTime)
	})

	log.Printf("Latest candle: %s", candles[len(candles)-1].CloseTime.Format(time.RFC3339))

	// Build SINGLE current window
	builder := window.NewBuilder(window.Config{
		W:              cfg.WindowLength,
		S:              cfg.StepSize,
		FeatureVersion: cfg.FeatureVersion,
		Symbol:         cfg.Symbol,
		Timeframe:      cfg.Timeframe,
	})

	// ProcessCandles usually handles sliding windows. Since we have exactly W candles (or slightly more),
	// passing them might just generate 1 window if count == W.
	windows := builder.ProcessCandles(candles)
	if len(windows) == 0 {
		log.Fatalf("Failed to build window from candles")
	}

	currentWindow := windows[len(windows)-1] // Take the very last one
	log.Printf("Built analysis window: %s (TEnd: %s)", currentWindow.WindowID, currentWindow.TEnd.Format(time.RFC3339))

	// Extract features
	extractor := feature.NewExtractor(cfg.FeatureVersion, 96) // 96 dim is standard for now
	_, embedding, err := extractor.Extract(currentWindow)
	if err != nil {
		log.Fatalf("Failed to extract features: %v", err)
	}

	// Initialize Milvus
	log.Println("Connecting to Milvus...")
	milvusClient, err := milvus.NewClient(ctx, milvus.Config{Address: cfg.MilvusAddr})
	if err != nil {
		log.Fatalf("Failed to connect to Milvus: %v", err)
	}
	defer milvusClient.Close()

	if err := milvusClient.LoadCollection(ctx, milvus.DefaultCollectionName); err != nil {
		log.Fatalf("Failed to load collection: %v", err)
	}

	// Search
	log.Printf("Searching for %d most similar windows...", cfg.TopK)
	filter := fmt.Sprintf("symbol == \"%s\" && timeframe == \"%s\"", cfg.Symbol, cfg.Timeframe)
	results, err := milvusClient.Search(ctx, milvus.DefaultCollectionName, embedding, filter, cfg.TopK)
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}

	// Results
	log.Println("\n=== Search Results ===")

	// Rerank (optional, using time decay as in backfill demo)
	reranker := rerank.NewReranker(rerank.DefaultTimeDecayConfig())
	ranked := reranker.Rerank(results, time.Now())

	fmt.Printf("%-5s %-32s %-20s %-10s %-10s\n", "Rank", "WindowID", "End Date", "Score", "Sim%")
	fmt.Println("--------------------------------------------------------------------------------")

	for i, r := range ranked {
		// Ignore the query window itself if it appears (which it might if it was backfilled)
		if r.WindowID == currentWindow.WindowID {
			continue
		}

		simPct := r.OriginalScore * 100
		fmt.Printf("%-5d %-32s %-20s %-.4f     %-.2f%%\n", i+1, r.WindowID, r.TEnd.Format("2006-01-02"), r.OriginalScore, simPct)
	}
}

func parseFlags() Config {
	cfg := Config{}

	flag.StringVar(&cfg.Symbol, "symbol", "BTCUSDT", "Trading symbol")
	flag.StringVar(&cfg.Timeframe, "timeframe", "1d", "Timeframe")
	flag.IntVar(&cfg.WindowLength, "window", 7, "Window length")
	flag.IntVar(&cfg.StepSize, "step", 1, "Step size")
	flag.IntVar(&cfg.FeatureVersion, "version", 1, "Feature version")
	flag.StringVar(&cfg.DuckDBPath, "duckdb", "etna.duckdb", "DuckDB path")
	flag.StringVar(&cfg.MilvusAddr, "milvus", "localhost:19530", "Milvus address")
	flag.IntVar(&cfg.TopK, "topk", 10, "Top K results")

	flag.Parse()
	return cfg
}
