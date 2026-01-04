package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/tunogya/etna/pkg/queue/nats"
	"github.com/tunogya/etna/pkg/store/duckdb"
)

// Config holds writer worker configuration
type Config struct {
	NATSUrl    string
	DuckDBPath string
	MilvusAddr string
}

func main() {
	cfg := parseFlags()

	log.Println("Starting Writer Worker...")
	log.Printf("NATS: %s, DuckDB: %s", cfg.NATSUrl, cfg.DuckDBPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	// Initialize NATS
	log.Println("Connecting to NATS...")
	natsClient, err := nats.NewClient(nats.Config{
		URL:        cfg.NATSUrl,
		StreamName: "etna",
	})
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer natsClient.Close()

	// Create stream
	subjects := []string{nats.SubjectCandleWrite, nats.SubjectWindowWrite}
	if err := natsClient.CreateStream(ctx, subjects); err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}
	log.Println("NATS stream ready")

	// Subscribe to candle writes
	candleConsumer, err := natsClient.Subscribe(ctx, nats.SubjectCandleWrite, "candle-writer", func(msg jetstream.Msg) error {
		batch, err := nats.DecodeCandleBatch(msg.Data())
		if err != nil {
			log.Printf("Failed to decode candle batch: %v", err)
			return err
		}

		if len(batch.Candles) == 0 {
			return nil
		}

		if err := candleRepo.InsertBatch(ctx, batch.Candles); err != nil {
			log.Printf("Failed to insert candles: %v", err)
			return err
		}

		log.Printf("Inserted %d candles", len(batch.Candles))
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe to candle writes: %v", err)
	}
	defer candleConsumer.Stop()

	// Subscribe to window writes
	windowConsumer, err := natsClient.Subscribe(ctx, nats.SubjectWindowWrite, "window-writer", func(msg jetstream.Msg) error {
		batch, err := nats.DecodeWindowBatch(msg.Data())
		if err != nil {
			log.Printf("Failed to decode window batch: %v", err)
			return err
		}

		if len(batch.Windows) == 0 {
			return nil
		}

		// Insert windows
		if err := windowRepo.InsertBatch(ctx, batch.Windows); err != nil {
			log.Printf("Failed to insert windows: %v", err)
			return err
		}

		// Insert features
		if len(batch.Features) > 0 {
			if err := featureRepo.InsertBatch(ctx, batch.Features); err != nil {
				log.Printf("Failed to insert features: %v", err)
				return err
			}
		}

		log.Printf("Inserted %d windows with features", len(batch.Windows))
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe to window writes: %v", err)
	}
	defer windowConsumer.Stop()

	log.Println("Writer Worker started, waiting for messages...")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down Writer Worker...")
}

func parseFlags() Config {
	cfg := Config{}

	flag.StringVar(&cfg.NATSUrl, "nats", "nats://localhost:4222", "NATS server URL")
	flag.StringVar(&cfg.DuckDBPath, "duckdb", "etna.duckdb", "DuckDB file path")
	flag.StringVar(&cfg.MilvusAddr, "milvus", "localhost:19530", "Milvus server address")

	flag.Parse()

	if cfg.DuckDBPath == "" {
		fmt.Println("Usage: writer [options]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	return cfg
}
