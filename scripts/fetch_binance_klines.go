package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
)

func main() {
	// Parse flags
	symbol := flag.String("symbol", "BTCUSDT", "Trading symbol")
	interval := flag.String("interval", "1w", "Kline interval (1m, 5m, 1h, 1d, 1w, etc.)")
	limit := flag.Int("limit", 1000, "Number of klines to fetch per request (max 1000)")
	output := flag.String("output", "", "Output CSV file path")
	all := flag.Bool("all", false, "Fetch all available history")
	startTime := flag.Int64("startTime", 1502928000000, "Start time in ms (default: 2017-08-17)")
	flag.Parse()

	if *output == "" {
		*output = fmt.Sprintf("data/%s_%s.csv", *symbol, *interval)
	}

	var allKlines [][]interface{}
	currentStartTime := *startTime

	for {
		url := fmt.Sprintf("https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&limit=%d",
			*symbol, *interval, *limit)

		if *all {
			url += fmt.Sprintf("&startTime=%d", currentStartTime)
		}

		log.Printf("Fetching %s %s klines from Binance (startTime: %d)...", *symbol, *interval, currentStartTime)

		resp, err := http.Get(url)
		if err != nil {
			log.Fatalf("Failed to fetch data: %v", err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close() // Close immediately after reading
		if err != nil {
			log.Fatalf("Failed to read response: %v", err)
		}

		var klines [][]interface{}
		if err := json.Unmarshal(body, &klines); err != nil {
			log.Fatalf("Failed to parse JSON: %v", err)
		}

		if len(klines) == 0 {
			break
		}

		allKlines = append(allKlines, klines...)
		log.Printf("Fetched %d klines (Total: %d)", len(klines), len(allKlines))

		if !*all {
			break
		}

		// Update startTime for next batch (last candle close time + 1ms)
		lastCandle := klines[len(klines)-1]
		closeTime := int64(lastCandle[6].(float64))
		currentStartTime = closeTime + 1

		// Simple rate limiting
		// time.Sleep(100 * time.Millisecond)
	}

	log.Printf("Total fetched: %d klines", len(allKlines))

	// Create output directory if needed
	if err := os.MkdirAll("data", 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Write CSV
	file, err := os.Create(*output)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header (matching CSVProvider expected format)
	writer.Write([]string{"symbol", "timeframe", "open_time", "close_time", "open", "high", "low", "close", "volume", "trades"})

	// Write data
	for _, k := range allKlines {
		// Binance kline format:
		// [0] Open time (ms), [1] Open, [2] High, [3] Low, [4] Close, [5] Volume,
		// [6] Close time (ms), [7] Quote volume, [8] Trades, ...
		openTimeMs := int64(k[0].(float64))
		closeTimeMs := int64(k[6].(float64))
		trades := int64(k[8].(float64))

		row := []string{
			*symbol,
			*interval,
			strconv.FormatInt(openTimeMs, 10),  // open_time in milliseconds
			strconv.FormatInt(closeTimeMs, 10), // close_time in milliseconds
			k[1].(string),                      // open
			k[2].(string),                      // high
			k[3].(string),                      // low
			k[4].(string),                      // close
			k[5].(string),                      // volume
			strconv.FormatInt(trades, 10),      // trades
		}
		writer.Write(row)
	}

	log.Printf("Saved to %s", *output)
}
