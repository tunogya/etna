# Etna

A Go-based K-line data pipeline for cryptocurrency market analysis, featuring sliding window processing, vector similarity search, and forward return statistics.

## Overview

Etna provides two main pipelines:

1. **Batch Pipeline (Backfill)**: Process historical K-line data → Build windows → Generate features → Store in DuckDB + Milvus
2. **Streaming Pipeline (Real-time)**: Subscribe to live K-line data → Incremental window updates → Real-time storage and analysis

Both pipelines share the same core components: window building, feature extraction, embedding, and storage.

## Architecture

```
Market Data → Window Builder → Feature/Normalization → Vector Store → Outcome Analysis
```

### Core Modules

| Module | Description |
|--------|-------------|
| **Market Data** | Unified K-line input layer (Backfill/Stream providers) |
| **Window Builder** | Sliding window manager with configurable length and step |
| **Feature Engine** | Normalize candle data and extract structured features |
| **DuckDB Storage** | Persist raw candles, windows, and features for SQL queries |
| **Milvus Vector Store** | Store embeddings for TopK similarity search |
| **Time Weighting & Reranker** | Prioritize recent data with decay-based ranking |
| **Outcome Engine** | Calculate forward returns and risk metrics |

## Project Structure

```
pkg/
├── model/       # Core data structures (Candle, Window, FeatureRow)
├── data/        # Data providers (BackfillProvider, StreamProvider)
├── window/      # Window builder with ring buffer implementation
├── feature/     # Feature calculation and normalization
├── embed/       # Embedding implementations (IdentityEmbedder)
├── store/
│   ├── duckdb/  # DuckDB schema, upsert, and query operations
│   └── milvus/  # Milvus collection management and search
├── rerank/      # Time decay reranking
└── outcome/     # Forward returns and MDD calculation

cmd/
├── backfill/    # Batch processing entry point
├── stream/      # Real-time processing entry point
└── api/         # Query interface (optional)
```

## Key Concepts

### Window Configuration

| Parameter | Description | Recommended Value |
|-----------|-------------|-------------------|
| `tf` | Timeframe | `1m` or `5m` (choose one) |
| `W` | Window length | 60 for 1m, 24 for 5m |
| `S` | Step size | 5 for backfill, 1 for streaming |
| `dim` | Vector dimension | 96 or 128 |
| `data_version` | Schema version | Start at 1, increment on changes |

### Window ID Generation

```
window_id = hash(symbol | tf | t_end | W | feature_version)
```

This ensures idempotent writes and prevents duplicate processing.

### Time-Weighted Processing

**Data Ingestion Priority:**
- Backfill in reverse chronological order (recent → historical)
- New data available for queries faster

**Query Reranking:**
```
final_score = similarity_score × time_decay(t_end)
time_decay = exp(-λ × age_days)
```

Or use segment weights:
- Last 3 days: ×1.0
- Last 30 days: ×0.7
- Older: ×0.4

## Database Schema

### DuckDB Tables

**candles** (fact table)
```sql
CREATE TABLE candles (
    symbol VARCHAR,
    tf VARCHAR,
    open_time TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    PRIMARY KEY (symbol, tf, open_time)
);
```

**windows** (index table)
```sql
CREATE TABLE windows (
    window_id VARCHAR PRIMARY KEY,
    symbol VARCHAR,
    tf VARCHAR,
    t_end TIMESTAMP,
    W INTEGER,
    feature_version INTEGER,
    created_at TIMESTAMP
);
```

**window_features** (feature table)
```sql
CREATE TABLE window_features (
    window_id VARCHAR PRIMARY KEY,
    trend_slope DOUBLE,
    realized_volatility DOUBLE,
    max_drawdown DOUBLE,
    atr DOUBLE,
    vol_bucket INTEGER,
    trend_bucket INTEGER,
    data_version INTEGER
);
```

**window_outcomes** (cache table, optional)
```sql
CREATE TABLE window_outcomes (
    window_id VARCHAR,
    horizon INTEGER,
    fwd_ret_mean DOUBLE,
    fwd_ret_p50 DOUBLE,
    mdd_p95 DOUBLE,
    PRIMARY KEY (window_id, horizon)
);
```

### Milvus Collection

**Collection:** `kline_windows`

| Field | Type | Description |
|-------|------|-------------|
| `window_id` | VARCHAR (PK) | Unique window identifier |
| `embedding` | FLOAT_VECTOR[dim] | Feature vector |
| `symbol` | VARCHAR | Trading pair |
| `tf` | VARCHAR | Timeframe |
| `t_end` | INT64 | End timestamp |
| `vol_bucket` | INT | Volatility bucket |
| `trend_bucket` | INT | Trend bucket |
| `data_version` | INT | Schema version |

**Search Pattern:**
```
filter: symbol == "BTCUSDT" AND tf == "1m" AND data_version == X AND t_end >= now - 30d
then: TopK nearest neighbors on embedding
```

## Milestones

### Milestone A: Batch Pipeline (No Real-time)

1. ✅ Import historical BTCUSDT candles → DuckDB
2. ✅ Window Builder produces windows
3. ✅ Feature/Normalization → window_features
4. ✅ Write to Milvus (using ShapeVector directly)
5. ✅ TopK search for a given window
6. ✅ Outcome Engine calculates +20 bar forward returns

### Milestone B: Time Priority & Caching

1. ⬜ Backfill with reverse chronological order
2. ⬜ Add time decay reranking to search
3. ⬜ Cache outcome results in window_outcomes

### Milestone C: Real-time Streaming

1. ⬜ Implement StreamProvider for live candles
2. ⬜ Incremental window production
3. ⬜ Real-time writes to DuckDB + Milvus
4. ⬜ Auto-trigger search and analysis on new candles

## Getting Started

### Prerequisites

- Go 1.25+
- DuckDB
- Milvus 2.x

### Installation

```bash
git clone https://github.com/tunogya/etna.git
cd etna
go mod tidy
```

### Usage

```bash
# Run backfill pipeline
go run cmd/backfill/main.go

# Run streaming pipeline
go run cmd/stream/main.go

# Start API server (optional)
go run cmd/api/main.go
```

## License

MIT License
