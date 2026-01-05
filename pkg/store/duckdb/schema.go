package duckdb

import "fmt"

// Schema contains table creation statements for all required tables

// CreateCandlesTable creates the candles fact table
const CreateCandlesTable = `
CREATE TABLE IF NOT EXISTS candles (
    symbol VARCHAR NOT NULL,
    timeframe VARCHAR NOT NULL,
    open_time TIMESTAMP NOT NULL,
    close_time TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    trades BIGINT,
    vwap DOUBLE,
    PRIMARY KEY (symbol, timeframe, open_time)
);
`

// CreateWindowsTable creates the windows index table
const CreateWindowsTable = `
CREATE TABLE IF NOT EXISTS windows (
    window_id VARCHAR PRIMARY KEY,
    symbol VARCHAR NOT NULL,
    timeframe VARCHAR NOT NULL,
    t_end TIMESTAMP NOT NULL,
    w INTEGER NOT NULL,
    feature_version INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_windows_symbol_tf ON windows(symbol, timeframe);
CREATE INDEX IF NOT EXISTS idx_windows_t_end ON windows(t_end);
`

// CreateWindowFeaturesTable creates the window features table
const CreateWindowFeaturesTable = `
CREATE TABLE IF NOT EXISTS window_features (
    window_id VARCHAR PRIMARY KEY,
    trend_slope DOUBLE,
    realized_volatility DOUBLE,
    max_drawdown DOUBLE,
    atr DOUBLE,
    vol_z_score DOUBLE,
    vol_bucket INTEGER,
    trend_bucket INTEGER,
    data_version INTEGER NOT NULL
);
`

// CreateWindowOutcomesTable creates the window outcomes cache table
const CreateWindowOutcomesTable = `
CREATE TABLE IF NOT EXISTS window_outcomes (
    window_id VARCHAR NOT NULL,
    horizon INTEGER NOT NULL,
    fwd_ret_mean DOUBLE,
    fwd_ret_p10 DOUBLE,
    fwd_ret_p50 DOUBLE,
    fwd_ret_p90 DOUBLE,
    mdd_p95 DOUBLE,
    PRIMARY KEY (window_id, horizon)
);
`

// InitializeSchema creates all required tables
func InitializeSchema(c *Client) error {
	schemas := []string{
		CreateCandlesTable,
		CreateWindowsTable,
		CreateWindowFeaturesTable,
		CreateWindowOutcomesTable,
	}

	for _, schema := range schemas {
		if err := c.Exec(schema); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
	}

	return nil
}

// DropAllTables drops all tables (use with caution)
func DropAllTables(c *Client) error {
	tables := []string{"window_outcomes", "window_features", "windows", "candles"}
	for _, table := range tables {
		if err := c.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", table, err)
		}
	}
	return nil
}
