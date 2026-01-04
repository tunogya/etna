package duckdb

import (
	"context"
	"fmt"

	"github.com/tunogya/etna/pkg/model"
)

// FeatureRepo handles window feature data persistence
type FeatureRepo struct {
	client *Client
}

// NewFeatureRepo creates a new feature repository
func NewFeatureRepo(client *Client) *FeatureRepo {
	return &FeatureRepo{client: client}
}

// Insert inserts a single feature row
func (r *FeatureRepo) Insert(ctx context.Context, f *model.FeatureRow) error {
	query := `
		INSERT INTO window_features (
			window_id, trend_slope, realized_volatility, max_drawdown,
			atr, vol_z_score, vol_bucket, trend_bucket, data_version
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (window_id) DO UPDATE SET
			trend_slope = EXCLUDED.trend_slope,
			realized_volatility = EXCLUDED.realized_volatility,
			max_drawdown = EXCLUDED.max_drawdown,
			atr = EXCLUDED.atr,
			vol_z_score = EXCLUDED.vol_z_score,
			vol_bucket = EXCLUDED.vol_bucket,
			trend_bucket = EXCLUDED.trend_bucket,
			data_version = EXCLUDED.data_version
	`
	return r.client.Exec(query,
		f.WindowID, f.TrendSlope, f.RealizedVolatility, f.MaxDrawdown,
		f.ATR, f.VolZScore, f.VolBucket, f.TrendBucket, f.DataVersion,
	)
}

// InsertBatch inserts multiple feature rows in a transaction
func (r *FeatureRepo) InsertBatch(ctx context.Context, features []*model.FeatureRow) error {
	tx, err := r.client.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO window_features (
			window_id, trend_slope, realized_volatility, max_drawdown,
			atr, vol_z_score, vol_bucket, trend_bucket, data_version
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (window_id) DO UPDATE SET
			trend_slope = EXCLUDED.trend_slope,
			realized_volatility = EXCLUDED.realized_volatility,
			max_drawdown = EXCLUDED.max_drawdown,
			atr = EXCLUDED.atr,
			vol_z_score = EXCLUDED.vol_z_score,
			vol_bucket = EXCLUDED.vol_bucket,
			trend_bucket = EXCLUDED.trend_bucket,
			data_version = EXCLUDED.data_version
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, f := range features {
		_, err := stmt.Exec(
			f.WindowID, f.TrendSlope, f.RealizedVolatility, f.MaxDrawdown,
			f.ATR, f.VolZScore, f.VolBucket, f.TrendBucket, f.DataVersion,
		)
		if err != nil {
			return fmt.Errorf("failed to insert feature: %w", err)
		}
	}

	return tx.Commit()
}

// GetByID retrieves a feature row by window ID
func (r *FeatureRepo) GetByID(ctx context.Context, windowID string) (*model.FeatureRow, error) {
	query := `
		SELECT window_id, trend_slope, realized_volatility, max_drawdown,
			   atr, vol_z_score, vol_bucket, trend_bucket, data_version
		FROM window_features
		WHERE window_id = ?
	`

	row := r.client.QueryRow(query, windowID)
	var f model.FeatureRow
	err := row.Scan(
		&f.WindowID, &f.TrendSlope, &f.RealizedVolatility, &f.MaxDrawdown,
		&f.ATR, &f.VolZScore, &f.VolBucket, &f.TrendBucket, &f.DataVersion,
	)
	if err != nil {
		return nil, err
	}

	return &f, nil
}

// GetByBuckets retrieves features matching specific bucket filters
func (r *FeatureRepo) GetByBuckets(ctx context.Context, volBucket, trendBucket int, limit int) ([]*model.FeatureRow, error) {
	query := `
		SELECT window_id, trend_slope, realized_volatility, max_drawdown,
			   atr, vol_z_score, vol_bucket, trend_bucket, data_version
		FROM window_features
		WHERE vol_bucket = ? AND trend_bucket = ?
		LIMIT ?
	`

	rows, err := r.client.Query(query, volBucket, trendBucket, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query features: %w", err)
	}
	defer rows.Close()

	var features []*model.FeatureRow
	for rows.Next() {
		var f model.FeatureRow
		err := rows.Scan(
			&f.WindowID, &f.TrendSlope, &f.RealizedVolatility, &f.MaxDrawdown,
			&f.ATR, &f.VolZScore, &f.VolBucket, &f.TrendBucket, &f.DataVersion,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan feature: %w", err)
		}
		features = append(features, &f)
	}

	return features, nil
}
