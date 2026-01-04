package nats

import (
	"encoding/json"
	"time"

	"github.com/tunogya/etna/pkg/model"
)

// Subject constants
const (
	SubjectCandleWrite = "etna.candles.write"
	SubjectWindowWrite = "etna.windows.write"
)

// CandleWriteMsg represents a single candle write request
type CandleWriteMsg struct {
	Candle *model.Candle `json:"candle"`
}

// CandleBatchMsg represents a batch candle write request
type CandleBatchMsg struct {
	Candles []model.Candle `json:"candles"`
}

// WindowWriteMsg represents a window write request with features
type WindowWriteMsg struct {
	Window  *model.Window     `json:"window"`
	Feature *model.FeatureRow `json:"feature"`
}

// WindowBatchMsg represents a batch window write request
type WindowBatchMsg struct {
	Windows  []*model.Window     `json:"windows"`
	Features []*model.FeatureRow `json:"features"`
}

// MilvusWriteMsg represents a Milvus vector write request
type MilvusWriteMsg struct {
	WindowID    string    `json:"window_id"`
	Embedding   []float32 `json:"embedding"`
	Symbol      string    `json:"symbol"`
	Timeframe   string    `json:"timeframe"`
	TEnd        time.Time `json:"t_end"`
	VolBucket   int32     `json:"vol_bucket"`
	TrendBucket int32     `json:"trend_bucket"`
	DataVersion int32     `json:"data_version"`
}

// Encode serializes a message to JSON bytes
func Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// DecodeCandleBatch deserializes a CandleBatchMsg from JSON bytes
func DecodeCandleBatch(data []byte) (*CandleBatchMsg, error) {
	var msg CandleBatchMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// DecodeWindowBatch deserializes a WindowBatchMsg from JSON bytes
func DecodeWindowBatch(data []byte) (*WindowBatchMsg, error) {
	var msg WindowBatchMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// DecodeCandleWrite deserializes a CandleWriteMsg from JSON bytes
func DecodeCandleWrite(data []byte) (*CandleWriteMsg, error) {
	var msg CandleWriteMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// DecodeWindowWrite deserializes a WindowWriteMsg from JSON bytes
func DecodeWindowWrite(data []byte) (*WindowWriteMsg, error) {
	var msg WindowWriteMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
