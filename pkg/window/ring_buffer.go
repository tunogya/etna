package window

import (
	"sync"

	"github.com/tunogya/etna/pkg/model"
)

// RingBuffer is a circular buffer for candles with fixed capacity
type RingBuffer struct {
	data     []model.Candle
	capacity int
	size     int
	head     int // points to the next write position
	mu       sync.RWMutex
}

// NewRingBuffer creates a new ring buffer with the specified capacity
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		data:     make([]model.Candle, capacity),
		capacity: capacity,
		size:     0,
		head:     0,
	}
}

// Push adds a candle to the buffer
// If the buffer is full, the oldest candle is overwritten
func (rb *RingBuffer) Push(c model.Candle) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.data[rb.head] = c
	rb.head = (rb.head + 1) % rb.capacity
	if rb.size < rb.capacity {
		rb.size++
	}
}

// Size returns the current number of elements in the buffer
func (rb *RingBuffer) Size() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size
}

// IsFull returns true if the buffer is at capacity
func (rb *RingBuffer) IsFull() bool {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size == rb.capacity
}

// Capacity returns the maximum capacity of the buffer
func (rb *RingBuffer) Capacity() int {
	return rb.capacity
}

// ToSlice returns all candles in chronological order (oldest first)
func (rb *RingBuffer) ToSlice() []model.Candle {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	result := make([]model.Candle, rb.size)
	if rb.size == 0 {
		return result
	}

	// Calculate the start position (oldest element)
	start := 0
	if rb.size == rb.capacity {
		start = rb.head
	}

	for i := 0; i < rb.size; i++ {
		idx := (start + i) % rb.capacity
		result[i] = rb.data[idx]
	}

	return result
}

// Last returns the most recent candle
func (rb *RingBuffer) Last() *model.Candle {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size == 0 {
		return nil
	}

	lastIdx := (rb.head - 1 + rb.capacity) % rb.capacity
	c := rb.data[lastIdx]
	return &c
}

// First returns the oldest candle
func (rb *RingBuffer) First() *model.Candle {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size == 0 {
		return nil
	}

	start := 0
	if rb.size == rb.capacity {
		start = rb.head
	}

	c := rb.data[start]
	return &c
}

// Clear empties the buffer
func (rb *RingBuffer) Clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.size = 0
	rb.head = 0
}

// Copy creates a deep copy of the ring buffer
func (rb *RingBuffer) Copy() *RingBuffer {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	newRb := NewRingBuffer(rb.capacity)
	candles := rb.ToSlice()
	for _, c := range candles {
		newRb.Push(c)
	}
	return newRb
}
