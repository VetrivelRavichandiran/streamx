package broker

import (
	"sync"

	"github.com/streamx/streamx/internal/broker/storage"
)

// Partition is a single partition hosted by this broker (as leader or
// follower). It wraps a segment log and tracks the high watermark.
type Partition struct {
	Topic    string
	ID       int32
	Replicas []string
	Log      *storage.Log
	ret      struct {
		MaxAgeMS int64
		MaxBytes int64
	}

	mu sync.Mutex
	// hw is the high watermark: the highest offset visible to consumers.
	hw int64
}

// LEO returns the log end offset.
func (p *Partition) LEO() int64 { return p.Log.LEO() }

// LSO returns the log start offset.
func (p *Partition) LSO() int64 { return p.Log.LSO() }

// HW returns the current high watermark.
func (p *Partition) HW() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hw
}

// SetHW updates the high watermark (never decreases).
func (p *Partition) SetHW(hw int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if hw > p.hw {
		p.hw = hw
	}
}

// Append writes a record and returns its offset.
func (p *Partition) Append(rec storage.Record) (int64, error) {
	return p.Log.Append(rec)
}

// ReadAt reads records from offset up to maxBytes.
func (p *Partition) ReadAt(offset int64, maxBytes int) ([]storage.StoredRecord, error) {
	return p.Log.ReadAt(offset, maxBytes)
}

// ReadRange reads all records from offset to LEO.
func (p *Partition) ReadRange(offset int64) ([]storage.StoredRecord, error) {
	return p.Log.ReadRange(offset)
}

// TruncateTo drops records >= offset.
func (p *Partition) TruncateTo(offset int64) error {
	return p.Log.TruncateTo(offset)
}

// Retention runs a retention sweep and returns the new LSO.
func (p *Partition) Retention() (int64, error) {
	return p.Log.Retention(p.ret.MaxAgeMS, p.ret.MaxBytes)
}

// Size returns total bytes.
func (p *Partition) Size() int64 { return p.Log.Size() }

// Close closes the underlying log.
func (p *Partition) Close() error { return p.Log.Close() }
