package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	rec := Record{
		Key:         "cust-123",
		Payload:     []byte(`{"order":"o-1","amount":42.5}`),
		Headers:     map[string]string{"type": "order.created", "b": "2"},
		ProducerSeq: 7,
		ProducerID:  "p-1",
	}
	enc := EncodeRecord(rec)
	dec, n, err := DecodeRecord(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if n != len(enc) {
		t.Fatalf("consumed %d, want %d", n, len(enc))
	}
	if dec.Key != rec.Key || !bytes.Equal(dec.Payload, rec.Payload) ||
		dec.ProducerSeq != rec.ProducerSeq || dec.ProducerID != rec.ProducerID {
		t.Fatalf("mismatch: %+v", dec)
	}
	if dec.Headers["type"] != "order.created" || dec.Headers["b"] != "2" {
		t.Fatalf("headers mismatch: %+v", dec.Headers)
	}
}

func TestEncodeDecodeCorruptCRC(t *testing.T) {
	rec := Record{Key: "k", Payload: []byte("payload")}
	enc := EncodeRecord(rec)
	enc[15] ^= 0xFF // corrupt a payload byte
	if _, _, err := DecodeRecord(enc); err == nil {
		t.Fatal("expected crc mismatch error")
	}
}

func TestAppendReadTruncate(t *testing.T) {
	dir := t.TempDir()
	l, err := OpenLog(dir, LogConfig{FsyncIntervalMS: 0, SegmentMaxBytes: 1 << 20, PendingBudget: 1 << 20, PendingBudgetN: 100000})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for i := 0; i < 50; i++ {
		off, err := l.Append(Record{Key: "k", Payload: []byte("payload"), ProducerID: "p"})
		if err != nil {
			t.Fatal(err)
		}
		if off != int64(i) {
			t.Fatalf("offset %d, want %d", off, i)
		}
	}
	if l.LEO() != 50 {
		t.Fatalf("LEO %d, want 50", l.LEO())
	}
	// Read from offset 10.
	recs, err := l.ReadAt(10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 40 {
		t.Fatalf("read %d, want 40", len(recs))
	}
	if recs[0].Offset != 10 {
		t.Fatalf("first offset %d, want 10", recs[0].Offset)
	}
	// Truncate to 20.
	if err := l.TruncateTo(20); err != nil {
		t.Fatal(err)
	}
	if l.LEO() != 20 {
		t.Fatalf("LEO after truncate %d, want 20", l.LEO())
	}
	recs, _ = l.ReadAt(0, 1<<20)
	if len(recs) != 20 {
		t.Fatalf("read after truncate %d, want 20", len(recs))
	}
}

func TestRecoveryTruncatesCorruptTail(t *testing.T) {
	dir := t.TempDir()
	l, err := OpenLog(dir, LogConfig{FsyncIntervalMS: 0, SegmentMaxBytes: 1 << 20, PendingBudget: 1 << 20, PendingBudgetN: 100000})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		l.Append(Record{Key: "k", Payload: []byte("payload"), ProducerID: "p"})
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Append a partial/corrupt tail to the active segment.
	logFile := filepath.Join(dir, "segment-00000000000000000000.log")
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0x00, 0x01, 0x02, 0x03, 0x04}) // partial record
	f.Close()
	// Reopen — recovery should truncate the tail and report LEO=10.
	l2, err := OpenLog(dir, LogConfig{FsyncIntervalMS: 0, SegmentMaxBytes: 1 << 20, PendingBudget: 1 << 20, PendingBudgetN: 100000})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.LEO() != 10 {
		t.Fatalf("LEO after recovery %d, want 10", l2.LEO())
	}
	recs, _ := l2.ReadAt(0, 1<<20)
	if len(recs) != 10 {
		t.Fatalf("read after recovery %d, want 10", len(recs))
	}
}

func TestSegmentRoll(t *testing.T) {
	dir := t.TempDir()
	// Tiny segment max so it rolls quickly.
	l, err := OpenLog(dir, LogConfig{FsyncIntervalMS: 0, SegmentMaxBytes: 512, PendingBudget: 1 << 20, PendingBudgetN: 100000})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for i := 0; i < 200; i++ {
		if _, err := l.Append(Record{Key: "k", Payload: bytes.Repeat([]byte("x"), 100), ProducerID: "p"}); err != nil {
			t.Fatal(err)
		}
	}
	if l.LEO() != 200 {
		t.Fatalf("LEO %d, want 200", l.LEO())
	}
	// Should have multiple segments.
	entries, _ := os.ReadDir(dir)
	nSeg := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			nSeg++
		}
	}
	if nSeg < 2 {
		t.Fatalf("expected segment roll, got %d segments", nSeg)
	}
	// Read across the roll boundary.
	recs, _ := l.ReadAt(0, 1<<30)
	if len(recs) != 200 {
		t.Fatalf("read %d, want 200", len(recs))
	}
	if recs[0].Offset != 0 || recs[199].Offset != 199 {
		t.Fatalf("offsets wrong: first=%d last=%d", recs[0].Offset, recs[199].Offset)
	}
}
