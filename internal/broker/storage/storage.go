// Package storage implements the append-only segment log (CONTRACTS.md §6).
//
// On-disk layout per partition:
//
//	<dir>/segment-<baseOffset20>.log   records
//	<dir>/segment-<baseOffset20>.idx   sparse index [relOffset u32][pos u32]
//
// Record binary format (big-endian):
//
//	[crc32c u64][total_len u32][key_len u32][key][payload_len u32][payload]
//	 [hdr_count u16][hdr: klen u32 k vlen u32 v]*[producer_seq u64][pid_len u32 pid]
//
// total_len covers everything after the length field; crc32c is computed over
// the same span. A corrupt/incomplete trailing record in the active segment is
// truncated at recovery.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrBackpressure is returned when the pending-append budget is exceeded.
var ErrBackpressure = errors.New("storage: backpressure budget exceeded")

// Record is a single stored stream record (the compact on-disk form).
type Record struct {
	Key         string
	Payload     []byte
	Headers     map[string]string
	ProducerSeq int64
	ProducerID  string
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// EncodeRecord serializes rec into the binary record format.
func EncodeRecord(rec Record) []byte {
	// Compute body length (everything after total_len).
	bodyLen := 4 + len(rec.Key) + 4 + len(rec.Payload) + 2
	for k, v := range rec.Headers {
		bodyLen += 4 + len(k) + 4 + len(v)
	}
	bodyLen += 8 + 4 + len(rec.ProducerID)

	var buf []byte
	put := func(b []byte) { buf = append(buf, b...) }
	crcPos := len(buf)
	put(make([]byte, 8)) // crc32c placeholder
	var tl [4]byte
	binary.BigEndian.PutUint32(tl[:], uint32(bodyLen))
	put(tl[:])
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(rec.Key)))
	put(kl[:])
	put([]byte(rec.Key))
	var pl [4]byte
	binary.BigEndian.PutUint32(pl[:], uint32(len(rec.Payload)))
	put(pl[:])
	put(rec.Payload)
	keys := make([]string, 0, len(rec.Headers))
	for k := range rec.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var hc [2]byte
	binary.BigEndian.PutUint16(hc[:], uint16(len(keys)))
	put(hc[:])
	for _, k := range keys {
		v := rec.Headers[k]
		var klen [4]byte
		binary.BigEndian.PutUint32(klen[:], uint32(len(k)))
		put(klen[:])
		put([]byte(k))
		var vlen [4]byte
		binary.BigEndian.PutUint32(vlen[:], uint32(len(v)))
		put(vlen[:])
		put([]byte(v))
	}
	var ps [8]byte
	binary.BigEndian.PutUint64(ps[:], uint64(rec.ProducerSeq))
	put(ps[:])
	var pidl [4]byte
	binary.BigEndian.PutUint32(pidl[:], uint32(len(rec.ProducerID)))
	put(pidl[:])
	put([]byte(rec.ProducerID))

	// CRC32c is computed over the body (everything after the length field,
	// i.e. from key_len to the end) — the same span total_len covers.
	body := buf[crcPos+12:]
	sum := crc32.Checksum(body, crc32cTable)
	var crc [8]byte
	binary.BigEndian.PutUint32(crc[0:4], sum)
	copy(buf[crcPos:crcPos+8], crc[:])
	return buf
}

// DecodeRecord parses a single record from buf (which must start at the record
// boundary). It returns the record and the number of bytes consumed (including
// the 12-byte crc+len header). It verifies the CRC.
func DecodeRecord(buf []byte) (Record, int, error) {
	if len(buf) < 12 {
		return Record{}, 0, errors.New("storage: record too short")
	}
	wantCRC := binary.BigEndian.Uint32(buf[0:4])
	totalLen := binary.BigEndian.Uint32(buf[8:12])
	if uint64(len(buf)) < 12+uint64(totalLen) {
		return Record{}, 0, errors.New("storage: truncated record")
	}
	body := buf[12 : 12+totalLen]
	if got := crc32.Checksum(body, crc32cTable); got != wantCRC {
		return Record{}, 0, errors.New("storage: crc mismatch")
	}
	rec, n, err := parseBody(body)
	if err != nil {
		return Record{}, 0, err
	}
	return rec, 12 + n, nil
}

func parseBody(b []byte) (Record, int, error) {
	var rec Record
	pos := 0
	read := func(n int) ([]byte, error) {
		if pos+n > len(b) {
			return nil, errors.New("storage: body truncated")
		}
		out := b[pos : pos+n]
		pos += n
		return out, nil
	}
	kl, err := read(4)
	if err != nil {
		return rec, 0, err
	}
	keyLen := binary.BigEndian.Uint32(kl)
	kb, err := read(int(keyLen))
	if err != nil {
		return rec, 0, err
	}
	rec.Key = string(kb)
	pl, err := read(4)
	if err != nil {
		return rec, 0, err
	}
	plen := binary.BigEndian.Uint32(pl)
	pb, err := read(int(plen))
	if err != nil {
		return rec, 0, err
	}
	rec.Payload = append([]byte(nil), pb...)
	hc, err := read(2)
	if err != nil {
		return rec, 0, err
	}
	hdrCount := binary.BigEndian.Uint16(hc)
	if hdrCount > 0 {
		rec.Headers = make(map[string]string, hdrCount)
	}
	for i := 0; i < int(hdrCount); i++ {
		klenB, err := read(4)
		if err != nil {
			return rec, 0, err
		}
		klen := binary.BigEndian.Uint32(klenB)
		k, err := read(int(klen))
		if err != nil {
			return rec, 0, err
		}
		vlenB, err := read(4)
		if err != nil {
			return rec, 0, err
		}
		vlen := binary.BigEndian.Uint32(vlenB)
		v, err := read(int(vlen))
		if err != nil {
			return rec, 0, err
		}
		rec.Headers[string(k)] = string(v)
	}
	ps, err := read(8)
	if err != nil {
		return rec, 0, err
	}
	rec.ProducerSeq = int64(binary.BigEndian.Uint64(ps))
	pidl, err := read(4)
	if err != nil {
		return rec, 0, err
	}
	pidLen := binary.BigEndian.Uint32(pidl)
	pid, err := read(int(pidLen))
	if err != nil {
		return rec, 0, err
	}
	rec.ProducerID = string(pid)
	return rec, pos, nil
}

// ---------------------------------------------------------------------------
// Segment
// ---------------------------------------------------------------------------

// Segment is a single on-disk segment (.log + .idx).
type Segment struct {
	baseOff int64
	dir     string
	logPath string
	idxPath string

	log *os.File
	buf *bufferedWriter

	nextOffset int64 // baseOff + count (LEO for this segment)
	idx        map[int64]int64

	lastIdxOffset int64
	lastIdxPos    int64
	lastIdxCount  int64
	lastIdxBytes  int64

	maxTS int64
}

const (
	idxStrideBytes = 4096
	idxStrideCount = 100
	bufWriterSize  = 1 << 20 // 1 MiB
)

func segmentName(base int64) string { return fmt.Sprintf("segment-%020d", base) }

func (s *Segment) Base() int64 { return s.baseOff }
func (s *Segment) Next() int64 { return s.nextOffset }
func (s *Segment) size() int64 {
	fi, err := s.log.Stat()
	if err != nil {
		return 0
	}
	return fi.Size()
}
func (s *Segment) maxTSValue() int64 {
	if s.maxTS > 0 {
		return s.maxTS
	}
	if fi, err := s.log.Stat(); err == nil {
		return fi.ModTime().UnixMilli()
	}
	return 0
}

// openSegment opens (or creates) a segment file for base offset base.
func openSegment(dir string, base int64) (*Segment, error) {
	name := segmentName(base)
	logPath := filepath.Join(dir, name+".log")
	idxPath := filepath.Join(dir, name+".idx")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	s := &Segment{
		baseOff: base,
		dir:     dir,
		logPath: logPath,
		idxPath: idxPath,
		log:     log,
		buf:     newBufferedWriter(log),
		idx:     map[int64]int64{},
	}
	if err := s.loadIndex(); err != nil {
		log.Close()
		return nil, err
	}
	return s, nil
}

// loadIndex scans the .log to (re)derive the LEO and in-memory index. It is
// also the recovery path: a corrupt/incomplete trailing record is truncated.
func (s *Segment) loadIndex() error {
	data, err := os.ReadFile(s.logPath)
	if err != nil {
		return err
	}
	valid := int64(0)
	pos := int64(0)
	for pos < int64(len(data)) {
		_, n, err := DecodeRecord(data[pos:])
		if err != nil {
			break // corrupt or truncated tail
		}
		off := s.baseOff + valid
		if pos-s.lastIdxBytes >= idxStrideBytes || (valid-s.lastIdxCount) >= idxStrideCount {
			s.idx[off] = pos
			s.lastIdxOffset = off
			s.lastIdxPos = pos
			s.lastIdxCount = valid
			s.lastIdxBytes = pos
		}
		valid++
		pos += int64(n)
	}
	if pos != int64(len(data)) {
		// Truncate corrupt/incomplete trailing bytes.
		if err := s.log.Truncate(pos); err != nil {
			return err
		}
		if _, err := s.log.Seek(0, 0); err != nil {
			return err
		}
	}
	s.nextOffset = s.baseOff + valid
	return s.writeIndex()
}

func (s *Segment) writeIndex() error {
	var buf []byte
	offsets := make([]int64, 0, len(s.idx))
	for off := range s.idx {
		offsets = append(offsets, off)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	for _, off := range offsets {
		var e [8]byte
		binary.BigEndian.PutUint32(e[0:4], uint32(off-s.baseOff))
		binary.BigEndian.PutUint32(e[4:8], uint32(s.idx[off]))
		buf = append(buf, e[:]...)
	}
	return os.WriteFile(s.idxPath, buf, 0o644)
}

// append writes one record to the segment and returns its offset.
func (s *Segment) append(rec Record, ts int64) (int64, error) {
	encoded := EncodeRecord(rec)
	off := s.nextOffset
	pos := s.nextOffset - s.baseOff
	filePos, err := s.buf.Write(encoded)
	if err != nil {
		return 0, err
	}
	if (filePos-s.lastIdxBytes) >= idxStrideBytes || (pos-s.lastIdxCount) >= idxStrideCount {
		s.idx[off] = filePos
		s.lastIdxOffset = off
		s.lastIdxPos = filePos
		s.lastIdxCount = pos
		s.lastIdxBytes = filePos
		if err := s.writeIndex(); err != nil {
			return 0, err
		}
	}
	s.nextOffset = off + 1
	if ts > s.maxTS {
		s.maxTS = ts
	}
	return off, nil
}

func (s *Segment) flush() error { return s.buf.Flush() }

// readAt reads records from offset off (absolute) up to maxBytes, scanning
// forward. It seeks to the last sparse-index entry at or before off, then
// scans forward, tracking the absolute offset incrementally.
func (s *Segment) readAt(off int64, maxBytes int) ([]StoredRecord, error) {
	if off >= s.nextOffset {
		return nil, nil
	}
	if off < s.baseOff {
		off = s.baseOff
	}
	data, err := os.ReadFile(s.logPath)
	if err != nil {
		return nil, err
	}
	startPos, startAbs := s.seekIndex(off)
	var out []StoredRecord
	bytes := 0
	pos := int64(startPos)
	abs := startAbs
	for pos < int64(len(data)) {
		rec, n, err := DecodeRecord(data[pos:])
		if err != nil {
			break
		}
		if abs >= off {
			if len(out) > 0 && bytes > maxBytes {
				break
			}
			out = append(out, StoredRecord{Offset: abs, Record: rec})
			bytes += len(rec.Payload)
		}
		abs++
		pos += int64(n)
	}
	return out, nil
}

// seekIndex returns the file position and absolute offset of the last
// sparse-index entry at or before off. If none, returns (0, baseOff).
func (s *Segment) seekIndex(off int64) (int64, int64) {
	if len(s.idx) == 0 {
		return 0, s.baseOff
	}
	offsets := make([]int64, 0, len(s.idx))
	for o := range s.idx {
		offsets = append(offsets, o)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	var bestPos, bestAbs int64
	found := false
	for _, o := range offsets {
		if o <= off {
			bestPos = s.idx[o]
			bestAbs = o
			found = true
		} else {
			break
		}
	}
	if !found {
		return 0, s.baseOff
	}
	return bestPos, bestAbs
}

// truncateTo drops all records >= off. If off <= base, the whole segment is
// removed. Returns whether the segment still has records.
func (s *Segment) truncateTo(off int64) (bool, error) {
	if off <= s.baseOff {
		if err := s.remove(); err != nil {
			return false, err
		}
		return false, nil
	}
	if off >= s.nextOffset {
		return true, nil
	}
	data, err := os.ReadFile(s.logPath)
	if err != nil {
		return false, err
	}
	var kept []byte
	pos := int64(0)
	count := int64(0)
	for pos < int64(len(data)) {
		_, n, err := DecodeRecord(data[pos:])
		if err != nil {
			break
		}
		abs := s.baseOff + count
		if abs >= off {
			break
		}
		kept = append(kept, data[pos:pos+int64(n)]...)
		pos += int64(n)
		count++
	}
	if err := s.buf.Reset(); err != nil {
		return false, err
	}
	if _, err := s.buf.Write(kept); err != nil {
		return false, err
	}
	if err := s.buf.Flush(); err != nil {
		return false, err
	}
	// Rebuild index + maxTS.
	s.idx = map[int64]int64{}
	s.lastIdxOffset = s.baseOff
	s.lastIdxPos = 0
	s.lastIdxCount = 0
	s.lastIdxBytes = 0
	s.maxTS = 0
	newData, _ := os.ReadFile(s.logPath)
	p := int64(0)
	c := int64(0)
	for p < int64(len(newData)) {
		_, n, err := DecodeRecord(newData[p:])
		if err != nil {
			break
		}
		if p-s.lastIdxBytes >= idxStrideBytes || (c-s.lastIdxCount) >= idxStrideCount {
			s.idx[s.baseOff+c] = p
			s.lastIdxOffset = s.baseOff + c
			s.lastIdxPos = p
			s.lastIdxCount = c
			s.lastIdxBytes = p
		}
		p += int64(n)
		c++
	}
	s.nextOffset = s.baseOff + c
	if err := s.writeIndex(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Segment) remove() error {
	s.buf.Close()
	os.Remove(s.logPath)
	os.Remove(s.idxPath)
	return nil
}

// StoredRecord pairs an absolute offset with its record.
type StoredRecord struct {
	Offset int64
	Record Record
}

// bufferedWriter is a simple buffered writer that tracks the byte position.
type bufferedWriter struct {
	f   *os.File
	buf []byte
	pos int64
}

func newBufferedWriter(f *os.File) *bufferedWriter {
	fi, _ := f.Stat()
	pos := int64(0)
	if fi != nil {
		pos = fi.Size()
	}
	return &bufferedWriter{f: f, buf: make([]byte, 0, bufWriterSize), pos: pos}
}

func (w *bufferedWriter) Write(b []byte) (int64, error) {
	start := w.pos
	w.buf = append(w.buf, b...)
	if len(w.buf) >= bufWriterSize {
		if err := w.Flush(); err != nil {
			return 0, err
		}
	}
	return start, nil
}

func (w *bufferedWriter) Flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	if _, err := w.f.Write(w.buf); err != nil {
		return err
	}
	w.pos += int64(len(w.buf))
	w.buf = w.buf[:0]
	return nil
}

func (w *bufferedWriter) Sync() error {
	if err := w.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *bufferedWriter) Reset() error {
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, 0); err != nil {
		return err
	}
	w.pos = 0
	w.buf = w.buf[:0]
	return nil
}

func (w *bufferedWriter) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}

// ---------------------------------------------------------------------------
// Log — a partition's ordered set of segments
// ---------------------------------------------------------------------------

// Log is the append-only segment log for a single partition.
type Log struct {
	dir          string
	mu           sync.Mutex
	segments     []*Segment
	nextOffset   int64
	logStart     int64
	fsyncPolicy  int64
	lastFsync    time.Time
	segmentMax   int64
	pendingBytes int64
	pendingRecs  int64
	budgetBytes  int64
	budgetRecs   int64
}

// LogConfig configures a Log.
type LogConfig struct {
	FsyncIntervalMS int64
	SegmentMaxBytes int64
	PendingBudget   int64
	PendingBudgetN  int64
}

// OpenLog opens (or creates) the log for a partition directory. It recovers
// any corrupt trailing record and rebuilds LEO/LSO.
func OpenLog(dir string, cfg LogConfig) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []*Segment
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "segment-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		baseStr := strings.TrimSuffix(strings.TrimPrefix(name, "segment-"), ".log")
		base, err := strconv.ParseInt(baseStr, 10, 64)
		if err != nil {
			continue
		}
		seg, err := openSegment(dir, base)
		if err != nil {
			return nil, err
		}
		segs = append(segs, seg)
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].baseOff < segs[j].baseOff })
	l := &Log{
		dir:         dir,
		segments:    segs,
		fsyncPolicy: cfg.FsyncIntervalMS,
		segmentMax:  cfg.SegmentMaxBytes,
		budgetBytes: cfg.PendingBudget,
		budgetRecs:  cfg.PendingBudgetN,
		lastFsync:   time.Now(),
	}
	if len(segs) == 0 {
		seg, err := openSegment(dir, 0)
		if err != nil {
			return nil, err
		}
		l.segments = append(l.segments, seg)
		l.nextOffset = 0
		l.logStart = 0
		return l, nil
	}
	last := segs[len(segs)-1]
	l.nextOffset = last.nextOffset
	l.logStart = segs[0].baseOff
	return l, nil
}

func (l *Log) active() *Segment { return l.segments[len(l.segments)-1] }

// Append writes rec to the log and returns its offset.
func (l *Log) Append(rec Record) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.budgetBytes > 0 && l.pendingBytes > l.budgetBytes {
		return 0, ErrBackpressure
	}
	if l.budgetRecs > 0 && l.pendingRecs > l.budgetRecs {
		return 0, ErrBackpressure
	}
	act := l.active()
	if act.size() >= l.segmentMax && l.segmentMax > 0 {
		if err := l.rollLocked(); err != nil {
			return 0, err
		}
		act = l.active()
	}
	ts := time.Now().UnixMilli()
	off, err := act.append(rec, ts)
	if err != nil {
		return 0, err
	}
	l.nextOffset = act.nextOffset
	l.pendingBytes += int64(len(rec.Payload)) + int64(len(rec.Key)) + 16
	l.pendingRecs++
	if err := l.fsyncLocked(); err != nil {
		return 0, err
	}
	return off, nil
}

func (l *Log) rollLocked() error {
	newBase := l.nextOffset
	seg, err := openSegment(l.dir, newBase)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, seg)
	return nil
}

func (l *Log) fsyncLocked() error {
	if l.fsyncPolicy == -1 {
		return nil
	}
	if l.fsyncPolicy == 0 {
		if err := l.active().flush(); err != nil {
			return err
		}
		l.pendingBytes = 0
		l.pendingRecs = 0
		return nil
	}
	if time.Since(l.lastFsync) >= time.Duration(l.fsyncPolicy)*time.Millisecond {
		if err := l.active().flush(); err != nil {
			return err
		}
		if err := l.active().log.Sync(); err != nil {
			return err
		}
		l.lastFsync = time.Now()
		l.pendingBytes = 0
		l.pendingRecs = 0
	}
	return nil
}

// ReadAt reads records from offset off (absolute) up to maxBytes, spanning
// segment boundaries as needed.
func (l *Log) ReadAt(off int64, maxBytes int) ([]StoredRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if off < l.logStart {
		off = l.logStart
	}
	if off >= l.nextOffset {
		return nil, nil
	}
	// Ensure the active segment's buffered writes are visible on disk.
	if err := l.active().flush(); err != nil {
		return nil, err
	}
	var out []StoredRecord
	used := 0
	for _, seg := range l.segments {
		if seg.nextOffset <= off {
			continue // segment entirely before off
		}
		if used > maxBytes {
			break
		}
		start := off
		if start < seg.baseOff {
			start = seg.baseOff
		}
		recs, err := seg.readAt(start, maxBytes-used)
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
		for _, r := range recs {
			used += len(r.Record.Payload)
		}
		off = seg.nextOffset
	}
	return out, nil
}

// ReadRange reads all records in [off, nextOffset).
func (l *Log) ReadRange(off int64) ([]StoredRecord, error) {
	return l.ReadAt(off, 1<<30)
}

// Len returns the number of records (LEO - LSO).
func (l *Log) Len() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextOffset - l.logStart
}

// LEO returns the next offset to assign.
func (l *Log) LEO() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextOffset
}

// LSO returns the earliest readable offset.
func (l *Log) LSO() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.logStart
}

// BaseOffset returns the base offset of the first segment.
func (l *Log) BaseOffset() int64 { return l.logStart }

// TruncateTo drops all records >= off.
func (l *Log) TruncateTo(off int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var kept []*Segment
	for _, seg := range l.segments {
		if off <= seg.baseOff {
			seg.remove()
			continue
		}
		remains, err := seg.truncateTo(off)
		if err != nil {
			return err
		}
		if remains {
			kept = append(kept, seg)
		}
	}
	if len(kept) == 0 {
		seg, err := openSegment(l.dir, 0)
		if err != nil {
			return err
		}
		kept = []*Segment{seg}
		l.nextOffset = 0
	}
	l.segments = kept
	l.nextOffset = kept[len(kept)-1].nextOffset
	l.logStart = kept[0].baseOff
	return nil
}

// Retention deletes rolled segments older than maxAgeMS or over maxBytes
// budget. Never deletes the active segment. Returns the new LSO.
func (l *Log) Retention(maxAgeMS, maxBytes int64) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.segments) <= 1 {
		return l.logStart, nil
	}
	now := time.Now().UnixMilli()
	if maxAgeMS > 0 {
		cutoff := now - maxAgeMS
		var kept []*Segment
		for i, seg := range l.segments {
			if i == len(l.segments)-1 {
				kept = append(kept, seg)
				continue
			}
			if seg.maxTSValue() > 0 && seg.maxTSValue() < cutoff {
				seg.remove()
			} else {
				kept = append(kept, seg)
			}
		}
		l.segments = kept
	}
	if maxBytes > 0 && len(l.segments) > 1 {
		total := int64(0)
		for _, seg := range l.segments {
			total += seg.size()
		}
		for len(l.segments) > 1 && total > maxBytes {
			seg := l.segments[0]
			total -= seg.size()
			seg.remove()
			l.segments = l.segments[1:]
		}
	}
	if len(l.segments) == 0 {
		seg, err := openSegment(l.dir, 0)
		if err != nil {
			return 0, err
		}
		l.segments = []*Segment{seg}
		l.nextOffset = 0
	}
	l.logStart = l.segments[0].baseOff
	l.nextOffset = l.segments[len(l.segments)-1].nextOffset
	return l.logStart, nil
}

// Size returns total bytes across all segments.
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var total int64
	for _, seg := range l.segments {
		total += seg.size()
	}
	return total
}

// Close flushes and closes all segments.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var firstErr error
	for _, seg := range l.segments {
		if err := seg.buf.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Dir returns the log directory.
func (l *Log) Dir() string { return l.dir }
