package qlog

import (
	"context"
	"crypto/rand"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type record struct {
	size   int
	minute int64
}
type diskFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}
type openFunc func(string) (diskFile, error)

// Writer is safe for concurrent producers. Construct it with New; do not copy it.
// One worker owns the log segments and performs their disk I/O.
type Writer struct {
	cfg               Config
	dir               string
	mu                sync.Mutex
	ring              []byte
	records           []record
	read, write, used int
	head, tail, count int
	closing           bool
	// closed is set by run before close(done) so Stats() can read it without
	// a channel receive inside the mutex.
	closed      atomic.Bool
	failures    []ioFailure
	wake        chan struct{}
	done        chan struct{}
	stats       Stats
	now         func() time.Time
	open        openFunc
	seal        func(string) error
	id          string
	diagnostics *diagnostics
	// b is owned exclusively by the run goroutine; allocated in newWriter so
	// that an OOM surfaces as a constructor error rather than a goroutine panic
	// that closes done and makes a concurrent Shutdown return nil.
	b batch
}

// New validates configuration and starts one worker. Directory creation is lazy,
// so an unavailable disk does not prevent construction.
// Active files end in .log.active; only synced, closed files are published as .log.
// Every restart and rotation uses a new segment, leaving sealed files immutable.
func New(cfg Config) (*Writer, error) {
	return newWriter(cfg, time.Now, openLogFile, sealLogFile)
}

func newWriter(cfg Config, now func() time.Time, open openFunc, seal func(string) error) (*Writer, error) {
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(cfg.BaseDir, cfg.Name)
	var id [16]byte
	// crypto/rand.Read always fills the buffer and never returns an error.
	_, _ = rand.Read(id[:])
	w := &Writer{
		cfg: cfg, dir: dir, ring: make([]byte, cfg.QueueBytes), records: make([]record, cfg.QueueRecords),
		wake: make(chan struct{}, 1), done: make(chan struct{}),
		now: now, open: open, seal: seal, id: fmt.Sprintf("%x", id),
		b: batch{data: make([]byte, 0, cfg.BatchBytes), lengths: make([]int, 0, cfg.BatchRecords)},
	}
	w.diagnostics = newDiagnostics(cfg.ErrorLogger, dir, cfg.Name)
	go w.run()
	return w, nil
}

func (w *Writer) segmentName(minute int64, sequence uint64) string {
	return fmt.Sprintf("%s_%s_%s_%06d.log.active", w.cfg.Name,
		time.Unix(minute*60, 0).In(w.cfg.Location).Format("200601021504"), w.id, sequence)
}

// Admission reports what the library did with a record at submission time.
// It never implies the record reached disk.
type Admission int

const (
	// AdmissionQueued means the record was copied into the bounded queue.
	AdmissionQueued Admission = iota
	// AdmissionDropped means the record was discarded (queue full or recovery
	// cooldown). Callers that care about loss should retry or count it.
	AdmissionDropped
	// AdmissionRejected means the call returned an error (closed / oversized).
	AdmissionRejected
)

// TryWrite queues one record without waiting for disk or queue capacity and
// reports whether it was actually admitted. It copies p, strips one trailing
// LF (and optional CR), and appends one LF; embedded LF is preserved.
// On rejection n is zero; otherwise n is len(original), including drops.
func (w *Writer) TryWrite(original []byte) (int, Admission, error) {
	p := original
	if len(p) > 0 && p[len(p)-1] == '\n' {
		p = p[:len(p)-1]
		if len(p) > 0 && p[len(p)-1] == '\r' {
			p = p[:len(p)-1]
		}
	}
	size := len(p) + 1
	var err error
	if size > w.cfg.MaxRecordBytes {
		err = ErrRecordTooLarge
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closing {
		err = ErrClosed
	}
	if err != nil {
		w.recordAttemptLocked(size, err)
		return 0, AdmissionRejected, err
	}
	w.recordAttemptLocked(size, nil)
	if w.count == len(w.records) || size > len(w.ring)-w.used {
		w.stats.FailedRecords++
		w.stats.FailedBytes += uint64(size)
		w.queueErrorLocked("enqueue", errQueueFull)
		w.signalLocked()
		return len(original), AdmissionDropped, nil
	}
	w.copyIn(p)
	w.ring[w.write] = '\n'
	w.write = (w.write + 1) % len(w.ring)
	w.used += size
	w.records[w.tail] = record{size: size, minute: w.now().Unix() / 60}
	w.tail = (w.tail + 1) % len(w.records)
	w.count++
	w.signalLocked()
	return len(original), AdmissionQueued, nil
}

// Write implements io.Writer. A nil error means the library handled the
// record, NOT that it was queued or persisted: queue overflow still returns
// len(p), nil. Use TryWrite when the caller must know whether a record was
// discarded.
func (w *Writer) Write(p []byte) (int, error) {
	n, _, err := w.TryWrite(p)
	return n, err
}

func (w *Writer) copyIn(p []byte) {
	n := copy(w.ring[w.write:], p)
	if n < len(p) {
		copy(w.ring, p[n:])
	}
	w.write = (w.write + len(p)) % len(w.ring)
}

func (w *Writer) signalLocked() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Shutdown stops admission, resolves queued records, and closes the current file.
// Healthy files are synced and sealed; damaged files remain .active.
// I/O errors are logged and counted, not returned. Only context errors are returned.
// Diagnostics finish independently; Shutdown never waits for a logging handler.
// Calls are idempotent and concurrent-safe. A deadline only limits the caller's
// wait: background draining continues; call Shutdown again to await it.
// A blocked OS write/fsync cannot be interrupted safely by a context.
// A nil context returns ErrNilContext without changing the Writer state.
func (w *Writer) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	w.mu.Lock()
	if !w.closing {
		w.closing = true
		w.signalLocked()
	}
	w.mu.Unlock()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		// Prefer a completed worker if it races with context cancellation.
		select {
		case <-w.done:
			return nil
		default:
			return ctx.Err()
		}
	}
}
