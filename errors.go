package qlog

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
)

var errQueueFull = errors.New("qlog: queue full")

type ioFailure struct {
	operation string
	err       error
}

// Keep at most one unresolved failure for each internal I/O operation.
func (w *Writer) recordIOError(operation string, err error) {
	w.mu.Lock()
	w.stats.IOErrors++
	if !slices.ContainsFunc(w.failures, func(f ioFailure) bool { return f.operation == operation }) {
		w.failures = append(w.failures, ioFailure{operation, fmt.Errorf("qlog: %s: %w", operation, err)})
	}
	w.queueErrorLocked(operation, err)
	w.mu.Unlock()
}

func (w *Writer) clearFailure(operation string) {
	w.mu.Lock()
	w.failures = slices.DeleteFunc(w.failures, func(f ioFailure) bool { return f.operation == operation })
	w.mu.Unlock()
}

// recordTransientIOError counts a one-shot failure and emits a diagnostic, but
// does not leave an unresolved failure in Err(), Stats.Failed or qlog_failed.
// Cleanup/Stat failures have no later "successful cleanup" that could clear
// them, so treating them as persistent keeps the failure alert firing forever.
func (w *Writer) recordTransientIOError(operation string, err error) {
	w.mu.Lock()
	w.stats.IOErrors++
	w.queueErrorLocked(operation, err)
	w.mu.Unlock()
}

func (w *Writer) firstFailureLocked() error {
	if len(w.failures) == 0 {
		return nil
	}
	return w.failures[0].err
}

func (w *Writer) queueErrorLocked(operation string, err error) {
	w.diagnostics.record(operation, err, w.stats.FailedRecords, w.stats.FailedBytes)
}

// The reporter owns only a bounded summary, logger and labels, never the Writer
// or its payload buffers. A blocked handler cannot stall writing or Shutdown.
type diagnostics struct {
	logger                     *slog.Logger
	dir, stream                string
	mu                         sync.Mutex
	operation                  string
	err                        error
	events                     uint64
	failedRecords, failedBytes uint64
	wake, stop, done           chan struct{}
}

func newDiagnostics(logger *slog.Logger, dir, stream string) *diagnostics {
	d := &diagnostics{
		logger: logger, dir: dir, stream: stream,
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
	}
	go d.run()
	return d
}

func (d *diagnostics) record(operation string, err error, failedRecords, failedBytes uint64) {
	d.mu.Lock()
	first := d.events == 0
	d.operation, d.err = operation, err
	d.events++
	d.failedRecords, d.failedBytes = failedRecords, failedBytes
	d.mu.Unlock()
	// One wakeup per pending summary, even when producers discard continuously.
	if !first {
		return
	}
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *diagnostics) run() {
	defer close(d.done)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	var next time.Time
	for {
		select {
		case <-d.stop:
			d.emit()
			return
		case <-d.wake:
		case <-timer.C:
		}
		if delay := time.Until(next); delay > 0 {
			timer.Reset(delay)
			continue
		}
		if d.emit() {
			next = time.Now().Add(time.Second)
		}
	}
}

func (d *diagnostics) emit() bool {
	d.mu.Lock()
	if d.events == 0 {
		d.mu.Unlock()
		return false
	}
	operation, err, events := d.operation, d.err, d.events
	failedRecords, failedBytes := d.failedRecords, d.failedBytes
	d.operation, d.err, d.events = "", nil, 0
	d.mu.Unlock()
	// Field names emphasise that operation/error reflect only the most recent
	// event while events and counters are cumulative over the period since the
	// previous emit. This avoids the reading "N failures, all of type X" when N
	// actually aggregates open + write + discard events across different operations.
	d.logger.Error("qlog: logging failure; records may be discarded",
		"directory", d.dir, "stream", d.stream, "last_operation", operation,
		"error", err, "diagnostic_events", events,
		"cumulative_failed_records", failedRecords, "cumulative_failed_bytes", failedBytes)
	return true
}
