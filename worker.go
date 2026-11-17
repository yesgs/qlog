package qlog

import (
	"errors"
	"os"
	"path/filepath"
	"time"
)

type batch struct {
	data    []byte
	lengths []int
	minute  int64
}

// fill copies queued records into one contiguous batch, releasing queue capacity.
// All records in a batch target the same minute file. Only this worker consumes
// the queue; producers cannot reuse occupied slots until the copy is committed.
//
// INVARIANT: Between the two mu acquisitions this function reads exactly the
// snapshot range [head, head+queued) of w.records and [read, read+size) of
// w.ring, and zeroes only those record slots. Producers append at w.tail,
// which equals (head+queued) mod len at snapshot time, so the accessed ranges
// never overlap. Any code added to the unlocked section must preserve this
// non-overlap property or it will race with concurrent producers.
func (w *Writer) fill(b *batch) (ready, stop bool) {
	w.mu.Lock()
	head, read, queued := w.head, w.read, w.count
	w.mu.Unlock()

	// Snapshot slots stay occupied while producers append to the free portion.
	// Scan metadata and copy at most two contiguous spans without holding mu.
	start := len(b.data)
	taken, size := 0, 0
	for taken < queued {
		r := w.records[head]
		if start+size > 0 && (b.minute != r.minute || start+size+r.size > cap(b.data) || len(b.lengths) == cap(b.lengths)) {
			ready = true
			break
		}
		b.minute = r.minute
		b.lengths = append(b.lengths, r.size)
		size += r.size
		taken++
		w.records[head] = record{}
		head = (head + 1) % len(w.records)
	}
	b.data = b.data[:start+size]
	n := copy(b.data[start:], w.ring[read:min(read+size, len(w.ring))])
	if n < size {
		copy(b.data[start+n:], w.ring[:size-n])
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.read = (read + size) % len(w.ring)
	w.used -= size
	w.head = head
	w.count -= taken
	return ready || len(b.data) == cap(b.data) || len(b.lengths) == cap(b.lengths) || w.closing, w.closing && w.count == 0
}

// records/bytes count the fully written records of this segment. They are
// committed to Sealed* only when sync, close and .log publication succeed, so bytes
// written into a damaged .active segment are never reported as transferable.
func (w *Writer) closeFile(f diskFile, path string, healthy, hasData bool, records, bytes uint64) bool {
	// A failed write/sync makes the segment unsafe to publish even if a later
	// sync succeeds. Close it once and leave its .active name for inspection.
	if healthy && w.syncFile(f) != nil {
		healthy = false
	}
	var empty os.FileInfo
	if !healthy && !hasData {
		if file, ok := f.(interface{ Stat() (os.FileInfo, error) }); ok {
			info, err := file.Stat()
			if err != nil {
				w.recordTransientIOError("cleanup", err)
			} else if info.Mode().IsRegular() && info.Size() == 0 {
				empty = info
			}
		}
	}
	// Never retry Close: the descriptor may have been released despite the error.
	if err := f.Close(); err != nil {
		w.recordIOError("close", err)
		healthy = false
	} else {
		w.clearFailure("close")
		if empty != nil {
			if err := removeEmptyLogFile(path, empty); err != nil {
				w.recordTransientIOError("cleanup", err)
			}
		}
	}
	if healthy {
		if err := w.seal(path); err != nil {
			var cleanupErr *sealCleanupError
			if !errors.As(err, &cleanupErr) {
				w.recordIOError("seal", err)
				return false
			}
			w.recordTransientIOError("cleanup", cleanupErr.Unwrap())
		}
		w.clearFailure("seal")
		w.mu.Lock()
		w.stats.SealedRecords += records
		w.stats.SealedBytes += bytes
		w.mu.Unlock()
	}
	return healthy
}

func untilNextMinute(now time.Time) time.Duration {
	return now.Truncate(time.Minute).Add(time.Minute).Sub(now)
}

func (w *Writer) run() {
	// Set the closed flag before signalling Shutdown waiters. Order matters:
	// close(diagnostics.stop) fires first (LIFO), then this defer sets the
	// flag and closes done. Stats() may observe closed=true slightly before
	// done is closed, which only makes the snapshot more up to date.
	defer func() {
		w.closed.Store(true)
		close(w.done)
	}()
	defer close(w.diagnostics.stop)
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()
	rotation := time.NewTimer(untilNextMinute(w.now()))
	defer rotation.Stop()
	var current diskFile
	var currentPath string
	var currentMinute int64
	var currentHasData bool
	// Fully written records of the current segment, committed to Sealed* only
	// after the segment is successfully sealed.
	var segmentRecords, segmentBytes uint64
	var sequence uint64
	// RecoverAfter and the cooldown check use time.Now (wall clock), not w.now:
	// I/O recovery is a physical-time behaviour and must not be governed by the
	// injectable admission clock. Tests drive recovery by setting RecoveryInterval
	// small, not by advancing w.now. Large RecoveryInterval does not retain
	// records during cooldown: batches are consumed and discarded continuously
	// until the cooldown elapses (see README).
	var recoverAfter time.Time
	force := false
	for {
		ready, stop := w.fill(&w.b)
		// A different admission minute, or an empty batch after the minute ends,
		// completes this segment. A racing producer always gets a new segment.
		if current != nil && ((len(w.b.data) > 0 && w.b.minute != currentMinute) ||
			(len(w.b.data) == 0 && (stop || w.now().Unix()/60 != currentMinute))) {
			if !w.closeFile(current, currentPath, true, currentHasData, segmentRecords, segmentBytes) {
				recoverAfter = time.Now().Add(w.cfg.RecoveryInterval)
			}
			current = nil
		}
		// I/O can span a minute boundary and opening rearms the rotation timer.
		// Drain a batch from another minute without waiting for the next tick.
		if len(w.b.data) > 0 && (ready || force || w.b.minute != w.now().Unix()/60) {
			if current == nil && !time.Now().Before(recoverAfter) {
				var err error
				sequence++
				currentPath = filepath.Join(w.dir, w.segmentName(w.b.minute, sequence))
				current, err = w.openFile(currentPath)
				if err == nil {
					currentHasData = false
					currentMinute = w.b.minute
					segmentRecords, segmentBytes = 0, 0
					rotation.Reset(untilNextMinute(w.now()))
				} else {
					recoverAfter = time.Now().Add(w.cfg.RecoveryInterval)
				}
			}
			written := 0
			writeOK := false
			if current != nil {
				var err error
				written, err = w.writeBatch(current, w.b.data)
				currentHasData = currentHasData || written > 0
				if err == nil && w.cfg.SyncEveryBatch {
					err = w.syncFile(current)
				}
				if err != nil {
					if w.cfg.SyncEveryBatch {
						written = 0
					}
					w.closeFile(current, currentPath, false, currentHasData, 0, 0)
					current = nil
					recoverAfter = time.Now().Add(w.cfg.RecoveryInterval)
				} else {
					writeOK = true
				}
			}
			records, bytes := w.resolve(w.b.lengths, written)
			// Only complete, healthy batches can end up in a published .log file.
			if writeOK {
				segmentRecords += records
				segmentBytes += bytes
			}
			w.b.data, w.b.lengths = w.b.data[:0], w.b.lengths[:0]
			force = false
			continue
		}
		if stop {
			return
		}
		select {
		case <-w.wake:
		case <-ticker.C:
			force = true
		case <-rotation.C:
			force = true
			rotation.Reset(untilNextMinute(w.now()))
		}
	}
}
