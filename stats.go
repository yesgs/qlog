package qlog

// Counters count completed Write attempts as logical records.
// Byte counts use on-disk sizes, including one trailing newline.
// Accepted includes records discarded on queue overflow. Failed counts these
// discards and records the worker could not write. Rejected counts invalid or
// closed-writer calls that returned an error.
// Sealed counts records in segments published as .log after sync and close
// succeeded, including publication followed by an .active cleanup error.
// It is a subset of Written. Written - Sealed includes the current healthy
// segment and complete records counted as written in abandoned segments.
// Sealed equals Written after Shutdown if every written segment was published.
type Counters struct {
	ReceivedRecords uint64
	ReceivedBytes   uint64
	AcceptedRecords uint64
	AcceptedBytes   uint64
	WrittenRecords  uint64
	WrittenBytes    uint64
	SealedRecords   uint64
	SealedBytes     uint64
	FailedRecords   uint64
	FailedBytes     uint64
	RejectedRecords uint64
	RejectedBytes   uint64
}

// Stats is a consistent snapshot. Queue includes records being copied and
// excludes batches whose copy is complete.
// Pending includes both the queue and the batch.
type Stats struct {
	Counters
	QueueRecords      int
	QueueBytes        int
	PendingRecords    uint64
	PendingBytes      uint64
	IOErrors          uint64 // Counts every I/O error, including failures followed by later recovery.
	WriteCalls        uint64
	WriteSeconds      float64
	SyncCalls         uint64
	SyncSeconds       float64
	PartialWriteBytes uint64
	Closing           bool // Shutdown has stopped admission.
	Closed            bool // The background worker has exited.
	Failed            bool // At least one I/O operation has not yet recovered.
}

// Stats returns a snapshot suitable for health checks and monitoring.
func (w *Writer) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.stats
	s.QueueRecords, s.QueueBytes = w.count, w.used
	s.PendingRecords = s.AcceptedRecords - s.WrittenRecords - s.FailedRecords
	s.PendingBytes = s.AcceptedBytes - s.WrittenBytes - s.FailedBytes
	s.Closing, s.Failed = w.closing, len(w.failures) > 0
	s.Closed = w.closed.Load()
	return s
}

// Err returns the oldest unresolved I/O failure, or nil. Only success of the
// same operation clears its failure. The Writer continues accepting records.
// Historical errors and discarded records remain in Stats. It is safe concurrently.
func (w *Writer) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.firstFailureLocked()
}

func (w *Writer) recordAttemptLocked(size int, err error) {
	w.stats.ReceivedRecords++
	w.stats.ReceivedBytes += uint64(size)
	if err != nil {
		w.stats.RejectedRecords++
		w.stats.RejectedBytes += uint64(size)
	} else {
		w.stats.AcceptedRecords++
		w.stats.AcceptedBytes += uint64(size)
	}
}

// resolve classifies one batch and returns how many records/bytes were counted
// as written. The caller decides whether those bytes belong to a segment that
// can eventually be sealed.
func (w *Writer) resolve(lengths []int, written int) (records, bytes uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	offset := 0
	failed := false
	for _, size := range lengths {
		offset += size
		if offset <= written {
			w.stats.WrittenRecords++
			w.stats.WrittenBytes += uint64(size)
			records++
			bytes += uint64(size)
		} else {
			failed = true
			w.stats.FailedRecords++
			w.stats.FailedBytes += uint64(size)
		}
	}
	if err := w.firstFailureLocked(); failed && err != nil {
		w.queueErrorLocked("discard", err)
	}
	return records, bytes
}
