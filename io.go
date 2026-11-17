package qlog

import (
	"io"
	"time"
)

func (w *Writer) openFile(path string) (diskFile, error) {
	f, err := w.open(path)
	if err != nil {
		w.recordIOError("open", err)
	} else {
		w.clearFailure("open")
	}
	return f, err
}

// writeBatch attempts one write. A short or failed write abandons the segment;
// its complete prefix records may still count as written in buffered mode.
func (w *Writer) writeBatch(f diskFile, p []byte) (int, error) {
	start := time.Now()
	n, err := f.Write(p)
	elapsed := time.Since(start).Seconds()
	validCount := n >= 0 && n <= len(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	w.mu.Lock()
	w.stats.WriteCalls++
	w.stats.WriteSeconds += elapsed
	if err != nil {
		w.stats.PartialWriteBytes += uint64(max(0, min(n, len(p))))
	}
	w.mu.Unlock()
	if err != nil {
		w.recordIOError("write", err)
	} else {
		w.clearFailure("write")
	}
	if !validCount {
		return 0, err
	}
	return n, err
}

func (w *Writer) syncFile(f diskFile) error {
	start := time.Now()
	err := f.Sync()
	w.mu.Lock()
	w.stats.SyncCalls++
	w.stats.SyncSeconds += time.Since(start).Seconds()
	w.mu.Unlock()
	if err != nil {
		w.recordIOError("sync", err)
	} else {
		w.clearFailure("sync")
	}
	return err
}
