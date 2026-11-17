package qlog

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TryWrite lets a caller distinguish a queued record from a silent discard,
// while Write keeps its io.Writer-compatible signature.
func TestTryWriteReportsAdmission(t *testing.T) {
	cfg := testConfig(t)
	cfg.QueueBytes, cfg.QueueRecords, cfg.BatchBytes, cfg.MaxRecordBytes = 16, 2, 16, 8
	cfg.BatchRecords = 1
	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	f := &testFile{writeFn: func(p []byte) (int, error) {
		if once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return len(p), nil
	}}
	w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) { return f, nil })
	if n, admission, err := w.TryWrite([]byte("1234567")); err != nil || n != 7 || admission != AdmissionQueued {
		t.Fatalf("first write: n=%d admission=%v err=%v", n, admission, err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not start writing")
	}
	for i := 0; i < 2; i++ {
		if n, admission, err := w.TryWrite([]byte("1234567")); err != nil || n != 7 || admission != AdmissionQueued {
			t.Fatalf("queued write %d: n=%d admission=%v err=%v", i, n, admission, err)
		}
	}
	if n, admission, err := w.TryWrite([]byte("1234567")); err != nil || n != 7 || admission != AdmissionDropped {
		t.Fatalf("overflow: n=%d admission=%v err=%v", n, admission, err)
	}
	if n, admission, err := w.TryWrite([]byte("12345678")); !errors.Is(err, ErrRecordTooLarge) || n != 0 || admission != AdmissionRejected {
		t.Fatalf("oversized: n=%d admission=%v err=%v", n, admission, err)
	}
	// Write must still look exactly like io.Writer.
	if n, err := w.Write([]byte("1234567")); err != nil || n != 7 {
		t.Fatalf("Write compatibility: n=%d err=%v", n, err)
	}
	unblock.Do(func() { close(release) })
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
}
