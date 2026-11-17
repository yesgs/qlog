package qlog

import (
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Healthy run: sealed counters must equal written counters and the bytes
// actually present in published .log files.
func TestSealedCountersMatchPublishedFiles(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords = 2
	w := newDiskWriter(t, cfg, func() time.Time { return time.Unix(1800000000, 0) })
	for _, text := range []string{"one", "two", "three", "four"} {
		if _, err := w.Write([]byte(text)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return w.Stats().WrittenRecords == 4 })
	if s := w.Stats(); s.SealedRecords != 0 || s.SealedBytes != 0 || s.IOErrors != 0 || s.Failed {
		t.Fatalf("healthy current segment: %+v", s)
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	files := logPaths(t, w, "*.log")
	if len(files) == 0 {
		t.Fatal("no sealed file")
	}
	total := 0
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		total += len(data)
	}
	s := w.Stats()
	if s.SealedRecords != s.WrittenRecords || s.SealedBytes != s.WrittenBytes ||
		s.SealedRecords != 4 || uint64(total) != s.SealedBytes {
		t.Fatalf("sealed mismatch: stats=%+v file_bytes=%d", s, total)
	}
}

func TestSealedCountersIncludePublicationWithCleanupFailure(t *testing.T) {
	cfg := testConfig(t)
	w, err := newWriter(cfg, time.Now, openLogFile, func(path string) error {
		if err := os.Link(path, strings.TrimSuffix(path, ".active")); err != nil {
			return err
		}
		return &sealCleanupError{err: &os.PathError{Op: "remove", Path: path, Err: syscall.EIO}}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(t, w) })
	if _, err := w.Write([]byte("published")); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	ready, active := logPaths(t, w, "*.log"), logPaths(t, w, "*.active")
	if len(ready) != 1 || len(active) != 1 || readLog(t, ready[0]) != "published\n" {
		t.Fatalf("unexpected published files: ready=%v active=%v", ready, active)
	}
	s := w.Stats()
	if s.SealedRecords != 1 || s.SealedBytes != uint64(len("published\n")) ||
		s.SealedRecords != s.WrittenRecords || s.SealedBytes != s.WrittenBytes {
		t.Fatalf("published segment missing from sealed counters: %+v", s)
	}
	if s.IOErrors != 1 || s.Failed || w.Err() != nil {
		t.Fatalf("cleanup failure state: stats=%+v err=%v", s, w.Err())
	}
}

// A short write counts complete prefix records as Written, but the damaged
// segment is never published: those bytes must not be reported as Sealed.
func TestShortWriteIsWrittenButNotSealed(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords = 3
	cfg.RecoveryInterval = time.Nanosecond
	var first atomic.Bool
	first.Store(true)
	w, err := newWriter(cfg, func() time.Time { return time.Unix(1800000000, 0) }, func(path string) (diskFile, error) {
		f, err := openLogFile(path)
		if err != nil {
			return nil, err
		}
		h := &hookedDiskFile{diskFile: f}
		h.writeFn = func(p []byte) (int, error) {
			if first.CompareAndSwap(true, false) {
				n, _ := f.Write(p[:2]) // one complete record, then fail
				return n, syscall.EIO
			}
			return f.Write(p)
		}
		return h, nil
	}, sealLogFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(t, w) })
	for _, text := range []string{"a", "b", "c"} {
		if _, err := w.Write([]byte(text)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return w.Stats().PendingRecords == 0 })
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	s := w.Stats()
	if s.WrittenRecords != 1 || s.WrittenBytes != 2 || s.SealedRecords != 0 || s.SealedBytes != 0 {
		t.Fatalf("damaged bytes counted as sealed: %+v", s)
	}
	if ready, active := logPaths(t, w, "*.log"), logPaths(t, w, "*.active"); len(ready) != 0 || len(active) != 1 {
		t.Fatalf("unexpected segment state: ready=%v active=%v", ready, active)
	}
}

// A segment whose seal fails must not contribute to Sealed*, while a later
// healthy segment must.
func TestSealFailureExcludesSegmentFromSealed(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords = 1
	cfg.RecoveryInterval = time.Nanosecond
	cfg.FlushInterval = time.Millisecond
	var clock atomic.Int64
	clock.Store(1800000000)
	var seals atomic.Int64
	w, err := newWriter(cfg, func() time.Time { return time.Unix(clock.Load(), 0) }, openLogFile,
		func(path string) error {
			if seals.Add(1) == 1 {
				return errors.New("injected seal failure")
			}
			return sealLogFile(path)
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(t, w) })
	if _, err := w.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w.Stats().WrittenRecords == 1 })
	clock.Add(60)
	// Finish the failed seal before submitting the recovery batch.
	waitFor(t, func() bool { return w.Stats().IOErrors == 1 })
	time.Sleep(time.Millisecond) // Let the physical-time cooldown elapse.
	if _, err := w.Write([]byte("second")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w.Stats().WrittenRecords == 2 })
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	s := w.Stats()
	if s.SealedRecords != 1 || s.SealedBytes != uint64(len("second\n")) || s.WrittenRecords != 2 {
		t.Fatalf("failed segment counted as sealed: %+v seals=%d", s, seals.Load())
	}
	if ready := logPaths(t, w, "*.log"); len(ready) != 1 || readLog(t, ready[0]) != "second\n" {
		t.Fatalf("unexpected published files: %v", ready)
	}
}
