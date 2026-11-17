package qlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type hookedDiskFile struct {
	diskFile
	writeFn func([]byte) (int, error)
	syncFn  func() error
	closeFn func() error
	statFn  func() (os.FileInfo, error)
}

func (f *hookedDiskFile) Stat() (os.FileInfo, error) {
	if f.statFn != nil {
		return f.statFn()
	}
	return f.diskFile.(interface{ Stat() (os.FileInfo, error) }).Stat()
}

func (f *hookedDiskFile) Write(p []byte) (int, error) {
	if f.writeFn != nil {
		return f.writeFn(p)
	}
	return f.diskFile.Write(p)
}

func (f *hookedDiskFile) Sync() error {
	if f.syncFn != nil {
		return f.syncFn()
	}
	return f.diskFile.Sync()
}

func (f *hookedDiskFile) Close() error {
	if f.closeFn != nil {
		return f.closeFn()
	}
	return f.diskFile.Close()
}

func logPaths(t *testing.T, w *Writer, pattern string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(w.dir, pattern))
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

func readLog(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func writeAndAwait(t *testing.T, w *Writer, text string, count uint64) {
	t.Helper()
	if _, err := w.Write([]byte(text)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w.Err() != nil || w.Stats().WrittenRecords == count })
	if err := w.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestSealedFilesSafeToMove(t *testing.T) {
	for _, mode := range []string{"rename", "copy-delete"} {
		t.Run(mode, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.BatchRecords = 1
			initial := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
			var clock atomic.Int64
			clock.Store(initial.Unix())
			now := func() time.Time { return time.Unix(clock.Load(), 0) }
			w := newDiskWriter(t, cfg, now)
			writeAndAwait(t, w, "before-transfer", 1)
			if ready, active := logPaths(t, w, "*.log"), logPaths(t, w, "*.log.active"); len(ready) != 0 || len(active) != 1 {
				t.Fatalf("unsealed file exposed: ready=%v active=%v", ready, active)
			}

			clock.Add(60)
			writeAndAwait(t, w, "next-minute", 2)
			ready := logPaths(t, w, "*.log")
			if len(ready) != 1 {
				t.Fatalf("sealed files: %v", ready)
			}
			source, archive := ready[0], filepath.Join(t.TempDir(), "archive.log")
			if mode == "rename" {
				if err := os.Rename(source, archive); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(archive, []byte(readLog(t, source)), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(source); err != nil {
					t.Fatal(err)
				}
			}
			clock.Store(initial.Unix())
			writeAndAwait(t, w, "clock-rollback", 3)
			if err := shutdown(t, w); err != nil {
				t.Fatal(err)
			}
			restarted := newDiskWriter(t, cfg, now)
			writeAndAwait(t, restarted, "same-minute-restart", 1)
			if err := shutdown(t, restarted); err != nil {
				t.Fatal(err)
			}
			if got := readLog(t, archive); got != "before-transfer\n" {
				t.Fatalf("archive changed after transfer: %q", got)
			}
			if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("transferred pathname was reused: %v", err)
			}
			want := map[string]bool{"next-minute\n": true, "clock-rollback\n": true, "same-minute-restart\n": true}
			for _, path := range logPaths(t, w, "*.log") {
				data := readLog(t, path)
				if !want[data] {
					t.Fatalf("unexpected or duplicate segment: %q", data)
				}
				delete(want, data)
			}
			if len(want) != 0 || len(logPaths(t, w, "*.active")) != 0 {
				t.Fatalf("missing records or unsealed files: %v", want)
			}
		})
	}
}

func TestMinuteTimerFlushesAndSealsIdleFile(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords = 2 // Leave the third record in an incomplete batch.
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 15, 10, 0, 59, 990000000, time.UTC).UnixNano())
	w := newDiskWriter(t, cfg, func() time.Time { return time.Unix(0, clock.Load()) })
	for _, text := range []string{"first", "second"} {
		if _, err := w.Write([]byte(text)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return w.Stats().WrittenRecords == 2 })
	if _, err := w.Write([]byte("pending")); err != nil {
		t.Fatal(err)
	}
	clock.Add(int64(10 * time.Millisecond))
	// No producer wakeup and no FlushInterval tick (one hour). The separate
	// minute timer must flush the old record and seal without another Write.
	waitFor(t, func() bool { return len(logPaths(t, w, "*.log")) == 1 })
	path := logPaths(t, w, "*.log")[0]
	if got := readLog(t, path); got != "first\nsecond\npending\n" {
		t.Fatalf("sealed before draining: %q", got)
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	if len(logPaths(t, w, "*.active")) != 0 || w.Stats().WrittenRecords != 3 {
		t.Fatalf("idle file not sealed: %+v", w.Stats())
	}
}

func TestMinuteTimerFlushesFirstIncompleteBatch(t *testing.T) {
	cfg := testConfig(t)
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 15, 10, 0, 59, 990000000, time.UTC).UnixNano())
	var observed atomic.Bool
	w := newDiskWriter(t, cfg, func() time.Time {
		now := time.Unix(0, clock.Load())
		observed.Store(true)
		return now
	})
	if _, err := w.Write([]byte("pending")); err != nil {
		t.Fatal(err)
	}
	// After admission only the worker reads the clock. Wait until it arms or
	// rearms the minute timer using the initial time before advancing it.
	observed.Store(false)
	waitFor(t, func() bool { return observed.Load() })
	clock.Add(int64(10 * time.Millisecond))
	waitFor(t, func() bool { return len(logPaths(t, w, "*.log")) == 1 })
	if got := readLog(t, logPaths(t, w, "*.log")[0]); got != "pending\n" {
		t.Fatalf("incomplete first batch not sealed: %q", got)
	}
}

func TestOpenAcrossMinuteFlushesAndSealsBacklog(t *testing.T) {
	for _, step := range []time.Duration{time.Minute, -time.Minute} {
		t.Run(step.String(), func(t *testing.T) {
			cfg := testConfig(t)
			cfg.BatchRecords = 2 // The third record leaves an incomplete batch.
			initial := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
			var clock atomic.Int64
			clock.Store(initial.Unix())
			entered, release := make(chan struct{}), make(chan struct{})
			var firstOpen, unblock sync.Once
			defer unblock.Do(func() { close(release) })
			w, err := newWriter(cfg, func() time.Time { return time.Unix(clock.Load(), 0) }, func(path string) (diskFile, error) {
				firstOpen.Do(func() { close(entered); <-release })
				return openLogFile(path)
			}, sealLogFile)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = shutdown(t, w) })
			for _, text := range []string{"first", "second"} {
				if _, err := w.Write([]byte(text)); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("worker did not start opening")
			}
			if _, err := w.Write([]byte("pending")); err != nil {
				t.Fatal(err)
			}
			clock.Store(initial.Add(step).Unix())
			unblock.Do(func() { close(release) })
			// Opening rearms rotation for a minute from now. The old batch must
			// finish without another producer, timer tick, or Shutdown.
			waitFor(t, func() bool { return len(logPaths(t, w, "*.log")) == 1 })
			path := logPaths(t, w, "*.log")[0]
			if got := readLog(t, path); got != "first\nsecond\npending\n" {
				t.Fatalf("incomplete sealed data: %q", got)
			}
			if got := minuteFileName(path); got != "request_202611151000.log" {
				t.Fatalf("admission minute changed: %s", got)
			}
			if s := w.Stats(); s.WrittenRecords != 3 || s.PendingRecords != 0 || s.FailedRecords != 0 || s.Failed {
				t.Fatalf("backlog not drained: %+v", s)
			}
			if err := shutdown(t, w); err != nil {
				t.Fatal(err)
			}
			if paths := logPaths(t, w, "*.active"); len(paths) != 0 {
				t.Fatalf("unsealed files: %v", paths)
			}
		})
	}
}

func TestBackloggedMinuteSealsAfterAllRecords(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords = 1
	initial := time.Date(2026, 11, 15, 10, 0, 59, 0, time.UTC)
	var clock atomic.Int64
	clock.Store(initial.Unix())
	entered, release := make(chan struct{}), make(chan struct{})
	var firstWrite, unblock sync.Once
	defer unblock.Do(func() { close(release) })
	w, err := newWriter(cfg, func() time.Time { return time.Unix(clock.Load(), 0) }, func(path string) (diskFile, error) {
		f, err := openLogFile(path)
		if err != nil {
			return nil, err
		}
		return &hookedDiskFile{diskFile: f, writeFn: func(p []byte) (int, error) {
			firstWrite.Do(func() { close(entered); <-release })
			return f.Write(p)
		}}, nil
	}, sealLogFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(t, w) })
	if _, err := w.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not start writing")
	}
	if _, err := w.Write([]byte("queued")); err != nil {
		t.Fatal(err)
	}
	clock.Add(1)
	if _, err := w.Write([]byte("next")); err != nil {
		t.Fatal(err)
	}
	if paths := logPaths(t, w, "*.log"); len(paths) != 0 {
		t.Fatalf("backlogged file published early: %v", paths)
	}
	unblock.Do(func() { close(release) })
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"request_202611151000.log": "first\nqueued\n", "request_202611151001.log": "next\n"}
	paths := logPaths(t, w, "*.log")
	if len(paths) != len(want) {
		t.Fatalf("sealed files: %v", paths)
	}
	for _, path := range paths {
		if got := readLog(t, path); got != want[minuteFileName(path)] {
			t.Fatalf("wrong minute or missing queued record: %s: %q", path, got)
		}
	}
}

func TestSealWaitsForWriteSyncAndClose(t *testing.T) {
	for _, operation := range []string{"write", "sync", "close"} {
		t.Run(operation, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			defer unblock.Do(func() { close(release) })
			pause := func() { close(entered); <-release }
			w, err := newWriter(testConfig(t), time.Now, func(path string) (diskFile, error) {
				f, err := openLogFile(path)
				if err != nil {
					return nil, err
				}
				wrapped := &hookedDiskFile{diskFile: f}
				switch operation {
				case "write":
					wrapped.writeFn = func(p []byte) (int, error) { pause(); return f.Write(p) }
				case "sync":
					wrapped.syncFn = func() error { pause(); return f.Sync() }
				case "close":
					wrapped.closeFn = func() error { pause(); return f.Close() }
				}
				return wrapped, nil
			}, sealLogFile)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = shutdown(t, w) })
			if _, err := w.Write([]byte("complete")); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- w.Shutdown(context.Background()) }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("worker did not enter " + operation)
			}
			if ready, active := logPaths(t, w, "*.log"), logPaths(t, w, "*.active"); len(ready) != 0 || len(active) != 1 {
				t.Fatalf("published before %s completed: ready=%v active=%v", operation, ready, active)
			}
			if s := w.Stats(); !s.Closing || s.Closed {
				t.Fatalf("worker still in %s: %+v", operation, s)
			}
			unblock.Do(func() { close(release) })
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("shutdown did not finish")
			}
			if paths := logPaths(t, w, "*.log"); len(paths) != 1 || readLog(t, paths[0]) != "complete\n" {
				t.Fatalf("missing sealed data: %v", paths)
			}
			if s := w.Stats(); !s.Closing || !s.Closed || s.PendingRecords != 0 {
				t.Fatalf("shutdown did not finish: %+v", s)
			}
		})
	}
}

func TestFailedFilesRemainActive(t *testing.T) {
	for _, operation := range []string{"write", "sync", "close", "seal", "batch-sync"} {
		t.Run(operation, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.SyncEveryBatch = operation == "batch-sync"
			failure := errors.New("injected " + operation + " failure")
			if operation == "batch-sync" {
				failure = syscall.ENOSPC // Even a later successful sync cannot repair it.
			}
			w, err := newWriter(cfg, time.Now, func(path string) (diskFile, error) {
				f, err := openLogFile(path)
				if err != nil {
					return nil, err
				}
				wrapped := &hookedDiskFile{diskFile: f}
				switch operation {
				case "write":
					wrapped.writeFn = func(p []byte) (int, error) {
						n, _ := f.Write(p[:2])
						return n, failure
					}
				case "sync":
					wrapped.syncFn = func() error { return failure }
				case "batch-sync":
					calls := 0
					wrapped.syncFn = func() error {
						calls++
						if calls == 1 {
							return failure
						}
						return f.Sync()
					}
				case "close":
					wrapped.closeFn = func() error { _ = f.Close(); return failure }
				}
				return wrapped, nil
			}, func(path string) error {
				if operation == "seal" {
					return failure
				}
				return sealLogFile(path)
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = shutdown(t, w) })
			if _, err := w.Write([]byte("record")); err != nil {
				t.Fatal(err)
			}
			if err := shutdown(t, w); err != nil {
				t.Fatalf("failure not reported: %v", err)
			}
			if ready, active := logPaths(t, w, "*.log"), logPaths(t, w, "*.active"); len(ready) != 0 || len(active) != 1 {
				t.Fatalf("failed file published: ready=%v active=%v", ready, active)
			}
			if s := w.Stats(); !s.Failed || s.PendingRecords != 0 || s.IOErrors == 0 {
				t.Fatalf("failure stats: %+v", s)
			}
		})
	}
}

func TestSegmentFilesNeverOverwrite(t *testing.T) {
	active := filepath.Join(t.TempDir(), "segment.log.active")
	sealed := strings.TrimSuffix(active, ".active")
	if err := os.WriteFile(active, []byte("new-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if f, err := openLogFile(active); !errors.Is(err, os.ErrExist) {
		if f != nil {
			_ = f.Close()
		}
		t.Fatalf("existing active segment reopened: %v", err)
	}
	if err := os.WriteFile(sealed, []byte("existing-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sealLogFile(active); !errors.Is(err, os.ErrExist) {
		t.Fatalf("existing sealed segment overwritten: %v", err)
	}
	if readLog(t, active) != "new-data" || readLog(t, sealed) != "existing-data" {
		t.Fatal("file contents changed")
	}
}

func TestRestartLeavesExistingSegmentsUntouched(t *testing.T) {
	cfg := testConfig(t)
	dir := filepath.Join(cfg.BaseDir, cfg.Name)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	existing := map[string]string{
		"request_202611151000_00000000000000000000000000000000_000001.log.active": "partial",
		"request_202611151000_00000000000000000000000000000000_000002.log":        "sealed\n",
	}
	for name, contents := range existing {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	w := newDiskWriter(t, cfg, func() time.Time { return time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC) })
	if _, err := w.Write([]byte("fresh")); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	for name, contents := range existing {
		if readLog(t, filepath.Join(dir, name)) != contents {
			t.Fatalf("restart modified existing segment %s", name)
		}
	}
	if ready, active := logPaths(t, w, "*.log"), logPaths(t, w, "*.active"); len(ready) != 2 || len(active) != 1 {
		t.Fatalf("restart published abandoned data: ready=%v active=%v", ready, active)
	}
}

// TestCloseFileRemoveFailureRecordsCleanupError covers the branch where
// removeEmptyLogFile cannot remove the file (directory not writable). The
// cleanup error must be recorded and the Writer must continue operating.
func TestCloseFileRemoveFailureRecordsCleanupError(t *testing.T) {
	if os.Getuid() == 0 || runtime.GOOS == "windows" {
		t.Skip("root bypasses, and Windows ignores, directory write-permission checks")
	}
	cfg := testConfig(t)
	cfg.BatchRecords = 1
	dir := filepath.Join(cfg.BaseDir, cfg.Name)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	w, err := newWriter(cfg, time.Now, func(path string) (diskFile, error) {
		f, err := openLogFile(path)
		if err != nil {
			return nil, err
		}
		return &hookedDiskFile{
			diskFile: f,
			writeFn: func(p []byte) (int, error) {
				// Revoke directory write permission before returning the error.
				// removeEmptyLogFile will find the file (Lstat succeeds) but
				// os.Remove will fail because the directory is not writable.
				_ = os.Chmod(dir, 0o500)
				return 0, syscall.EIO
			},
		}, nil
	}, sealLogFile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(dir, 0o750) }() // restore so t.TempDir cleanup can proceed
	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o750)
		_ = shutdown(t, w)
	})
	if _, err := w.Write([]byte("lost")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w.Stats().IOErrors >= 2 })
	_ = os.Chmod(dir, 0o750)
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	s := w.Stats()
	// write EIO + cleanup Remove error = at least 2 IO errors.
	if s.IOErrors < 2 || s.FailedRecords != 1 {
		t.Fatalf("expected write+cleanup errors: %+v", s)
	}
}

// TestCloseFileStatFailureRecordsCleanupError covers the branch in closeFile
// where Stat on an empty failed segment returns an error; the operation must
// be recorded as a "cleanup" I/O error and the file left in place.
func TestCloseFileStatFailureRecordsCleanupError(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords = 1
	statErr := errors.New("injected stat failure")
	w, err := newWriter(cfg, time.Now, func(path string) (diskFile, error) {
		f, err := openLogFile(path)
		if err != nil {
			return nil, err
		}
		return &hookedDiskFile{
			diskFile: f,
			writeFn:  func([]byte) (int, error) { return 0, syscall.EIO }, // fail the write
			statFn:   func() (os.FileInfo, error) { return nil, statErr }, // fail the cleanup Stat
		}, nil
	}, sealLogFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(t, w) })
	if _, err := w.Write([]byte("lost")); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(t, w); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	s := w.Stats()
	// write EIO + cleanup stat error both counted as IOErrors; the empty .active
	// file remains because cleanup could not confirm its identity.
	if s.IOErrors < 2 || !s.Failed || s.FailedRecords != 1 {
		t.Fatalf("expected write+cleanup errors: %+v", s)
	}
	if active := logPaths(t, w, "*.active"); len(active) != 1 {
		t.Fatalf("empty failed segment removed despite stat error: %v", active)
	}
	if !errors.Is(w.Err(), syscall.EIO) {
		t.Fatalf("unresolved failure: %v", w.Err())
	}
}
