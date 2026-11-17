package qlog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestBlockedDiagnosticsAllowRecoveryAndShutdown(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords, cfg.RecoveryInterval = 1, time.Nanosecond
	entered, release := make(chan struct{}), make(chan struct{})
	var once, unblock sync.Once
	defer unblock.Do(func() { close(release) })
	cfg.ErrorLogger = slog.New(slog.NewTextHandler(errorSinkFunc(func(p []byte) (int, error) {
		once.Do(func() { close(entered); <-release })
		return len(p), nil
	}), nil))
	var broken atomic.Bool
	broken.Store(true)
	file := &testFile{}
	w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) {
		if broken.Load() {
			return nil, syscall.EIO
		}
		return file, nil
	})
	if _, err := w.Write([]byte("lost")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("missing diagnostic")
	}
	broken.Store(false)
	if _, err := w.Write([]byte("recovered")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w.Stats().WrittenRecords == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown waited for diagnostics: %v", err)
	}
	if file.String() != "recovered\n" || w.Stats().FailedRecords != 1 {
		t.Fatalf("data=%q stats=%+v", file.String(), w.Stats())
	}
	unblock.Do(func() { close(release) })
	awaitDiagnostics(t, w)
}

func TestFailedEmptySegmentsAreRemoved(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords, cfg.RecoveryInterval = 1, time.Nanosecond
	dir := filepath.Join(cfg.BaseDir, cfg.Name)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	historical := filepath.Join(dir, "historical.log.active")
	if err := os.WriteFile(historical, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := newWriter(cfg, time.Now, func(path string) (diskFile, error) {
		f, err := openLogFile(path)
		if err != nil {
			return nil, err
		}
		return &hookedDiskFile{diskFile: f, writeFn: func([]byte) (int, error) { return 0, syscall.EIO }}, nil
	}, sealLogFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(t, w) })
	for i := uint64(1); i <= 5; i++ {
		if _, err := w.Write([]byte("lost")); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return w.Stats().FailedRecords == i })
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	paths := logPaths(t, w, "*.active")
	if len(paths) != 1 || paths[0] != historical {
		t.Fatalf("empty failures accumulated or historical file removed: %v", paths)
	}
}

func TestZeroWriteFailurePreservesEarlierData(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords = 1
	var fail atomic.Bool
	w, err := newWriter(cfg, func() time.Time { return time.Unix(1800000000, 0) }, func(path string) (diskFile, error) {
		f, err := openLogFile(path)
		if err != nil {
			return nil, err
		}
		return &hookedDiskFile{diskFile: f, writeFn: func(p []byte) (int, error) {
			if fail.Load() {
				return 0, syscall.EIO
			}
			return f.Write(p)
		}}, nil
	}, sealLogFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(t, w) })
	writeAndAwait(t, w, "preserve", 1)
	fail.Store(true)
	if _, err := w.Write([]byte("lost")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w.Stats().FailedRecords == 1 })
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	paths := logPaths(t, w, "*.active")
	if len(paths) != 1 || readLog(t, paths[0]) != "preserve\n" {
		t.Fatalf("earlier data discarded: %v", paths)
	}
}

func TestFailureClearsOnlyAfterSameOperationSucceeds(t *testing.T) {
	for _, operation := range []string{"sync", "close", "seal"} {
		t.Run(operation, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.BatchRecords, cfg.RecoveryInterval = 1, time.Nanosecond
			cfg.FlushInterval = time.Millisecond
			var clock atomic.Int64
			clock.Store(1800000000)
			var broken atomic.Bool
			broken.Store(true)
			w, err := newWriter(cfg, func() time.Time { return time.Unix(clock.Load(), 0) }, func(path string) (diskFile, error) {
				f, err := openLogFile(path)
				if err != nil {
					return nil, err
				}
				return &hookedDiskFile{diskFile: f,
					syncFn: func() error {
						if operation == "sync" && broken.Load() {
							return syscall.EIO
						}
						return f.Sync()
					},
					closeFn: func() error {
						err := f.Close()
						if operation == "close" && broken.Load() {
							return syscall.EIO
						}
						return err
					},
				}, nil
			}, func(path string) error {
				if operation == "seal" && broken.Load() {
					return syscall.EIO
				}
				return sealLogFile(path)
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = shutdown(t, w) })
			for i := uint64(1); i <= 3; i++ {
				if _, err := w.Write([]byte("line")); err != nil {
					t.Fatal(err)
				}
				waitFor(t, func() bool { return w.Stats().WrittenRecords == i })
				if i < 3 {
					clock.Add(60)
					// Finish the failed rotation before submitting the recovery batch.
					waitFor(t, func() bool { return w.Stats().IOErrors == i })
					time.Sleep(time.Millisecond) // Let the physical-time cooldown elapse.
				}
			}
			if !errors.Is(w.Err(), syscall.EIO) || !w.Stats().Failed || w.Stats().Closing {
				t.Fatalf("write hid persistent %s failure: err=%v stats=%+v", operation, w.Err(), w.Stats())
			}
			if paths := logPaths(t, w, "*.log"); len(paths) != 0 {
				t.Fatalf("published despite %s failure: %v", operation, paths)
			}
			broken.Store(false)
			if err := shutdown(t, w); err != nil {
				t.Fatal(err)
			}
			if w.Err() != nil || w.Stats().Failed {
				t.Fatalf("successful %s did not clear failure: %v", operation, w.Err())
			}
		})
	}
}

func TestRecoveringWriteKeepsUnresolvedCloseFailure(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords, cfg.RecoveryInterval = 1, time.Nanosecond
	failed := &testFile{
		writeFn:  func([]byte) (int, error) { return 0, syscall.EIO },
		closeErr: syscall.EACCES,
	}
	opens := 0
	w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) {
		opens++
		if opens == 1 {
			return failed, nil
		}
		return &testFile{}, nil
	})
	if _, err := w.Write([]byte("lost")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w.Stats().FailedRecords == 1 })
	if !errors.Is(w.Err(), syscall.EIO) {
		t.Fatalf("first unresolved error: %v", w.Err())
	}
	if _, err := w.Write([]byte("recovered")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w.Stats().WrittenRecords == 1 })
	if !errors.Is(w.Err(), syscall.EACCES) || !w.Stats().Failed {
		t.Fatalf("write recovery cleared close failure: %v", w.Err())
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	if w.Err() != nil || w.Stats().Failed {
		t.Fatalf("successful close did not recover: %v", w.Err())
	}
}

func TestConcurrentHealthyWritesPreserveEveryRecord(t *testing.T) {
	cfg := testConfig(t)
	cfg.QueueBytes, cfg.QueueRecords = 8192, 1024 // All records fit without overflow.
	file := &testFile{}
	w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) { return file, nil })
	var producers sync.WaitGroup
	for producer := 0; producer < 8; producer++ {
		producers.Go(func() {
			for i := 0; i < 100; i++ {
				if _, err := fmt.Fprintf(w, "%d/%d", producer, i); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	producers.Wait()
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for _, line := range strings.Split(strings.TrimSuffix(file.String(), "\n"), "\n") {
		if seen[line] {
			t.Fatalf("duplicate record %q", line)
		}
		seen[line] = true
	}
	for producer := 0; producer < 8; producer++ {
		for i := 0; i < 100; i++ {
			if !seen[fmt.Sprintf("%d/%d", producer, i)] {
				t.Fatalf("missing record %d/%d", producer, i)
			}
		}
	}
	if s := w.Stats(); s.WrittenRecords != 800 || s.FailedRecords != 0 || s.PendingRecords != 0 {
		t.Fatalf("healthy workload: %+v", s)
	}
}
