package qlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestIOFailuresRecoverWithoutRestart(t *testing.T) {
	for _, operation := range []string{"open", "write", "batch-sync", "rotation-sync", "close", "seal"} {
		for _, failure := range []error{syscall.ENOSPC, syscall.EIO} {
			t.Run(fmt.Sprintf("%s/%v", operation, failure), func(t *testing.T) {
				cfg := testConfig(t)
				cfg.BatchRecords = 1
				cfg.RecoveryInterval = time.Nanosecond
				cfg.SyncEveryBatch = operation == "batch-sync"
				var logs bytes.Buffer
				cfg.ErrorLogger = slog.New(slog.NewTextHandler(&logs, nil))
				var broken atomic.Bool
				broken.Store(true)
				var clock atomic.Int64
				clock.Store(time.Date(2026, 11, 17, 12, 0, 59, 990000000, time.UTC).UnixNano())
				w, err := newWriter(cfg, func() time.Time { return time.Unix(0, clock.Load()) }, func(path string) (diskFile, error) {
					if operation == "open" && broken.Load() {
						return nil, failure
					}
					f, err := openLogFile(path)
					if err != nil {
						return nil, err
					}
					return &hookedDiskFile{
						diskFile: f,
						writeFn: func(p []byte) (int, error) {
							if operation == "write" && broken.Load() {
								n, _ := f.Write(p[:min(2, len(p))])
								return n, failure
							}
							return f.Write(p)
						},
						syncFn: func() error {
							if (operation == "batch-sync" || operation == "rotation-sync") && broken.Load() {
								return failure
							}
							return f.Sync()
						},
						closeFn: func() error {
							err := f.Close()
							if operation == "close" && broken.Load() {
								return failure
							}
							return err
						},
					}, nil
				}, func(path string) error {
					if operation == "seal" && broken.Load() {
						return failure
					}
					return sealLogFile(path)
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = shutdown(t, w) })
				if n, err := w.Write([]byte("before")); n != 6 || err != nil {
					t.Fatalf("write during failure: n=%d err=%v", n, err)
				}
				wantFailed, wantWritten := uint64(1), uint64(1)
				if operation == "rotation-sync" || operation == "close" || operation == "seal" {
					waitFor(t, func() bool { return w.Stats().WrittenRecords == 1 })
					clock.Add(int64(20 * time.Millisecond))
					waitFor(t, func() bool { return w.Err() != nil })
					// Allow the failed close and physical-time cooldown to finish.
					time.Sleep(time.Millisecond)
					wantFailed, wantWritten = 0, 2 // Earlier write success cannot be retracted.
				} else {
					waitFor(t, func() bool { return w.Stats().FailedRecords == 1 })
				}
				if !errors.Is(w.Err(), failure) || w.Stats().Closing {
					t.Fatalf("failure stopped admission or lost diagnostic: err=%v stats=%+v", w.Err(), w.Stats())
				}
				broken.Store(false)
				if n, err := w.Write([]byte("recovered")); n != 9 || err != nil {
					t.Fatalf("write after recovery: n=%d err=%v", n, err)
				}
				waitFor(t, func() bool { return w.Stats().WrittenRecords == wantWritten })
				if err := shutdown(t, w); err != nil {
					t.Fatalf("I/O failure leaked into shutdown: %v", err)
				}
				awaitDiagnostics(t, w)
				s := w.Stats()
				if w.Err() != nil || s.Failed || s.FailedRecords != wantFailed || s.AcceptedRecords != 2 || s.PendingRecords != 0 || s.IOErrors == 0 {
					t.Fatalf("recovery statistics: %+v, err=%v", s, w.Err())
				}
				ready := logPaths(t, w, "*.log")
				if len(ready) != 1 || readLog(t, ready[0]) != "recovered\n" {
					t.Fatalf("damaged data published or recovery missing: %v", ready)
				}
				wantActive := 1
				if operation == "open" {
					wantActive = 0
				}
				if active := logPaths(t, w, "*.active"); len(active) != wantActive {
					t.Fatalf("damaged segments: %v", active)
				}
				if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "stream=request") {
					t.Fatalf("missing error diagnostic: %q", logs.String())
				}
			})
		}
	}
}

func TestPersistentFailureCooldownDropsWithoutGrowingQueue(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords, cfg.RecoveryInterval = 1, time.Hour
	var opens atomic.Int64
	w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) {
		opens.Add(1)
		return nil, syscall.EIO
	})
	if _, err := w.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w.Stats().FailedRecords == 1 })
	for i := 0; i < 1000; i++ {
		if n, err := w.Write([]byte("next")); n != 4 || err != nil {
			t.Fatalf("failure leaked to producer: n=%d err=%v", n, err)
		}
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	s := w.Stats()
	if opens.Load() != 1 || s.AcceptedRecords != 1001 || s.FailedRecords != 1001 || s.FailedBytes != 5006 || s.PendingRecords != 0 || s.QueueBytes != 0 || s.RejectedRecords != 0 {
		t.Fatalf("cooldown did not bound work or account for discards: opens=%d stats=%+v", opens.Load(), s)
	}
	if len(w.ring) != cfg.QueueBytes || len(w.records) != cfg.QueueRecords {
		t.Fatal("queue grew during outage")
	}
}

func TestErrorLogsAreRateLimitedAndSummarized(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords, cfg.RecoveryInterval = 1, time.Nanosecond
	var logs bytes.Buffer
	firstLog := make(chan struct{})
	var once sync.Once
	cfg.ErrorLogger = slog.New(slog.NewJSONHandler(errorSinkFunc(func(p []byte) (int, error) {
		n, err := logs.Write(p)
		once.Do(func() { close(firstLog) })
		return n, err
	}), nil))
	w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) { return nil, syscall.EIO })
	for i := uint64(1); i <= 20; i++ {
		if _, err := w.Write([]byte("discard")); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return w.Stats().FailedRecords == i })
		if i == 1 {
			select {
			case <-firstLog:
			case <-time.After(time.Second):
				t.Fatal("first diagnostic did not arrive")
			}
		}
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	awaitDiagnostics(t, w)
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	var events uint64
	var previous time.Time
	for i, line := range lines {
		var record struct {
			Time      time.Time `json:"time"`
			Level     string    `json:"level"`
			Events    uint64    `json:"diagnostic_events"`
			Operation string    `json:"last_operation"`
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		if record.Level != "ERROR" || (record.Operation != "open" && record.Operation != "discard") {
			t.Fatalf("wrong diagnostic: %s", line)
		}
		if i > 0 && i < len(lines)-1 && record.Time.Sub(previous) < time.Second {
			t.Fatalf("error log flood: %s", logs.String())
		}
		previous = record.Time
		events += record.Events
	}
	if events != 40 || len(lines) < 2 {
		t.Fatalf("lost error summary: %s", logs.String())
	}
}

type errorSinkFunc func([]byte) (int, error)

func (f errorSinkFunc) Write(p []byte) (int, error) { return f(p) }

func TestBlockedErrorLoggerDoesNotBlockProducers(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords, cfg.QueueRecords = 1, 1
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer close(release)
	cfg.ErrorLogger = slog.New(slog.NewTextHandler(errorSinkFunc(func(p []byte) (int, error) {
		once.Do(func() { close(entered); <-release })
		return len(p), nil
	}), nil))
	w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) { return nil, syscall.EIO })
	if _, err := w.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("error logger was not called")
	}
	finished := make(chan error, 1)
	go func() {
		for i := 0; i < 10; i++ {
			if _, err := w.Write([]byte("next")); err != nil {
				finished <- err
				return
			}
		}
		finished <- nil
	}()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("producer blocked on error logging or queue capacity")
	}
}
