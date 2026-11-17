package qlog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// A cleanup failure is a one-shot event. Later healthy segments must clear
// Err(), Stats.Failed and qlog_failed, otherwise QlogUnresolvedIOFailure
// keeps firing for the rest of the process lifetime.
func TestCleanupFailureIsTransient(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords = 1
	cfg.RecoveryInterval = time.Nanosecond
	cfg.ErrorLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	var first atomic.Bool
	first.Store(true)
	w, err := newWriter(cfg, time.Now, func(path string) (diskFile, error) {
		f, err := openLogFile(path)
		if err != nil {
			return nil, err
		}
		h := &hookedDiskFile{diskFile: f}
		if first.CompareAndSwap(true, false) {
			h.writeFn = func([]byte) (int, error) { return 0, syscall.EIO }
			h.statFn = func() (os.FileInfo, error) { return nil, errors.New("injected stat failure") }
		}
		return h, nil
	}, sealLogFile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("lost")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w.Stats().FailedRecords == 1 })
	for i := 0; i < 3; i++ {
		if _, err := w.Write([]byte("healthy")); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return w.Stats().WrittenRecords == uint64(i+1) })
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(NewCollector(w)); err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	failed := -1.0
	for _, family := range families {
		if family.GetName() == "qlog_failed" {
			failed = family.Metric[0].GetGauge().GetValue()
		}
	}
	s := w.Stats()
	if w.Err() != nil || s.Failed || failed != 0 {
		t.Fatalf("cleanup failure did not recover: err=%v failed=%v qlog_failed=%v stats=%+v",
			w.Err(), s.Failed, failed, s)
	}
	if s.IOErrors < 2 || s.WrittenRecords != 3 || len(logPaths(t, w, "*.log")) != 1 {
		t.Fatalf("diagnostics or data lost: %+v", s)
	}
}
