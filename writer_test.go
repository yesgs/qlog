package qlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type testFile struct {
	bytes.Buffer
	writeFn           func([]byte) (int, error)
	syncErr, closeErr error
	syncs, closes     int
}

func (f *testFile) Write(p []byte) (int, error) {
	if f.writeFn != nil {
		return f.writeFn(p)
	}
	return f.Buffer.Write(p)
}
func (f *testFile) Sync() error  { f.syncs++; return f.syncErr }
func (f *testFile) Close() error { f.closes++; return f.closeErr }

func testConfig(t testing.TB) Config {
	t.Helper()
	return Config{BaseDir: t.TempDir(), Name: "request", QueueBytes: 256, QueueRecords: 8, MaxRecordBytes: 64, BatchBytes: 128, FlushInterval: time.Hour, Location: time.UTC, ErrorLogger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}
func shutdown(t testing.TB, w *Writer) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return w.Shutdown(ctx)
}

func awaitDiagnostics(t *testing.T, w *Writer) {
	t.Helper()
	select {
	case <-w.diagnostics.done:
	case <-time.After(3 * time.Second):
		t.Fatal("diagnostics did not finish")
	}
}
func newTestWriter(t testing.TB, cfg Config, now func() time.Time, open openFunc) *Writer {
	t.Helper()
	w, err := newWriter(cfg, now, open, func(string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(t, w) }) // Individual tests assert diagnostics and counters.
	return w
}

func newDiskWriter(t testing.TB, cfg Config, now func() time.Time) *Writer {
	t.Helper()
	w, err := newWriter(cfg, now, openLogFile, sealLogFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(t, w) })
	return w
}

// minuteFileName groups uniquely named segments by their admission minute.
func minuteFileName(path string) string {
	name := strings.TrimSuffix(strings.TrimSuffix(filepath.Base(path), ".active"), ".log")
	name = name[:strings.LastIndexByte(name, '_')]
	name = name[:strings.LastIndexByte(name, '_')]
	return name + ".log"
}

func waitFor(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal("condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Config)
	}{
		{"base", func(c *Config) { c.BaseDir = "" }}, {"empty name", func(c *Config) { c.Name = "" }},
		{"traversal", func(c *Config) { c.Name = "../x" }}, {"dot", func(c *Config) { c.Name = "." }},
		{"backslash", func(c *Config) { c.Name = "a\\b" }}, {"windows punctuation", func(c *Config) { c.Name = "a:b" }},
		{"reserved name", func(c *Config) { c.Name = "CON.log" }}, {"control character", func(c *Config) { c.Name = "a\x00b" }},
		{"trailing space", func(c *Config) { c.Name = "request " }}, {"queue", func(c *Config) { c.QueueBytes = -1 }},
		{"record capacity", func(c *Config) { c.QueueRecords = -1 }}, {"size", func(c *Config) { c.MaxRecordBytes = -1 }},
		{"batch", func(c *Config) { c.BatchBytes = 1 }}, {"queue fit", func(c *Config) { c.QueueBytes = 1 }},
		{"interval", func(c *Config) { c.FlushInterval = -1 }},
		{"recovery interval", func(c *Config) { c.RecoveryInterval = -1 }},
		{"memory budget", func(c *Config) { c.QueueBytes, c.BatchBytes = 1<<30, 1<<30 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			tc.change(&cfg)
			if _, err := New(cfg); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestShutdownRejectsNilContext(t *testing.T) {
	w := newTestWriter(t, testConfig(t), time.Now, func(string) (diskFile, error) { return &testFile{}, nil })
	if err := w.Shutdown(nil); !errors.Is(err, ErrNilContext) { //nolint:staticcheck // Exercise the invalid-context contract.
		t.Fatalf("Shutdown(nil) = %v", err)
	}
	if _, err := w.Write([]byte("still open")); err != nil {
		t.Fatalf("writer closed after invalid shutdown: %v", err)
	}
}

func TestCompletedShutdownWithCanceledContext(t *testing.T) {
	w := newTestWriter(t, testConfig(t), time.Now, func(string) (diskFile, error) { return &testFile{}, nil })
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.Shutdown(ctx); err != nil {
		t.Fatalf("completed shutdown returned cancellation: %v", err)
	}
}

// TestShutdownRaceCompletedWithCanceledContext covers the defensive inner
// case <-w.done branch inside the ctx.Done() select arm. When both channels
// are ready Go picks nondeterministically; 200 iterations are enough to hit
// the inner branch with overwhelming probability.
func TestShutdownRaceCompletedWithCanceledContext(t *testing.T) {
	w := newTestWriter(t, testConfig(t), time.Now, func(string) (diskFile, error) { return &testFile{}, nil })
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 200; i++ {
		if err := w.Shutdown(ctx); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
}

func TestLinesRotationAndRestart(t *testing.T) {
	cfg := testConfig(t)
	cfg.FlushInterval = time.Millisecond
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 12, 23, 50, 0, 0, time.UTC).Unix())
	now := func() time.Time { return time.Unix(clock.Load(), 0) }
	w := newDiskWriter(t, cfg, now)
	payload := []byte("original")
	if n, err := w.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("write: %d %v", n, err)
	}
	copy(payload, "mutated!")
	for _, line := range []string{"crlf\r\n", "", "lf\n"} {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return w.Stats().WrittenRecords == 4 })
	clock.Add(60)
	if _, err := w.Write([]byte("next")); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("late")); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
	restarted := newDiskWriter(t, cfg, now)
	if _, err := restarted.Write([]byte("restart")); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(t, restarted); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(cfg.BaseDir, cfg.Name, "*.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 3 {
		t.Fatalf("sealed files: %v", paths)
	}
	want := map[string]bool{"original\ncrlf\n\nlf\n": true, "next\n": true, "restart\n": true}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !want[string(data)] {
			t.Fatalf("%s: unexpected contents %q", path, data)
		}
		delete(want, string(data))
	}
	s := w.Stats()
	if s.AcceptedRecords != 5 || s.WrittenRecords != 5 || s.PendingRecords != 0 || s.RejectedRecords != 1 {
		t.Fatalf("stats: %+v", s)
	}
}

func TestInputErrors(t *testing.T) {
	cfg := testConfig(t)
	f := &testFile{}
	w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) { return f, nil })
	if n, err := w.Write([]byte(strings.Repeat("x", 64))); n != 0 || !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("got %d %v", n, err)
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	if s := w.Stats(); s.ReceivedRecords != 1 || s.RejectedRecords != 1 || s.AcceptedRecords != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestEmbeddedNewlines(t *testing.T) {
	cfg := testConfig(t)
	f := &testFile{}
	w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) { return f, nil })
	for _, input := range []string{"first\nsecond", "third\nfourth\r\n"} {
		if n, err := w.Write([]byte(input)); err != nil || n != len(input) {
			t.Fatalf("write: %d %v", n, err)
		}
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	want := "first\nsecond\nthird\nfourth\n"
	if got := f.String(); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if s := w.Stats(); s.WrittenRecords != 2 || s.WrittenBytes != uint64(len(want)) {
		t.Fatalf("stats %+v", s)
	}
}

func TestDefaultRecordLimit(t *testing.T) {
	cfg := Config{BaseDir: t.TempDir(), Name: "request"}
	w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) { return &testFile{}, nil })
	if w.cfg.MaxRecordBytes != 128<<10 {
		t.Fatalf("default MaxRecordBytes = %d", w.cfg.MaxRecordBytes)
	}
	if n, err := w.Write([]byte(strings.Repeat("x", (128<<10)-1))); err != nil || n != (128<<10)-1 {
		t.Fatalf("maximum record: %d %v", n, err)
	}
	if n, err := w.Write([]byte(strings.Repeat("x", 128<<10))); n != 0 || !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("oversized record: %d %v", n, err)
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	if s := w.Stats(); s.WrittenRecords != 1 || s.WrittenBytes != 128<<10 || s.RejectedRecords != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestQueueOverflowDropsAndShutdownDeadline(t *testing.T) {
	cfg := testConfig(t)
	cfg.QueueBytes = 16
	cfg.QueueRecords = 2
	cfg.BatchBytes = 16
	cfg.MaxRecordBytes = 8
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f := &testFile{writeFn: func(p []byte) (int, error) { once.Do(func() { close(entered); <-release }); return len(p), nil }}
	w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) { return f, nil })
	defer close(release) // Unblock even on failure; registered before test cleanup.
	for i := 0; i < 2; i++ {
		if _, err := w.Write([]byte("1234567")); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not start")
	}
	for i := 0; i < 2; i++ {
		if _, err := w.Write([]byte("1234567")); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := w.Write([]byte("full")); n != 4 || err != nil {
		t.Fatalf("queue overflow: n=%d err=%v", n, err)
	}
	result := make(chan error, 1)
	go func() { _, err := w.Write([]byte("wait")); result <- err }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Write waited for queue space")
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer closeCancel()
	if err := w.Shutdown(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("closed")); !errors.Is(err, ErrClosed) {
		t.Fatalf("write after shutdown: %v", err)
	}
	s := w.Stats()
	if s.AcceptedRecords != 6 || s.FailedRecords != 2 || s.FailedBytes != 10 || s.PendingRecords != 4 || s.QueueBytes != 16 || s.RejectedRecords != 1 {
		t.Fatalf("stats %+v", s)
	}
	if !s.Closing || s.Closed {
		t.Fatalf("shutdown state before drain: %+v", s)
	}
}

func TestIOFailures(t *testing.T) {
	diskErr := errors.New("disk failure")
	cases := []struct {
		name                                 string
		openErr, writeErr, syncErr, closeErr error
		n                                    int
		durable                              bool
		written                              uint64
	}{
		{name: "open", openErr: diskErr},
		{name: "write", writeErr: diskErr, n: 5, written: 1},
		{name: "short write", n: 0},
		{name: "durable partial", writeErr: diskErr, n: 5, durable: true},
		{name: "durable sync", n: 9, syncErr: diskErr, durable: true},
		{name: "shutdown sync", n: 9, syncErr: diskErr, written: 3},
		{name: "close", n: 9, closeErr: diskErr, written: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.BatchRecords = 3
			cfg.SyncEveryBatch = tc.durable
			f := &testFile{syncErr: tc.syncErr, closeErr: tc.closeErr, writeFn: func([]byte) (int, error) { return tc.n, tc.writeErr }}
			// Keep all three records in one admission minute and one disk write.
			w := newTestWriter(t, cfg, func() time.Time { return time.Unix(1800000000, 0) }, func(string) (diskFile, error) {
				if tc.openErr != nil {
					return nil, tc.openErr
				}
				return f, nil
			})
			for i := 0; i < 3; i++ {
				if _, err := w.Write([]byte("xx")); err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, func() bool { return w.Stats().PendingRecords == 0 })
			if err := shutdown(t, w); err != nil {
				t.Fatal(err)
			}
			if w.Err() == nil {
				t.Fatal("missing I/O diagnostic")
			}
			if _, err := w.Write([]byte("late")); !errors.Is(err, ErrClosed) {
				t.Fatalf("got %v", err)
			}
			s := w.Stats()
			if s.WrittenRecords != tc.written || s.WrittenBytes != tc.written*3 || s.FailedRecords != 3-tc.written || s.PendingRecords != 0 || s.IOErrors == 0 {
				t.Fatalf("stats %+v", s)
			}
			if tc.openErr == nil && f.closes != 1 {
				t.Fatalf("close calls %d", f.closes)
			}
		})
	}
}

func TestConcurrentProducersShutdownAndScrape(t *testing.T) {
	cfg := testConfig(t)
	cfg.FlushInterval = time.Millisecond
	f := &testFile{}
	w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) { return f, nil })
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(NewCollector(w)); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	var accepted atomic.Uint64
	for producer := 0; producer < 8; producer++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := 0; i < 1000; i++ {
				_, err := w.Write([]byte("hello"))
				if errors.Is(err, ErrClosed) {
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
				accepted.Add(1)
			}
		}()
	}
	for i := 0; i < 10; i++ {
		if _, err := registry.Gather(); err != nil {
			t.Fatal(err)
		}
	}
	var closers sync.WaitGroup
	for i := 0; i < 4; i++ {
		closers.Add(1)
		go func() {
			defer closers.Done()
			if err := shutdown(t, w); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	closers.Wait()
	s := w.Stats()
	if s.AcceptedRecords != accepted.Load() || s.WrittenRecords+s.FailedRecords != accepted.Load() || s.PendingRecords != 0 || s.WrittenBytes+s.FailedBytes != accepted.Load()*6 {
		t.Fatalf("stats %+v accepted %d", s, accepted.Load())
	}
	if bytes.Count(f.Bytes(), []byte("\n")) != int(s.WrittenRecords) {
		t.Fatal("written count disagrees with file")
	}
}

func TestTimezone(t *testing.T) {
	cfg := testConfig(t)
	cfg.FlushInterval = time.Millisecond
	cfg.Location = time.FixedZone("UTC+8", 8*3600)
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 12, 15, 50, 0, 0, time.UTC).Unix())
	f := &testFile{}
	w := newTestWriter(t, cfg, func() time.Time { return time.Unix(clock.Load(), 0) }, func(string) (diskFile, error) { return f, nil })
	for i := 0; i < 5; i++ {
		if _, err := w.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return w.Stats().WrittenRecords == uint64(i+1) })
		clock.Add(60)
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	s := w.Stats()
	if s.ReceivedRecords != 5 || s.WrittenRecords != 5 {
		t.Fatalf("stats %+v", s)
	}
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(NewCollector(w)); err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, family := range families {
		if family.GetName() == "qlog_stream_written_records_total" {
			if family.Metric[0].GetCounter().GetValue() != 5 {
				t.Fatal(family)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("missing stream metrics")
	}
}

func TestClockRollback(t *testing.T) {
	cfg := testConfig(t)
	cfg.QueueRecords = 32
	var clock atomic.Int64
	clock.Store(1800000000)
	f := &testFile{}
	w := newTestWriter(t, cfg, func() time.Time { return time.Unix(clock.Load(), 0) }, func(string) (diskFile, error) { return f, nil })
	// Admission timestamp must survive queueing and clock movement in either direction.
	for i := 0; i < 30; i++ {
		clock.Add(int64((i%3)-1) * 60)
		if _, err := fmt.Fprint(w, i); err != nil {
			t.Fatal(err)
		}
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	s := w.Stats()
	if s.WrittenRecords != 30 || s.PendingRecords != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestDefaultConfigAndDurableWrite(t *testing.T) {
	cfg := Config{BaseDir: t.TempDir(), Name: "default", SyncEveryBatch: true}
	w, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(t, w) }) // The explicit Shutdown below checks the error.
	if _, err := w.Write([]byte("line")); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	if s := w.Stats(); s.WrittenRecords != 1 || s.SyncCalls < 2 {
		t.Fatalf("stats %+v", s)
	}
}

func TestFailureDrainsRemainingQueue(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchBytes = 64
	entered, release := make(chan struct{}), make(chan struct{})
	var first sync.Once
	f := &testFile{writeFn: func([]byte) (int, error) { first.Do(func() { close(entered); <-release }); return 0, io.ErrClosedPipe }}
	w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) { return f, nil })
	if _, err := w.Write([]byte(strings.Repeat("x", 63))); err != nil {
		t.Fatal(err)
	}
	<-entered
	for i := 0; i < 4; i++ {
		if _, err := w.Write([]byte("queued")); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	s := w.Stats()
	if s.FailedRecords != 5 || s.PendingRecords != 0 || s.QueueRecords != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func FuzzQueueRoundTrip(f *testing.F) {
	f.Add([]byte("a longer record to wrap around the ring"))
	f.Add([]byte{0, 255, 10, 13})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1024 {
			data = data[:1024]
		}
		cfg := testConfig(t)
		cfg.QueueBytes = 73
		cfg.QueueRecords = 3
		cfg.BatchBytes, cfg.BatchRecords = 64, 1
		written := make(chan struct{}, 1)
		file := &testFile{}
		file.writeFn = func(p []byte) (int, error) {
			n, err := file.Buffer.Write(p)
			written <- struct{}{}
			return n, err
		}
		w := newTestWriter(t, cfg, time.Now, func(string) (diskFile, error) { return file, nil })
		var want bytes.Buffer
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for _, value := range data {
			line := bytes.Repeat([]byte{'a' + value%26}, int(value%60)+1)
			if _, err := w.Write(line); err != nil {
				t.Fatal(err)
			}
			select {
			case <-written:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			want.Write(line)
			want.WriteByte('\n')
		}
		if err := shutdown(t, w); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(file.Bytes(), want.Bytes()) {
			t.Fatalf("queue corruption: got %d want %d", file.Len(), want.Len())
		}
	})
}
