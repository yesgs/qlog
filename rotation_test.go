package qlog

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriteUsesAdmissionMinute(t *testing.T) {
	cfg := testConfig(t)
	cfg.QueueRecords = 2
	cfg.BatchRecords = 1
	initial := time.Date(2026, 11, 14, 23, 59, 59, 0, time.UTC)
	var clock atomic.Int64
	clock.Store(initial.Unix())
	entered, release := make(chan struct{}), make(chan struct{})
	var firstWrite, unblock sync.Once
	files := make(map[string]string)
	w := newTestWriter(t, cfg, func() time.Time { return time.Unix(clock.Load(), 0) }, func(path string) (diskFile, error) {
		return &testFile{writeFn: func(p []byte) (int, error) {
			firstWrite.Do(func() { close(entered); <-release })
			files[minuteFileName(path)] += string(p)
			return len(p), nil
		}}, nil
	})
	defer unblock.Do(func() { close(release) })
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
	clock.Store(initial.Add(time.Second).Unix())
	if _, err := w.Write([]byte("next")); err != nil {
		t.Fatal(err)
	}
	unblock.Do(func() { close(release) })
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"request_202611142359.log": "first\nqueued\n",
		"request_202611150000.log": "next\n",
	} {
		if files[name] != want {
			t.Errorf("%s: got %q, want %q", name, files[name], want)
		}
	}
}

func TestQueuedRecordsKeepAdmissionMinute(t *testing.T) {
	for _, tc := range []struct {
		name     string
		step     time.Duration
		location *time.Location
	}{
		{"next minute", time.Second, time.UTC},
		{"clock rollback", -time.Minute, time.UTC},
		{"local midnight", time.Second, time.FixedZone("UTC+8", 8*3600)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.BatchBytes = 64
			cfg.Location = tc.location
			initial := time.Date(2026, 11, 14, 23, 59, 59, 0, tc.location)
			var clock atomic.Int64
			clock.Store(initial.Unix())
			entered, release := make(chan struct{}), make(chan struct{})
			var firstWrite, unblock sync.Once
			files := make(map[string]string)
			w := newTestWriter(t, cfg, func() time.Time { return time.Unix(clock.Load(), 0) }, func(path string) (diskFile, error) {
				name := minuteFileName(path)
				f := &testFile{}
				f.writeFn = func(p []byte) (int, error) {
					firstWrite.Do(func() { close(entered); <-release })
					files[name] += string(p)
					return len(p), nil
				}
				return f, nil
			})
			defer unblock.Do(func() { close(release) })
			first := strings.Repeat("x", cfg.BatchBytes-1)
			if _, err := w.Write([]byte(first)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("worker did not start writing")
			}

			// Keep the worker blocked while admission crosses a minute and returns.
			// The queued records fit in one batch unless split by their file times.
			oldName := "request_" + initial.Format("200601021504") + ".log"
			want := map[string]string{oldName: first + "\n"}
			for _, entry := range []struct {
				offset time.Duration
				text   string
			}{
				{0, "before-a"}, {0, "before-b"},
				{tc.step, "after-a"}, {tc.step, "after-b"},
				{0, "returned"},
			} {
				admitted := initial.Add(entry.offset)
				clock.Store(admitted.Unix())
				if _, err := w.Write([]byte(entry.text)); err != nil {
					t.Fatal(err)
				}
				name := "request_" + admitted.Format("200601021504") + ".log"
				want[name] += entry.text + "\n"
			}
			unblock.Do(func() { close(release) })
			if err := shutdown(t, w); err != nil {
				t.Fatal(err)
			}
			if len(files) != len(want) {
				t.Errorf("created %d files, want %d", len(files), len(want))
			}
			for name, content := range want {
				got, ok := files[name]
				if !ok {
					t.Errorf("missing minute file %s", name)
				} else if got != content {
					t.Errorf("%s: got %q, want %q", name, got, content)
				}
			}
			if s := w.Stats(); s.WrittenRecords != 6 || s.FailedRecords != 0 || s.PendingRecords != 0 {
				t.Fatalf("stats: %+v", s)
			}
		})
	}
}
