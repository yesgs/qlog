package qlog

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"
)

type discardFile struct{}

func (discardFile) Write(p []byte) (int, error) { return len(p), nil }
func (discardFile) Sync() error                 { return nil }
func (discardFile) Close() error                { return nil }

// BenchmarkQueue8KiB measures submission overhead, including overflow discards,
// without disk I/O. written-MB/s and dropped-% describe the useful throughput.
func BenchmarkQueue8KiB(b *testing.B) {
	for _, parallel := range []bool{false, true} {
		name := "serial"
		if parallel {
			name = "parallel"
		}
		b.Run(name, func(b *testing.B) {
			benchmarkQueue8KiB(b, parallel, 0)
		})
	}
}

// BenchmarkBatchBytes8KiB compares batch sizes with concurrent producers.
// The queue size and record limits stay at their defaults; there is no disk I/O.
func BenchmarkBatchBytes8KiB(b *testing.B) {
	for _, mib := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("%dMiB", mib), func(b *testing.B) {
			benchmarkQueue8KiB(b, true, mib<<20)
		})
	}
}

func benchmarkQueue8KiB(b *testing.B, parallel bool, batchBytes int) {
	b.Helper()
	w := newTestWriter(b, Config{
		BaseDir: b.TempDir(), Name: "bench", BatchBytes: batchBytes,
		ErrorLogger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, time.Now, func(string) (diskFile, error) { return discardFile{}, nil })
	p := bytes.Repeat([]byte("x"), 8192)
	p[len(p)-1] = '\n'
	// Wait for the worker to complete its first flush so goroutine startup and
	// file-open latency are excluded from both time and allocations per operation.
	// Batch buffers are now allocated in newWriter, not asynchronously in run.
	if _, err := w.Write(p); err != nil {
		b.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for w.Stats().WrittenRecords != 1 {
		if time.Now().After(deadline) {
			b.Fatal("worker warmup timed out")
		}
		time.Sleep(time.Millisecond)
	}
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := w.Write(p); err != nil {
					b.Error(err)
					return
				}
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			if _, err := w.Write(p); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := shutdown(b, w); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	s := w.Stats()
	if s.WrittenRecords+s.FailedRecords != uint64(b.N)+1 || s.PendingRecords != 0 {
		b.Fatal("unresolved benchmark records")
	}
	b.ReportMetric(float64(s.FailedRecords)*100/float64(b.N), "dropped-%")
	b.ReportMetric(float64(s.WrittenBytes-uint64(len(p)))/b.Elapsed().Seconds()/1e6, "written-MB/s")
}
