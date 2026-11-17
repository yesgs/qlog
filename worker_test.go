package qlog

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConcurrentCopiesAcrossRingWrap(t *testing.T) {
	const waves, producers, perProducer = 20, 4, 32
	cfg := testConfig(t)
	cfg.QueueBytes, cfg.QueueRecords = 512<<10, producers*perProducer
	cfg.BatchBytes, cfg.BatchRecords, cfg.MaxRecordBytes = 64<<10, 13, 4096
	cfg.FlushInterval = time.Millisecond
	file := &testFile{}
	w := newTestWriter(t, cfg, func() time.Time { return time.Unix(1800000000, 0) }, func(string) (diskFile, error) { return file, nil })
	want := make(map[string]bool)
	for wave := 0; wave < waves; wave++ {
		var inputs [producers][perProducer][]byte
		for producer := range inputs {
			for i := range inputs[producer] {
				header := fmt.Sprintf("%d/%d/%d:", wave, producer, i)
				size := 1024 + (wave*53+producer*29+i*137)%3072
				line := header + strings.Repeat(string(rune('a'+producer)), size-len(header)-1)
				inputs[producer][i] = []byte(line + "\n")
				want[line] = true
			}
		}
		var group sync.WaitGroup
		for producer := range inputs {
			group.Go(func() {
				for _, input := range inputs[producer] {
					if _, err := w.Write(input); err != nil {
						t.Error(err)
						return
					}
					// The caller may immediately reuse its input buffer.
					clear(input)
					s := w.Stats()
					if s.ReceivedRecords != s.AcceptedRecords+s.RejectedRecords ||
						s.AcceptedRecords != s.WrittenRecords+s.FailedRecords+s.PendingRecords ||
						s.AcceptedBytes != s.WrittenBytes+s.FailedBytes+s.PendingBytes ||
						s.QueueRecords < 0 || uint64(s.QueueRecords) > s.PendingRecords ||
						s.QueueBytes < 0 || uint64(s.QueueBytes) > s.PendingBytes {
						t.Errorf("inconsistent snapshot during copy: %+v", s)
					}
				}
			})
		}
		group.Wait()
		// Each wave fits the queue even if the worker has not started copying.
		waitFor(t, func() bool { return w.Stats().PendingRecords == 0 })
		if s := w.Stats(); s.FailedRecords != 0 || s.WrittenRecords != uint64((wave+1)*producers*perProducer) {
			t.Fatalf("wave %d lost records: %+v", wave, s)
		}
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSuffix(file.String(), "\n"), "\n") {
		if !want[line] {
			t.Fatalf("duplicate or corrupted record: %.80q", line)
		}
		delete(want, line)
	}
	if len(want) != 0 {
		t.Fatalf("missing %d records", len(want))
	}
}
