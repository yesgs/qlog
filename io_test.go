package qlog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type ioTestFile struct {
	testFile
	syncFn func() error
}

func (f *ioTestFile) Sync() error {
	if f.syncFn != nil {
		return f.syncFn()
	}
	return f.testFile.Sync()
}

func TestIOFailureDropsOnceAndLaterRecordsRecover(t *testing.T) {
	for _, operation := range []string{"open", "write", "sync"} {
		for _, cause := range []error{syscall.ENOSPC, syscall.EDQUOT, syscall.EAGAIN, syscall.EINTR, syscall.EIO, syscall.EACCES} {
			t.Run(fmt.Sprintf("%s/%v", operation, cause), func(t *testing.T) {
				cfg := testConfig(t)
				cfg.BatchRecords, cfg.RecoveryInterval = 1, time.Nanosecond
				cfg.SyncEveryBatch = operation == "sync"
				failure := &os.PathError{Op: operation, Path: "log", Err: cause}
				var broken atomic.Bool
				broken.Store(true)
				var attempts atomic.Int64
				var files []*ioTestFile
				w := newTestWriter(t, cfg, func() time.Time { return time.Unix(1800000000, 0) }, func(string) (diskFile, error) {
					if operation == "open" && broken.Load() {
						attempts.Add(1)
						return nil, failure
					}
					file := &ioTestFile{}
					file.writeFn = func(p []byte) (int, error) {
						if operation == "write" && broken.Load() {
							attempts.Add(1)
							return 0, failure
						}
						return file.Buffer.Write(p)
					}
					file.syncFn = func() error {
						if operation == "sync" && broken.Load() {
							attempts.Add(1)
							return failure
						}
						return nil
					}
					files = append(files, file)
					return file, nil
				})
				if _, err := w.Write([]byte("lost")); err != nil {
					t.Fatal(err)
				}
				waitFor(t, func() bool { return w.Stats().FailedRecords == 1 })
				if attempts.Load() != 1 || !errors.Is(w.Err(), cause) || w.Stats().Closing {
					t.Fatalf("failure retried or stopped Writer: calls=%d err=%v stats=%+v", attempts.Load(), w.Err(), w.Stats())
				}
				broken.Store(false)
				if n, err := w.Write([]byte("recovered")); n != 9 || err != nil {
					t.Fatalf("after recovery: n=%d err=%v", n, err)
				}
				waitFor(t, func() bool { return w.Stats().WrittenRecords == 1 })
				if err := shutdown(t, w); err != nil {
					t.Fatal(err)
				}
				s := w.Stats()
				if s.AcceptedRecords != 2 || s.FailedRecords != 1 || s.FailedBytes != 5 || s.WrittenBytes != 10 || s.PendingRecords != 0 || s.IOErrors != 1 || s.Failed {
					t.Fatalf("recovery stats: %+v", s)
				}
				if files[len(files)-1].String() != "recovered\n" {
					t.Fatalf("failed record replayed: %q", files[len(files)-1].String())
				}
				for _, file := range files {
					if file.closes != 1 {
						t.Fatalf("close count: %d", file.closes)
					}
				}
			})
		}
	}
}

func TestSingleWriteAccountsForCompletePrefix(t *testing.T) {
	for _, durable := range []bool{false, true} {
		for _, tc := range []struct {
			name string
			n    int
			err  error
		}{
			{"short", 5, nil}, {"partial", 5, syscall.ENOSPC},
			{"zero", 0, nil}, {"full with error", 9, syscall.EAGAIN},
			{"negative count", -1, nil}, {"excessive count", 10, nil},
			{"invalid count with error", 10, syscall.EIO},
		} {
			t.Run(fmt.Sprintf("durable=%t/%s", durable, tc.name), func(t *testing.T) {
				cfg := testConfig(t)
				cfg.BatchRecords, cfg.SyncEveryBatch = 3, durable
				calls := 0
				file := &testFile{writeFn: func(p []byte) (int, error) {
					calls++
					if string(p) != "aa\nbb\ncc\n" {
						t.Errorf("unexpected write or replay: %q", p)
					}
					return tc.n, tc.err
				}}
				// Minute rotation must not split the batch under test.
				w := newTestWriter(t, cfg, func() time.Time { return time.Unix(1800000000, 0) }, func(string) (diskFile, error) { return file, nil })
				for _, line := range []string{"aa", "bb", "cc"} {
					if _, err := w.Write([]byte(line)); err != nil {
						t.Fatal(err)
					}
				}
				waitFor(t, func() bool { return w.Stats().PendingRecords == 0 })
				if err := shutdown(t, w); err != nil {
					t.Fatal(err)
				}
				wantErr := tc.err
				if wantErr == nil {
					wantErr = io.ErrShortWrite
				}
				written := uint64(0)
				if !durable && tc.n >= 0 && tc.n <= 9 {
					written = uint64(tc.n / 3)
				}
				s := w.Stats()
				if calls != 1 || file.closes != 1 || !errors.Is(w.Err(), wantErr) || s.IOErrors != 1 || !s.Failed || s.SyncCalls != 0 {
					t.Fatalf("failure handling: calls=%d closes=%d err=%v stats=%+v", calls, file.closes, w.Err(), s)
				}
				if s.WrittenRecords != written || s.WrittenBytes != written*3 || s.FailedRecords != 3-written || s.FailedBytes != (3-written)*3 || s.PendingRecords != 0 || s.PartialWriteBytes != uint64(max(0, min(tc.n, 9))) {
					t.Fatalf("partial write accounting: %+v", s)
				}
			})
		}
	}
}

func TestCloseFailureIsNotRetried(t *testing.T) {
	file := &testFile{closeErr: syscall.EINTR}
	w := newTestWriter(t, testConfig(t), time.Now, func(string) (diskFile, error) { return file, nil })
	if _, err := w.Write([]byte("record")); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	if file.closes != 1 || !errors.Is(w.Err(), syscall.EINTR) || w.Stats().IOErrors != 1 {
		t.Fatalf("close retried: closes=%d err=%v stats=%+v", file.closes, w.Err(), w.Stats())
	}
}
