package qlog

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestOpenFailureDoesNotSyncOrClose(t *testing.T) {
	w := newDiskWriter(t, testConfig(t), time.Now)
	// A file at the directory path prevents lazy directory creation.
	if err := os.WriteFile(w.dir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("record")); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(t, w); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
	s := w.Stats()
	if s.IOErrors != 1 || s.SyncCalls != 0 || s.WriteCalls != 0 {
		t.Fatalf("failed open caused extra file operations: %+v", s)
	}
	if !s.Failed || s.FailedRecords != 1 || s.PendingRecords != 0 {
		t.Fatalf("failed record not resolved: %+v", s)
	}
}

func TestEmptyCleanupPreservesChangedFiles(t *testing.T) {
	for _, mode := range []string{"unchanged", "nonempty", "replaced", "missing"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "segment.log.active")
			file, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			info, err := file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "nonempty":
				if err := os.WriteFile(path, []byte("preserve"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "replaced":
				if err := os.Rename(path, path+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			if err := removeEmptyLogFile(path, info); err != nil {
				t.Fatal(err)
			}
			_, err = os.Stat(path)
			if mode == "unchanged" || mode == "missing" {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("empty segment remains: %v", err)
				}
			} else if err != nil {
				t.Fatalf("changed file was removed: %v", err)
			}
			if mode == "nonempty" && readLog(t, path) != "preserve" {
				t.Fatal("nonempty file changed")
			}
		})
	}
}

func TestNewWithUnavailableDirectoryRecovers(t *testing.T) {
	cfg := testConfig(t)
	cfg.BatchRecords, cfg.RecoveryInterval = 1, time.Nanosecond
	dir := filepath.Join(cfg.BaseDir, cfg.Name)
	if err := os.WriteFile(dir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := New(cfg)
	if err != nil {
		t.Fatalf("disk fault prevented startup: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(t, w) })
	if _, err := w.Write([]byte("lost")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w.Stats().FailedRecords == 1 })
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("recovered")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w.Stats().WrittenRecords == 1 })
	if err := shutdown(t, w); err != nil {
		t.Fatal(err)
	}
	paths := logPaths(t, w, "*.log")
	if len(paths) != 1 || readLog(t, paths[0]) != "recovered\n" || w.Err() != nil {
		t.Fatalf("directory recovery failed: files=%v err=%v", paths, w.Err())
	}
}

// TestRemoveEmptyLogFileLstatError covers the branch where os.Lstat returns a
// non-ErrNotExist error (e.g. permission denied on the parent directory).
func TestRemoveEmptyLogFileLstatError(t *testing.T) {
	if os.Getuid() == 0 || runtime.GOOS == "windows" {
		t.Skip("root bypasses, and Windows ignores, directory execute-permission checks")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "segment.log.active")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	// Revoke execute permission on the parent so Lstat on the child fails.
	if err := os.Chmod(dir, 0o600); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(dir, 0o750) }() // restore so t.TempDir cleanup can proceed
	if err := removeEmptyLogFile(path, info); err == nil {
		t.Fatal("expected Lstat error on untraversable directory")
	}
}
