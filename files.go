package qlog

import (
	"os"
	"path/filepath"
	"strings"
)

func openLogFile(path string) (diskFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	// Never reopen an existing segment, including one left by an interrupted run.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// sealCleanupError reports an unlink failure after the .log was published.
type sealCleanupError struct {
	err error
}

func (e *sealCleanupError) Error() string { return e.err.Error() }
func (e *sealCleanupError) Unwrap() error { return e.err }

func sealLogFile(path string) error {
	// Linking publishes the closed inode atomically without replacing an existing
	// sealed file. A plain os.Rename could silently overwrite the destination.
	// If interrupted between link and unlink, both names refer to sealed data;
	// the .active name is never reopened automatically.
	if err := os.Link(path, strings.TrimSuffix(path, ".active")); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return &sealCleanupError{err: err}
	}
	return nil
}

// Only remove the empty inode just closed by this Writer. Historical files,
// nonempty files, symlinks and replaced pathnames are left untouched.
func removeEmptyLogFile(path string, opened os.FileInfo) error {
	current, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if current.Mode().IsRegular() && current.Size() == 0 && os.SameFile(opened, current) {
		return os.Remove(path)
	}
	return nil
}
