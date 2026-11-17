// Package qlog writes records asynchronously to minute files.
// Writes are best effort: I/O failures and queue overflow are logged and counted,
// without stopping the Writer. Call Shutdown before exiting.
package qlog

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Admission errors do not transfer ownership of the record to the writer.
var (
	ErrClosed         = errors.New("qlog: writer closed")
	ErrRecordTooLarge = errors.New("qlog: record too large")
)

// ErrNilContext reports a Shutdown call without a context.
var ErrNilContext = errors.New("qlog: nil shutdown context")

const (
	// Keep one Writer's preallocated buffers within a bounded budget. This is a
	// configuration guard, not a promise about total process memory.
	maxConfiguredMemory = int64(1 << 30)
	// Conservative metadata sizes for both 32-bit and 64-bit platforms.
	recordMetadataBytes = int64(16)
	batchMetadataBytes  = int64(8)
	// Leave room in a 255-byte component for the timestamp, writer ID, a
	// 20-digit sequence number, separators and the .log.active suffix.
	maxNameBytes = 177
)

// Config controls one logical log stream. Zero optional fields use defaults.
// The directory BaseDir/Name must have only one active Writer, across processes.
// Preallocated payload and metadata buffers are limited to 1 GiB per Writer.
type Config struct {
	BaseDir string
	// Name is a valid UTF-8 path component of at most 177 bytes. Windows reserved
	// characters/device names, control characters and trailing dots/spaces are invalid.
	Name string
	// QueueBytes bounds queued payload memory (default 32 MiB).
	QueueBytes int
	// QueueRecords bounds queue metadata and tiny-record accumulation (default 65536).
	QueueRecords int
	// MaxRecordBytes includes the terminating newline (default 128 KiB).
	MaxRecordBytes int
	// BatchBytes bounds the contiguous disk-write buffer (default 1 MiB).
	BatchBytes int
	// BatchRecords bounds the number of records per disk-write batch (default 4096).
	// Zero uses the default limit.
	BatchRecords int
	// FlushInterval is the maximum batching delay when the disk keeps up (default 100ms).
	FlushInterval time.Duration
	// RecoveryInterval is the cooldown before opening a new segment after an
	// I/O failure (default 100ms). Batches during cooldown drop. Failed operations
	// are never retried; subsequent batches attempt recovery after the cooldown.
	RecoveryInterval time.Duration
	// ErrorLogger receives rate-limited ERROR diagnostics from a dedicated reporter.
	// Nil uses a separate stderr logger, independent of slog.Default().
	// Writing and Shutdown do not wait for this logger. A blocked handler can
	// retain its one reporter goroutine until it returns, but not payload buffers.
	// A custom logger must not write back into this Writer.
	//
	// Emitted log fields (subject to handler formatting):
	//   - last_operation: the operation of the most recent diagnostic event
	//     (open/write/sync/close/seal/cleanup/enqueue/discard); events across
	//     different operations in the same emit window are not enumerated.
	//   - diagnostic_events: count of diagnostic events since the previous emit.
	//   - cumulative_failed_records / cumulative_failed_bytes: lifetime totals
	//     at emit time, not per-window deltas.
	ErrorLogger *slog.Logger
	// Location controls file names. Nil uses time.Local.
	Location *time.Location
	// SyncEveryBatch makes successful batches wait for fsync; otherwise success
	// means os.File.Write succeeded. Rotation and Shutdown always fsync.
	SyncEveryBatch bool
}

func (c Config) normalized() (Config, error) {
	if c.BaseDir == "" || strings.ContainsRune(c.BaseDir, 0) || !validName(c.Name) {
		return c, errors.New("qlog: BaseDir and a portable single-component Name are required")
	}
	if c.QueueBytes == 0 {
		c.QueueBytes = 32 << 20
	}
	if c.QueueRecords == 0 {
		c.QueueRecords = 65536
	}
	if c.MaxRecordBytes == 0 {
		c.MaxRecordBytes = 128 << 10
	}
	if c.BatchBytes == 0 {
		c.BatchBytes = 1 << 20
	}
	if c.BatchRecords == 0 {
		c.BatchRecords = 4096
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = 100 * time.Millisecond
	}
	if c.RecoveryInterval == 0 {
		c.RecoveryInterval = 100 * time.Millisecond
	}
	if c.RecoveryInterval < 0 {
		return c, errors.New("qlog: RecoveryInterval must be nonnegative")
	}
	if c.Location == nil {
		c.Location = time.Local
	}
	if c.ErrorLogger == nil {
		c.ErrorLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	}
	if c.QueueBytes < 1 || c.QueueRecords < 1 || c.MaxRecordBytes < 1 || c.BatchBytes < c.MaxRecordBytes || c.QueueBytes < c.MaxRecordBytes || c.FlushInterval < 0 || c.BatchRecords < 1 {
		return c, errors.New("qlog: positive limits required; MaxRecordBytes must fit QueueBytes and BatchBytes; BatchRecords must be positive")
	}
	// Each component is checked against the remaining budget before being
	// subtracted; a single error site avoids repeating the same string.
	budget := maxConfiguredMemory
	for _, item := range []struct{ size, unit int64 }{
		{int64(c.QueueBytes), 1},
		{int64(c.BatchBytes), 1},
		{int64(c.QueueRecords), recordMetadataBytes},
		{int64(c.BatchRecords), batchMetadataBytes},
	} {
		if item.size > budget/item.unit {
			return c, errors.New("qlog: configured buffers exceed 1 GiB")
		}
		budget -= item.size * item.unit
	}
	absolute, err := filepath.Abs(c.BaseDir)
	if err != nil {
		return c, fmt.Errorf("qlog: resolve directory: %w", err)
	}
	c.BaseDir = absolute
	return c, nil
}

func validName(name string) bool {
	if name == "" || len(name) > maxNameBytes || !utf8.ValidString(name) || strings.ContainsAny(name, `/\\:*?"<>|`) {
		return false
	}
	if strings.HasSuffix(name, " ") || strings.HasSuffix(name, ".") {
		return false
	}
	for _, r := range name {
		// Reject all Unicode "Other" categories: control (Cc), format (Cf),
		// surrogate (Cs), private use (Co) and unassigned (Cn). Using IsControl
		// alone would miss Cf characters such as zero-width space (U+200B) and
		// BOM (U+FEFF) that are invisible in directory listings and unreachable
		// on Windows and some SMB mounts.
		if unicode.Is(unicode.C, r) {
			return false
		}
	}
	base, _, _ := strings.Cut(name, ".")
	base = strings.ToUpper(strings.TrimRight(base, " "))
	switch base {
	case "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$":
		return false
	}
	// Windows reserves COM1–COM9, LPT1–LPT9 and their Unicode superscript
	// equivalents (¹²³). A range check on the suffix avoids enumerating each
	// digit; superscripts are listed explicitly because they fall outside ASCII.
	if strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT") {
		suffix := base[3:]
		if len(suffix) == 1 && suffix[0] >= '1' && suffix[0] <= '9' {
			return false
		}
		if suffix == "¹" || suffix == "²" || suffix == "³" {
			return false
		}
	}
	return true
}
