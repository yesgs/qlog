package qlog

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
)

func TestPortableStreamNames(t *testing.T) {
	for _, tc := range []struct {
		valid bool
		names []string
	}{
		{true, []string{"request", "access-01.log", "a b", ".hidden", "日志", "COM10", "LPT0", strings.Repeat("x", 177)}},
		{false, []string{
			"", ".", "..", "../request", "a/b", `a\b`, "a:b", "a*b", "a?b", `a"b`, "a<b", "a>b", "a|b",
			"trailing ", "trailing.", "a\x00b", "a\nb", "a\x7fb", "a\u0085b", "a\xffb",
			// Cf (format) characters must be rejected: invisible on-screen, unreachable
			// on Windows/SMB and not caught by unicode.IsControl (Cc only).
			"a\u200bb", // zero-width space U+200B
			"a\ufeffb", // byte-order mark U+FEFF
			"a\u202eb", // right-to-left override U+202E
			"a\u2060b", // word joiner U+2060
			"CON", "prn", "AuX.log", "NUL", "COM1", "lpt9.txt", "COM¹", "LPT².log", "com³",
			"CONIN$", "conout$", "con .log", strings.Repeat("x", 178),
		}},
	} {
		for _, name := range tc.names {
			t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
				cfg := testConfig(t)
				cfg.Name = name
				w, err := New(cfg)
				if w != nil {
					t.Cleanup(func() { _ = shutdown(t, w) })
				}
				if !tc.valid {
					if err == nil || w != nil {
						t.Fatalf("invalid name accepted: writer=%v error=%v", w != nil, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err := w.Write([]byte("record")); err != nil {
					t.Fatal(err)
				}
				if err := shutdown(t, w); err != nil {
					t.Fatal(err)
				}
				paths := logPaths(t, w, "*.log")
				if w.Err() != nil || len(paths) != 1 || readLog(t, paths[0]) != "record\n" {
					t.Fatalf("valid name could not be written: files=%v error=%v", paths, w.Err())
				}
			})
		}
	}
}

func TestBufferMemoryBudget(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, tc := range []struct {
		name   string
		change func(*Config)
		valid  bool
	}{
		{"small", func(*Config) {}, true},
		{"exact limit", func(c *Config) { c.QueueBytes = (1 << 30) - 25 }, true},
		{"over limit", func(c *Config) { c.QueueBytes = (1 << 30) - 24 }, false},
		{"payload sum", func(c *Config) { c.QueueBytes, c.BatchBytes = 1<<29, 1<<29 }, false},
		{"queue metadata", func(c *Config) { c.QueueRecords = 1 << 26 }, false},
		{"batch metadata", func(c *Config) { c.BatchRecords = 1 << 27 }, false},
		{"queue integer limit", func(c *Config) { c.QueueBytes = maxInt }, false},
		{"batch integer limit", func(c *Config) { c.BatchBytes = maxInt }, false},
		{"queue metadata integer limit", func(c *Config) { c.QueueRecords = maxInt }, false},
		{"batch metadata integer limit", func(c *Config) { c.BatchRecords = maxInt }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{BaseDir: t.TempDir(), Name: "request", QueueBytes: 1, BatchBytes: 1, MaxRecordBytes: 1, QueueRecords: 1, BatchRecords: 1}
			tc.change(&cfg)
			// Validate boundary sizes without allocating large buffers.
			_, err := cfg.normalized()
			if tc.valid && err != nil {
				t.Fatal(err)
			}
			if !tc.valid && (err == nil || !strings.Contains(err.Error(), "1 GiB")) {
				t.Fatalf("expected memory budget error: %v", err)
			}
		})
	}
}

func TestRelativeBaseDirWithDeletedWorkingDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows cannot remove the current working directory")
	}
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	w, err := New(Config{BaseDir: ".", Name: "request"})
	if w != nil {
		_ = shutdown(t, w)
		t.Fatal("created a writer without a resolvable base directory")
	}
	if !errors.Is(err, os.ErrNotExist) || !strings.Contains(err.Error(), "resolve directory") {
		t.Fatalf("expected directory resolution error: %v", err)
	}
}

// FuzzValidName searches for inputs that validName accepts but are either empty,
// contain path separators or Windows-reserved characters, end in space or dot,
// contain Unicode "Other"-category codepoints (Cc, Cf, …), or exceed the byte
// limit. These properties are independent of OS and must hold for all inputs.
func FuzzValidName(f *testing.F) {
	// Seed from the known-valid and known-invalid corpus.
	for _, seed := range []string{
		"request", "access-01.log", "a b", ".hidden", "日志",
		"COM10", "LPT0",
		"", ".", "..", "../x", "a/b", `a\b`, "a:b", "a*b",
		"trailing ", "trailing.", "CON", "COM1", "LPT9",
		"a\x00b", "a\u200bb", "a\ufeffb", "a\u202eb",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if !validName(name) {
			return // rejected: nothing to check
		}
		// Accepted names must satisfy all portability invariants.
		if name == "" || len(name) > maxNameBytes {
			t.Fatalf("accepted empty or oversized name %q (len=%d)", name, len(name))
		}
		for _, banned := range []string{"/", `\`, ":", "*", "?", `"`, "<", ">", "|"} {
			if strings.Contains(name, banned) {
				t.Fatalf("accepted name with reserved character %q: %q", banned, name)
			}
		}
		if strings.HasSuffix(name, " ") || strings.HasSuffix(name, ".") {
			t.Fatalf("accepted name with trailing space or dot: %q", name)
		}
		for _, r := range name {
			if r > 0 && r <= 0x10FFFF && (r>>11) == (0xD800>>11) {
				// surrogate half — utf8.ValidString would catch this, but check anyway
				t.Fatalf("accepted name with surrogate rune %U: %q", r, name)
			}
		}
	})
}
