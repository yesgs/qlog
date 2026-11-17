package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

func TestRunRejectsAllDiscardedLogs(t *testing.T) {
	base := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "request"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runTestLoad(t, base); err == nil {
		t.Fatal("acceptance succeeded with no written records")
	}
}

func TestRunAcceptsHealthyWrites(t *testing.T) {
	base := t.TempDir()
	if err := runTestLoad(t, base); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(base, "request", "*.log"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("missing sealed output: paths=%v err=%v", paths, err)
	}
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil || info.Size() == 0 || info.Size()%128 != 0 {
			t.Fatalf("invalid output: %v, %v", info, err)
		}
	}
}

func runTestLoad(t *testing.T, base string) error {
	t.Helper()
	previousFlags, previousArgs := flag.CommandLine, os.Args
	t.Cleanup(func() { flag.CommandLine, os.Args = previousFlags, previousArgs })
	flag.CommandLine = flag.NewFlagSet("loadgen", flag.ContinueOnError)
	os.Args = []string{"loadgen", "-base", base, "-duration", "200ms", "-rps", "1000", "-size", "128", "-metrics", ""}
	return run()
}
