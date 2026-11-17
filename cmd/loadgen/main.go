// Command loadgen runs an end-to-end load test and serves Prometheus metrics.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/yesgs/qlog"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	base := flag.String("base", "./data", "base output directory")
	name := flag.String("name", "request", "logical stream name")
	duration := flag.Duration("duration", 10*time.Second, "load duration")
	rps := flag.Int("rps", 20000, "records per second; zero means unlimited")
	size := flag.Int("size", 8192, "bytes per line, including LF")
	syncBatch := flag.Bool("sync", false, "fsync each batch")
	listen := flag.String("metrics", "127.0.0.1:9090", "metrics address; empty disables HTTP")
	flag.Parse()
	if *duration <= 0 || *size < 1 || *size > 64<<10 || *rps < 0 {
		return errors.New("invalid duration, size or rps")
	}
	w, err := qlog.New(qlog.Config{BaseDir: *base, Name: *name, SyncEveryBatch: *syncBatch})
	if err != nil {
		return err
	}
	// Also clean up on setup errors. The explicit final Shutdown below reports errors.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := w.Shutdown(ctx); err != nil {
			log.Printf("cleanup: %v", err)
		}
	}()
	var server *http.Server
	serverErrors := make(chan error, 1)
	if *listen != "" {
		registry := prometheus.NewRegistry()
		for _, collector := range []prometheus.Collector{qlog.NewCollector(w), collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})} {
			if err := registry.Register(collector); err != nil {
				return err
			}
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
		listener, err := net.Listen("tcp", *listen)
		if err != nil {
			return err
		}
		server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() { serverErrors <- server.Serve(listener) }()
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.Shutdown(ctx); err != nil {
				log.Printf("HTTP shutdown: %v", err)
			}
			if err := <-serverErrors; err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("HTTP server: %v", err)
			}
		}()
		log.Printf("metrics: http://%s/metrics", listener.Addr())
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, *duration)
	defer cancel()
	payload := bytes.Repeat([]byte("x"), *size)
	payload[len(payload)-1] = '\n'
	start := time.Now()
	var accepted int64
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var writeErr error
loop:
	for ctx.Err() == nil {
		if *rps > 0 {
			select {
			case <-ctx.Done():
				break loop
			case <-ticker.C:
			}
		}
		target := accepted + 1
		if *rps > 0 {
			target = int64(min(time.Since(start), *duration).Seconds() * float64(*rps))
		}
		for accepted < target {
			if ctx.Err() != nil {
				break loop
			}
			if _, err := w.Write(payload); err != nil {
				writeErr = err
				break loop
			}
			accepted++
		}
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	closeErr := w.Shutdown(shutdownCtx)
	elapsed := time.Since(start)
	stats := w.Stats()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	result := struct {
		ElapsedSeconds   float64    `json:"elapsed_seconds_including_shutdown"`
		RecordsPerSecond float64    `json:"written_records_per_second"`
		MBPerSecond      float64    `json:"written_MB_per_second"`
		HeapBytes        uint64     `json:"heap_bytes"`
		Goroutines       int        `json:"goroutines"`
		Stats            qlog.Stats `json:"stats"`
	}{elapsed.Seconds(), float64(stats.WrittenRecords) / elapsed.Seconds(), float64(stats.WrittenBytes) / elapsed.Seconds() / 1e6, memory.HeapAlloc, runtime.NumGoroutine(), stats}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	return errors.Join(writeErr, closeErr, validateResult(stats))
}

// Library calls are best effort; acceptance must inspect actual outcomes.
func validateResult(stats qlog.Stats) error {
	if stats.WrittenRecords == 0 || stats.FailedRecords != 0 || stats.PendingRecords != 0 ||
		stats.WrittenRecords != stats.AcceptedRecords || stats.WrittenBytes != stats.AcceptedBytes ||
		stats.SealedRecords != stats.WrittenRecords || stats.SealedBytes != stats.WrittenBytes ||
		stats.Failed || stats.IOErrors != 0 {
		return fmt.Errorf("loadgen: acceptance failed: written=%d sealed=%d accepted=%d discarded=%d pending=%d io_errors=%d unresolved_failure=%t",
			stats.WrittenRecords, stats.SealedRecords, stats.AcceptedRecords, stats.FailedRecords, stats.PendingRecords, stats.IOErrors, stats.Failed)
	}
	return nil
}
