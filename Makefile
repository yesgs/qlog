.PHONY: test vet lint bench bench-batches fuzz build check-monitoring

test:
	go test -race -cover ./...
vet:
	go vet ./...
lint:
	golangci-lint run
bench:
	go test -run '^$$' -bench BenchmarkQueue8KiB -benchmem -benchtime=2s -cpu=1,4
bench-batches:
	go test -run '^$$' -bench BenchmarkBatchBytes8KiB -benchmem -benchtime=2s -cpu=4
fuzz:
# Concurrent-worker coverage is scheduling-noisy; cap minimization so exec throughput does not stall.
	go test -run '^$$' -fuzz FuzzQueueRoundTrip -fuzztime=10s -fuzzminimizetime=1s -parallel=2
	go test -run '^$$' -fuzz FuzzValidName       -fuzztime=10s -fuzzminimizetime=1s
build:
	go build -o bin/loadgen ./cmd/loadgen
check-monitoring:
	promtool check config monitoring/prometheus.yml
	cd monitoring && promtool test rules qlog-alerts.test.yml
