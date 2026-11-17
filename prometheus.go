package qlog

import "github.com/prometheus/client_golang/prometheus"

type counterMetric struct {
	name, help string
	value      func(Counters) uint64
	stream     *prometheus.Desc
}
type scalarMetric struct {
	desc  *prometheus.Desc
	kind  prometheus.ValueType
	value func(Stats) float64
}

// Collector exports stream-level lifetime counters, queue gauges, and I/O counters.
// Register Go/process collectors separately if needed.
// Register one Collector per Writer in a registry; duplicate stream paths conflict.
type Collector struct {
	writer   *Writer
	counters []counterMetric
	scalars  []scalarMetric
}

// NewCollector creates a Prometheus collector without changing any global registry.
func NewCollector(w *Writer) *Collector {
	c := &Collector{writer: w, counters: []counterMetric{
		{name: "received_records_total", help: "Completed admission attempts in records.", value: func(s Counters) uint64 { return s.ReceivedRecords }},
		{name: "received_bytes_total", help: "Completed admission attempts in normalized bytes.", value: func(s Counters) uint64 { return s.ReceivedBytes }},
		{name: "accepted_records_total", help: "Records handled, including queue-overflow discards.", value: func(s Counters) uint64 { return s.AcceptedRecords }},
		{name: "accepted_bytes_total", help: "Normalized bytes handled, including queue-overflow discards.", value: func(s Counters) uint64 { return s.AcceptedBytes }},
		{name: "written_records_total", help: "Complete records successfully written.", value: func(s Counters) uint64 { return s.WrittenRecords }},
		{name: "written_bytes_total", help: "Bytes belonging to complete successfully written records.", value: func(s Counters) uint64 { return s.WrittenBytes }},
		{name: "sealed_records_total", help: "Records in segments published as .log after sync, close and seal.", value: func(s Counters) uint64 { return s.SealedRecords }},
		{name: "sealed_bytes_total", help: "Bytes in segments published as .log after sync, close and seal.", value: func(s Counters) uint64 { return s.SealedBytes }},
		{name: "failed_records_total", help: "Records discarded on queue overflow or failed background writing.", value: func(s Counters) uint64 { return s.FailedRecords }},
		{name: "failed_bytes_total", help: "Full logical bytes of discarded or failed records.", value: func(s Counters) uint64 { return s.FailedBytes }},
		{name: "rejected_records_total", help: "Admission attempts rejected before entering the queue.", value: func(s Counters) uint64 { return s.RejectedRecords }},
		{name: "rejected_bytes_total", help: "Normalized bytes rejected before entering the queue.", value: func(s Counters) uint64 { return s.RejectedBytes }},
	}}
	labels := prometheus.Labels{"directory": w.dir, "stream": w.cfg.Name}
	for i := range c.counters {
		m := &c.counters[i]
		m.stream = prometheus.NewDesc("qlog_stream_"+m.name, m.help, nil, labels)
	}
	add := func(name, help string, kind prometheus.ValueType, value func(Stats) float64) {
		c.scalars = append(c.scalars, scalarMetric{prometheus.NewDesc("qlog_"+name, help, nil, labels), kind, value})
	}
	add("queue_records", "Records in the queue, excluding the batch.", prometheus.GaugeValue, func(s Stats) float64 { return float64(s.QueueRecords) })
	add("queue_bytes", "Payload bytes in the queue, excluding the batch.", prometheus.GaugeValue, func(s Stats) float64 { return float64(s.QueueBytes) })
	add("queue_capacity_bytes", "Configured queue payload capacity.", prometheus.GaugeValue, func(Stats) float64 { return float64(w.cfg.QueueBytes) })
	add("pending_records", "Accepted records not yet resolved, including the batch.", prometheus.GaugeValue, func(s Stats) float64 { return float64(s.PendingRecords) })
	add("pending_bytes", "Accepted bytes not yet resolved, including the batch.", prometheus.GaugeValue, func(s Stats) float64 { return float64(s.PendingBytes) })
	add("io_errors_total", "Open, write, sync, close, seal or segment cleanup errors, including recovered errors.", prometheus.CounterValue, func(s Stats) float64 { return float64(s.IOErrors) })
	add("write_calls_total", "File write calls.", prometheus.CounterValue, func(s Stats) float64 { return float64(s.WriteCalls) })
	add("write_seconds_total", "Time spent in file write calls.", prometheus.CounterValue, func(s Stats) float64 { return s.WriteSeconds })
	add("sync_calls_total", "File sync calls.", prometheus.CounterValue, func(s Stats) float64 { return float64(s.SyncCalls) })
	add("sync_seconds_total", "Time spent in file sync calls.", prometheus.CounterValue, func(s Stats) float64 { return s.SyncSeconds })
	add("partial_write_bytes_total", "Bytes reported by writes that returned an error or short count.", prometheus.CounterValue, func(s Stats) float64 { return float64(s.PartialWriteBytes) })
	add("failed", "One while any I/O operation has not recovered; only success of the same operation clears its failure.", prometheus.GaugeValue, func(s Stats) float64 {
		if s.Failed {
			return 1
		}
		return 0
	})
	return c
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range c.counters {
		ch <- m.stream
	}
	for _, m := range c.scalars {
		ch <- m.desc
	}
}

// Collect implements prometheus.Collector. Allocations occur at scrape time,
// not once per record, and all metrics in a scrape use one consistent snapshot.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	s := c.writer.Stats()
	for _, m := range c.counters {
		ch <- prometheus.MustNewConstMetric(m.stream, prometheus.CounterValue, float64(m.value(s.Counters)))
	}
	for _, m := range c.scalars {
		ch <- prometheus.MustNewConstMetric(m.desc, m.kind, m.value(s))
	}
}
