package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Registry is the dedicated Prometheus registry for rag-chunk-lifecycle-service.
	Registry = prometheus.NewRegistry()

	factory = promauto.With(Registry)

	// IngestedChunksTotal tracks the total number of child chunks ingested.
	// Used in Prometheus alerting and dashboards with rate() to measure chunks per second.
	IngestedChunksTotal = factory.NewCounter(prometheus.CounterOpts{
		Name: "chunk_ingestion_chunks_per_second",
		Help: "Total processed child chunks count for calculating chunks per second throughput rate.",
	})

	// CompilationDuration tracks parent container compilation latency in seconds.
	CompilationDuration = factory.NewHistogram(prometheus.HistogramOpts{
		Name:    "chunk_compilation_duration_seconds",
		Help:    "Latency of parent context container compilation in seconds.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.12, 0.2, 0.5, 1.0},
	})

	// StaleQuarantineTotal tracks total sections flagged as SECTION_STALE_QUARANTINE.
	StaleQuarantineTotal = factory.NewCounter(prometheus.CounterOpts{
		Name: "chunk_section_stale_quarantine_total",
		Help: "Total number of section patches transitioned to SECTION_STALE_QUARANTINE due to upstream CDC conflicts.",
	})

	// ParentLookupDuration tracks context retrieval serving latency in seconds.
	ParentLookupDuration = factory.NewHistogram(prometheus.HistogramOpts{
		Name:    "chunk_parent_lookup_duration_seconds",
		Help:    "Online retrieval serving latency for parent context lookups in seconds.",
		Buckets: []float64{0.001, 0.002, 0.004, 0.008, 0.015, 0.03, 0.05, 0.1},
	})

	// MemoryAllocBytes tracks current allocated heap memory bytes.
	MemoryAllocBytes = factory.NewGauge(prometheus.GaugeOpts{
		Name: "go_memstats_alloc_bytes",
		Help: "Current bytes of allocated heap objects according to Go runtime MemStats.",
	})
)

// RecordChunkIngestion increments the chunk ingestion counter by count.
func RecordChunkIngestion(count int) {
	if count > 0 {
		IngestedChunksTotal.Add(float64(count))
	}
}

// RecordCompilationDuration records parent compilation latency.
func RecordCompilationDuration(durationSeconds float64) {
	CompilationDuration.Observe(durationSeconds)
}

// IncrementStaleQuarantine increments the quarantine conflict counter.
func IncrementStaleQuarantine() {
	StaleQuarantineTotal.Inc()
}

// RecordParentLookupDuration records parent retrieval serving latency.
func RecordParentLookupDuration(durationSeconds float64) {
	ParentLookupDuration.Observe(durationSeconds)
}

// UpdateMemoryAllocBytes sets the memory allocation gauge.
func UpdateMemoryAllocBytes(bytes uint64) {
	MemoryAllocBytes.Set(float64(bytes))
}
