package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var buckets = prometheus.LinearBuckets(0, 3, 40)

// Metrics to measure success and latency of different use cases
type Metrics struct {
	TotalDuration        *prometheus.HistogramVec
	ConnectorErrorCount  *prometheus.CounterVec
	ConnectorDuration    *prometheus.HistogramVec
	ValidationErrorCount *prometheus.CounterVec
	ValidationDuration   *prometheus.HistogramVec
}

// NewMetrics for Prometheus Metrics
func NewMetrics() *Metrics {
	return &Metrics{
		TotalDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "total_duration",
			Help: "The time it takes to complete an entire use case, " +
				"including validation and db requests",
			Buckets: buckets,
		}, []string{"verb", "resource"}),
		ConnectorDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "connector_duration",
			Help: "The time it takes to complete an a database interaction, " +
				"while serving a use case",
			Buckets: buckets,
		}, []string{"verb", "resource"}),
		ValidationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "validation_duration",
			Help: "The time it takes to complete an a validation interaction, " +
				"while serving a use case. Note that some validations require db queries.",
			Buckets: buckets,
		}, []string{"verb", "resource"}),
		ConnectorErrorCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "connector_errors",
			Help: "The number of errors occurred while interacting with the connected db",
		}, []string{"verb", "resource"}),
		ValidationErrorCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "validation_errors",
			Help: "The number of errors occurred while validating user input",
		}, []string{"verb", "resource"}),
	}
}
