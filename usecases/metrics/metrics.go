package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var buckets = []float64{1, 5, 10, 20, 50, 75, 100, 125, 150, 250, 375, 500, 750, 1000}

// Metrics to measure success and latency of different use cases
type Metrics struct {
	UseCase       *prometheus.HistogramVec
	ConnectorErr  *prometheus.CounterVec
	Connector     *prometheus.HistogramVec
	Locking       *prometheus.HistogramVec
	ValidationErr *prometheus.CounterVec
	Validation    *prometheus.HistogramVec
	APIUsage      *prometheus.CounterVec
}

// NewMetrics for Prometheus Metrics
func NewMetrics() *Metrics {
	return &Metrics{
		UseCase: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "usecase_duration",
			Help: "The time it takes to complete an entire use case, " +
				"including, locking, validation and db requests",
			Buckets: buckets,
		}, []string{"verb", "resource"}),
		Locking: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "locking_duration",
			Help:    "Time spend waiting for locks",
			Buckets: buckets,
		}, []string{"lock_type", "verb", "resource"}),
		Connector: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "connector_duration",
			Help: "The time it takes to complete an a database interaction, " +
				"while serving a use case",
			Buckets: buckets,
		}, []string{"verb", "resource"}),
		Validation: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "validation_duration",
			Help: "The time it takes to complete an a validation interaction, " +
				"while serving a use case. Note that some validations require db queries.",
			Buckets: buckets,
		}, []string{"verb", "resource"}),
		ConnectorErr: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "connector_errors",
			Help: "The number of errors occurred while interacting with the connected db",
		}, []string{"verb", "resource"}),
		ValidationErr: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "validation_errors",
			Help: "The number of errors occurred while validating user input",
		}, []string{"verb", "resource"}),
		APIUsage: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "api_usage",
			Help: "Count the number of times an API was used (by type)",
		}, []string{"type"}),
	}
}
