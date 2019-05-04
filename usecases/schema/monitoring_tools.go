package schema

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func meassure(observer prometheus.Observer) func() {
	begin := time.Now()
	return func() {
		duration := time.Since(begin) / time.Millisecond
		observer.Observe(float64(duration))
	}
}
