package gkc

import (
	"github.com/go-kit/kit/metrics"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

type Counter interface {
	Update(map[string]string, float64)
}

func NewCounter(namespace, subsystem string, fieldKeys []string) Counter {
	return &counter{
		totalRequestCount: kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_request_count",
			Help:      "Number of requests received",
		}, fieldKeys),
		successRequestCount: kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "success_request_count",
			Help:      "Number of successful requests received",
		}, fieldKeys),
		requestLatency: kitprometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "request_latency",
			Help:      "Total duration of requests in seconds",
		}, fieldKeys),

		fieldKeys: fieldKeys,
	}
}

type counter struct {
	totalRequestCount   metrics.Counter
	successRequestCount metrics.Counter
	requestLatency      metrics.Histogram
	fieldKeys           []string
}

func (c *counter) Update(kv map[string]string, value float64) {
	kvp := make([]string, 0, len(kv))
	for k, v := range kv {
		kvp = append(kvp, k, v)
	}
	c.totalRequestCount.With(kvp...).Add(value)
}
