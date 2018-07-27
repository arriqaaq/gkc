package gkc

import (
	"github.com/go-kit/kit/metrics"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"time"
)

type Counter interface {
	UpdateTotal(float64)
	UpdateSuccess(float64)
	UpdateLatency(time.Duration)
}

func NewCounter(namespace, subsystem string) Counter {
	return &counter{
		totalRequestCount: kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_request_count",
			Help:      "Number of requests received",
		}, nil),
		successRequestCount: kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "success_request_count",
			Help:      "Number of successful requests received",
		}, nil),
		requestLatency: kitprometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "request_latency",
			Help:      "Total duration of requests in seconds",
		}, nil),
	}
}

type counter struct {
	totalRequestCount   metrics.Counter
	successRequestCount metrics.Counter
	requestLatency      metrics.Histogram
}

func (c *counter) UpdateTotal(value float64) {
	c.totalRequestCount.Add(value)
}

func (c *counter) UpdateSuccess(value float64) {
	c.successRequestCount.Add(value)
}

func (c *counter) UpdateLatency(dur time.Duration) {
	c.requestLatency.Observe(dur.Seconds())
}
