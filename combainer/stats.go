package combainer

import (
	"time"

	"github.com/noxiouz/Combaine/vendor/github.com/rcrowley/go-metrics"
)

type Stats struct {
	Registry metrics.Registry

	sessions metrics.Counter

	timingPreparing metrics.Timer
	timingIdle      metrics.Timer

	successParsing metrics.Counter
	failedParsing  metrics.Counter
	timingParsing  metrics.Timer

	successAggregate metrics.Counter
	failedAggregate  metrics.Counter
	timingAggregate  metrics.Timer

	last metrics.Gauge
}

func NewStats() *Stats {
	registry := metrics.NewRegistry()

	return &Stats{
		Registry: registry,

		sessions: metrics.NewRegisteredCounter("sessions", registry),

		timingPreparing: metrics.NewRegisteredTimer("preparing_timings", registry),
		timingIdle:      metrics.NewRegisteredTimer("idle_timings", registry),

		successParsing: metrics.NewRegisteredCounter("parsing_ok", registry),
		failedParsing:  metrics.NewRegisteredCounter("parsing_fail", registry),
		timingParsing:  metrics.NewRegisteredTimer("parsing_timings", registry),

		successAggregate: metrics.NewRegisteredCounter("aggregate_ok", registry),
		failedAggregate:  metrics.NewRegisteredCounter("aggregate_fail", registry),
		timingAggregate:  metrics.NewRegisteredTimer("aggregate_timings", registry),

		last: metrics.NewRegisteredGauge("last", registry),
	}
}

func (s *Stats) AddSuccessParsing() {
	s.successParsing.Inc(1)
	s.last.Update(time.Now().Unix())
}

func (s *Stats) AddFailedParsing() {
	s.failedParsing.Inc(1)
	s.last.Update(time.Now().Unix())
}

func (s *Stats) AddSuccessAggregate() {
	s.successAggregate.Inc(1)
	s.last.Update(time.Now().Unix())
}

func (s *Stats) AddFailedAggregate() {
	s.failedAggregate.Inc(1)
	s.last.Update(time.Now().Unix())
}
