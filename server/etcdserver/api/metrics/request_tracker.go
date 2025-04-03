package metrics

import (
	"sync"
	"sync/atomic"
	"time"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

// RequestTracker tracks the number of get/put requests within configurable time windows
type RequestTracker struct {
	// Tracking interval in milliseconds
	interval int
	// Counter for put requests in current interval
	putCounter uint64
	// Counter for get requests in current interval
	getCounter uint64
	// Channel to publish metrics
	metricsCh chan uint64
	// Logger
	logger *zap.Logger
	// Mutex for stopping
	mu      sync.Mutex
	stopped bool
	// Done channel for clean shutdown
	doneCh chan struct{}
}

// NewRequestTracker creates a new tracker with the specified interval
func NewRequestTracker(interval int, logger *zap.Logger) *RequestTracker {
	if interval <= 0 {
		interval = 10 // Default to 10 milliseconds
	}

	metricsCh := make(chan uint64, 1)

	tracker := &RequestTracker{
		interval:   interval,
		putCounter: 0,
		getCounter: 0,
		metricsCh:  metricsCh,
		logger:     logger,
		doneCh:     make(chan struct{}),
	}

	go tracker.run()

	return tracker
}

func (t *RequestTracker) ProcessRaftMessage(m raftpb.Message) {
	if m.Type == raftpb.MsgApp {
		for _, entry := range m.Entries {
			if entry.Type == raftpb.EntryNormal {
				// Check if the entry is a put request
				if len(entry.Data) > 0 {
					var r etcdserverpb.InternalRaftRequest
					if err := r.Unmarshal(entry.Data); err == nil {
						if r.Put != nil {
							t.RecordPutRequest()
						}
					}
				}
			}
		}
	}
}

// RecordRequest increments the counter for each put request
func (t *RequestTracker) RecordPutRequest() {
	atomic.AddUint64(&t.putCounter, 1)
}

func (t *RequestTracker) RecordGetRequest() {
	atomic.AddUint64(&t.getCounter, 1)
}

// run periodically reports metrics and resets the counter
func (t *RequestTracker) run() {
	ticker := time.NewTicker(time.Duration(t.interval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Get current count and reset atomically
			putCounter := atomic.SwapUint64(&t.putCounter, 0)
			getCounter := atomic.SwapUint64(&t.getCounter, 0)

			// Log the metric
			t.logger.Debug("Request count",
				zap.Int("interval_milliseconds", t.interval),
				zap.Uint64("put_count", putCounter),
				zap.Uint64("get_count", getCounter),
			)

			// Send to metrics channel if anyone is listening
			select {
			case t.metricsCh <- putCounter:
			default:
				// Nobody is listening, just continue
			}

		case <-t.doneCh:
			return
		}
	}
}

// Stop gracefully shuts down the tracker
func (t *RequestTracker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.stopped {
		t.stopped = true
		close(t.doneCh)
	}
}

// GetMetricsChannel returns a read-only channel for consuming metrics
func (t *RequestTracker) GetMetricsChannel() <-chan uint64 {
	return t.metricsCh
}
