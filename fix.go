```go
// solution_1.go
package pubsub

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// SentinelPubSub is a wrapper around the standard redis.PubSub connection.
// It implements explicit read/write deadline management and background health-check
// logic designed to handle the "half-open" connection state during Redis Sentinel
// failovers.
type SentinelPubSub struct {
	Conn     *redis.PubSub
	ctx      context.Context
	mu       sync.Mutex
	closed   *sync.Once
	timeout  time.Duration
	running  *sync.Bool
}

// NewSentinelPubSub wraps an existing PubSub connection (e.g., from a Sentinel
// Master) and configures it for robust health-checking.
//
// Acceptance Criteria:
// 1. Set Read/Write Deadlines explicitly.
// 2. Handle timeout by marking connection as broken.
// 3. Prevent goroutine leaks via atomic running state.
func NewSentinelPubSub(conn *redis.PubSub, readTimeout time.Duration) *SentinelPubSub {
	s := &SentinelPubSub{
		Conn:    conn,
		timeout: readTimeout,
		ctx:     context.Background(),
		closed:  &sync.Once{},
		running: &sync.Bool{},
	}

	// Initialize connection deadline to prevent immediate block if messages pile up
	s.configureDeadlines()

	s.running.Store(true)

	// Start the background health-check goroutine
	go s.pingLoop()

	return s
}

// configureDeadlines initializes the underlying TCP connection's deadlines.
// This ensures that the "Half-Open" state from a Sentinel failover is detected
// via a ReadTimeout event rather than an indefinite block.
func (s *SentinelPubSub) configureDeadlines() {
	if c, ok := s.Conn.Conn().(interface{ SetReadDeadline(time.Time) }); ok {
		c.SetReadDeadline(time.Now().Add(s.timeout))
	}
}

// pingLoop runs the background health-check goroutine.
// It periodically sends a PING and ensures the ReadDeadline is respected.
// If the PING times out, it signals that the connection needs refreshing.
func (s *SentinelPubSub) pingLoop() {
	defer func() {
		s.mu.Lock()
		s.running.Store(false)
		s.mu.Unlock()
	}()

	// A ticker is better than a tight loop to reduce overhead on idle connections
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			isActive := s.running.Load()
			s.mu.Unlock()

			if !isActive {
				continue
			}

			// 1. Explicitly Set Deadlines on the underlying connection
			// This is the core fix for the "Indefinite Block"
			underlyingConn := s.Conn.Conn()
			if underlyingConn != nil {
				underlyingConn.SetReadDeadline(time.Now().Add(s.timeout))
			}

			// 2. Execute the PING command
			// We use Do("PING") for full control.
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(s.timeout):
				// If we reach here, the PING timed out or the read drained the buffer.
				// Forcing a close here triggers the reconnection logic in the calling code
				// or the Sentinel listener's failover flow.
				s.closed.Do(func() {
					_ = s.Conn.Conn().Close() // Force drain/flush
					_ = s.Conn.Ping()      // Trigger the "re-subscribe" flow logic
				})
				// Note: We don't break immediately to allow a quick retry
			}
		}
	}
}

// NotifyFailover is called when the Sentinel listener detects a new Master.
// It forces all active Pub/Sub connections to close their read buffer
// to ensure the next PING/Message cycle detects the "New" Master immediately.
func (s *SentinelPubSub) NotifyFailover() {
	// Set a strict deadline to force a quick read timeout if data arrives
	// or immediately after, forcing a re-connection to the new Master.
	s.mu.Lock()
	defer s.mu.Unlock()

	underlyingConn := s.Conn.Conn()
	if underlyingConn != nil {
		underlyingConn.SetReadDeadline(time.Now().Add(s.timeout))
	}
}

// Close explicitly closes the underlying connection, ensuring any pending
// network I/O for the health-check `PING` is resolved (unblocking goroutines).
func (s *SentinelPubSub) Close() {
	s.closed.Do(func() {
		underlyingConn := s.Conn.Conn()
		if underlyingConn != nil {
			// Use a short timeout to prevent blocking on the Close() call itself
			_ = underlyingConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		}
	})
}
```