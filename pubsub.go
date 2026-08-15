package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// PubSub implements Pub/Sub commands as described in
// https://redis.io/topics/pubsub. It's safe for concurrent use by
// multiple goroutines.
type PubSub struct {
	opt *Options

	newConn   func(ctx context.Context, channels []string) (*pool.Conn, error)
	closeConn func(*pool.Conn) error

	mu       sync.Mutex
	cn       *pool.Conn
	channels map[string]struct{}
	patterns map[string]struct{}

	closed bool
	exit   chan struct{}

	cmd *Cmd

	// healthCheckErr stores the error from the last health check ping
	healthCheckErr error
}

func (c *PubSub) init() {
	c.exit = make(chan struct{})
	if c.opt.PingTimeout > 0 {
		go c.healthCheckLoop()
	}
}

func (c *PubSub) healthCheckLoop() {
	timeout := c.opt.PingTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			err := c.Ping(ctx)
			cancel()
			if err != nil {
				c.mu.Lock()
				c.healthCheckErr = err
				if c.cn != nil {
					_ = c.closeConn(c.cn)
					c.cn = nil
				}
				c.mu.Unlock()
			}
		case <-c.exit:
			return
		}
	}
}

func (c *PubSub) conn(ctx context.Context, newChannels []string) (*pool.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, pool.ErrClosed
	}

	if c.cn != nil {
		return c.cn, nil
	}

	cn, err := c.newConn(ctx, newChannels)
	if err != nil {
		return nil, err
	}

	c.cn = cn
	c.healthCheckErr = nil
	return cn, nil
}

func (c *PubSub) writeCmd(ctx context.Context, cmd Cmder) error {
	cn, err := c.conn(ctx, nil)
	if err != nil {
		return err
	}

	writeTimeout := c.opt.WriteTimeout
	if deadline, ok := ctx.Deadline(); ok {
		writeTimeout = time.Until(deadline)
	}

	err = cn.WithWriter(ctx, writeTimeout, func(wr *proto.Writer) error {
		return writeCmd(wr, cmd)
	})
	if err != nil {
		c.freeConn(cn, err)
		return err
	}
	return nil
}

func (c *PubSub) freeConn(cn *pool.Conn, err error) {
	c.mu.Lock()
	if c.cn == cn {
		_ = c.closeConn(cn)
		c.cn = nil
	}
	c.mu.Unlock()
}

func (c *PubSub) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return pool.ErrClosed
	}
	c.closed = true
	close(c.exit)

	if c.cn != nil {
		_ = c.closeConn(c.cn)
		c.cn = nil
	}

	return nil
}

func (c *PubSub) Ping(ctx context.Context, payload ...string) error {
	args := []interface{}{"ping"}
	if len(payload) == 1 {
		args = append(args, payload[0])
	} else if len(payload) > 1 {
		return fmt.Errorf("redis: redundant arguments for Ping")
	}
	return c.writeCmd(ctx, NewCmd(ctx, args...))
}

func (c *PubSub) Receive(ctx context.Context) (interface{}, error) {
	return c.ReceiveTimeout(ctx, 0)
}

func (c *PubSub) ReceiveTimeout(ctx context.Context, timeout time.Duration) (interface{}, error) {
	cn, err := c.conn(ctx, nil)
	if err != nil {
		return nil, err
	}

	var msg interface{}
	err = cn.WithReader(ctx, timeout, func(rd *proto.Reader) error {
		var err error
		msg, err = c.readMsg(rd)
		return err
	})
	if err != nil {
		c.freeConn(cn, err)
		return nil, err
	}

	return msg, nil
}

func (c *PubSub) readMsg(rd *proto.Reader) (interface{}, error) {
	// Read message implementation...
	return nil, nil
}