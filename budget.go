// Copyright 2021-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connect

import (
	"context"
	"io"
	"sync"
)

// StreamBudget bounds the in-flight bytes of a streaming RPC. It gives
// bidirectional streams application-level backpressure: a Send blocks once
// the bytes queued for the peer's transport reach SendMaxBytes, and a
// Receive blocks once the bytes of delivered messages the caller has not
// yet processed reach ReceiveMaxBytes.
//
// On the send side a reservation is held until the write completes: the
// peer's transport has then accepted the bytes, so a slow peer blocks the
// writer inside Send rather than letting this process queue unbounded
// data. On the receive side a reservation stays held from delivery until
// the caller's next Receive, giving application code time to process each
// message.
//
// A budget is attached to an RPC through its context, so the whole logical
// call shares one budget: interceptor-wrapped streams and any transport
// retries acquire from and release into the same byte windows. A budget is
// not safe for use by more than one RPC at a time.
//
// Transports are not required to enforce a StreamBudget; the HTTP and
// in-process transports in this module do. Unary RPCs ignore budgets
// because their single request and response never queue.
type StreamBudget struct {
	send    budgetSide
	receive budgetSide

	closeOnce sync.Once
}

// NewStreamBudget returns a StreamBudget with the given byte windows.
// A zero window is valid: it rejects the first non-empty message instead
// of deadlocking, so callers always get a [CodeResourceExhausted] error
// rather than a hung RPC. Negative windows are rejected the same way.
//
// sendMaxBytes limits request messages waiting to be read by the peer;
// receiveMaxBytes limits response messages waiting to be processed by the
// caller after Receive returns them.
func NewStreamBudget(sendMaxBytes, receiveMaxBytes int) *StreamBudget {
	budget := &StreamBudget{}
	budget.send.init(sendMaxBytes)
	budget.receive.init(receiveMaxBytes)
	return budget
}

// SendBytes returns the send-side byte window: the bytes currently in
// flight, the configured window size, and the number of messages and
// payload bytes actually delivered. It is safe to call at any time.
func (b *StreamBudget) SendBytes() BudgetStats {
	if b == nil {
		return BudgetStats{}
	}
	return b.send.stats()
}

// ReceiveBytes returns the receive-side byte window: the bytes of
// delivered messages the caller has not yet released, the configured
// window size, and the number of messages and payload bytes actually
// delivered. It is safe to call at any time.
func (b *StreamBudget) ReceiveBytes() BudgetStats {
	if b == nil {
		return BudgetStats{}
	}
	return b.receive.stats()
}

// AcquireSend reserves bytes for an outgoing message. It blocks until the
// send window has room, the context ends, or the send side closes. On
// success the reservation must later be paired with ReleaseSend: when the
// message fails to go on the wire, or when the peer has consumed it.
//
// Transports call this; application code normally lets the transport
// enforce the budget instead of calling it directly.
func (b *StreamBudget) AcquireSend(ctx context.Context, bytes int) error {
	return b.send.acquire(ctx, bytes)
}

// ReleaseSend returns previously reserved send bytes. Releasing more
// bytes than are currently reserved is ignored so error paths cannot
// drive the window negative.
func (b *StreamBudget) ReleaseSend(bytes int) {
	b.send.release(bytes)
}

// AcquireReceive reserves bytes for an incoming message before its body is
// read. It blocks until the receive window has room, the context ends, or
// the receive side closes.
//
// Transports call this; application code normally lets the transport
// enforce the budget instead of calling it directly.
func (b *StreamBudget) AcquireReceive(ctx context.Context, bytes int) error {
	return b.receive.acquire(ctx, bytes)
}

// ReleaseReceive returns previously reserved receive bytes. Callers
// release a message after they have finished processing it.
func (b *StreamBudget) ReleaseReceive(bytes int) {
	b.receive.release(bytes)
}

// NoteSent records a message actually delivered to the peer. Failed or
// retried sends are not recorded.
func (b *StreamBudget) NoteSent(bytes int) {
	b.send.noteDelivered(bytes)
}

// NoteReceived records a message actually delivered to the caller.
func (b *StreamBudget) NoteReceived(bytes int) {
	b.receive.noteDelivered(bytes)
}

// CloseSend ends the send side: any blocked or future AcquireSend fails
// with io.EOF and all reserved send bytes return to zero. It is
// idempotent. Transports call it when the request side closes.
func (b *StreamBudget) CloseSend() {
	b.send.close(io.EOF)
}

// CloseReceive ends the receive side the same way, waking any blocked
// AcquireReceive. It is idempotent. Transports call it when the response
// side ends.
func (b *StreamBudget) CloseReceive() {
	b.receive.close(io.EOF)
}

// Close ends both sides and releases every reserved byte, waking all
// blocked acquisitions. It is idempotent.
func (b *StreamBudget) Close() error {
	if b == nil {
		return nil
	}
	b.closeOnce.Do(func() {
		b.send.close(io.EOF)
		b.receive.close(io.EOF)
	})
	return nil
}

// BudgetStats is a point-in-time view of one direction of a
// [StreamBudget].
type BudgetStats struct {
	// InFlight is the number of bytes currently reserved against the
	// window. On the send side these messages are waiting for the peer to
	// read them; on the receive side they are waiting for the caller to
	// finish processing them.
	InFlight int
	// Limit is the configured window size.
	Limit int
	// Messages is the number of messages actually delivered in this
	// direction. Failed or retried sends are not counted.
	Messages int
	// Bytes is the total number of payload bytes delivered in this
	// direction. Failed or retried sends are not counted.
	Bytes int64
}

// budgetSide is one directional byte window of a StreamBudget.
type budgetSide struct {
	mu       sync.Mutex
	wake     chan struct{} // closed and replaced on every signal
	limit    int
	inFlight int
	closed   bool
	err      error

	messages int
	bytes    int64
}

func (s *budgetSide) init(limit int) {
	s.limit = limit
	s.wake = make(chan struct{})
}

// signal wakes every current waiter. The caller must hold mu.
func (s *budgetSide) signal() {
	close(s.wake)
	s.wake = make(chan struct{})
}

// acquire reserves bytes, blocking until the window has room, the context
// ends, or the side closes. A zero-length message never blocks and never
// fails on an exhausted window, so empty messages and header-only flushes
// still go through.
func (s *budgetSide) acquire(ctx context.Context, bytes int) error {
	if bytes <= 0 {
		return nil
	}
	s.mu.Lock()
	for {
		if s.closed {
			err := s.err
			s.mu.Unlock()
			return err
		}
		// A zero or negative window can never admit a message, and a
		// message larger than the whole window could only be admitted by
		// draining the window completely. Either case must surface
		// immediately instead of waiting forever.
		if s.limit <= 0 || bytes > s.limit {
			err := Errorf(
				CodeResourceExhausted,
				"message of %d bytes exceeds stream budget window of %d bytes",
				bytes,
				s.limit,
			)
			s.mu.Unlock()
			return err
		}
		if s.inFlight+bytes <= s.limit {
			s.inFlight += bytes
			s.mu.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return err
		}
		// Wait on a channel rather than sync.Cond: a context cancellation
		// only closes its own channel and never has to take this mutex,
		// which a parked waiter may hold.
		wake := s.wake
		s.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
		}
		s.mu.Lock()
	}
}

// release returns reserved bytes to the window and wakes waiters.
func (s *budgetSide) release(bytes int) {
	if bytes <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if bytes > s.inFlight {
		bytes = s.inFlight
	}
	s.inFlight -= bytes
	if !s.closed {
		s.signal()
	}
}

// noteDelivered records a delivered message's payload size.
func (s *budgetSide) noteDelivered(bytes int) {
	if bytes <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages++
	s.bytes += int64(bytes)
}

// close ends the side, fails any blocked acquisition with err, and
// releases every reserved byte. It is idempotent: only the first close
// sets the error.
func (s *budgetSide) close(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if err == nil {
		err = io.EOF
	}
	s.err = err
	s.inFlight = 0
	s.signal()
}

func (s *budgetSide) stats() BudgetStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return BudgetStats{
		InFlight: s.inFlight,
		Limit:    s.limit,
		Messages: s.messages,
		Bytes:    s.bytes,
	}
}

// streamBudgetKey attaches a [*StreamBudget] to a context.
type streamBudgetKey struct{}

// WithStreamBudget returns a context carrying budget so the stream opened
// with it enforces the budget. The budget applies to the whole logical
// call, including interceptor wrappers and transport retries.
func WithStreamBudget(ctx context.Context, budget *StreamBudget) context.Context {
	if budget == nil {
		return ctx
	}
	return context.WithValue(ctx, streamBudgetKey{}, budget)
}

// StreamBudgetFromContext returns the [*StreamBudget] attached to ctx, if
// any.
func StreamBudgetFromContext(ctx context.Context) (*StreamBudget, bool) {
	budget, ok := ctx.Value(streamBudgetKey{}).(*StreamBudget)
	return budget, ok
}

// MessageSizer is implemented by messages that can report their own
// serialized size for stream budgeting. Transports use it when the
// on-the-wire byte count is not known before handing a message to the
// peer, as in the in-process transport. [google.golang.org/protobuf/proto.Message]
// values satisfy it through proto.Size.
type MessageSizer interface {
	// ProtoSize returns the size in bytes of the message's current
	// serialization.
	ProtoSize() int
}
