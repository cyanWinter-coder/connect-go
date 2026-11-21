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

package connecthttp_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/connect/v2/internal/assert"
	pingv1 "connectrpc.com/connect/v2/internal/gen/connect/ping/v1"
	"connectrpc.com/connect/v2/internal/gen/connect/ping/v1/pingv1connect"
	"connectrpc.com/connect/v2/internal/memhttp/memhttptest"
)

// newBudgetTestServer starts the ping service over an in-memory HTTP/2
// server and returns a client talking to it.
func newBudgetTestServer(t *testing.T, opts ...connecthttp.Option) pingv1connect.PingServiceClient {
	t.Helper()
	mux := http.NewServeMux()
	srv := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(srv, pingServer{})
	connecthttp.Mount(mux, srv, opts...)
	server := memhttptest.NewServer(t, mux)
	transport := connecthttp.NewTransport(server.Client(), server.URL(), opts...)
	return pingv1connect.NewPingServiceClient(connect.NewClient(transport))
}

// TestStreamBudgetBidiBackpressure verifies a small send window is
// enforced on every envelope through a real HTTP/2 transport and that the
// full exchange still drains cleanly. Deterministic producer blocking is
// covered by the in-process transport, whose rendezvous releases a send
// reservation only when the server explicitly reads the message.
func TestStreamBudgetBidiBackpressure(t *testing.T) {
	t.Parallel()
	client := newBudgetTestServer(t)
	// Every CumSum message is a 7-byte envelope (5 framing + 2 payload);
	// the window admits only a few at a time, so every send acquires and
	// releases against the budget.
	budget := connect.NewStreamBudget(3*7, 1024)
	ctx, cancel := context.WithCancel(
		connect.WithStreamBudget(t.Context(), budget),
	)
	defer cancel()

	stream, err := client.CumSum(ctx)
	assert.Nil(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 12 {
			if err := stream.Send(&pingv1.CumSumRequest{Number: 1}); err != nil {
				return
			}
		}
		_ = stream.CloseSend()
	}()
	var sums []int64
	go func() {
		defer wg.Done()
		for range 12 {
			msg, err := stream.Receive()
			if err != nil {
				return
			}
			sums = append(sums, msg.GetSum())
		}
		_, _ = stream.Receive() // io.EOF
	}()
	wg.Wait()
	assert.Nil(t, stream.Close())
	assert.Equal(t, sums, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})

	// Every delivered message is counted once, including framing bytes,
	// and all reservations return to zero at the end.
	assert.Equal(t, budget.SendBytes().Messages, 12)
	assert.Equal(t, budget.SendBytes().Bytes, int64(12*7))
	assert.Zero(t, budget.SendBytes().InFlight)
	assert.Equal(t, budget.ReceiveBytes().Messages, 12)
	assert.Zero(t, budget.ReceiveBytes().InFlight)
}

// TestStreamBudgetReceiveWindow blocks the server once the caller has
// unprocessed responses, then releases as the caller drains them.
func TestStreamBudgetReceiveWindow(t *testing.T) {
	t.Parallel()
	client := newBudgetTestServer(t)
	// One response at a time may sit unprocessed. The window is sized for
	// a full envelope rather than the payload alone, since framing and
	// compressor framing occupy transport buffers too.
	budget := connect.NewStreamBudget(1024, 64)
	ctx, cancel := context.WithCancel(
		connect.WithStreamBudget(t.Context(), budget),
	)
	defer cancel()

	stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 4})
	assert.Nil(t, err)

	first, err := stream.Receive()
	assert.Nil(t, err)
	assert.Equal(t, first.GetNumber(), int64(1))
	// The next response is reserved and held; no further one can be
	// pulled off the wire until this is processed.
	assert.True(t, budget.ReceiveBytes().InFlight > 0)
	assert.Equal(t, budget.ReceiveBytes().Messages, 1)

	var got []int64
	got = append(got, first.GetNumber())
	for range 3 {
		msg, err := stream.Receive()
		assert.Nil(t, err)
		got = append(got, msg.GetNumber())
	}
	_, err = stream.Receive()
	assert.True(t, errors.Is(err, io.EOF))
	assert.Nil(t, stream.Close())
	assert.Equal(t, got, []int64{1, 2, 3, 4})
	assert.Zero(t, budget.ReceiveBytes().InFlight)
}

// TestStreamBudgetZeroWindowFails is the deadlock guard: a zero window
// produces an explicit ResourceExhausted error instead of a hang.
func TestStreamBudgetZeroWindowFails(t *testing.T) {
	t.Parallel()
	client := newBudgetTestServer(t)
	budget := connect.NewStreamBudget(0, 1024)
	ctx, cancel := context.WithCancel(
		connect.WithStreamBudget(t.Context(), budget),
	)
	defer cancel()

	stream, err := client.CumSum(ctx)
	assert.Nil(t, err)
	err = stream.Send(&pingv1.CumSumRequest{Number: 1})
	assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)
	assert.Nil(t, stream.Close())
}

// TestStreamBudgetOversizedMessageFails covers a message larger than the
// whole window: it must error immediately, never block.
func TestStreamBudgetOversizedMessageFails(t *testing.T) {
	t.Parallel()
	client := newBudgetTestServer(t)
	budget := connect.NewStreamBudget(1, 1024)
	ctx, cancel := context.WithCancel(
		connect.WithStreamBudget(t.Context(), budget),
	)
	defer cancel()

	stream, err := client.CumSum(ctx)
	assert.Nil(t, err)
	err = stream.Send(&pingv1.CumSumRequest{Number: 1})
	assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)
	assert.Nil(t, stream.Close())
}

// TestStreamBudgetContextCancelUnblocksSend ensures a blocked acquisition
// exits immediately when the call context is canceled.
func TestStreamBudgetContextCancelUnblocksSend(t *testing.T) {
	t.Parallel()
	client := newBudgetTestServer(t)
	budget := connect.NewStreamBudget(7, 1024)
	ctx, cancel := context.WithCancel(
		connect.WithStreamBudget(t.Context(), budget),
	)

	stream, err := client.CumSum(ctx)
	assert.Nil(t, err)
	assert.Nil(t, stream.Send(&pingv1.CumSumRequest{Number: 1}))

	errCh := make(chan error, 1)
	go func() { errCh <- stream.Send(&pingv1.CumSumRequest{Number: 1}) }()
	cancel()
	select {
	case err := <-errCh:
		assert.True(t, errors.Is(err, context.Canceled))
	case <-t.Context().Done():
		t.Fatal("blocked Send did not return after context cancellation")
	}
	assert.Nil(t, stream.Close())
}

// TestStreamBudgetCloseUnblocksReceive covers the cross-direction rule:
// closing the stream wakes a Receive blocked on the receive window.
func TestStreamBudgetCloseUnblocksReceive(t *testing.T) {
	t.Parallel()
	client := newBudgetTestServer(t)
	budget := connect.NewStreamBudget(1024, 64)
	ctx, cancel := context.WithCancel(
		connect.WithStreamBudget(t.Context(), budget),
	)
	defer cancel()

	stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 100})
	assert.Nil(t, err)
	_, err = stream.Receive()
	assert.Nil(t, err)

	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Receive()
		errCh <- err
	}()
	assert.Nil(t, stream.Close())
	select {
	case err := <-errCh:
		assert.NotNil(t, err)
	case <-t.Context().Done():
		t.Fatal("blocked Receive did not return after Close")
	}
}

// TestStreamBudgetEndToEndAcrossProtocols runs a normal bidi exchange with
// a budget under every client protocol, proving framing and compression
// semantics are unchanged.
func TestStreamBudgetEndToEndAcrossProtocols(t *testing.T) {
	t.Parallel()
	for _, opts := range [][]connecthttp.Option{
		nil,
		{connecthttp.WithGRPC()},
		{connecthttp.WithGRPCWeb()},
		{connecthttp.WithSendGzip()},
	} {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			client := newBudgetTestServer(t, opts...)
			budget := connect.NewStreamBudget(4096, 4096)
			ctx := connect.WithStreamBudget(t.Context(), budget)

			stream, err := client.CumSum(ctx)
			assert.Nil(t, err)
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				for _, n := range []int64{3, 5, 1} {
					assert.Nil(t, stream.Send(&pingv1.CumSumRequest{Number: n}))
				}
				assert.Nil(t, stream.CloseSend())
			}()
			go func() {
				defer wg.Done()
				var got []int64
				for {
					msg, err := stream.Receive()
					if errors.Is(err, io.EOF) {
						break
					}
					assert.Nil(t, err)
					got = append(got, msg.GetSum())
				}
				assert.Equal(t, got, []int64{3, 8, 9})
				assert.Nil(t, stream.Close())
			}()
			wg.Wait()
			assert.Equal(t, budget.SendBytes().Messages, 3)
			assert.Equal(t, budget.ReceiveBytes().Messages, 3)
		})
	}
}

// TestStreamBudgetIgnoredByUnary confirms unary calls and header/trailer
// handling keep their existing behavior even with a zero budget attached.
func TestStreamBudgetIgnoredByUnary(t *testing.T) {
	t.Parallel()
	client := newBudgetTestServer(t)
	budget := connect.NewStreamBudget(0, 0)
	ctx := connect.WithStreamBudget(t.Context(), budget)

	res, err := client.Ping(ctx, &pingv1.PingRequest{Number: 42})
	assert.Nil(t, err)
	assert.Equal(t, res.GetNumber(), int64(42))
	// Unary never touches the budget.
	assert.Zero(t, budget.SendBytes().Messages)
	assert.Zero(t, budget.ReceiveBytes().Messages)
}

// TestStreamBudgetClientStreaming covers the StreamTypeClient path: many
// request messages against a small send window, followed by one response.
func TestStreamBudgetClientStreaming(t *testing.T) {
	t.Parallel()
	client := newBudgetTestServer(t)
	budget := connect.NewStreamBudget(3*7, 1024)
	ctx, cancel := context.WithCancel(
		connect.WithStreamBudget(t.Context(), budget),
	)
	defer cancel()

	stream, err := client.Sum(ctx)
	assert.Nil(t, err)
	for _, n := range []int64{3, 5, 1, 4} {
		assert.Nil(t, stream.Send(&pingv1.SumRequest{Number: n}))
	}
	res, err := stream.CloseAndReceive()
	assert.Nil(t, err)
	assert.Equal(t, res.GetSum(), int64(13))
	assert.Equal(t, budget.SendBytes().Messages, 4)
	assert.Zero(t, budget.SendBytes().InFlight)
	assert.Zero(t, budget.ReceiveBytes().InFlight)
}
