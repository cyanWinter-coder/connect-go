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

package connectinprocess_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"
	"connectrpc.com/connect/v2/internal/assert"
	pingv1 "connectrpc.com/connect/v2/internal/gen/connect/ping/v1"
	pingv1connect "connectrpc.com/connect/v2/internal/gen/connect/ping/v1/pingv1connect"
)

func newBudgetInProcessClient() pingv1connect.PingServiceClient {
	handler := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(handler, pingServer{})
	return pingv1connect.NewPingServiceClient(connect.NewClient(connectinprocess.New(handler)))
}

// TestInProcessStreamBudgetSendBlocksUntilPeerReads is the deterministic
// backpressure case: with a one-message window, a second Send stays
// blocked until the server's Receive consumes the first message.
func TestInProcessStreamBudgetSendBlocksUntilPeerReads(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		client := newBudgetInProcessClient()
		// A CumSumRequest serializes to two bytes; the window admits
		// exactly one at a time.
		budget := connect.NewStreamBudget(2, 1024)
		ctx, cancel := context.WithCancel(
			connect.WithStreamBudget(t.Context(), budget),
		)
		defer cancel()

		stream, err := client.CumSum(ctx)
		assert.Nil(t, err)

		assert.Nil(t, stream.Send(&pingv1.CumSumRequest{Number: 1}))
		assert.Equal(t, budget.SendBytes().InFlight, 0) // released on rendezvous

		// The server has not run yet, so this Send must block.
		sendErr := make(chan error, 1)
		go func() { sendErr <- stream.Send(&pingv1.CumSumRequest{Number: 2}) }()
		synctest.Wait()
		select {
		case err := <-sendErr:
			t.Fatalf("second Send returned before the server read: %v", err)
		default:
		}

		// One server Receive unblocks exactly one producer Send.
		resp, err := stream.Receive()
		assert.Nil(t, err)
		assert.Equal(t, resp.GetSum(), int64(1))
		synctest.Wait()
		assert.Nil(t, <-sendErr)

		resp, err = stream.Receive()
		assert.Nil(t, err)
		assert.Equal(t, resp.GetSum(), int64(3))

		assert.Nil(t, stream.CloseSend())
		_, err = stream.Receive()
		assert.True(t, errors.Is(err, io.EOF))
		assert.Nil(t, stream.Close())
		assert.Equal(t, budget.SendBytes().Messages, 2)
		assert.Equal(t, budget.ReceiveBytes().Messages, 2)
	})
}

// TestInProcessStreamBudgetReceiveBlocksUntilProcessed verifies the
// receive window holds a delivered message until the caller's next
// Receive processes it.
func TestInProcessStreamBudgetReceiveBlocksUntilProcessed(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		client := newBudgetInProcessClient()
		// One response (two bytes) may sit unprocessed at a time.
		budget := connect.NewStreamBudget(1024, 2)
		ctx, cancel := context.WithCancel(
			connect.WithStreamBudget(t.Context(), budget),
		)
		defer cancel()

		stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 3})
		assert.Nil(t, err)

		first, err := stream.Receive()
		assert.Nil(t, err)
		assert.Equal(t, first.GetNumber(), int64(1))
		// The second response is held against the window until this
		// caller's next Receive.
		assert.Equal(t, budget.ReceiveBytes().InFlight, 2)
		assert.Equal(t, budget.ReceiveBytes().Messages, 1)

		for want := int64(2); want <= 3; want++ {
			msg, err := stream.Receive()
			assert.Nil(t, err)
			assert.Equal(t, msg.GetNumber(), want)
		}
		_, err = stream.Receive()
		assert.True(t, errors.Is(err, io.EOF))
		assert.Nil(t, stream.Close())
		assert.Zero(t, budget.ReceiveBytes().InFlight)
	})
}

// TestInProcessStreamBudgetZeroWindow gives an explicit error rather than
// hanging on the first non-empty message.
func TestInProcessStreamBudgetZeroWindow(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		client := newBudgetInProcessClient()
		budget := connect.NewStreamBudget(0, 0)
		ctx, cancel := context.WithCancel(
			connect.WithStreamBudget(t.Context(), budget),
		)
		defer cancel()

		stream, err := client.CumSum(ctx)
		assert.Nil(t, err)
		err = stream.Send(&pingv1.CumSumRequest{Number: 1})
		assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)
		assert.Nil(t, stream.Close())
	})
}

// TestInProcessStreamBudgetContextCancel unblocks a Send parked on the
// send window.
func TestInProcessStreamBudgetContextCancel(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		client := newBudgetInProcessClient()
		budget := connect.NewStreamBudget(2, 1024)
		ctx, cancel := context.WithCancel(
			connect.WithStreamBudget(t.Context(), budget),
		)

		stream, err := client.CumSum(ctx)
		assert.Nil(t, err)
		assert.Nil(t, stream.Send(&pingv1.CumSumRequest{Number: 1}))

		errCh := make(chan error, 1)
		go func() { errCh <- stream.Send(&pingv1.CumSumRequest{Number: 1}) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		assert.True(t, errors.Is(<-errCh, context.Canceled))
		assert.Nil(t, stream.Close())
	})
}

// TestInProcessStreamBudgetSharedAcrossWrappers proves the budget rides
// the call context: a stream wrapper sees the same delivery counters as
// the underlying transport stream.
func TestInProcessStreamBudgetSharedAcrossWrappers(t *testing.T) {
	t.Parallel()
	client := pingv1connect.NewPingServiceClient(connect.NewClient(
		connectinprocess.New(mustBudgetServer(t)),
		connect.ClientInterceptor(func(next connect.ClientFunc) connect.ClientFunc {
			return func(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
				inner, err := next(ctx, spec)
				if err != nil {
					return nil, err
				}
				return &countingStream{ClientStream: inner}, nil
			}
		}),
	))
	budget := connect.NewStreamBudget(1024, 1024)
	ctx := connect.WithStreamBudget(t.Context(), budget)

	stream, err := client.CumSum(ctx)
	assert.Nil(t, err)
	assert.Nil(t, stream.Send(&pingv1.CumSumRequest{Number: 7}))
	resp, err := stream.Receive()
	assert.Nil(t, err)
	assert.Equal(t, resp.GetSum(), int64(7))
	assert.Nil(t, stream.CloseSend())
	_, err = stream.Receive()
	assert.True(t, errors.Is(err, io.EOF))
	assert.Nil(t, stream.Close())

	// The interceptor did not create a second budget: the transport's
	// delivery counters reflect the real messages.
	assert.Equal(t, budget.SendBytes().Messages, 1)
	assert.Equal(t, budget.ReceiveBytes().Messages, 1)
}

func mustBudgetServer(t *testing.T) *connect.Server {
	t.Helper()
	handler := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(handler, pingServer{})
	return handler
}

// countingStream wraps a ClientStream without changing its semantics.
type countingStream struct {
	connect.ClientStream
}
