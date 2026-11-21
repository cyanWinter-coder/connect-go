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

package connect_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/internal/assert"
)

func TestStreamBudgetWindowAdmitsThenBlocks(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		budget := connect.NewStreamBudget(100, 100)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		assert.Nil(t, budget.AcquireSend(ctx, 60))
		assert.Equal(t, budget.SendBytes().InFlight, 60)
		// The remaining window is too small: the acquisition must block
		// instead of exceeding the limit.
		acquired := make(chan error, 1)
		go func() { acquired <- budget.AcquireSend(ctx, 50) }()
		synctest.Wait()
		select {
		case err := <-acquired:
			t.Fatalf("AcquireSend returned before release: %v", err)
		default:
		}

		budget.ReleaseSend(60)
		synctest.Wait()
		assert.Nil(t, <-acquired)
		assert.Equal(t, budget.SendBytes().InFlight, 50)
	})
}

func TestStreamBudgetZeroWindowFailsImmediately(t *testing.T) {
	t.Parallel()
	budget := connect.NewStreamBudget(0, 0)
	ctx := t.Context()

	err := budget.AcquireSend(ctx, 1)
	assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)
	err = budget.AcquireReceive(ctx, 1)
	assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)

	// Empty messages never need the window and must not deadlock.
	assert.Nil(t, budget.AcquireSend(ctx, 0))
	assert.Nil(t, budget.AcquireReceive(ctx, 0))
}

func TestStreamBudgetOversizedMessageFailsImmediately(t *testing.T) {
	t.Parallel()
	budget := connect.NewStreamBudget(10, 10)
	ctx := t.Context()

	// A message bigger than the whole window must not wait for a drain it
	// could never observe.
	err := budget.AcquireReceive(ctx, 11)
	assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)

	assert.Nil(t, budget.AcquireReceive(ctx, 10))
}

func TestStreamBudgetContextCancellationUnblocks(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		budget := connect.NewStreamBudget(10, 10)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		assert.Nil(t, budget.AcquireSend(ctx, 10))

		errCh := make(chan error, 1)
		go func() { errCh <- budget.AcquireSend(ctx, 1) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		err := <-errCh
		assert.True(t, errors.Is(err, context.Canceled))
	})
}

func TestStreamBudgetCloseWakesBothSides(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		budget := connect.NewStreamBudget(10, 10)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		assert.Nil(t, budget.AcquireSend(ctx, 4))
		assert.Nil(t, budget.AcquireReceive(ctx, 3))

		sendErr := make(chan error, 1)
		receiveErr := make(chan error, 1)
		go func() { sendErr <- budget.AcquireSend(ctx, 10) }()
		go func() { receiveErr <- budget.AcquireReceive(ctx, 10) }()
		synctest.Wait()

		assert.Nil(t, budget.Close())
		synctest.Wait()

		assert.True(t, errors.Is(<-sendErr, io.EOF))
		assert.True(t, errors.Is(<-receiveErr, io.EOF))
		// Reserved bytes return to zero when the side closes.
		assert.Zero(t, budget.SendBytes().InFlight)
		assert.Zero(t, budget.ReceiveBytes().InFlight)

		// Close is idempotent and later acquisitions fail at once.
		assert.Nil(t, budget.Close())
		assert.True(t, errors.Is(budget.AcquireSend(ctx, 1), io.EOF))
		assert.True(t, errors.Is(budget.AcquireReceive(ctx, 1), io.EOF))
	})
}

func TestStreamBudgetHalfClose(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		budget := connect.NewStreamBudget(10, 10)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		assert.Nil(t, budget.AcquireSend(ctx, 10))

		errCh := make(chan error, 1)
		go func() { errCh <- budget.AcquireSend(ctx, 1) }()
		synctest.Wait()
		budget.CloseSend()
		synctest.Wait()
		assert.True(t, errors.Is(<-errCh, io.EOF))

		// The other direction keeps working.
		assert.Nil(t, budget.AcquireReceive(ctx, 10))
	})
}

func TestStreamBudgetDeliveryStats(t *testing.T) {
	t.Parallel()
	budget := connect.NewStreamBudget(100, 100)
	ctx := t.Context()

	assert.Nil(t, budget.AcquireSend(ctx, 10))
	budget.NoteSent(10)
	assert.Nil(t, budget.AcquireSend(ctx, 20))
	// A failed delivery is released and never recorded.
	budget.ReleaseSend(20)

	assert.Nil(t, budget.AcquireReceive(ctx, 30))
	budget.NoteReceived(30)

	sendStats := budget.SendBytes()
	assert.Equal(t, sendStats.Messages, 1)
	assert.Equal(t, sendStats.Bytes, int64(10))
	assert.Equal(t, sendStats.Limit, 100)

	receiveStats := budget.ReceiveBytes()
	assert.Equal(t, receiveStats.Messages, 1)
	assert.Equal(t, receiveStats.Bytes, int64(30))
	assert.Equal(t, receiveStats.InFlight, 30)
}

func TestStreamBudgetContextRoundTrip(t *testing.T) {
	t.Parallel()
	budget := connect.NewStreamBudget(1, 2)
	ctx := connect.WithStreamBudget(t.Context(), budget)

	got, ok := connect.StreamBudgetFromContext(ctx)
	assert.True(t, ok)
	assert.True(t, got == budget)

	if _, ok := connect.StreamBudgetFromContext(t.Context()); ok {
		t.Fatal("unexpected budget on bare context")
	}
	// A nil budget is a no-op rather than an untyped nil value.
	if _, ok := connect.StreamBudgetFromContext(connect.WithStreamBudget(t.Context(), nil)); ok {
		t.Fatal("nil budget should not attach")
	}
}

func TestStreamBudgetReleaseNeverNegative(t *testing.T) {
	t.Parallel()
	budget := connect.NewStreamBudget(10, 10)
	ctx := t.Context()
	assert.Nil(t, budget.AcquireSend(ctx, 3))
	// Over-releasing (a doubled error-path release, for example) clamps at
	// zero instead of corrupting the window.
	budget.ReleaseSend(3)
	budget.ReleaseSend(3)
	assert.Zero(t, budget.SendBytes().InFlight)
	assert.Nil(t, budget.AcquireSend(ctx, 10))
}
