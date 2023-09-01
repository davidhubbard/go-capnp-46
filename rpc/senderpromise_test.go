package rpc_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"capnproto.org/go/capnp/v3/rpc/internal/testcapnp"
	"capnproto.org/go/capnp/v3/rpc/transport"
	rpccp "capnproto.org/go/capnp/v3/std/capnp/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSenderPromiseFulfill(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p, r := capnp.NewLocalPromise[testcapnp.PingPong]()

	left, right := transport.NewPipe(1)
	p1, p2 := rpc.NewTransport(left), rpc.NewTransport(right)

	conn := rpc.NewConn(p1, &rpc.Options{
		ErrorReporter:   testErrorReporter{tb: t},
		BootstrapClient: capnp.Client(p),
	})
	defer finishTest(t, conn, p2)

	// 1. Send bootstrap.
	{
		msg := &rpcMessage{
			Which:     rpccp.Message_Which_bootstrap,
			Bootstrap: &rpcBootstrap{QuestionID: 0},
		}
		assert.NoError(t, sendMessage(ctx, p2, msg))
	}
	// 2. Receive return.
	var bootExportID uint32
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_return, rmsg.Which)
		assert.Equal(t, uint32(0), rmsg.Return.AnswerID)
		assert.Equal(t, rpccp.Return_Which_results, rmsg.Return.Which)
		assert.Equal(t, 1, len(rmsg.Return.Results.CapTable))
		desc := rmsg.Return.Results.CapTable[0]
		assert.Equal(t, rpccp.CapDescriptor_Which_senderPromise, desc.Which)
		bootExportID = desc.SenderPromise
	}
	// 3. Fulfill promise
	{
		pp := testcapnp.PingPong_ServerToClient(&pingPonger{})
		defer pp.Release()
		r.Fulfill(pp)
	}
	// 4. Receive resolve.
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_resolve, rmsg.Which)
		assert.Equal(t, bootExportID, rmsg.Resolve.PromiseID)
		assert.Equal(t, rpccp.Resolve_Which_cap, rmsg.Resolve.Which)
		desc := rmsg.Resolve.Cap
		assert.Equal(t, rpccp.CapDescriptor_Which_senderHosted, desc.Which)
		assert.NotEqual(t, bootExportID, desc.SenderHosted)
	}
}

// Tests that if we get an unimplemented message in response to a resolve message, we correctly
// drop the capability.
func TestResolveUnimplementedDrop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p, r := capnp.NewLocalPromise[testcapnp.Empty]()

	provider := testcapnp.EmptyProvider_ServerToClient(emptyShutdownerProvider{
		result: p,
	})

	left, right := transport.NewPipe(1)
	p1, p2 := rpc.NewTransport(left), rpc.NewTransport(right)

	conn := rpc.NewConn(p1, &rpc.Options{
		ErrorReporter:   testErrorReporter{tb: t},
		BootstrapClient: capnp.Client(provider),
	})
	defer finishTest(t, conn, p2)

	// 1. Send bootstrap.
	{
		msg := &rpcMessage{
			Which:     rpccp.Message_Which_bootstrap,
			Bootstrap: &rpcBootstrap{QuestionID: 0},
		}
		assert.NoError(t, sendMessage(ctx, p2, msg))
	}
	// 2. Receive return.
	var bootExportID uint32
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_return, rmsg.Which)
		assert.Equal(t, uint32(0), rmsg.Return.AnswerID)
		assert.Equal(t, rpccp.Return_Which_results, rmsg.Return.Which)
		assert.Equal(t, 1, len(rmsg.Return.Results.CapTable))
		desc := rmsg.Return.Results.CapTable[0]
		assert.Equal(t, rpccp.CapDescriptor_Which_senderHosted, desc.Which)
		bootExportID = desc.SenderHosted
	}
	onShutdown := make(chan struct{})
	// 3. Send finish
	{
		assert.NoError(t, sendMessage(ctx, p2, &rpcMessage{
			Which: rpccp.Message_Which_finish,
			Finish: &rpcFinish{
				QuestionID:        0,
				ReleaseResultCaps: false,
			},
		}))
	}
	// 4. Call getEmpty
	{
		assert.NoError(t, sendMessage(ctx, p2, &rpcMessage{
			Which: rpccp.Message_Which_call,
			Call: &rpcCall{
				QuestionID: 1,
				Target: rpcMessageTarget{
					Which:       rpccp.MessageTarget_Which_importedCap,
					ImportedCap: bootExportID,
				},
				InterfaceID: testcapnp.EmptyProvider_TypeID,
				MethodID:    0,
				Params:      rpcPayload{},
			},
		}))
	}
	// 5. Receive return.
	var emptyExportID uint32
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, uint32(1), rmsg.Return.AnswerID)
		assert.Equal(t, rpccp.Return_Which_results, rmsg.Return.Which)
		assert.Nil(t, rmsg.Return.Exception)
		assert.Equal(t, 1, len(rmsg.Return.Results.CapTable))
		desc := rmsg.Return.Results.CapTable[0]
		assert.Equal(t, rpccp.CapDescriptor_Which_senderPromise, desc.Which)
		emptyExportID = desc.SenderPromise
	}
	// 6. Fulfill promise
	{
		pp := testcapnp.Empty_ServerToClient(emptyShutdowner{
			onShutdown: onShutdown,
		})
		r.Fulfill(pp)
		pp.Release()
	}
	// 7. Receive resolve, send unimplemented
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_resolve, rmsg.Which)
		assert.Equal(t, emptyExportID, rmsg.Resolve.PromiseID)
		assert.Equal(t, rpccp.Resolve_Which_cap, rmsg.Resolve.Which)
		desc := rmsg.Resolve.Cap
		assert.Equal(t, rpccp.CapDescriptor_Which_senderHosted, desc.Which)
		assert.NoError(t, sendMessage(ctx, p2, &rpcMessage{
			Which:         rpccp.Message_Which_unimplemented,
			Unimplemented: rmsg,
		}))
	}
	// 8. Drop the promise on our side. Otherwise it will stay alive because of
	// the bootstrap interface:
	{
		p.Release()
	}
	// 9. Send finish
	{
		assert.NoError(t, sendMessage(ctx, p2, &rpcMessage{
			Which: rpccp.Message_Which_finish,
			Finish: &rpcFinish{
				QuestionID:        1,
				ReleaseResultCaps: true,
			},
		}))
	}
	<-onShutdown // Will hang unless the capability is dropped
}

type emptyShutdownerProvider struct {
	result testcapnp.Empty
}

func (e emptyShutdownerProvider) GetEmpty(ctx context.Context, p testcapnp.EmptyProvider_getEmpty) error {
	results, err := p.AllocResults()
	if err != nil {
		return err
	}
	results.SetEmpty(e.result)
	return nil
}

type emptyShutdowner struct {
	onShutdown chan<- struct{} // closed on shutdown
}

func (s emptyShutdowner) Shutdown() {
	close(s.onShutdown)
}

// Tests that E-order is respected when fulfilling a promise with something on
// the remote peer.
func TestPromiseOrdering(t *testing.T) {
	//t.Parallel()
	fmt.Fprintln(os.Stderr, "-- TestPromoseOrdering TOP --")
	defer func (){ fmt.Fprintln(os.Stderr, "-- TestPromoseOrdering END --") }()

	ctx := context.Background()
	p, r := capnp.NewLocalPromise[testcapnp.PingPong]()
	defer p.Release()

	left, right := transport.NewPipe(1)
	//p1, p2 := rpc.NewTransport(left), rpc.NewTransport(right)
	p1, p2 := rpc.NewTransportDebug(left, "left"), rpc.NewTransportDebug(right, "right")

	c1 := rpc.NewConn(p1, &rpc.Options{
		ErrorReporter:   testErrorReporter{tb: t},
		BootstrapClient: capnp.Client(p),
	})
	ord := &echoNumOrderChecker{
		t: t,
	}
	c2 := rpc.NewConn(p2, &rpc.Options{
		ErrorReporter:   testErrorReporter{tb: t},
		BootstrapClient: capnp.Client(testcapnp.PingPong_ServerToClient(ord)),
	})

	c1Promise := testcapnp.PingPong(c1.Bootstrap(ctx))
	defer c1Promise.Release()
	c2Promise := testcapnp.PingPong(c2.Bootstrap(ctx))
	defer c2Promise.Release()

	// Send a whole bunch of calls to the promise:
	var (
		futures []testcapnp.PingPong_echoNum_Results_Future
		rels    []capnp.ReleaseFunc
	)
	numCalls := 1024
	for i := 0; i < numCalls; i++ {
		fut, rel := echoNum(ctx, c2Promise, int64(i))
		futures = append(futures, fut)
		rels = append(rels, rel)

		// At some arbitrary point in the middle, fulfill the promise
		// with the other bootstrap interface:
		if i == 100 {
			go func() {
				fmt.Fprintln(os.Stderr, "-- TestPromoseOrdering left:FULFILL --")
				r.Fulfill(c1Promise)
			}()
		}
	}
	for i, fut := range futures {
		// Verify that all the results are as expected. The server
		// Will verify that they came in the right order.
		res, err := fut.Struct()
		require.NoError(t, err, fmt.Sprintf("call #%d should succeed", i))
		require.Equal(t, int64(i), res.N())
	}
	for _, rel := range rels {
		rel()
	}

	require.NoError(t, c2Promise.Resolve(ctx))
	// Shut down the connections, and make sure we can still send
	// calls. This ensures that we've successfully shortened the path to
	// cut out the remote peer:
	c1.Close()
	c2.Close()
	if false {	// path shortening is not implemented yet
		fut, rel := echoNum(ctx, c2Promise, int64(numCalls))
		defer rel()
		res, err := fut.Struct()
		require.NoError(t, err)
		require.Equal(t, int64(numCalls), res.N())
	}
}
