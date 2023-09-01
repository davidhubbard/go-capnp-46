package rpc

import (
	"context"
	"errors"
	"fmt"
	"os"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/internal/str"
	"capnproto.org/go/capnp/v3/internal/syncutil"
	rpccp "capnproto.org/go/capnp/v3/std/capnp/rpc"
)

// An importID is an index into the imports table.
type importID uint32

// impent is an entry in the import table.  All fields are protected by
// Conn.mu.
type impent struct {
	id       importID
	c        *Conn

	// wireRefs is the number of times that the importID has appeared in
	// messages received from the remote vat.  Used to populate the
	// Release.referenceCount field.
	wireRefs int

	// useSnap takes the place of snapshot.IsValid() only because a no-op snapshot is possible
	// which would have snapshot.IsValid() == false. See resolveNow() which sets useSnap = true.
	useSnap  bool

	snapshot capnp.ClientSnapshot
}

// addImport returns a client that represents the given import, counting the
// number of references to this import from this vat. capnp.Client reference
// counting will call impent.Shutdown when the Client.Release() is done,
// which lets the impent clean up the reference in c.lk.imports.
//
// The caller must be holding onto c.mu.
func (c *lockedConn) addImport(id importID) capnp.Client {
	if ent, ok := c.lk.imports[id]; ok {
		ent.wireRefs++
		fmt.Fprintln(os.Stderr, fmt.Sprintf("addImport(id=%v): wireRefs=%v", id, ent.wireRefs))
		return capnp.NewClient(ent)
	}

	fmt.Fprintln(os.Stderr, fmt.Sprintf("addImport(id=%v): wireRefs=1", id))
	ent := &impent{
		id:       id,
		c:        (*Conn)(c),
		wireRefs: 1,
	}
	c.lk.imports[id] = ent
	return capnp.NewClient(ent)
}

func (imp *impent) String() string {
	return fmt.Sprintf("impent{c: 0x%s, id: %v}", str.PtrToHex(imp.c), imp.id)
}

func (imp *impent) Send(ctx context.Context, s capnp.Send) (*capnp.Answer, capnp.ReleaseFunc) {
	// This is an implementation of Send that makes any impent a valid capnp.clientHook.
	// If this impent holds a ClientSnapshot, forward this Send to it
	if (imp.useSnap) {
		return imp.snapshot.Send(ctx, s)
	}

	// This import points to a senderHosted Capability
	return withLockedConn2(imp.c, func(c *lockedConn) (*capnp.Answer, capnp.ReleaseFunc) {
		if !c.startTask() {
			return capnp.ErrorAnswer(s.Method, ExcClosed), func() {}
		}
		defer c.tasks.Done()
		if _, ok := c.lk.imports[imp.id]; !ok {
			return capnp.ErrorAnswer(s.Method, rpcerr.Disconnected(errors.New("send on closed import"))), func() {}
		}
		q := c.newQuestion(s.Method)

		// Send call message.
		c.sendMessage(ctx, func(m rpccp.Message) error {
			return c.newImportCallMessage(m, imp.id, q.id, s)
		}, func(err error) {
			if err != nil {
				syncutil.With(&imp.c.lk, func() {
					imp.c.lk.questions[q.id] = nil
				})
				q.p.Reject(rpcerr.WrapFailed("send message", err))
				syncutil.With(&imp.c.lk, func() {
					imp.c.lk.questionID.remove(q.id)
				})
				return
			}

			q.c.tasks.Add(1)
			go func() {
				defer q.c.tasks.Done()
				q.handleCancel(ctx)
			}()
		})

		ans := q.p.Answer()
		return ans, func() {
			<-ans.Done()
			q.p.ReleaseClients()
			q.release()
		}
	})
}

// newImportCallMessage builds a Call message targeted to an import.
func (c *lockedConn) newImportCallMessage(msg rpccp.Message, imp importID, qid questionID, s capnp.Send) error {
	call, err := msg.NewCall()
	if err != nil {
		return rpcerr.WrapFailed("build call message", err)
	}
	call.SetQuestionId(uint32(qid))
	call.SetInterfaceId(s.Method.InterfaceID)
	call.SetMethodId(s.Method.MethodID)
	target, err := call.NewTarget()
	if err != nil {
		return rpcerr.WrapFailed("build call message", err)
	}
	target.SetImportedCap(uint32(imp))
	payload, err := call.NewParams()
	if err != nil {
		return rpcerr.WrapFailed("build call message", err)
	}
	args, err := capnp.NewStruct(payload.Segment(), s.ArgsSize)
	if err != nil {
		return rpcerr.WrapFailed("build call message", err)
	}
	if err := payload.SetContent(args.ToPtr()); err != nil {
		return rpcerr.WrapFailed("build call message", err)
	}

	if s.PlaceArgs == nil {
		return nil
	}
	if err := s.PlaceArgs(args); err != nil {
		return rpcerr.WrapFailed("place arguments", err)
	}
	// TODO(soon): save param refs
	_, err = c.fillPayloadCapTable(payload)
	if err != nil {
		return rpcerr.Annotate(err, "build call message")
	}
	return nil
}

func (imp *impent) Recv(ctx context.Context, r capnp.Recv) capnp.PipelineCaller {
	// This is an implementation of Recv that makes any impent a valid capnp.clientHook.
	// If this impent holds a ClientSnapshot, forward this Recv to it
	if (imp.useSnap) {
		return imp.snapshot.Recv(ctx, r)
	}

	ans, finish := imp.Send(ctx, capnp.Send{
		Method:   r.Method,
		ArgsSize: r.Args.Size(),
		PlaceArgs: func(s capnp.Struct) error {
			err := s.CopyFrom(r.Args)
			r.ReleaseArgs()
			return err
		},
	})
	r.ReleaseArgs()
	select {
	case <-ans.Done():
		returnAnswer(r.Returner, ans, finish)
		return nil
	default:
		go returnAnswer(r.Returner, ans, finish)
		return ans
	}
}

func returnAnswer(ret capnp.Returner, ans *capnp.Answer, finish func()) {
	defer finish()
	defer ret.ReleaseResults()
	result, err := ans.Struct()
	if err != nil {
		ret.PrepareReturn(err)
		ret.Return()
		return
	}
	recvResult, err := ret.AllocResults(result.Size())
	if err != nil {
		ret.PrepareReturn(err)
		ret.Return()
		return
	}
	if err := recvResult.CopyFrom(result); err != nil {
		ret.PrepareReturn(err)
		ret.Return()
		return
	}
	ret.PrepareReturn(nil)
	ret.Return()
}

func (imp *impent) Brand() capnp.Brand {
	return capnp.Brand{Value: imp}
}

func (imp *impent) Shutdown() {
	fmt.Fprintln(os.Stderr, fmt.Sprintf("import %v Shutdown: wireRefs=%v delete import", imp.id, imp.wireRefs))
	imp.c.withLocked(func(c *lockedConn) {
		if !c.startTask() {
			return
		}
		defer c.tasks.Done()
		if (imp.useSnap) {
			imp.useSnap = false
			imp.snapshot.Release()
			imp.snapshot = capnp.ClientSnapshot{}
		}

		_, ok := c.lk.imports[imp.id]
		if !ok {
			panic(fmt.Sprintf("importClient ID %v Shutdown: not in imports table", imp.id))
		}
		fmt.Fprintln(os.Stderr, fmt.Sprintf("import %v Shutdown: delete import", imp.id))
		delete(imp.c.lk.imports, imp.id)
		c.sendMessage(c.bgctx, func(msg rpccp.Message) error {
			rel, err := msg.NewRelease()
			if err == nil {
				rel.SetId(uint32(imp.id))
				rel.SetReferenceCount(uint32(imp.wireRefs))
			}
			return err
		}, func(err error) {
			if err != nil {
				imp.c.er.ReportError(rpcerr.Annotate(err, "send release"))
			}
		})
	})
}

// resolveNow is called from handleResolve() in rpc.go. It updates imp.c.lk.imports (the
// imports table) to replace a promise with the resolved Interface from client.
//
// handleResolve() is responsible for all the message parsing and messages sent.
// This function is responsible for updating the imports table.
//
// The caller must be holding onto imp.c.mu. It is sufficient to call this
// from an impent obtained from lockedConn.lk.imports[] because the invariant
// holds that all importClients in lockedConn.lk.imports[] have the same Conn
// in imp.c (see implementation of lockedConn.addImport() above)
func (imp *impent) resolveNow(ctx context.Context, client capnp.Client) {
	clientSnapshot := client.Snapshot()
	otherImp, ok := clientSnapshot.Brand().Value.(*impent)
	if ok {
		// client is an importClient because lockedConn.recvCap() was just called inside handleResolve()
		// recvCap() got a CapDescriptor containing an import ID, so it produced this otherImp:
		fmt.Fprintln(os.Stderr, fmt.Sprintf("resolve import %v to new id %v", imp.id, otherImp.id))
		_, ok := imp.c.lk.imports[imp.id]
		if !ok {
			panic(fmt.Sprintf("resolve: import %v not in imports table - want to move it", imp.id))
		}
		delete(imp.c.lk.imports, imp.id)
		imp.id = otherImp.id
		imp.useSnap = false
		clientSnapshot.Release()
		// Update Conn.lk.imports with new imp.id. By-ref imp updates everywhere.
		imp.c.lk.imports[imp.id] = imp
	} else {
		// client is an interface (really anything beside an importClient). This means imp.id
		// didn't change but imp.snapshot now gets the resolved client.
		//
		// Clarifying note: recvCap() produces a no-op Client{} for the case when
		// desc.Which() == rpccp.CapDescriptor_Which_none. This is interpreted as the other side
		// "changing their mind" and tombstoning this interface.
		fmt.Fprintln(os.Stderr, fmt.Sprintf("resolve import %v to client %v", imp.id, client))
		imp.useSnap = true
		imp.snapshot = clientSnapshot
	}
}
