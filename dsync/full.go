package dsync

import (
	"context"
	stderrs "errors"
	"os"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/bobg/bs"
	"github.com/bobg/bs/split"
)

// Receiver is the receiving end of a full-sync operation (see Tree.Full).
// Note that *Tree implements Receiver.
type Receiver interface {
	// Offer is an offer to send the blobs with the given refs.
	// The receiver must respond with a subset of those refs
	// to indicate which ones it does not already have.
	Offer(context.Context, []bs.Ref) ([]bs.Ref, error)

	// Blobs supplies blobs that the receiver previously indicated it needed
	// (by returning their refs from one or more calls to Offer).
	Blobs(context.Context, []bs.Blob) error

	// Anchors supplies anchor information for some of the blobs received with Blobs.
	// In a given run of Tree.Full,
	// all calls to Blobs will occur before any calls to Anchors.
	Anchors(context.Context, []AnchorTuple) error
}

type refType int

const (
	dirRef refType = 1 + iota
	treeNodeRef
	leafRef
)

type refInfo struct {
	a   bs.Anchor
	typ refType
}

const (
	offerLimit  = 100
	anchorLimit = 100
)

// Full performs a full sync between a Tree and a Receiver.
// A sync begins at the root of the Tree.
// The Ref of the Dir blob is offered with r.Offer.
// If the receiver needs it, the blob is sent with r.Blobs,
// and then the child blobs of the root are offered.
// The complete Tree (consisting of dirs, file tree nodes, and file leaves)
// is traversed in breadth-first fashion,
// with blob subtrees pruned if the receiver already has them.
// After all needed blobs are sent to the receiver,
// anchors for all requested blobs are sent with r.Anchors.
// (These come last to ensure that the receiver has all the pieces necessary
// to reconstitute the dirs and files indicated by the anchors.)
func (t *Tree) Full(ctx context.Context, r Receiver) error {
	rootAnchor, err := t.dirAnchor(t.Root)
	if err != nil {
		return errors.Wrapf(err, "computing dir anchor for root %s", t.Root)
	}

	now := time.Now()

	rootRef, _, err := t.S.GetAnchor(ctx, rootAnchor, now)
	if err != nil {
		return errors.Wrapf(err, "getting anchor for root anchor %s", rootAnchor)
	}

	toOffer := map[bs.Ref]refInfo{
		rootRef: refInfo{a: rootAnchor, typ: dirRef},
	}
	sent := make(map[bs.Ref]refInfo)

	for len(toOffer) > 0 {
		offered := make(map[bs.Ref]refInfo)
		for k, v := range toOffer {
			offered[k] = v
			delete(toOffer, k)
			if len(offered) >= offerLimit {
				break
			}
		}

		refs := make([]bs.Ref, 0, len(offered))
		for ref := range offered {
			refs = append(refs, ref)
		}

		need, err := r.Offer(ctx, refs)
		if err != nil {
			return errors.Wrap(err, "offering blobs")
		}

		if len(need) == 0 {
			continue
		}

		results, err := t.S.GetMulti(ctx, need)
		if err != nil {
			return errors.Wrapf(err, "getting %d requested blobs", len(need))
		}

		blobs := make([]bs.Blob, 0, len(results))
		for ref, fn := range results {
			blob, err := fn(ctx)
			if err != nil {
				return errors.Wrapf(err, "getting blob %s", ref)
			}
			blobs = append(blobs, blob)
			info := offered[ref]
			sent[ref] = info

			switch info.typ {
			case dirRef:
				var dp Dir
				err = proto.Unmarshal(blob, &dp)
				if err != nil {
					return errors.Wrapf(err, "unmarshaling Dir proto %s", ref)
				}
				for _, entry := range dp.Entries {
					var (
						subAnchor = bs.Anchor(string(info.a) + entry.Name)
						typ       = treeNodeRef
					)
					if os.FileMode(entry.Mode).IsDir() {
						subAnchor += "/"
						typ = dirRef
					}
					subref, _, err := t.S.GetAnchor(ctx, subAnchor, now)
					if err != nil {
						return errors.Wrapf(err, "getting anchor %s", subAnchor)
					}
					toOffer[subref] = refInfo{a: subAnchor, typ: typ}
				}

			case treeNodeRef:
				var tn split.Node
				err = proto.Unmarshal(blob, &tn)
				if err != nil {
					return errors.Wrapf(err, "unmarshaling split.Node proto %s", ref)
				}
				for _, n := range tn.Nodes {
					nref := bs.RefFromBytes(n)
					toOffer[nref] = refInfo{typ: treeNodeRef}
				}
				for _, l := range tn.Leaves {
					lref := bs.RefFromBytes(l)
					toOffer[lref] = refInfo{typ: leafRef}
				}
			}
		}

		err = r.Blobs(ctx, blobs)
		if err != nil {
			return errors.Wrapf(err, "sending %d requested blobs", len(need))
		}
	}

	for len(sent) > 0 {
		var tuples []AnchorTuple
		for ref, info := range sent {
			delete(sent, ref)
			if info.a == "" {
				continue
			}
			tuples = append(tuples, AnchorTuple{A: info.a, Ref: ref, T: now})
			if len(tuples) >= anchorLimit {
				break
			}
		}
		if len(tuples) == 0 {
			continue
		}
		err = r.Anchors(ctx, tuples)
		if err != nil {
			return errors.Wrapf(err, "sending %d anchors", len(tuples))
		}
	}

	return nil
}

// Offer implements Receiver.Offer.
func (t *Tree) Offer(ctx context.Context, refs []bs.Ref) ([]bs.Ref, error) {
	results, err := t.S.GetMulti(ctx, refs)
	if err != nil {
		return nil, errors.Wrapf(err, "getting %d refs", len(refs))
	}

	var need []bs.Ref
	for ref, fn := range results {
		_, err := fn(ctx)
		if stderrs.Is(err, bs.ErrNotFound) {
			need = append(need, ref)
		} else if err != nil {
			return nil, errors.Wrapf(err, "getting blob %s", ref)
		}
	}

	return need, nil
}

// Blobs implements Receivers.Blobs.
func (t *Tree) Blobs(ctx context.Context, blobs []bs.Blob) error {
	results, err := t.S.PutMulti(ctx, blobs)
	if err != nil {
		return errors.Wrapf(err, "storing %d blobs", len(blobs))
	}

	for i, fn := range results {
		_, _, err = fn(ctx)
		if err != nil {
			return errors.Wrapf(err, "storing blob %s", blobs[i].Ref())
		}
	}

	return nil
}

// Anchors implements Receivers.Anchors.
func (t *Tree) Anchors(ctx context.Context, tuples []AnchorTuple) error {
	for _, tuple := range tuples {
		err := t.ReplicaAnchor(ctx, tuple.A, tuple.Ref) // TODO: use tuple.T too
		if err != nil {
			return errors.Wrapf(err, "handling anchor %s (%s)", tuple.A, tuple.Ref)
		}
	}
	return nil
}
