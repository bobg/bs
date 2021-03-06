// Package split implements reading and writing of hashsplit trees in a blob store.
// See github.com/bobg/hashsplit for more information.
package split

import (
	"context"
	"io"

	"github.com/bobg/hashsplit"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/bobg/bs"
)

// Write writes the contents of `r` to the blob store `s`,
// splitting the input into a tree of blobs according to `splitter`.
// It returns the ref of the root blob,
// which is a serialized Node.
//
// Splitting is done with the "hashsplitting" technique,
// which finds blob boundaries based on the content of the data
// rather than by position.
// If a new version of the same data is written to the store,
// but with a change,
// only the region of the change will need a new blob;
// the others will be unaffected.
//
// If splitter is nil,
// a default splitter is used that produces chunks that are typically 5-10kb in size.
func Write(ctx context.Context, s bs.Store, r io.Reader, splitter *hashsplit.Splitter) (bs.Ref, error) {
	if splitter == nil {
		splitter = &hashsplit.Splitter{
			MinSize: 1024, // xxx ?
		}
	}

	tb := hashsplit.NewTreeBuilder()

	err := splitter.Split(ctx, r, func(bytes []byte, level uint) error {
		size := len(bytes)
		ref, _, err := s.Put(ctx, bytes, nil)
		if err != nil {
			return errors.Wrap(err, "writing split chunk to store")
		}
		tb.Add(ref[:], size, level/2) // TODO: does level/2 produce the best fan-out?
		return nil
	})
	if err != nil {
		return bs.Ref{}, err
	}

	root := tb.Root()

	return splitWrite(ctx, s, root)
}

func splitWrite(ctx context.Context, s bs.Store, n *hashsplit.Node) (bs.Ref, error) {
	tn := &Node{Size: n.Size}
	if len(n.Leaves) > 0 {
		tn.Leaves = n.Leaves
	} else {
		for _, child := range n.Nodes {
			childRef, err := splitWrite(ctx, s, child)
			if err != nil {
				return bs.Ref{}, err
			}
			tn.Nodes = append(tn.Nodes, childRef[:])
		}
	}
	ref, _, err := bs.PutProto(ctx, s, tn)
	return ref, err
}

// Read reads blobs from `g`,
// reassembling the content of the blob tree created with Write
// and writing it to `w`.
// The ref of the root Node is given by `ref`.
func Read(ctx context.Context, g bs.Getter, ref bs.Ref, w io.Writer) error {
	var tn Node
	err := bs.GetProto(ctx, g, ref, &tn)
	if err != nil {
		return err
	}
	return splitRead(ctx, g, &tn, w)
}

func splitRead(ctx context.Context, g bs.Getter, n *Node, w io.Writer) error {
	if len(n.Leaves) > 0 {
		return splitReadHelper(ctx, g, n.Leaves, func(m []byte) error {
			_, err := w.Write(m)
			return err
		})
	}
	return splitReadHelper(ctx, g, n.Nodes, func(m []byte) error {
		var tn Node
		err := proto.Unmarshal(m, &tn)
		if err != nil {
			return err
		}
		return splitRead(ctx, g, &tn, w)
	})
}

func splitReadHelper(ctx context.Context, g bs.Getter, subrefsBytes [][]byte, do func([]byte) error) error {
	for _, s := range subrefsBytes {
		b, _, err := g.Get(ctx, bs.RefFromBytes(s))
		if err != nil {
			return errors.Wrapf(err, "getting %x", s)
		}
		err = do(b)
		if err != nil {
			return err
		}
	}
	return nil
}
